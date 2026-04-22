"""
GexBot REST Scraper — Poll periodico degli snapshot GEX e salvataggio in DB.

GexBot non fornisce storico REST, ma lo snapshot corrente e' accessibile via:
  GET https://api.gexbot.com/{TICKER}/{package}/{category}

Questo script poll periodicamente tutti gli endpoint di interesse per
ES_SPX (e altri ticker configurabili) e salva ogni snapshot in DuckDB,
costruendo uno storico nel tempo.

Endpoint disponibili (verificati):
  classic/gex_full, classic/gex_zero, classic/gex_one
  state/gex_full, state/gex_zero, state/gex_one
  state/delta_zero, state/delta_one
  state/gamma_zero, state/gamma_one
  state/vanna_zero, state/vanna_one
  state/charm_zero, state/charm_one
  orderflow/orderflow

Usage:
    python scripts/gexbot_scraper.py                       # default: ES_SPX, 60s
    python scripts/gexbot_scraper.py --ticker NQ_NDX       # altro ticker
    python scripts/gexbot_scraper.py --interval 30         # ogni 30 sec
    python scripts/gexbot_scraper.py --one-shot            # una sola iterazione
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.core.secrets import get_secret

BASE_URL = "https://api.gexbot.com"
DB_PATH = str(BASE_DIR / "data" / "gexbot_snapshots.duckdb")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [SCRAPER] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gexbot")


ENDPOINTS = [
    ("classic", "gex_full"),
    ("classic", "gex_zero"),
    ("classic", "gex_one"),
    ("state", "gex_full"),
    ("state", "gex_zero"),
    ("state", "gex_one"),
    ("state", "delta_zero"),
    ("state", "delta_one"),
    ("state", "gamma_zero"),
    ("state", "gamma_one"),
    ("state", "vanna_zero"),
    ("state", "vanna_one"),
    ("state", "charm_zero"),
    ("state", "charm_one"),
    ("orderflow", "orderflow"),
]


def ensure_db(conn: duckdb.DuckDBPyConnection) -> None:
    """Crea tabella unica con JSON payload (schema-agnostic)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gex_snapshots (
            ts_fetched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ts_server BIGINT,
            ticker VARCHAR,
            package VARCHAR,
            category VARCHAR,
            spot DOUBLE,
            min_dte INTEGER,
            sec_min_dte INTEGER,
            zero_gamma DOUBLE,
            major_pos_vol DOUBLE,
            major_pos_oi DOUBLE,
            major_neg_vol DOUBLE,
            major_neg_oi DOUBLE,
            major_positive DOUBLE,
            major_negative DOUBLE,
            major_long_gamma DOUBLE,
            major_short_gamma DOUBLE,
            raw_json JSON
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gex_ts ON gex_snapshots(ts_server DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gex_ticker_cat ON gex_snapshots(ticker, package, category)")


def fetch_endpoint(session: requests.Session, ticker: str, package: str, category: str) -> dict[str, Any] | None:
    """Scarica un singolo endpoint."""
    url = f"{BASE_URL}/{ticker}/{package}/{category}"
    try:
        r = session.get(url, timeout=10)
        if r.status_code == 200:
            return r.json()
        log.warning(f"{ticker}/{package}/{category}: HTTP {r.status_code}")
    except Exception as e:
        log.warning(f"{ticker}/{package}/{category}: {e}")
    return None


def save_snapshot(conn: duckdb.DuckDBPyConnection, ticker: str, package: str, category: str, data: dict) -> None:
    """Salva uno snapshot nel DB."""
    conn.execute(
        """
        INSERT INTO gex_snapshots (
            ts_server, ticker, package, category, spot, min_dte, sec_min_dte,
            zero_gamma, major_pos_vol, major_pos_oi, major_neg_vol, major_neg_oi,
            major_positive, major_negative, major_long_gamma, major_short_gamma,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            data.get("timestamp"),
            ticker,
            package,
            category,
            float(data.get("spot", 0)) if data.get("spot") is not None else None,
            int(data.get("min_dte", 0)) if data.get("min_dte") is not None else None,
            int(data.get("sec_min_dte", 0)) if data.get("sec_min_dte") is not None else None,
            data.get("zero_gamma"),
            data.get("major_pos_vol"),
            data.get("major_pos_oi"),
            data.get("major_neg_vol"),
            data.get("major_neg_oi"),
            data.get("major_positive"),
            data.get("major_negative"),
            data.get("major_long_gamma"),
            data.get("major_short_gamma"),
            json.dumps(data),
        ],
    )


def scrape_once(conn: duckdb.DuckDBPyConnection, session: requests.Session, tickers: list[str]) -> int:
    """Una passata completa su tutti gli endpoint e ticker. Ritorna snapshot salvati."""
    count = 0
    for ticker in tickers:
        for package, category in ENDPOINTS:
            data = fetch_endpoint(session, ticker, package, category)
            if data:
                save_snapshot(conn, ticker, package, category, data)
                count += 1
    return count


def export_latest_json(conn: duckdb.DuckDBPyConnection, ticker: str) -> None:
    """
    Esporta lo snapshot piu recente per categoria in gexbot_latest.json.

    Questo file e' usato dal V35Bridge per le feature GEX live senza
    dover aprire il DB (che e' lockato da questo processo).
    Scrittura atomica (tmp -> rename) per evitare letture parziali.
    """
    try:
        # Latest snapshot per category
        rows = conn.execute("""
            SELECT package, category, spot, zero_gamma,
                   major_pos_vol, major_pos_oi, major_neg_vol, major_neg_oi,
                   major_positive, major_negative, major_long_gamma, major_short_gamma,
                   ts_server, raw_json
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY package, category ORDER BY ts_server DESC
                ) as rn
                FROM gex_snapshots WHERE ticker = ?
            ) t WHERE rn = 1
        """, [ticker]).fetchdf()

        if rows.empty:
            return

        export: dict = {
            "ticker": ticker,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "snapshots": [],
        }

        def _safe_float(v):
            if v is None:
                return None
            try:
                import math
                f = float(v)
                return f if math.isfinite(f) else None
            except (ValueError, TypeError):
                return None

        for _, row in rows.iterrows():
            raw = {}
            try:
                rj = row.get("raw_json")
                raw = json.loads(rj) if isinstance(rj, str) and rj else {}
            except Exception:
                pass
            snap = {
                "package": str(row["package"]),
                "category": str(row["category"]),
                "spot": _safe_float(row["spot"]),
                "zero_gamma": _safe_float(row["zero_gamma"]),
                "major_positive": _safe_float(row["major_positive"]),
                "major_negative": _safe_float(row["major_negative"]),
                "major_long_gamma": _safe_float(row["major_long_gamma"]),
                "major_short_gamma": _safe_float(row["major_short_gamma"]),
                "major_pos_vol": _safe_float(row["major_pos_vol"]),
                "major_neg_vol": _safe_float(row["major_neg_vol"]),
                "major_pos_oi": _safe_float(row["major_pos_oi"]),
                "major_neg_oi": _safe_float(row["major_neg_oi"]),
                "ts_server": int(row["ts_server"]) if row["ts_server"] else None,
                "raw": raw,
            }
            export["snapshots"].append(snap)

        # History for temporal features: last 15 rows of classic/gex_zero
        hist = conn.execute("""
            SELECT spot, zero_gamma, major_positive, major_negative,
                   major_long_gamma, major_short_gamma,
                   major_pos_vol, major_pos_oi, ts_server, raw_json
            FROM gex_snapshots
            WHERE ticker = ? AND package = 'classic' AND category = 'gex_zero'
            ORDER BY ts_server DESC LIMIT 15
        """, [ticker]).fetchdf()

        export["gex_zero_history"] = []
        for _, row in hist.iterrows():
            raw_h = {}
            try:
                rj = row.get("raw_json")
                raw_h = json.loads(rj) if isinstance(rj, str) and rj else {}
            except Exception:
                pass
            export["gex_zero_history"].append({
                "spot": _safe_float(row["spot"]),
                "zero_gamma": _safe_float(row["zero_gamma"]),
                "major_positive": _safe_float(row["major_positive"]),
                "major_negative": _safe_float(row["major_negative"]),
                "major_long_gamma": _safe_float(row["major_long_gamma"]),
                "major_short_gamma": _safe_float(row["major_short_gamma"]),
                "major_pos_vol": _safe_float(row["major_pos_vol"]),
                "major_pos_oi": _safe_float(row["major_pos_oi"]),
                "ts_server": int(row["ts_server"]) if row["ts_server"] else None,
                "raw": raw_h,
            })

        # Atomic write to avoid partial reads by V35Bridge
        out_path = Path(DB_PATH).parent / "gexbot_latest.json"
        tmp_path = out_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(export, default=str), encoding="utf-8")
        tmp_path.replace(out_path)

        log.debug(f"Exported latest snapshot ({len(export['snapshots'])} categories) to {out_path.name}")

    except Exception as e:
        log.warning(f"export_latest_json failed: {e}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", nargs="+", default=["ES_SPX"], help="Ticker(s) da scaricare")
    parser.add_argument("--interval", type=int, default=60, help="Secondi tra poll (default 60)")
    parser.add_argument("--one-shot", action="store_true", help="Una sola iterazione ed esci")
    args = parser.parse_args()

    gex_key = get_secret("GEXBOT_API_KEY", required=True)

    log.info(f"GexBot Scraper")
    log.info(f"  Ticker: {args.ticker}")
    log.info(f"  Interval: {args.interval}s")
    log.info(f"  DB: {DB_PATH}")
    log.info(f"  Endpoints: {len(ENDPOINTS)}")

    # Ensure DB schema once at startup (short-lived connection)
    _init_conn = duckdb.connect(DB_PATH)
    ensure_db(_init_conn)
    _init_conn.close()

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Basic {gex_key}",
        "Accept-Encoding": "gzip",
    })

    iteration = 0
    try:
        while True:
            iteration += 1
            t0 = time.time()

            # Open connection per iteration so readers (nt8_bridge_from_scraper.py)
            # can acquire the file lock between our writes. DuckDB holds an
            # exclusive file lock while a writable connection is open; keeping
            # it open for the whole lifetime starved the bridge reader.
            conn = duckdb.connect(DB_PATH)
            try:
                saved = scrape_once(conn, session, args.ticker)
                total = conn.execute("SELECT COUNT(*) FROM gex_snapshots").fetchone()[0]
                for ticker in args.ticker:
                    export_latest_json(conn, ticker)
            finally:
                conn.close()

            elapsed = time.time() - t0
            log.info(f"Iter {iteration}: {saved}/{len(ENDPOINTS) * len(args.ticker)} OK ({elapsed:.1f}s). Total in DB: {total:,}")

            if args.one_shot:
                break

            sleep_for = max(0, args.interval - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        # Stats finali (short-lived connection)
        try:
            _stats_conn = duckdb.connect(DB_PATH, read_only=True)
            try:
                stats = _stats_conn.execute("""
                    SELECT ticker, package, category, COUNT(*) as cnt,
                           MIN(ts_server) as first_ts, MAX(ts_server) as last_ts
                    FROM gex_snapshots
                    GROUP BY ticker, package, category
                    ORDER BY ticker, package, category
                """).fetchdf()
                log.info(f"\n=== STATS FINALI ===\n{stats.to_string(index=False)}")
            finally:
                _stats_conn.close()
        except Exception as e:
            log.warning(f"Final stats unavailable: {e}")


if __name__ == "__main__":
    main()
