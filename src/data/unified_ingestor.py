"""
Unified Ingestor — Fusione intelligente di flussi eterogenei nel DB.

Gestisce: Databento (trades), GexBot (WS live + REST fallback), P1-Lite (ranges).

TAGGING OBBLIGATORIO per ogni record:
  - source_type: 'DATABENTO_STD', 'GEX_LIVE_WS', 'GEX_FALLBACK_API', 'P1_LITE_SNAP'
  - is_gap_filled: Boolean (True se scaricato da API storica per recupero)
  - session_ref: 'FULL', '0DTE', '1DTE', 'MR_0900', 'OR_1530'

LOGICA NOTTURNA (Post-Market 22:15):
  1. Check qualita: GexBot WS ha perso dati? (gap temporali)
  2. Fallback: Scarica da REST API /{ticker}/{package}/{category}
     Priorita: ES_SPX prima, poi altri ticker
  3. Ingestione P1-Lite: Salva snapshot MR/OR congelati
  4. Ingestione Databento: Consolida batch storico della giornata
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any

import duckdb
import requests

logger = logging.getLogger("p1uni.data.ingestor")

GEXBOT_BASE = "https://api.gexbot.com"

# Endpoint GexBot REST (verificati funzionanti)
GEXBOT_ENDPOINTS = [
    ("classic", "gex_full"), ("classic", "gex_zero"), ("classic", "gex_one"),
    ("state", "gex_full"), ("state", "gex_zero"), ("state", "gex_one"),
    ("state", "delta_zero"), ("state", "delta_one"),
    ("state", "gamma_zero"), ("state", "gamma_one"),
    ("state", "vanna_zero"), ("state", "vanna_one"),
    ("state", "charm_zero"), ("state", "charm_one"),
    ("orderflow", "orderflow"),
]

# Priorita ticker per fallback notturno
TICKER_PRIORITY = ["ES_SPX", "NQ_NDX", "SPY", "QQQ", "VIX", "IWM", "TLT", "GLD", "UVXY"]


class UnifiedIngestor:
    """Gestisce ingestione unificata da tutte le fonti."""

    def __init__(self, config: dict[str, Any]) -> None:
        from src.core.secrets import get_secret

        self.config = config
        self.gexbot_key = get_secret("GEXBOT_API_KEY", "")
        self.databento_key = get_secret("DATABENTO_API_KEY", "")

        # DB paths
        self.live_db_path = config.get("database", {}).get("path", "data/p1uni_live.duckdb")
        self.gold_db_path = config.get("database", {}).get("gold_path", "")
        self.snapshot_db_path = str(Path(config.get("_base_dir", ".")) / "data" / "gexbot_snapshots.duckdb")

        self._session = requests.Session()
        if self.gexbot_key:
            self._session.headers["Authorization"] = f"Basic {self.gexbot_key}"

    # ============================================================
    # GexBot REST Fallback (per gap filling)
    # ============================================================

    def gexbot_fetch_current(self, ticker: str, package: str, category: str) -> dict | None:
        """Scarica snapshot corrente da GexBot REST API."""
        url = f"{GEXBOT_BASE}/{ticker}/{package}/{category}"
        try:
            r = self._session.get(url, timeout=10)
            if r.status_code == 200:
                return r.json()
            logger.warning(f"GexBot {ticker}/{package}/{category}: HTTP {r.status_code}")
        except Exception as e:
            logger.error(f"GexBot fetch error: {e}")
        return None

    def gexbot_fill_gaps(self, tickers: list[str] | None = None, max_calls: int = 100) -> int:
        """Scarica snapshot GexBot per tutti i ticker prioritari.

        Usa REST API (non WS). Salva in gexbot_snapshots.duckdb.

        Args:
            tickers: Lista ticker (default: TICKER_PRIORITY)
            max_calls: Limite API call (default 100, max giornaliero 1000)

        Returns:
            Numero di snapshot salvati.
        """
        if not self.gexbot_key:
            logger.warning("GexBot API key not configured, skip gap fill")
            return 0

        if tickers is None:
            tickers = TICKER_PRIORITY

        conn = duckdb.connect(self.snapshot_db_path)
        try:
            self._ensure_snapshot_table(conn)

            saved = 0
            calls = 0

            for ticker in tickers:
                for package, category in GEXBOT_ENDPOINTS:
                    if calls >= max_calls:
                        logger.info(f"Max calls reached ({max_calls})")
                        break

                    data = self.gexbot_fetch_current(ticker, package, category)
                    calls += 1

                    if data:
                        self._save_snapshot(conn, ticker, package, category, data, is_gap_filled=True)
                        saved += 1

                    time.sleep(0.3)  # rate limit

                if calls >= max_calls:
                    break

            total = conn.execute("SELECT COUNT(*) FROM gex_snapshots").fetchone()[0]
            logger.info(f"GexBot gap fill: {saved} snapshots saved ({calls} API calls). Total: {total:,}")
            return saved
        finally:
            conn.close()

    def _ensure_snapshot_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gex_snapshots (
                ts_fetched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ts_server BIGINT,
                ticker VARCHAR,
                package VARCHAR,
                category VARCHAR,
                source_type VARCHAR DEFAULT 'GEX_FALLBACK_API',
                is_gap_filled BOOLEAN DEFAULT FALSE,
                spot DOUBLE,
                zero_gamma DOUBLE,
                major_pos_vol DOUBLE, major_pos_oi DOUBLE,
                major_neg_vol DOUBLE, major_neg_oi DOUBLE,
                major_positive DOUBLE, major_negative DOUBLE,
                major_long_gamma DOUBLE, major_short_gamma DOUBLE,
                raw_json JSON
            )
        """)

    def _save_snapshot(self, conn: duckdb.DuckDBPyConnection, ticker: str, package: str,
                       category: str, data: dict, is_gap_filled: bool = False) -> None:
        source_type = "GEX_FALLBACK_API" if is_gap_filled else "GEX_LIVE_WS"
        conn.execute(
            """INSERT INTO gex_snapshots (
                ts_server, ticker, package, category, source_type, is_gap_filled,
                spot, zero_gamma, major_pos_vol, major_pos_oi, major_neg_vol, major_neg_oi,
                major_positive, major_negative, major_long_gamma, major_short_gamma,
                raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                data.get("timestamp"), ticker, package, category,
                source_type, is_gap_filled,
                data.get("spot"), data.get("zero_gamma"),
                data.get("major_pos_vol"), data.get("major_pos_oi"),
                data.get("major_neg_vol"), data.get("major_neg_oi"),
                data.get("major_positive"), data.get("major_negative"),
                data.get("major_long_gamma"), data.get("major_short_gamma"),
                json.dumps(data),
            ],
        )

    # ============================================================
    # Databento Historical Consolidation
    # ============================================================

    def databento_consolidate_day(self, day: date) -> int:
        """Scarica e consolida i trade Databento per un giorno specifico.

        Salva in p1uni_history.duckdb (non nel live DB).
        """
        if not self.databento_key:
            logger.warning("Databento key not configured")
            return 0

        try:
            import databento as db
            client = db.Historical(key=self.databento_key)
            import pandas as pd

            start = day.strftime("%Y-%m-%d")
            end = (day + timedelta(days=1)).strftime("%Y-%m-%d")

            logger.info(f"Databento: downloading {start}...")
            data = client.timeseries.get_range(
                dataset="GLBX.MDP3", symbols=["ES.c.0"], schema="trades",
                start=start, end=end, stype_in="continuous",
            )
            df = data.to_df()
            if df.empty:
                return 0

            # Converti
            records = []
            for _, r in df.iterrows():
                p = float(r["price"])
                if p > 1e10:
                    p /= 1e9
                s = str(r["side"].value) if hasattr(r["side"], "value") else str(r["side"])
                records.append((r["ts_event"], "ES", p, int(r["size"]), s, int(r.get("flags", 0))))

            idf = pd.DataFrame(records, columns=["ts_event", "ticker", "price", "size", "side", "flags"])

            # Salva
            hist_db = str(Path(self.config.get("_base_dir", ".")) / "data" / "p1uni_history.duckdb")
            conn = duckdb.connect(hist_db)
            try:
                conn.execute("""CREATE TABLE IF NOT EXISTS historical_trades (
                    ts_event TIMESTAMP, ticker VARCHAR, price DOUBLE,
                    size INTEGER, side VARCHAR, flags INTEGER,
                    source VARCHAR DEFAULT 'DATABENTO_HIST',
                    download_date DATE DEFAULT CURRENT_DATE
                )""")
                conn.register("_h", idf)
                conn.execute("INSERT INTO historical_trades (ts_event, ticker, price, size, side, flags) SELECT * FROM _h")
                conn.unregister("_h")
                total = conn.execute("SELECT COUNT(*) FROM historical_trades").fetchone()[0]
            finally:
                conn.close()

            logger.info(f"Databento: {len(records):,} trades saved for {start}. Total: {total:,}")
            return len(records)

        except Exception as e:
            logger.error(f"Databento consolidate error: {e}")
            return 0

    # ============================================================
    # P1-Lite Snapshot Saver
    # ============================================================

    def save_p1_snapshot(self, mr_levels: dict, or_levels: dict, session_date: date) -> None:
        """Salva snapshot MR/OR congelati nel DB live."""
        try:
            conn = duckdb.connect(self.live_db_path)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_ranges_snapshot (
                    session_date DATE, ticker VARCHAR DEFAULT 'ES',
                    source VARCHAR DEFAULT 'P1_LITE_SNAP',
                    mr1d DOUBLE, mr1u DOUBLE, mr2d DOUBLE, mr2u DOUBLE,
                    or1d DOUBLE, or1u DOUBLE, or2d DOUBLE, or2u DOUBLE,
                    vwap DOUBLE, session_high DOUBLE, session_low DOUBLE,
                    is_final BOOLEAN DEFAULT FALSE, frozen_at TIMESTAMP
                )
            """)
            conn.execute(
                """INSERT INTO daily_ranges_snapshot
                   (session_date, source, mr1d, mr1u, mr2d, mr2u,
                    or1d, or1u, or2d, or2u, frozen_at)
                   VALUES (?, 'P1_LITE_SNAP', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    session_date,
                    mr_levels.get("mr1d"), mr_levels.get("mr1u"),
                    mr_levels.get("mr2d"), mr_levels.get("mr2u"),
                    or_levels.get("or1d"), or_levels.get("or1u"),
                    or_levels.get("or2d"), or_levels.get("or2u"),
                    datetime.now(timezone.utc),
                ],
            )
            conn.close()
            logger.info(f"P1 snapshot saved for {session_date}")
        except Exception as e:
            logger.error(f"P1 snapshot save error: {e}")

    # ============================================================
    # Nightly Routine (post-market 22:15)
    # ============================================================

    def run_nightly(self, today: date | None = None) -> dict[str, Any]:
        """Routine notturna completa: gap fill + consolidation.

        Chiamata dal master_orchestrator alle 22:15 UTC.
        """
        if today is None:
            today = datetime.now(timezone.utc).date()

        logger.info(f"=== NIGHTLY INGESTOR: {today} ===")
        results: dict[str, Any] = {"date": str(today)}

        # 1. GexBot gap fill (priorita ES_SPX)
        gex_saved = self.gexbot_fill_gaps(max_calls=100)
        results["gexbot_snapshots"] = gex_saved

        # 2. Databento historical consolidation
        db_saved = self.databento_consolidate_day(today)
        results["databento_trades"] = db_saved

        logger.info(f"Nightly complete: {results}")
        return results
