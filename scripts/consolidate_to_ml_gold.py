"""Consolidate fragmented P1UNI DuckDB stores into ml_gold.duckdb.

Sources -> targets in ml_gold.duckdb:
  1. p1uni_history.duckdb  (Databento historical trades)  -> historical_trades
  2. gexbot_snapshots.duckdb (GexBot REST)                 -> gexbot_snapshots
  3. gexbot_ws.duckdb        (GexBot WebSocket)            -> gexbot_ws_events
  4. p1-lite snapshots_*.csv (range snapshots)             -> p1lite_range_snapshots
  5. p1-lite ninja/range_levels.csv (live row)             -> p1lite_range_live

Idempotent: uses watermarks (MAX(timestamp) in target) to copy only new rows.

Usage:
    # As part of nightly run:
    python scripts/nightly_data_harvest.py --consolidate

    # Standalone (full sync of whatever exists today):
    python scripts/consolidate_to_ml_gold.py
"""
from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

log = logging.getLogger("consolidate")

ML_GOLD_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
P1UNI_HISTORY_PATH = str(Path(__file__).parent.parent / "data" / "p1uni_history.duckdb")
GEXBOT_SNAPSHOTS_PATH = str(Path(__file__).parent.parent / "data" / "gexbot_snapshots.duckdb")
GEXBOT_WS_PATH = str(Path(__file__).parent.parent / "data" / "gexbot_ws.duckdb")
P1_LITE_DATA_DIR = r"C:\Users\annal\Desktop\p1-lite\data"


# ------------------------------------------------------------------
# Table DDL in ml_gold (CREATE TABLE IF NOT EXISTS, safe to rerun)
# ------------------------------------------------------------------
DDL_P1LITE_RANGE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS p1lite_range_snapshots (
    timestamp TIMESTAMP,
    slot VARCHAR,               -- '09:00' (MR) | 'RTH' (OR)
    date DATE,
    base_label VARCHAR,         -- 'VWAP' | 'OPEN'
    base_value DOUBLE,
    spx_open DOUBLE,
    spread DOUBLE,
    iv_daily DOUBLE,
    iv_straddle DOUBLE,
    r1_up DOUBLE, r2_up DOUBLE, center DOUBLE, r2_dn DOUBLE, r1_dn DOUBLE,
    source VARCHAR DEFAULT 'p1-lite',
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_P1LITE_RANGE_LIVE = """
CREATE TABLE IF NOT EXISTS p1lite_range_live (
    timestamp TIMESTAMP,
    session VARCHAR,            -- NIGHT | MORNING | AFTERNOON
    mr1d DOUBLE, mr1u DOUBLE, mr2d DOUBLE, mr2u DOUBLE,
    or1d DOUBLE, or1u DOUBLE, or2d DOUBLE, or2u DOUBLE,
    live_r1_up DOUBLE, live_r1_dn DOUBLE,
    live_r2_up DOUBLE, live_r2_dn DOUBLE,
    settlement_signal VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

DDL_HISTORICAL_TRADES_GOLD = """
CREATE TABLE IF NOT EXISTS historical_trades (
    ts_event TIMESTAMP,
    ticker VARCHAR,
    price DOUBLE,
    size INTEGER,
    side VARCHAR,
    flags INTEGER,
    source VARCHAR DEFAULT 'DATABENTO_HIST',
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# NOTE: gexbot_snapshots and gexbot_ws_events schemas are inferred from source
# DBs at first consolidation (CREATE TABLE AS SELECT ... WHERE 1=0 + then INSERT).


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _get_watermark(conn: duckdb.DuckDBPyConnection, table: str, ts_col: str) -> datetime | None:
    """Return max(ts) for incremental copy. None if table empty or missing."""
    try:
        row = conn.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()
        return row[0] if row and row[0] is not None else None
    except duckdb.CatalogException:
        return None


def _attach_readonly(conn: duckdb.DuckDBPyConnection, path: str, alias: str) -> bool:
    """ATTACH another DuckDB file read-only. Returns False if missing."""
    if not os.path.exists(path):
        log.warning(f"source DB missing: {path}")
        return False
    conn.execute(f"ATTACH '{path}' AS {alias} (READ_ONLY)")
    return True


def _detach(conn: duckdb.DuckDBPyConnection, alias: str) -> None:
    try:
        conn.execute(f"DETACH {alias}")
    except Exception:
        pass


# ------------------------------------------------------------------
# 1. Databento historical trades (p1uni_history -> ml_gold)
# ------------------------------------------------------------------
def consolidate_historical_trades(conn: duckdb.DuckDBPyConnection,
                                   p1uni_history_path: str) -> int:
    if not _attach_readonly(conn, p1uni_history_path, "hist_src"):
        return 0

    conn.execute(DDL_HISTORICAL_TRADES_GOLD)
    wm = _get_watermark(conn, "historical_trades", "ts_event")
    wm_clause = f"WHERE ts_event > TIMESTAMP '{wm}'" if wm else ""

    rows = conn.execute(f"""
        INSERT INTO historical_trades (ts_event, ticker, price, size, side, flags, source)
        SELECT ts_event, ticker, price, size, side, flags,
               COALESCE(source, 'DATABENTO_HIST')
        FROM hist_src.historical_trades
        {wm_clause}
    """).fetchone()
    # DuckDB INSERT returns changed rows differently; use separate count.
    n = conn.execute(f"""
        SELECT COUNT(*) FROM hist_src.historical_trades {wm_clause}
    """).fetchone()[0]
    log.info(f"  historical_trades: +{n:,} rows (watermark={wm})")

    _detach(conn, "hist_src")
    return n


# ------------------------------------------------------------------
# 2. GexBot REST snapshots (gexbot_snapshots -> ml_gold)
# ------------------------------------------------------------------
def consolidate_gexbot_snapshots(conn: duckdb.DuckDBPyConnection,
                                  gexbot_snapshots_path: str) -> int:
    if not _attach_readonly(conn, gexbot_snapshots_path, "gex_src"):
        return 0

    # Discover source tables (first run only).
    src_tables = [r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog='gex_src'"
    ).fetchall()]

    total = 0
    for tbl in src_tables:
        target = f"gexbot_{tbl}" if not tbl.startswith("gexbot_") else tbl
        # Create target schema from source on first run.
        conn.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM gex_src.{tbl} WHERE 1=0")

        # Heuristic watermark: look for common ts columns.
        wm_col = None
        for c in ("timestamp", "ts", "ts_event", "ingested_at", "captured_at"):
            try:
                conn.execute(f"SELECT {c} FROM gex_src.{tbl} LIMIT 1")
                wm_col = c
                break
            except Exception:
                continue

        if wm_col:
            wm = _get_watermark(conn, target, wm_col)
            wm_clause = f"WHERE {wm_col} > TIMESTAMP '{wm}'" if wm else ""
        else:
            wm_clause = ""  # no watermark — full copy on first run; afterwards duplicates dropped by dedup job

        n = conn.execute(f"SELECT COUNT(*) FROM gex_src.{tbl} {wm_clause}").fetchone()[0]
        if n > 0:
            conn.execute(f"INSERT INTO {target} SELECT * FROM gex_src.{tbl} {wm_clause}")
        log.info(f"  {target}: +{n:,} rows (wm_col={wm_col})")
        total += n

    _detach(conn, "gex_src")
    return total


# ------------------------------------------------------------------
# 3. GexBot WS events (gexbot_ws -> ml_gold)
# ------------------------------------------------------------------
def consolidate_gexbot_ws(conn: duckdb.DuckDBPyConnection, gexbot_ws_path: str) -> int:
    if not _attach_readonly(conn, gexbot_ws_path, "ws_src"):
        return 0

    src_tables = [r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog='ws_src'"
    ).fetchall()]

    total = 0
    for tbl in src_tables:
        target = f"gexbot_ws_{tbl}" if not tbl.startswith("gexbot_ws_") else tbl
        conn.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM ws_src.{tbl} WHERE 1=0")

        wm_col = None
        for c in ("timestamp", "ts", "event_time", "ingested_at"):
            try:
                conn.execute(f"SELECT {c} FROM ws_src.{tbl} LIMIT 1")
                wm_col = c
                break
            except Exception:
                continue

        if wm_col:
            wm = _get_watermark(conn, target, wm_col)
            wm_clause = f"WHERE {wm_col} > TIMESTAMP '{wm}'" if wm else ""
        else:
            wm_clause = ""

        n = conn.execute(f"SELECT COUNT(*) FROM ws_src.{tbl} {wm_clause}").fetchone()[0]
        if n > 0:
            conn.execute(f"INSERT INTO {target} SELECT * FROM ws_src.{tbl} {wm_clause}")
        log.info(f"  {target}: +{n:,} rows (wm_col={wm_col})")
        total += n

    _detach(conn, "ws_src")
    return total


# ------------------------------------------------------------------
# 4. P1-Lite snapshots (CSV -> ml_gold.p1lite_range_snapshots)
# ------------------------------------------------------------------
def consolidate_p1lite_snapshots(conn: duckdb.DuckDBPyConnection,
                                  p1lite_data_dir: str) -> int:
    conn.execute(DDL_P1LITE_RANGE_SNAPSHOTS)
    data_path = Path(p1lite_data_dir)
    if not data_path.exists():
        log.warning(f"p1-lite data dir missing: {data_path}")
        return 0

    wm = _get_watermark(conn, "p1lite_range_snapshots", "timestamp")
    snap_files = sorted(data_path.glob("snapshots_*.csv"))
    total = 0

    for f in snap_files:
        try:
            with open(f, "r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                rows = []
                for r in reader:
                    ts = r.get("timestamp", "").strip()
                    if not ts:
                        continue
                    try:
                        ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except ValueError:
                        try:
                            ts_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            continue
                    if wm and ts_dt <= wm:
                        continue

                    def _f(k):
                        v = r.get(k, "")
                        return float(v) if v not in ("", None) else None

                    rows.append((
                        ts_dt, r.get("slot"), r.get("date"),
                        r.get("base_label"), _f("base_value"),
                        _f("spx_open"), _f("spread"),
                        _f("iv_daily"), _f("iv_straddle"),
                        _f("R1_UP"), _f("R2_UP"), _f("CENTER"),
                        _f("R2_DN"), _f("R1_DN"),
                    ))
            if rows:
                conn.executemany("""
                    INSERT INTO p1lite_range_snapshots
                    (timestamp, slot, date, base_label, base_value,
                     spx_open, spread, iv_daily, iv_straddle,
                     r1_up, r2_up, center, r2_dn, r1_dn)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, rows)
                log.info(f"  p1lite_range_snapshots: +{len(rows):,} rows from {f.name}")
                total += len(rows)
        except Exception as e:
            log.error(f"  {f.name}: {e}")

    return total


# ------------------------------------------------------------------
# 5. P1-Lite live row (range_levels.csv -> ml_gold.p1lite_range_live)
# ------------------------------------------------------------------
def consolidate_p1lite_live(conn: duckdb.DuckDBPyConnection,
                             p1lite_data_dir: str) -> int:
    conn.execute(DDL_P1LITE_RANGE_LIVE)
    live_path = Path(p1lite_data_dir) / "ninja" / "range_levels.csv"
    if not live_path.exists():
        log.warning(f"range_levels.csv missing: {live_path}")
        return 0

    wm = _get_watermark(conn, "p1lite_range_live", "timestamp")

    with open(live_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        row = next(reader, None)
    if not row:
        return 0

    try:
        ts_dt = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
    except ValueError:
        return 0

    if wm and ts_dt <= wm:
        return 0  # no new data

    def _f(k):
        v = row.get(k, "")
        return float(v) if v not in ("", None) else None

    conn.execute("""
        INSERT INTO p1lite_range_live
        (timestamp, session, mr1d, mr1u, mr2d, mr2u,
         or1d, or1u, or2d, or2u,
         live_r1_up, live_r1_dn, live_r2_up, live_r2_dn,
         settlement_signal)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ts_dt, row.get("session"),
        _f("mr1d"), _f("mr1u"), _f("mr2d"), _f("mr2u"),
        _f("or1d"), _f("or1u"), _f("or2d"), _f("or2u"),
        # Prefer new 4-level columns; fall back to legacy live_range_up/dn for R1.
        _f("live_r1_up") if row.get("live_r1_up") else _f("live_range_up"),
        _f("live_r1_dn") if row.get("live_r1_dn") else _f("live_range_dn"),
        _f("live_r2_up"),
        _f("live_r2_dn"),
        row.get("settlement_signal", "NONE"),
    ))
    log.info(f"  p1lite_range_live: +1 row ({ts_dt})")
    return 1


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
def run_consolidation(
    p1uni_history_path: str = P1UNI_HISTORY_PATH,
    ml_gold_path: str = ML_GOLD_PATH,
    gexbot_snapshots_path: str = GEXBOT_SNAPSHOTS_PATH,
    gexbot_ws_path: str = GEXBOT_WS_PATH,
    p1_lite_data_dir: str = P1_LITE_DATA_DIR,
) -> dict[str, int]:
    """Run all 5 consolidation steps. Returns counts by source."""
    if not os.path.exists(ml_gold_path):
        raise FileNotFoundError(f"ml_gold.duckdb not found: {ml_gold_path}")

    log.info(f"Opening ml_gold.duckdb at {ml_gold_path}")
    conn = duckdb.connect(ml_gold_path, read_only=False)

    counts: dict[str, int] = {}
    try:
        counts["historical_trades"] = consolidate_historical_trades(conn, p1uni_history_path)
        counts["gexbot_snapshots"] = consolidate_gexbot_snapshots(conn, gexbot_snapshots_path)
        counts["gexbot_ws"] = consolidate_gexbot_ws(conn, gexbot_ws_path)
        counts["p1lite_snapshots"] = consolidate_p1lite_snapshots(conn, p1_lite_data_dir)
        counts["p1lite_live"] = consolidate_p1lite_live(conn, p1_lite_data_dir)
    finally:
        conn.close()

    total = sum(counts.values())
    log.info(f"\n=== CONSOLIDATION COMPLETE: +{total:,} rows across {len(counts)} sources ===")
    for k, v in counts.items():
        log.info(f"  {k}: {v:,}")
    return counts


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    run_consolidation()
