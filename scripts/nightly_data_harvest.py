"""
Nightly Data Harvest — Scarica dati storici Databento + valida.

Piano Standard: Trades/MBP-1 fino a 12 mesi, MBO 1 mese, OHLCV 7 anni.
Scarica per ES, NQ in batch giornalieri per non saturare la memoria.

Usage:
    python scripts/nightly_data_harvest.py --days 7   # ultima settimana
    python scripts/nightly_data_harvest.py --days 30  # ultimo mese
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.core.secrets import get_secret

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("harvest")

API_KEY = get_secret("DATABENTO_API_KEY", required=True)
DATASET = "GLBX.MDP3"
DB_PATH = str(Path(__file__).parent.parent / "data" / "p1uni_history.duckdb")

SYMBOLS = {
    "ES": "ES.c.0",
    "NQ": "NQ.c.0",
}


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS historical_trades (
            ts_event TIMESTAMP, ticker VARCHAR, price DOUBLE,
            size INTEGER, side VARCHAR, flags INTEGER,
            source VARCHAR DEFAULT 'DATABENTO_HIST',
            download_date DATE DEFAULT CURRENT_DATE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS harvest_log (
            symbol VARCHAR, schema_name VARCHAR, start_date DATE, end_date DATE,
            records_downloaded INTEGER, download_time_sec DOUBLE,
            status VARCHAR, error_msg VARCHAR,
            harvested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def harvest_day(conn: duckdb.DuckDBPyConnection, symbol: str, ticker: str,
                date: datetime, schema: str = "trades") -> int:
    """Scarica un giorno di dati per un simbolo."""
    import databento as db

    start = date.strftime("%Y-%m-%dT00:00:00")
    end = (date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

    # Check se gia' scaricato
    existing = conn.execute(
        "SELECT COUNT(*) FROM harvest_log WHERE symbol=? AND start_date=? AND status='OK'",
        [symbol, date.date()]
    ).fetchone()[0]
    if existing > 0:
        log.info(f"  {ticker} {date.date()}: gia' scaricato, skip")
        return 0

    t0 = time.time()
    try:
        client = db.Historical(key=API_KEY)
        data = client.timeseries.get_range(
            dataset=DATASET,
            symbols=[symbol],
            schema=schema,
            start=start,
            end=end,
            stype_in="continuous" if ".c." in symbol else "raw_symbol",
        )

        df = data.to_df()
        if df.empty:
            log.info(f"  {ticker} {date.date()}: nessun dato (weekend/holiday?)")
            conn.execute(
                "INSERT INTO harvest_log (symbol, schema_name, start_date, end_date, records_downloaded, download_time_sec, status, error_msg) VALUES (?,?,?,?,0,?,?,?)",
                [symbol, schema, date.date(), date.date(), time.time()-t0, "EMPTY", ""]
            )
            return 0

        # Converti e inserisci
        records = []
        for _, row in df.iterrows():
            ts = row.get("ts_event")
            price = float(row.get("price", 0))
            if price > 1e10:
                price /= 1e9
            size = int(row.get("size", 0))

            side_val = row.get("side", "")
            if hasattr(side_val, "value"):
                side = str(side_val.value)
            else:
                side = str(side_val)

            records.append((ts, ticker, price, size, side, int(row.get("flags", 0))))

        if records:
            insert_df = pd.DataFrame(records, columns=["ts_event", "ticker", "price", "size", "side", "flags"])
            conn.register("_hist_batch", insert_df)
            conn.execute("INSERT INTO historical_trades (ts_event, ticker, price, size, side, flags) SELECT * FROM _hist_batch")
            conn.unregister("_hist_batch")

        elapsed = time.time() - t0
        log.info(f"  {ticker} {date.date()}: {len(records):,} trades ({elapsed:.1f}s)")

        conn.execute(
            "INSERT INTO harvest_log (symbol, schema_name, start_date, end_date, records_downloaded, download_time_sec, status, error_msg) VALUES (?,?,?,?,?,?,?,?)",
            [symbol, schema, date.date(), date.date(), len(records), elapsed, "OK", ""]
        )
        return len(records)

    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"  {ticker} {date.date()}: ERROR {e}")
        conn.execute(
            "INSERT INTO harvest_log (symbol, schema_name, start_date, end_date, records_downloaded, download_time_sec, status, error_msg) VALUES (?,?,?,?,0,?,?,?)",
            [symbol, schema, date.date(), date.date(), elapsed, "ERROR", str(e)[:200]]
        )
        return 0


def validate_harvest(conn: duckdb.DuckDBPyConnection) -> None:
    """Valida i dati scaricati."""
    log.info("\n=== VALIDAZIONE ===")
    total = conn.execute("SELECT COUNT(*) FROM historical_trades").fetchone()[0]
    log.info(f"Totale trade storici: {total:,}")

    if total == 0:
        return

    stats = conn.execute("""
        SELECT ticker, COUNT(*) as cnt, MIN(price) as min_p, MAX(price) as max_p,
               MIN(ts_event) as first_ts, MAX(ts_event) as last_ts
        FROM historical_trades GROUP BY ticker
    """).fetchdf()
    for _, row in stats.iterrows():
        log.info(f"  {row['ticker']}: {row['cnt']:,} trades, price {row['min_p']:.2f}-{row['max_p']:.2f}, "
                 f"{row['first_ts']} -> {row['last_ts']}")

    sides = conn.execute("SELECT side, COUNT(*) FROM historical_trades GROUP BY side").fetchdf()
    log.info(f"  Side distribution: {dict(zip(sides.iloc[:,0], sides.iloc[:,1]))}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Giorni da scaricare")
    parser.add_argument("--symbols", nargs="+", default=["ES"], help="Ticker da scaricare")
    args = parser.parse_args()

    log.info(f"=== NIGHTLY DATA HARVEST ===")
    log.info(f"DB: {DB_PATH}")
    log.info(f"Symbols: {args.symbols}, Days: {args.days}")

    conn = duckdb.connect(DB_PATH)
    create_tables(conn)

    total_records = 0
    end_date = datetime.now(timezone.utc)

    for ticker in args.symbols:
        symbol = SYMBOLS.get(ticker, f"{ticker}.FUT.CME")
        log.info(f"\n--- {ticker} ({symbol}) ---")

        for day_offset in range(args.days):
            date = end_date - timedelta(days=day_offset + 1)
            # Skip weekends
            if date.weekday() >= 5:
                continue
            n = harvest_day(conn, symbol, ticker, date)
            total_records += n
            time.sleep(0.5)  # rate limit

    validate_harvest(conn)
    conn.close()

    log.info(f"\n=== HARVEST COMPLETE: {total_records:,} records ===")


if __name__ == "__main__":
    main()
