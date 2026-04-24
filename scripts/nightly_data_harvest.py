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

# Default dataset for legacy callers (ES/NQ futures).
DATASET = "GLBX.MDP3"

# Centralized gold DB (cross-source consolidation target). Historical trades
# keep landing in p1uni_history.duckdb; a separate consolidation step copies
# them into ml_gold.duckdb (see consolidate_to_ml_gold()).
DB_PATH = str(Path(__file__).parent.parent / "data" / "p1uni_history.duckdb")
ML_GOLD_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"

# Per-ticker Databento dataset + native symbol.
# - GLBX.MDP3 : CME futures continuous front-month (ES, NQ)
# - XNAS.ITCH : Nasdaq equities (SPY, QQQ, IWM, GLD, TLT, UVXY via Nasdaq)
# - OPRA.PILLAR : options (not used here)
# - XCHI.PITCH / XNYS.PILLAR : NYSE-listed (GLD, TLT are NYSE Arca — use
#   DBEQ.BASIC consolidated feed for cross-venue equities)
# - VIX is the CBOE index; Databento sells it under OPRA for options, but the
#   underlying VIX level comes from XCBO/index feeds. Flagged as optional.
SYMBOLS = {
    "ES":   {"dataset": "GLBX.MDP3",  "symbol": "ES.c.0",  "stype": "continuous"},
    "NQ":   {"dataset": "GLBX.MDP3",  "symbol": "NQ.c.0",  "stype": "continuous"},
    "SPY":  {"dataset": "DBEQ.BASIC", "symbol": "SPY",     "stype": "raw_symbol"},
    "QQQ":  {"dataset": "DBEQ.BASIC", "symbol": "QQQ",     "stype": "raw_symbol"},
    "IWM":  {"dataset": "DBEQ.BASIC", "symbol": "IWM",     "stype": "raw_symbol"},
    "GLD":  {"dataset": "DBEQ.BASIC", "symbol": "GLD",     "stype": "raw_symbol"},
    "TLT":  {"dataset": "DBEQ.BASIC", "symbol": "TLT",     "stype": "raw_symbol"},
    "UVXY": {"dataset": "DBEQ.BASIC", "symbol": "UVXY",    "stype": "raw_symbol"},
    # VIX index: Databento OPRA covers options but not the spot index. If the
    # user has an index feed subscription, swap dataset to "XCBO" and re-enable.
    "VIX":  {"dataset": "XCBO",       "symbol": "VIX",     "stype": "raw_symbol"},
}

# All tickers the system considers "universe". Keep in sync with
# config/settings.yaml -> tickers.valid.
ALL_TICKERS = ["ES", "NQ", "SPY", "QQQ", "VIX", "IWM", "GLD", "TLT", "UVXY"]


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
                date: datetime, schema: str = "trades",
                dataset: str | None = None, stype: str | None = None) -> int:
    """Scarica un giorno di dati per un simbolo.

    dataset/stype optional: if None, fall back to SYMBOLS[ticker] or module
    defaults (back-compat for callers that don't yet pass per-ticker config).
    """
    import databento as db

    if dataset is None or stype is None:
        cfg = SYMBOLS.get(ticker, {})
        dataset = dataset or cfg.get("dataset", DATASET)
        stype = stype or cfg.get("stype") or ("continuous" if ".c." in symbol else "raw_symbol")

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
            dataset=dataset,
            symbols=[symbol],
            schema=schema,
            start=start,
            end=end,
            stype_in=stype,
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


def replay_quarantine(live_db_path: str, quarantine_base: Path, date_str: str | None = None, dry_run: bool = False) -> int:
    """Re-inserisce i record in quarantena nel DB live.

    Legge i file JSON da data/quarantine/YYYY-MM-DD/ e fa INSERT INTO
    la tabella originale in p1uni_live.duckdb.
    Sposta i file processati in data/quarantine/replayed/YYYY-MM-DD/.

    Args:
        live_db_path: Path a p1uni_live.duckdb
        quarantine_base: Path base cartella quarantine (es: data/quarantine)
        date_str: Data da replayare (es: "2026-04-16"); None = tutte le date
        dry_run: Se True, mostra cosa verrebbe insertito senza farlo

    Returns:
        Numero di record replayati con successo.
    """
    import shutil

    if date_str:
        day_dirs = [quarantine_base / date_str] if (quarantine_base / date_str).exists() else []
    else:
        day_dirs = sorted(d for d in quarantine_base.iterdir() if d.is_dir() and d.name != "replayed")

    if not day_dirs:
        log.info("Nessuna cartella quarantine trovata.")
        return 0

    if dry_run:
        log.info("[DRY RUN] Nessuna scrittura effettuata.")

    conn = duckdb.connect(live_db_path) if not dry_run else None
    total_replayed = 0

    try:
        for day_dir in day_dirs:
            json_files = sorted(day_dir.glob("*.json"))
            log.info(f"\n--- Replay quarantine {day_dir.name}: {len(json_files)} file ---")

            for jf in json_files:
                try:
                    data = json.loads(jf.read_text(encoding="utf-8"))
                    records = data.get("records", [])
                    target_table = data.get("target_table", "trades_live")
                    adapter = data.get("adapter", "?")
                    n = len(records)

                    if not records:
                        log.info(f"  {jf.name}: vuoto, skip")
                        continue

                    log.info(f"  {jf.name}: {n} records -> {target_table} [{adapter}]")

                    if dry_run:
                        total_replayed += n
                        continue

                    df = pd.DataFrame(records)
                    # Converti colonne timestamp
                    for col in df.columns:
                        if "ts_" in col or col in ("timestamp", "time"):
                            try:
                                df[col] = pd.to_datetime(df[col], utc=True)
                            except Exception:
                                pass

                    conn.register("_qr_batch", df)
                    try:
                        cols = ", ".join(df.columns)
                        # DuckDB does not support INSERT OR IGNORE (SQLite syntax).
                        # Use plain INSERT; duplicates are rare (nanosec timestamps).
                        conn.execute(f"INSERT INTO {target_table} ({cols}) SELECT * FROM _qr_batch")
                        total_replayed += n
                    except Exception as e:
                        log.error(f"    INSERT failed: {e}")
                        conn.unregister("_qr_batch")
                        continue
                    conn.unregister("_qr_batch")

                    # Sposta in replayed/
                    replayed_dir = quarantine_base / "replayed" / day_dir.name
                    replayed_dir.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(jf), str(replayed_dir / jf.name))

                except Exception as e:
                    log.error(f"  {jf.name}: errore {e}")

            log.info(f"  Totale replayati da {day_dir.name}: {total_replayed}")

    finally:
        if conn is not None:
            conn.close()

    log.info(f"\n=== QUARANTINE REPLAY COMPLETE: {total_replayed:,} records ===")
    return total_replayed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Giorni da scaricare")
    parser.add_argument("--symbols", nargs="+", default=ALL_TICKERS,
                        help="Ticker da scaricare (default: tutti 9)")
    parser.add_argument("--consolidate", action="store_true",
                        help="Dopo l'harvest, consolida tutti i DB frammentati in ml_gold.duckdb")
    parser.add_argument("--replay-quarantine", action="store_true",
                        help="Re-inserisce i file in quarantine nel DB live invece di scaricare nuovi dati")
    parser.add_argument("--replay-date", type=str, default=None,
                        help="Data da replayare (es: 2026-04-16); default tutte le date")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra cosa verrebbe replayato senza scrivere")
    parser.add_argument("--live-db", type=str, default=None,
                        help="Path al DB live (default: legge da config/settings.yaml -> database.path)")
    args = parser.parse_args()

    if args.replay_quarantine:
        base_dir = Path(__file__).parent.parent

        # Determina il DB live: CLI arg > config settings.yaml > fallback
        if args.live_db:
            live_db = Path(args.live_db)
        else:
            # Prova a leggere da config/settings.yaml
            settings_path = base_dir / "config" / "settings.yaml"
            if settings_path.exists():
                import yaml
                with open(settings_path, "r", encoding="utf-8") as _f:
                    _cfg = yaml.safe_load(_f.read())
                _db_path = _cfg.get("database", {}).get("path", "")
                live_db = Path(_db_path) if _db_path else base_dir / "data" / "p1uni_live.duckdb"
            else:
                # Fallback: usa lo stesso DB del harvest storico
                live_db = Path(DB_PATH).parent / "p1uni_live.duckdb"
                log.warning(f"config/settings.yaml non trovato, usando {live_db}")

        quarantine_base = base_dir / "data" / "quarantine"
        log.info(f"=== QUARANTINE REPLAY ===")
        log.info(f"Live DB: {live_db}")
        log.info(f"Quarantine: {quarantine_base}")
        log.info(f"Date filter: {args.replay_date or 'tutte'}")
        if args.dry_run:
            log.info("DRY RUN: nessuna scrittura")
        n = replay_quarantine(str(live_db), quarantine_base, args.replay_date, dry_run=args.dry_run)
        log.info(f"Replayati: {n:,} records")
        return

    log.info(f"=== NIGHTLY DATA HARVEST ===")
    log.info(f"DB: {DB_PATH}")
    log.info(f"Symbols: {args.symbols}, Days: {args.days}")

    conn = duckdb.connect(DB_PATH)
    create_tables(conn)

    total_records = 0
    end_date = datetime.now(timezone.utc)

    for ticker in args.symbols:
        cfg = SYMBOLS.get(ticker)
        if cfg is None:
            log.warning(f"  {ticker}: not in SYMBOLS map; skip (add to SYMBOLS to enable)")
            continue
        symbol = cfg["symbol"]
        dataset = cfg["dataset"]
        stype = cfg["stype"]
        log.info(f"\n--- {ticker} ({symbol} / {dataset}) ---")

        for day_offset in range(args.days):
            date = end_date - timedelta(days=day_offset + 1)
            # Skip weekends (note: some datasets have Sat/Sun OPRA sessions;
            # adjust if needed per-dataset, but trades/MBP-1 only trade M-F).
            if date.weekday() >= 5:
                continue
            n = harvest_day(conn, symbol, ticker, date,
                            dataset=dataset, stype=stype)
            total_records += n
            time.sleep(0.5)  # rate limit

    validate_harvest(conn)
    conn.close()

    log.info(f"\n=== HARVEST COMPLETE: {total_records:,} records ===")

    if args.consolidate:
        log.info("\n=== CONSOLIDATING INTO ml_gold.duckdb ===")
        try:
            from consolidate_to_ml_gold import run_consolidation
            run_consolidation(
                p1uni_history_path=DB_PATH,
                ml_gold_path=ML_GOLD_PATH,
            )
        except ImportError:
            log.error("consolidate_to_ml_gold module not found; skipping consolidation")
        except Exception as e:
            log.error(f"Consolidation failed: {e}")


if __name__ == "__main__":
    main()
