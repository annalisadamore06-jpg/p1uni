"""
Data Validator - Gatekeeper pre-write per il database ml_gold.duckdb

18 Regole di Validazione (R01-R18) derivate dal DATA_CONTRACT.md e dal report forense.
Ogni record DEVE superare TUTTE le regole prima dell'inserimento.
Se fallisce -> tabella data_quarantine con codice regola specifico.

Performance: validazione single-record e batch (vettorizzata via pandas).
Thread-safe: nessuno stato mutabile condiviso tra chiamate.

Schema data_quarantine:
    CREATE TABLE IF NOT EXISTS data_quarantine (
        raw_data JSON,
        failure_reason VARCHAR,   -- es: "R03: spot <= 0 (got: 0.0)"
        rule_code VARCHAR,        -- es: "R03"
        target_table VARCHAR,
        source VARCHAR,
        ticker VARCHAR,
        ts_record TIMESTAMP,
        ts_quarantined TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        resolved BOOLEAN DEFAULT FALSE
    );
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from src.core.normalizer import VALID_TICKERS

logger = logging.getLogger("p1uni.core.validator")

# ============================================================
# Costanti di validazione
# ============================================================
MIN_DATE = datetime(2023, 1, 1, tzinfo=timezone.utc)
FUTURE_TOLERANCE = timedelta(minutes=5)
MIN_SPOT = 0.0          # esclusivo: spot DEVE essere > 0
MAX_SPOT = 100_000.0    # esclusivo: spot DEVE essere < 100k
MIN_PRICE = 0.0         # per trades_live: price > 0
MAX_PRICE = 1_000_000.0 # per trades_live
MIN_SIZE = 1            # per trades_live: size >= 1
VALID_SIDES = {"B", "A"}
VALID_HUBS = {"classic", "state_gex"}
VALID_AGGREGATIONS = {"gex_full", "gex_zero", "gex_one"}
VALID_GREEK_TYPES = {"delta", "gamma", "vanna", "charm", "volume"}
VALID_DTE_TYPES = {"zero", "one"}
VALID_SOURCES = {"hist_rest", "ws_p1clean", "WS", "P1", "P1_LITE", "DATABENTO"}

# Campi obbligatori per tabella
REQUIRED_FIELDS: dict[str, list[str]] = {
    "gex_summary": ["ts_utc", "source", "ticker", "hub", "aggregation", "spot"],
    "greeks_summary": ["ts_utc", "source", "ticker", "hub", "greek_type", "dte_type", "spot"],
    "orderflow": ["ts_utc", "source", "ticker", "spot"],
    "trades_live": ["ts_event", "price", "size", "side"],
    "intraday_ranges_stream": ["ts_utc", "session_date", "spot"],
    "settlement_ranges": ["date"],
}


# ============================================================
# ValidationError con codice regola
# ============================================================
class ValidationError:
    """Singola violazione con codice regola tracciabile."""

    __slots__ = ("rule_code", "reason", "field", "value")

    def __init__(self, rule_code: str, reason: str, field: str, value: Any = None) -> None:
        self.rule_code = rule_code
        self.reason = reason
        self.field = field
        self.value = value

    def __str__(self) -> str:
        return f"{self.rule_code}: {self.reason} (field={self.field}, got={self.value!r})"

    def __repr__(self) -> str:
        return self.__str__()


# ============================================================
# Funzioni di check atomiche (stateless, testabili)
# ============================================================

def _is_nan_or_inf(value: Any) -> bool:
    """Controlla se un valore e' NaN o Inf. Gestisce float, int, str."""
    if value is None:
        return False
    try:
        f = float(value)
        return math.isnan(f) or math.isinf(f)
    except (ValueError, TypeError):
        return False


def _is_weekend_market_closed(ts: datetime) -> bool:
    """Verifica se il mercato ES futures e' chiuso (weekend).

    Orari mercato ES (CME Globex):
    - Apre: Domenica 22:00 UTC (= 17:00 CT)
    - Chiude: Venerdi 22:00 UTC (= 17:00 CT)
    - Pausa giornaliera: 21:00-22:00 UTC (ma non la contiamo come weekend)

    Weekend = Sabato 01:00 UTC --> Domenica 22:00 UTC
    (Venerdi notte fino a Sabato 01:00 e' ancora sessione attiva)
    """
    weekday = ts.weekday()  # 0=Mon, 5=Sat, 6=Sun
    hour = ts.hour

    # Sabato dopo 01:00 UTC -> mercato chiuso
    if weekday == 5 and hour >= 1:
        return True
    # Tutta la domenica prima delle 22:00 UTC -> mercato chiuso
    if weekday == 6 and hour < 22:
        return True
    return False


# ============================================================
# 18 Regole di Validazione
# ============================================================

def validate_R01_required_fields(
    record: dict[str, Any],
    target_table: str,
) -> list[ValidationError]:
    """R01: Nessun campo obbligatorio puo essere NULL."""
    errors: list[ValidationError] = []
    required = REQUIRED_FIELDS.get(target_table, [])
    for field in required:
        val = record.get(field)
        if val is None:
            errors.append(ValidationError("R01", f"Campo obbligatorio NULL", field, None))
    return errors


def validate_R02_ticker(record: dict[str, Any]) -> list[ValidationError]:
    """R02: ticker DEVE essere uno dei 9 ticker Gold Standard."""
    ticker = record.get("ticker")
    if ticker is None:
        return []  # R01 copre il caso NULL
    if ticker not in VALID_TICKERS:
        return [ValidationError("R02", f"Ticker non valido (ammessi: {sorted(VALID_TICKERS)})", "ticker", ticker)]
    return []


def validate_R03_spot_positive(record: dict[str, Any]) -> list[ValidationError]:
    """R03: spot DEVE essere > 0 e < 100,000. Zero NON e' ammesso."""
    spot = record.get("spot")
    if spot is None:
        return []  # R01 copre
    try:
        spot_f = float(spot)
    except (ValueError, TypeError):
        return [ValidationError("R03", "spot non convertibile a float", "spot", spot)]

    if spot_f <= MIN_SPOT:
        return [ValidationError("R03", f"spot <= 0 (deve essere strettamente positivo)", "spot", spot_f)]
    if spot_f >= MAX_SPOT:
        return [ValidationError("R03", f"spot >= {MAX_SPOT} (fuori range)", "spot", spot_f)]
    return []


def validate_R04_spot_nan_inf(record: dict[str, Any]) -> list[ValidationError]:
    """R04: spot NON puo essere NaN o Inf."""
    spot = record.get("spot")
    if spot is None:
        return []
    if _is_nan_or_inf(spot):
        return [ValidationError("R04", "spot e' NaN o Inf", "spot", spot)]
    return []


def validate_R05_price_positive(record: dict[str, Any]) -> list[ValidationError]:
    """R05: price (trades_live) DEVE essere > 0."""
    price = record.get("price")
    if price is None:
        return []
    try:
        price_f = float(price)
    except (ValueError, TypeError):
        return [ValidationError("R05", "price non convertibile a float", "price", price)]

    if price_f <= MIN_PRICE:
        return [ValidationError("R05", "price <= 0", "price", price_f)]
    if price_f >= MAX_PRICE:
        return [ValidationError("R05", f"price >= {MAX_PRICE}", "price", price_f)]
    return []


def validate_R06_price_nan_inf(record: dict[str, Any]) -> list[ValidationError]:
    """R06: price NON puo essere NaN o Inf."""
    price = record.get("price")
    if price is None:
        return []
    if _is_nan_or_inf(price):
        return [ValidationError("R06", "price e' NaN o Inf", "price", price)]
    return []


def validate_R07_size_positive(record: dict[str, Any]) -> list[ValidationError]:
    """R07: size (trades_live) DEVE essere >= 1."""
    size = record.get("size")
    if size is None:
        return []
    try:
        size_i = int(size)
    except (ValueError, TypeError):
        return [ValidationError("R07", "size non convertibile a int", "size", size)]

    if size_i < MIN_SIZE:
        return [ValidationError("R07", f"size < {MIN_SIZE}", "size", size_i)]
    return []


def validate_R08_side_valid(record: dict[str, Any]) -> list[ValidationError]:
    """R08: side DEVE essere 'B' o 'A'."""
    side = record.get("side")
    if side is None:
        return []
    if side not in VALID_SIDES:
        return [ValidationError("R08", f"side non valido (ammessi: {VALID_SIDES})", "side", side)]
    return []


def validate_R09_timestamp_not_future(record: dict[str, Any]) -> list[ValidationError]:
    """R09: timestamp NON puo essere nel futuro (tolleranza +5 min)."""
    ts = _extract_timestamp(record)
    if ts is None:
        return []
    now_utc = datetime.now(timezone.utc)
    if ts > now_utc + FUTURE_TOLERANCE:
        field = "ts_utc" if "ts_utc" in record else "ts_event"
        return [ValidationError("R09", f"Timestamp nel futuro (max: +{FUTURE_TOLERANCE})", field, ts.isoformat())]
    return []


def validate_R10_timestamp_not_too_old(record: dict[str, Any]) -> list[ValidationError]:
    """R10: timestamp DEVE essere >= 2023-01-01."""
    ts = _extract_timestamp(record)
    if ts is None:
        return []
    if ts < MIN_DATE:
        field = "ts_utc" if "ts_utc" in record else "ts_event"
        return [ValidationError("R10", f"Timestamp prima di {MIN_DATE.date()}", field, ts.isoformat())]
    return []


def validate_R11_not_weekend(record: dict[str, Any]) -> list[ValidationError]:
    """R11: timestamp NON deve cadere nel weekend (mercato chiuso)."""
    ts = _extract_timestamp(record)
    if ts is None:
        return []
    if _is_weekend_market_closed(ts):
        field = "ts_utc" if "ts_utc" in record else "ts_event"
        return [ValidationError(
            "R11",
            f"Mercato chiuso (weekend: sab 01:00 - dom 22:00 UTC)",
            field, ts.isoformat()
        )]
    return []


def validate_R12_source_valid(record: dict[str, Any]) -> list[ValidationError]:
    """R12: source DEVE essere un tag riconosciuto."""
    source = record.get("source")
    if source is None:
        return []
    if source not in VALID_SOURCES:
        return [ValidationError("R12", f"Source non valido (ammessi: {sorted(VALID_SOURCES)})", "source", source)]
    return []


def validate_R13_hub_valid(record: dict[str, Any]) -> list[ValidationError]:
    """R13: hub DEVE essere 'classic' o 'state_gex' (solo gex_summary)."""
    hub = record.get("hub")
    if hub is None:
        return []
    if hub not in VALID_HUBS:
        return [ValidationError("R13", f"Hub non valido (ammessi: {sorted(VALID_HUBS)})", "hub", hub)]
    return []


def validate_R14_aggregation_valid(record: dict[str, Any]) -> list[ValidationError]:
    """R14: aggregation DEVE essere 'gex_full', 'gex_zero', 'gex_one' (solo gex_summary)."""
    agg = record.get("aggregation")
    if agg is None:
        return []
    if agg not in VALID_AGGREGATIONS:
        return [ValidationError("R14", f"Aggregation non valida (ammesse: {sorted(VALID_AGGREGATIONS)})", "aggregation", agg)]
    return []


def validate_R15_greek_type_valid(record: dict[str, Any]) -> list[ValidationError]:
    """R15: greek_type DEVE essere uno dei 5 tipi (solo greeks_summary)."""
    gt = record.get("greek_type")
    if gt is None:
        return []
    if gt not in VALID_GREEK_TYPES:
        return [ValidationError("R15", f"greek_type non valido (ammessi: {sorted(VALID_GREEK_TYPES)})", "greek_type", gt)]
    return []


def validate_R16_dte_type_valid(record: dict[str, Any]) -> list[ValidationError]:
    """R16: dte_type DEVE essere 'zero' o 'one' (solo greeks_summary)."""
    dt = record.get("dte_type")
    if dt is None:
        return []
    if dt not in VALID_DTE_TYPES:
        return [ValidationError("R16", f"dte_type non valido (ammessi: {sorted(VALID_DTE_TYPES)})", "dte_type", dt)]
    return []


def validate_R17_numeric_fields_no_nan(record: dict[str, Any]) -> list[ValidationError]:
    """R17: NESSUN campo numerico puo essere NaN o Inf.

    Controlla tutti i campi che hanno valore float/int nel record.
    """
    errors: list[ValidationError] = []
    for field, value in record.items():
        if isinstance(value, (int, float)) and _is_nan_or_inf(value):
            errors.append(ValidationError("R17", f"Valore NaN/Inf nel campo numerico", field, value))
    return errors


def validate_R18_timestamp_has_timezone(record: dict[str, Any]) -> list[ValidationError]:
    """R18: timestamp datetime DEVE avere timezone (UTC). Naive datetime non ammesso."""
    ts = _extract_timestamp(record)
    if ts is None:
        return []
    if not isinstance(ts, datetime):
        return []
    if ts.tzinfo is None:
        field = "ts_utc" if "ts_utc" in record else "ts_event"
        return [ValidationError("R18", "Timestamp naive (senza timezone), richiesto UTC", field, ts.isoformat())]
    return []


def _extract_timestamp(record: dict[str, Any]) -> datetime | None:
    """Estrae il campo timestamp dal record (ts_utc o ts_event)."""
    ts = record.get("ts_utc") or record.get("ts_event")
    if isinstance(ts, datetime):
        return ts
    return None


# ============================================================
# Registry: regole per tabella
# ============================================================

# Regole universali (applicate a TUTTI i record)
UNIVERSAL_RULES = [
    validate_R01_required_fields,  # richiede target_table
    validate_R09_timestamp_not_future,
    validate_R10_timestamp_not_too_old,
    validate_R11_not_weekend,
    validate_R17_numeric_fields_no_nan,
    validate_R18_timestamp_has_timezone,
]

# Regole specifiche per tabella
TABLE_RULES: dict[str, list] = {
    "gex_summary": [
        validate_R02_ticker,
        validate_R03_spot_positive,
        validate_R04_spot_nan_inf,
        validate_R12_source_valid,
        validate_R13_hub_valid,
        validate_R14_aggregation_valid,
    ],
    "greeks_summary": [
        validate_R02_ticker,
        validate_R03_spot_positive,
        validate_R04_spot_nan_inf,
        validate_R12_source_valid,
        validate_R13_hub_valid,
        validate_R15_greek_type_valid,
        validate_R16_dte_type_valid,
    ],
    "orderflow": [
        validate_R02_ticker,
        validate_R03_spot_positive,
        validate_R04_spot_nan_inf,
        validate_R12_source_valid,
    ],
    "trades_live": [
        validate_R05_price_positive,
        validate_R06_price_nan_inf,
        validate_R07_size_positive,
        validate_R08_side_valid,
    ],
    "intraday_ranges_stream": [
        validate_R02_ticker,
        validate_R03_spot_positive,
        validate_R04_spot_nan_inf,
    ],
}


# ============================================================
# DataValidator: classe principale
# ============================================================

class DataValidator:
    """Gatekeeper: valida record prima della scrittura in DuckDB.

    Esegue le 18 regole (R01-R18) e quarantina i record rifiutati.
    Thread-safe: nessuno stato mutabile. Puo essere usato da piu thread.

    Usage:
        validator = DataValidator(db_manager)
        if validator.validate_and_quarantine(record, "gex_summary"):
            db.execute_write("INSERT INTO gex_summary ...", record)
        # altrimenti il record e' gia in quarantena
    """

    def __init__(self, db_manager: Any = None) -> None:
        self.db = db_manager
        self._ensure_quarantine_table()

    def _ensure_quarantine_table(self) -> None:
        """Crea la tabella data_quarantine se non esiste."""
        if self.db is None:
            return
        try:
            self.db.execute_write("""
                CREATE TABLE IF NOT EXISTS data_quarantine (
                    raw_data JSON,
                    failure_reason VARCHAR,
                    rule_code VARCHAR,
                    target_table VARCHAR,
                    source VARCHAR,
                    ticker VARCHAR,
                    ts_record TIMESTAMP,
                    ts_quarantined TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved BOOLEAN DEFAULT FALSE
                )
            """)
        except Exception as e:
            logger.error(f"Failed to create quarantine table: {e}")

    def validate_record(
        self,
        record: dict[str, Any],
        target_table: str,
    ) -> list[ValidationError]:
        """Valida un singolo record contro TUTTE le regole applicabili.

        Args:
            record: Dizionario dati da validare.
            target_table: Nome tabella destinazione (determina quali regole applicare).

        Returns:
            Lista vuota se valido. Lista di ValidationError se rifiutato.
        """
        errors: list[ValidationError] = []

        # 1. Regole universali
        for rule_fn in UNIVERSAL_RULES:
            if rule_fn is validate_R01_required_fields:
                errors.extend(rule_fn(record, target_table))
            else:
                errors.extend(rule_fn(record))

        # 2. Regole specifiche per tabella
        table_specific = TABLE_RULES.get(target_table, [])
        for rule_fn in table_specific:
            errors.extend(rule_fn(record))

        return errors

    def validate_batch(
        self,
        records: pd.DataFrame | list[dict[str, Any]],
        target_table: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Valida un batch di record (DataFrame o list[dict]).

        Versione vettorizzata per performance su grandi volumi.
        Applica le regole piu critiche a livello di colonna.
        Accetta sia pd.DataFrame che list[dict] (conversione automatica).

        Args:
            records: DataFrame o lista di dizionari da validare.
            target_table: Nome tabella destinazione.

        Returns:
            Tupla (valid_df, rejected_df). rejected_df ha colonna extra 'rejection_reason'.
        """
        # Fix 1: accetta sia DataFrame che list[dict]
        if isinstance(records, list):
            if len(records) == 0:
                return pd.DataFrame(), pd.DataFrame()
            df = pd.DataFrame(records)
        else:
            df = records

        if df.empty:
            return df, pd.DataFrame()

        mask_valid = pd.Series(True, index=df.index)
        reasons = pd.Series("", index=df.index)

        # R02: ticker valido
        if "ticker" in df.columns:
            bad_ticker = ~df["ticker"].isin(VALID_TICKERS)
            mask_valid &= ~bad_ticker
            reasons = reasons.where(~bad_ticker, reasons + "R02:ticker_invalido; ")

        # R03 + R04: spot > 0, non NaN/Inf/None
        # Fix 2: esplicito notna() + fillna() per gestire None che in Python puro
        # non fallisce su <= 0 ma in Pandas diventa NaN
        if "spot" in df.columns:
            spot = pd.to_numeric(df["spot"], errors="coerce")
            spot_present = spot.notna()
            bad_spot = ~spot_present | (spot.fillna(0) <= 0) | (spot.fillna(0) >= MAX_SPOT)
            mask_valid &= ~bad_spot
            reasons = reasons.where(~bad_spot, reasons + "R03/R04:spot_invalido; ")

        # R05 + R06: price > 0, non NaN/Inf/None (trades_live)
        if "price" in df.columns:
            price = pd.to_numeric(df["price"], errors="coerce")
            price_present = price.notna()
            bad_price = ~price_present | (price.fillna(0) <= 0) | (price.fillna(0) >= MAX_PRICE)
            mask_valid &= ~bad_price
            reasons = reasons.where(~bad_price, reasons + "R05/R06:price_invalido; ")

        # R07: size >= 1
        if "size" in df.columns:
            size = pd.to_numeric(df["size"], errors="coerce")
            bad_size = (size < MIN_SIZE) | size.isna()
            mask_valid &= ~bad_size
            reasons = reasons.where(~bad_size, reasons + "R07:size_invalido; ")

        # R08: side valido
        if "side" in df.columns:
            bad_side = ~df["side"].isin(VALID_SIDES)
            mask_valid &= ~bad_side
            reasons = reasons.where(~bad_side, reasons + "R08:side_invalido; ")

        # R09 + R10: timestamp range
        ts_col = "ts_utc" if "ts_utc" in df.columns else "ts_event" if "ts_event" in df.columns else None
        if ts_col is not None:
            ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
            now_utc = pd.Timestamp.now(tz="UTC")
            bad_ts = ts.isna() | (ts > now_utc + FUTURE_TOLERANCE) | (ts < pd.Timestamp(MIN_DATE))
            mask_valid &= ~bad_ts
            reasons = reasons.where(~bad_ts, reasons + "R09/R10:timestamp_invalido; ")

        # R12: source valido
        if "source" in df.columns:
            bad_source = ~df["source"].isin(VALID_SOURCES)
            mask_valid &= ~bad_source
            reasons = reasons.where(~bad_source, reasons + "R12:source_invalido; ")

        valid_df = df[mask_valid].copy()
        rejected_df = df[~mask_valid].copy()
        if not rejected_df.empty:
            rejected_df["rejection_reason"] = reasons[~mask_valid]
            logger.warning(f"Batch validation: {len(rejected_df)}/{len(df)} records rejected for {target_table}")

        return valid_df, rejected_df

    def quarantine_record(
        self,
        record: dict[str, Any],
        errors: list[ValidationError],
        target_table: str,
    ) -> None:
        """Scrive un record rifiutato in data_quarantine.

        NON solleva eccezioni. Se la quarantena fallisce, logga e basta.
        """
        if self.db is None:
            logger.warning(f"No DB for quarantine: {[str(e) for e in errors]}")
            return

        try:
            failure_reasons = "; ".join(str(e) for e in errors)
            rule_codes = ",".join(sorted(set(e.rule_code for e in errors)))
            ts_record = record.get("ts_utc") or record.get("ts_event")

            self.db.execute_write(
                """INSERT INTO data_quarantine
                   (raw_data, failure_reason, rule_code, target_table, source, ticker, ts_record)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [
                    json.dumps(record, default=str),
                    failure_reasons,
                    rule_codes,
                    target_table,
                    record.get("source", "unknown"),
                    record.get("ticker", "unknown"),
                    ts_record,
                ],
            )
        except Exception as e:
            logger.error(f"Failed to quarantine record: {e}")

    def quarantine_batch(
        self,
        rejected_df: pd.DataFrame,
        target_table: str,
    ) -> None:
        """Scrive un batch di record rifiutati in quarantena."""
        if self.db is None or rejected_df.empty:
            return

        for _, row in rejected_df.iterrows():
            record = row.to_dict()
            reason = record.pop("rejection_reason", "unknown")
            errors = [ValidationError("BATCH", reason, "batch", None)]
            self.quarantine_record(record, errors, target_table)

    def validate_and_quarantine(
        self,
        record: dict[str, Any],
        target_table: str,
    ) -> bool:
        """Valida un record. Se fallisce, lo mette in quarantena.

        Returns:
            True se valido e pronto per INSERT.
            False se rifiutato (gia quarantinato).
        """
        errors = self.validate_record(record, target_table)
        if errors:
            logger.warning(
                f"Record rifiutato per {target_table}: "
                f"{[e.rule_code for e in errors]} - {errors[0]}"
            )
            self.quarantine_record(record, errors, target_table)
            return False
        return True

    def validate_batch_and_quarantine(
        self,
        records: pd.DataFrame | list[dict[str, Any]],
        target_table: str,
    ) -> pd.DataFrame:
        """Valida un batch. Quarantina i rifiutati. Ritorna solo i validi.

        Accetta sia pd.DataFrame che list[dict].

        Usage:
            clean_df = validator.validate_batch_and_quarantine(raw_df, "trades_live")
            clean_df = validator.validate_batch_and_quarantine(list_of_dicts, "trades_live")
            # clean_df contiene solo record validi, pronti per INSERT
        """
        valid_df, rejected_df = self.validate_batch(records, target_table)
        if not rejected_df.empty:
            self.quarantine_batch(rejected_df, target_table)
        return valid_df
