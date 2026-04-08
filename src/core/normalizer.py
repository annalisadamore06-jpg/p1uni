"""
Universal Normalizer - Traduttore universale dati grezzi -> Gold Standard

Prende record da qualsiasi fonte (WebSocket GexBot, P1-Lite, Databento, Storico)
e li trasforma nello schema Gold Standard definito nel DATA_CONTRACT.md.

PRINCIPI:
1. IMMUTABILITA: non modifica mai il record originale, crea sempre una copia nuova.
2. PULIZIA SINTATTICA: converte tipi, rinomina colonne, normalizza timestamp.
3. NON fa validazione logica (prezzo negativo, weekend): quello lo fa il Validator.
4. Ogni campo numerico stringa ("5023.50") viene convertito in float.
5. Stringhe vuote ("") diventano None.
6. Tutti i timestamp in output sono datetime UTC con tzinfo.

Fonte di verita: DATA_CONTRACT.md
"""

from __future__ import annotations

import copy
import logging
import math
from datetime import datetime, timezone
from typing import Any

import pytz

logger = logging.getLogger("p1uni.core.normalizer")

# ============================================================
# TICKER MAP — Input (qualsiasi variante) -> Gold Standard
# ============================================================
# Fonte: DATA_CONTRACT.md sezione 3.1
# I 9 ticker Gold Standard: ES, NQ, SPY, QQQ, VIX, IWM, TLT, GLD, UVXY
TICKER_MAP: dict[str, str] = {
    # ES (E-mini S&P 500)
    "ES": "ES", "ES_SPX": "ES", "ES.c.0": "ES",
    "ESM6": "ES", "ESZ5": "ES", "ESH6": "ES", "ESU5": "ES",
    "ESM5": "ES", "ESZ6": "ES", "ESH7": "ES", "ESU6": "ES",
    # NQ (E-mini Nasdaq)
    "NQ": "NQ", "NQ_NDX": "NQ", "NQ.c.0": "NQ",
    "NQM6": "NQ", "NQZ5": "NQ", "NQH6": "NQ", "NQU5": "NQ",
    # SPY (S&P 500 ETF)
    "SPX": "SPY", "SPY": "SPY", "SPDR": "SPY",
    # QQQ (Nasdaq ETF)
    "NDX": "QQQ", "QQQ": "QQQ", "TQQQ": "QQQ",
    # VIX
    "VIX": "VIX", "^VIX": "VIX", "VX": "VIX",
    # IWM (Russell 2000 ETF)
    "IWM": "IWM", "RUT": "IWM",
    # TLT (Treasury Bond ETF)
    "TLT": "TLT",
    # GLD (Gold ETF)
    "GLD": "GLD", "XAUUSD": "GLD",
    # UVXY (Volatility ETF)
    "UVXY": "UVXY", "VXX": "UVXY",
}

# Set per lookup O(1) nel validator
VALID_TICKERS: set[str] = {"ES", "NQ", "SPY", "QQQ", "VIX", "IWM", "TLT", "GLD", "UVXY"}

# Versione case-insensitive pre-calcolata per performance
_TICKER_MAP_UPPER: dict[str, str] = {k.upper(): v for k, v in TICKER_MAP.items()}


# ============================================================
# COLUMN MAPS — Alias da fonti diverse -> Gold Standard
# ============================================================
# Fonte: DATA_CONTRACT.md sezione 3.2, 3.3, 3.4

# Per gex_summary e campi generici
GEX_COLUMN_MAP: dict[str, str] = {
    # GEX-specifici
    "call_gex": "call_wall_vol",
    "call_gamma_oi": "call_wall_oi",
    "put_gex": "put_wall_vol",
    "put_gamma_oi": "put_wall_oi",
    "gamma_flip": "zero_gamma",
    "gamma_level": "zero_gamma",
    "net_gamma": "net_gex_oi",
    "net_gex": "net_gex_oi",
    "total_gex": "net_gex_vol",
    # Timestamp aliases
    "timestamp": "ts_utc",
    "time": "ts_utc",
    "datetime": "ts_utc",
    "ts": "ts_utc",
    # Price aliases
    "price": "spot",
    "last_price": "spot",
    "underlying": "spot",
    # Ticker aliases
    "symbol": "ticker",
    "asset": "ticker",
    "instrument": "ticker",
    # Misc
    "num_strikes": "n_strikes",
    "strike_count": "n_strikes",
}

# Per greeks_summary
GREEKS_COLUMN_MAP: dict[str, str] = {
    "delta_pos": "major_positive",
    "delta_neg": "major_negative",
    "gamma_long": "major_long_gamma",
    "gamma_short": "major_short_gamma",
    "vanna_pos": "major_positive",
    "charm_pos": "major_positive",
    "contracts": "n_contracts",
    "dte": "min_dte",
    # Eredita anche gli alias generici
    **{k: v for k, v in GEX_COLUMN_MAP.items()
       if v in ("ts_utc", "spot", "ticker")},
}

# Per Databento trades
DATABENTO_COLUMN_MAP: dict[str, str] = {
    "ts_event": "ts_event",   # resta uguale, ma applichiamo conversione nanosec
    "price": "price",
    "size": "size",
    "side": "side",
    "flags": "flags",
}

# Mappa automatica: source_type -> column_map
SOURCE_COLUMN_MAPS: dict[str, dict[str, str]] = {
    "WS": GEX_COLUMN_MAP,
    "hist_rest": GEX_COLUMN_MAP,
    "ws_p1clean": GEX_COLUMN_MAP,
    "P1": GEX_COLUMN_MAP,
    "P1_LITE": GEX_COLUMN_MAP,
    "DATABENTO": DATABENTO_COLUMN_MAP,
    "GREEKS": GREEKS_COLUMN_MAP,
}

# ============================================================
# Timezone constants
# ============================================================
ZURICH_TZ = pytz.timezone("Europe/Zurich")


# ============================================================
# Funzioni atomiche (stateless, pure, testabili)
# ============================================================

def normalize_ticker(raw_ticker: Any) -> str | None:
    """Normalizza un ticker al Gold Standard.

    Cerca prima nel TICKER_MAP esatto, poi case-insensitive.

    Args:
        raw_ticker: Ticker grezzo (str o qualsiasi tipo).

    Returns:
        Ticker normalizzato ('ES', 'SPY', ...) o None se non mappabile.
        None viene gestito dal Validator come record da quarantinare.
    """
    if raw_ticker is None:
        return None
    if not isinstance(raw_ticker, str):
        raw_ticker = str(raw_ticker)

    cleaned = raw_ticker.strip()
    if not cleaned:
        return None

    # Lookup esatto (case-sensitive)
    result = TICKER_MAP.get(cleaned)
    if result is not None:
        return result

    # Lookup case-insensitive (pre-calcolato)
    return _TICKER_MAP_UPPER.get(cleaned.upper())


def normalize_timestamp_utc(ts: Any, source: str = "unknown") -> datetime | None:
    """Converte qualsiasi timestamp in datetime UTC con tzinfo.

    Regole per fonte (DATA_CONTRACT sezione 4):
    - GexBot Storico (hist_rest): gia' UTC ISO8601, parsare direttamente
    - WebSocket Live (WS, ws_p1clean): gia' UTC ISO8601 con ms
    - P1-Lite (P1, P1_LITE): timezone ZURIGO (CET=UTC+1 / CEST=UTC+2)
      -> DEVE essere convertito a UTC via pytz (gestisce automaticamente DST)
    - Databento (DATABENTO): nanosec UNIX epoch (int > 1e15)
      -> dividere per 1e9, convertire a datetime UTC

    Args:
        ts: Timestamp grezzo (str ISO8601, int nanosec, float epoch, datetime).
        source: Tag fonte per determinare la regola di conversione.

    Returns:
        datetime con tzinfo=UTC, o None se non parsabile.
    """
    try:
        if ts is None:
            return None

        # --- Caso 1: gia' datetime ---
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                # Naive datetime: se P1-Lite assume Zurigo, altrimenti assume UTC
                if source in ("P1", "P1_LITE"):
                    return ZURICH_TZ.localize(ts).astimezone(timezone.utc)
                return ts.replace(tzinfo=timezone.utc)
            return ts.astimezone(timezone.utc)

        # --- Caso 2: numerico (epoch) ---
        if isinstance(ts, (int, float)):
            # Scarta valori assurdi (negativi, zero, NaN, Inf)
            if ts <= 0 or math.isnan(ts) or math.isinf(ts):
                return None

            # Databento: nanosec (valore > 1e15, cioe' > anno 2001 in nanosec)
            if ts > 1e15:
                return datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)

            # Millisec epoch (valore > 1e12, cioe' > anno 2001 in millisec)
            if ts > 1e12:
                return datetime.fromtimestamp(ts / 1e3, tz=timezone.utc)

            # Secondi epoch normali (valore > 1e9, cioe' > ~2001)
            if ts > 1e9:
                return datetime.fromtimestamp(ts, tz=timezone.utc)

            # Valore troppo piccolo per essere un epoch valido
            return None

        # --- Caso 3: stringa ISO8601 ---
        if isinstance(ts, str):
            ts_str = ts.strip()
            if not ts_str:
                return None

            # P1-Lite: timestamp in timezone Zurigo (CET/CEST)
            # Esempio: "2026-04-08 16:30:00" (ora locale Zurigo)
            if source in ("P1", "P1_LITE"):
                # Se ha gia' un offset (+XX:XX o Z), parsalo e converti
                if "+" in ts_str or ts_str.endswith("Z"):
                    parsed = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    return parsed.astimezone(timezone.utc)
                # Altrimenti: assume Zurigo, localizza con pytz (gestisce DST)
                naive = datetime.fromisoformat(ts_str)
                localized = ZURICH_TZ.localize(naive)
                return localized.astimezone(timezone.utc)

            # Tutto il resto: assume UTC
            # Gestisci "Z" -> "+00:00" per fromisoformat
            parsed = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

    except (ValueError, TypeError, OSError, OverflowError):
        logger.debug(f"Failed to parse timestamp: {ts!r} (source={source})")
        return None

    return None


def _clean_numeric(value: Any) -> float | None:
    """Converte un valore in float, o None se non numerico.

    Gestisce:
    - float/int: ritorna float
    - str numerica ("5023.50"): converte a float
    - str vuota ("") o whitespace: ritorna None
    - None: ritorna None
    - NaN/Inf: ritorna None (il Validator li rifiuterebbe comunque)
    """
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            f = float(stripped)
        except ValueError:
            return None
    elif isinstance(value, (int, float)):
        f = float(value)
    else:
        return None

    # NaN e Inf -> None (pulizia sintattica, non validazione)
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _clean_integer(value: Any) -> int | None:
    """Converte un valore in int, o None se non numerico."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(float(stripped))  # "10.0" -> 10
        except ValueError:
            return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return int(value)
    if isinstance(value, int):
        return value
    return None


def _clean_string(value: Any) -> str | None:
    """Converte un valore in stringa pulita, o None se vuoto."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _apply_column_map(record: dict[str, Any], column_map: dict[str, str]) -> dict[str, Any]:
    """Rinomina le chiavi di un record secondo la mappa.

    IMMUTABILE: crea un nuovo dizionario, non modifica l'originale.
    Se due chiavi mappano allo stesso Gold Standard, l'ultimo vince (log warning).
    """
    result: dict[str, Any] = {}
    for key, value in record.items():
        new_key = column_map.get(key, key)
        if new_key in result and new_key != key:
            logger.debug(f"Column collision: '{key}' -> '{new_key}' (already set)")
        result[new_key] = value
    return result


# ============================================================
# NORMALIZZATORI PER FONTE
# ============================================================

def _normalize_gex_record(record: dict[str, Any], source: str) -> dict[str, Any]:
    """Normalizza un record GEX (gex_summary) da WS/hist_rest/ws_p1clean.

    Output: dizionario con chiavi Gold Standard per gex_summary.
    """
    # 1. Rinomina colonne
    out = _apply_column_map(record, GEX_COLUMN_MAP)

    # 2. Normalizza ticker
    out["ticker"] = normalize_ticker(out.get("ticker"))

    # 3. Normalizza timestamp
    out["ts_utc"] = normalize_timestamp_utc(out.get("ts_utc"), source=source)

    # 4. Source tag
    out["source"] = source

    # 5. Pulizia numerica: converti stringhe -> float, "" -> None
    for field in ("spot", "zero_gamma", "call_wall_vol", "call_wall_oi",
                  "put_wall_vol", "put_wall_oi", "net_gex_vol", "net_gex_oi",
                  "delta_rr"):
        out[field] = _clean_numeric(out.get(field))

    for field in ("n_strikes", "min_dte", "sec_min_dte"):
        out[field] = _clean_integer(out.get(field))

    # 6. Default per campi categorici se mancanti
    if out.get("hub") is None:
        out["hub"] = "classic"
    if out.get("aggregation") is None:
        out["aggregation"] = "gex_full"

    return out


def _normalize_greeks_record(record: dict[str, Any], source: str) -> dict[str, Any]:
    """Normalizza un record Greeks (greeks_summary)."""
    out = _apply_column_map(record, GREEKS_COLUMN_MAP)

    out["ticker"] = normalize_ticker(out.get("ticker"))
    out["ts_utc"] = normalize_timestamp_utc(out.get("ts_utc"), source=source)
    out["source"] = source

    for field in ("spot", "major_positive", "major_negative",
                  "major_long_gamma", "major_short_gamma"):
        out[field] = _clean_numeric(out.get(field))

    for field in ("n_contracts", "min_dte", "sec_min_dte"):
        out[field] = _clean_integer(out.get(field))

    out["greek_type"] = _clean_string(out.get("greek_type"))
    out["dte_type"] = _clean_string(out.get("dte_type"))

    if out.get("hub") is None:
        out["hub"] = "classic"

    return out


def _normalize_databento_trade(record: dict[str, Any]) -> dict[str, Any]:
    """Normalizza un trade Databento per trades_live.

    Campi Databento specifici:
    - ts_event: nanosec UNIX epoch -> datetime UTC (dividi per 1e9)
    - price: potrebbe essere scaled (> 1e12 -> dividi per 1e9)
    - side: int (1=buyer, altro=seller) -> 'B'/'A'
    """
    out: dict[str, Any] = {}

    # Timestamp: nanosec -> UTC datetime
    out["ts_event"] = normalize_timestamp_utc(record.get("ts_event", 0), source="DATABENTO")

    # Price: gestisci scaling Databento
    price_raw = record.get("price", 0)
    price = _clean_numeric(price_raw)
    if price is not None and price > 1e12:
        # Databento fixed-point: prezzo in decimi di nanodollaro -> dividi per 1e9
        price = price / 1e9
    out["price"] = price

    # Size: deve essere intero >= 1
    out["size"] = _clean_integer(record.get("size", 0))

    # Side: int -> stringa 'B'/'A'
    side_raw = record.get("side", "")
    if isinstance(side_raw, int):
        out["side"] = "B" if side_raw == 1 else "A"
    elif isinstance(side_raw, str):
        s = side_raw.strip().upper()
        out["side"] = "B" if s in ("B", "BUY", "BUYER", "1") else "A"
    else:
        out["side"] = "A"  # default conservativo

    # Flags: intero opzionale
    out["flags"] = _clean_integer(record.get("flags", 0)) or 0

    return out


def _normalize_p1lite_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalizza un record P1-Lite per intraday_ranges_stream.

    CRITICO: P1-Lite usa timezone Zurigo (CET/CEST), DEVE essere convertito a UTC.
    """
    out: dict[str, Any] = {}

    # Timestamp: Zurigo -> UTC
    ts_raw = record.get("timestamp") or record.get("ts_utc") or record.get("ts")
    out["ts_utc"] = normalize_timestamp_utc(ts_raw, source="P1_LITE")

    # Session date: derivata dal timestamp UTC
    if out["ts_utc"] is not None:
        out["session_date"] = out["ts_utc"].date()
    else:
        out["session_date"] = None

    out["ticker"] = "ES"  # P1-Lite e' solo ES
    out["source"] = "P1_LITE"

    # Pulizia numerica
    for field in ("spot", "running_high", "running_low", "running_vwap",
                  "mr1d", "mr1u", "or1d", "or1u"):
        raw_key = field
        # Cerca anche alias (high -> running_high, etc.)
        val = record.get(raw_key) or record.get(field.replace("running_", ""))
        out[field] = _clean_numeric(val)

    out["volume_cumulative"] = _clean_integer(record.get("volume_cumulative") or record.get("volume"))

    return out


def _normalize_orderflow_record(record: dict[str, Any], source: str) -> dict[str, Any]:
    """Normalizza un record orderflow."""
    out = _apply_column_map(record, GEX_COLUMN_MAP)

    out["ticker"] = normalize_ticker(out.get("ticker"))
    out["ts_utc"] = normalize_timestamp_utc(out.get("ts_utc"), source=source)
    out["source"] = source

    # Tutti i campi numerici dell'orderflow
    numeric_fields = [
        "spot", "zero_vanna", "zero_charm", "zero_cvr", "zero_gex_ratio",
        "one_vanna", "one_charm", "one_cvr", "one_gex_ratio",
        "zero_major_long_gamma", "zero_major_short_gamma",
        "one_major_long_gamma", "one_major_short_gamma",
        "zero_major_call", "zero_major_put", "one_major_call", "one_major_put",
        "zero_agg_dex", "one_agg_dex",
        "zero_agg_call_dex", "one_agg_call_dex",
        "zero_agg_put_dex", "one_agg_put_dex",
        "zero_net_dex", "one_net_dex",
        "zero_net_call_dex", "one_net_call_dex",
        "zero_net_put_dex", "one_net_put_dex",
        "dex_flow", "gex_flow", "cvr_flow",
        "one_dex_flow", "one_gex_flow", "one_cvr_flow",
    ]
    for field in numeric_fields:
        out[field] = _clean_numeric(out.get(field))

    return out


# ============================================================
# Dispatch: source_type -> normalizzatore specifico
# ============================================================
_NORMALIZER_DISPATCH: dict[str, str] = {
    "WS": "gex",
    "hist_rest": "gex",
    "ws_p1clean": "gex",
    "P1": "p1lite",
    "P1_LITE": "p1lite",
    "DATABENTO": "databento",
    "GREEKS": "greeks",
    "ORDERFLOW": "orderflow",
}


# ============================================================
# UniversalNormalizer — Classe principale
# ============================================================

class UniversalNormalizer:
    """Traduttore universale: dati grezzi da qualsiasi fonte -> Gold Standard.

    IMMUTABILE: non modifica mai il record originale.
    STATELESS: nessun stato interno, puo essere condiviso tra thread.

    Usage:
        normalizer = UniversalNormalizer()
        clean = normalizer.normalize(raw_record, source_type="WS")
        # clean e' un nuovo dict pronto per il Validator
        clean_batch = normalizer.normalize_batch(raw_list, source_type="DATABENTO")
    """

    def normalize(self, record: dict[str, Any] | None, source_type: str) -> dict[str, Any]:
        """Normalizza un singolo record grezzo.

        IMMUTABILE: crea una deep copy del record prima di modificarlo.
        Il record originale NON viene mai toccato.

        Args:
            record: Dizionario grezzo dalla fonte.
            source_type: Tipo fonte ('WS', 'P1_LITE', 'DATABENTO', 'hist_rest',
                         'ws_p1clean', 'GREEKS', 'ORDERFLOW').

        Returns:
            Nuovo dizionario con chiavi Gold Standard, timestamp UTC,
            tipi puliti (float/int/None), pronto per il Validator.
        """
        # Guard: rifiuta input non-dict
        if record is None or not isinstance(record, dict):
            raise ValueError(f"Invalid record type: {type(record)}. Expected dict.")

        # Deep copy per immutabilita
        raw = copy.deepcopy(record)

        # Dispatch al normalizzatore specifico per fonte
        norm_type = _NORMALIZER_DISPATCH.get(source_type, "gex")

        if norm_type == "databento":
            return _normalize_databento_trade(raw)
        elif norm_type == "p1lite":
            return _normalize_p1lite_record(raw)
        elif norm_type == "greeks":
            return _normalize_greeks_record(raw, source_type)
        elif norm_type == "orderflow":
            return _normalize_orderflow_record(raw, source_type)
        else:
            return _normalize_gex_record(raw, source_type)

    def normalize_batch(
        self,
        records: list[dict[str, Any]],
        source_type: str,
    ) -> list[dict[str, Any]]:
        """Normalizza una lista di record.

        Args:
            records: Lista di dizionari grezzi.
            source_type: Tipo fonte.

        Returns:
            Lista di dizionari normalizzati. Record non normalizzabili
            vengono inclusi con campi None (il Validator li scartera).
        """
        results: list[dict[str, Any]] = []
        for i, record in enumerate(records):
            try:
                normalized = self.normalize(record, source_type)
                results.append(normalized)
            except Exception as e:
                logger.error(f"Failed to normalize record #{i}: {e}")
                # Includi il record originale con source per tracciabilita
                results.append({"_normalization_error": str(e), "source": source_type})
        return results


# ============================================================
# Funzioni legacy compatibili (usate nei moduli esistenti)
# ============================================================
# Mantenute per retrocompatibilita con adapter_ws.py, adapter_databento.py etc.
# I nuovi moduli dovrebbero usare UniversalNormalizer.

def normalize_columns(record: dict[str, Any], column_map: dict[str, str] | None = None) -> dict[str, Any]:
    """Rinomina le colonne di un record. Wrapper legacy.

    IMMUTABILE: ritorna un nuovo dizionario.
    """
    if column_map is None:
        column_map = GEX_COLUMN_MAP
    return _apply_column_map(record, column_map)


def normalize_databento_trade(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalizza un trade Databento. Wrapper legacy."""
    return _normalize_databento_trade(copy.deepcopy(raw))
