"""
Databento Adapter - Riceve trades ES in tempo reale via Databento Live API

Eredita da BaseAdapter. Implementa connect(), listen(), disconnect().
Batch writing, validazione, quarantena e reconnect gestiti da BaseAdapter.

PECULIARITA DATABENTO:
  1. TIMESTAMP: nanosecondi UNIX epoch (int64, es: 1712345678123456789)
     -> dividere per 1e9 -> datetime UTC
     Attenzione: anche millisecondi possibili (> 1e12 ma < 1e15)
  2. PREZZI: fixed-point integer (es: 502325000000 per 5023.25)
     -> se > 1e10: dividere per 1e9
  3. SIDE/AGGRESSOR: campo numerico o stringa
     -> 1 / "B" / "BUY" = buyer aggressor
     -> 2 / "A" / "ASK" / "SELL" = seller aggressor
  4. SIMBOLI: ESH6, ESM6, NQU5, etc. (con suffisso mese/anno)
     -> estrarre radice "ES", "NQ" e normalizzare
  5. SALE CONDITION: Databento trade_condition/flags
     -> '0' = Regular, 'Z' = Late, etc.

API:
  import databento as db
  client = db.Live(key=api_key)     # NON db.set_api_key() !!!
  client.subscribe(dataset="GLBX.MDP3", schema="trades", symbols=["ES.c.0"])
  for record in client:
      process(record)

CONFIGURAZIONE (settings.yaml):
  databento:
    api_key: "${DATABENTO_API_KEY}"
    symbol: "ES.c.0"           # continuous front-month
    dataset: "GLBX.MDP3"
    schema: "trades"
    flush_interval_sec: 300
    buffer_size: 200000
    filter_irregular: false    # se true, scarta trade non-regular
"""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

from src.core.database import DatabaseManager
from src.ingestion.base_adapter import BaseAdapter

logger = logging.getLogger("p1uni.ingestion.databento")

# ============================================================
# Symbol mapping: suffisso mese/anno futures -> ticker radice
# ============================================================
# Mesi CME: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
#            N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
# Pattern: ES + mese(1 char) + anno(1-2 digits) -> "ES"
_FUTURES_SYMBOL_RE = re.compile(
    r"^(ES|NQ|RTY|YM|CL|GC|SI|ZB|ZN|ZF)"  # radice
    r"[FGHJKMNQUVXZ]"                       # mese
    r"\d{1,2}$"                              # anno (1 o 2 cifre)
)

# Mappa radice futures -> ticker Gold Standard
_FUTURES_ROOT_MAP: dict[str, str] = {
    "ES": "ES",
    "NQ": "NQ",
    "RTY": "IWM",  # Russell 2000 futures -> IWM ETF
    "YM": "SPY",   # Dow futures (approssimazione)
    "GC": "GLD",   # Gold futures -> GLD ETF
}

# Sale conditions (Databento trade_condition codes)
_REGULAR_CONDITIONS: set[str] = {"", "0", " ", "regular"}


def _extract_ticker_root(symbol: str) -> str:
    """Estrae la radice del ticker da un simbolo futures Databento.

    Esempi:
        "ESH6"    -> "ES"
        "ESM6"    -> "ES"
        "NQU5"    -> "NQ"
        "ES.c.0"  -> "ES"
        "ES"      -> "ES"
    """
    if not symbol:
        return symbol

    s = symbol.strip()

    # Continuous contract: "ES.c.0" -> "ES"
    if ".c." in s:
        return s.split(".")[0].upper()

    # Futures con suffisso: "ESH6" -> "ES"
    match = _FUTURES_SYMBOL_RE.match(s.upper())
    if match:
        return match.group(1)

    # Gia' pulito o non riconosciuto
    return s.upper()


def _convert_price(raw_price: Any) -> float | None:
    """Converte un prezzo Databento (possibilmente fixed-point) in float.

    Databento puo inviare prezzi come:
    - float normale: 5023.25
    - fixed-point int64: 502325000000 (dividi per 1e9 -> 5023.25)
    - fixed-point int64: 50232500000000 (dividi per 1e9 -> 50232.5, check)

    Regola: se il valore numerico > 1e10, e' fixed-point -> /1e9
    (un prezzo ES non supera mai 100000, quindi > 1e10 e' sicuramente scaled)
    """
    if raw_price is None:
        return None
    try:
        p = float(raw_price)
    except (ValueError, TypeError):
        return None

    if p != p:  # NaN
        return None
    if p == float("inf") or p == float("-inf"):
        return None

    # Fixed-point Databento: prezzo * 1e9
    if p > 1e10:
        p = p / 1e9

    if p <= 0:
        return None

    return p


def _convert_timestamp(raw_ts: Any) -> datetime | None:
    """Converte un timestamp Databento (nanosecondi epoch) in datetime UTC.

    Gestisce:
    - Nanosecondi (> 1e15): /1e9
    - Millisecondi (> 1e12): /1e3
    - Secondi (> 1e9): diretto
    - Gia' datetime: converti a UTC
    """
    if raw_ts is None:
        return None

    if isinstance(raw_ts, datetime):
        if raw_ts.tzinfo is None:
            return raw_ts.replace(tzinfo=timezone.utc)
        return raw_ts.astimezone(timezone.utc)

    try:
        ts_num = float(raw_ts)
    except (ValueError, TypeError):
        return None

    if ts_num != ts_num or ts_num <= 0:  # NaN or negative
        return None

    try:
        if ts_num > 1e15:
            # Nanosecondi
            return datetime.fromtimestamp(ts_num / 1e9, tz=timezone.utc)
        elif ts_num > 1e12:
            # Millisecondi
            return datetime.fromtimestamp(ts_num / 1e3, tz=timezone.utc)
        elif ts_num > 1e9:
            # Secondi
            return datetime.fromtimestamp(ts_num, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None

    return None


def _convert_side(raw_side: Any) -> str:
    """Converte il campo side/aggressor Databento in 'B' o 'A'.

    Databento TradeMsg.side e' un enum Side con:
    - Side.BID (value="B") = buyer aggressor (hit the ask)
    - Side.ASK (value="A") = seller aggressor (hit the bid)
    - Side.NONE (value="N") = unknown

    Supporta anche: int (1=buy, 2=sell), str ("B"/"A")
    """
    # Enum con .value (Databento Side enum)
    if hasattr(raw_side, "value"):
        val = str(raw_side.value).strip().upper()
        if val in ("B", "BID"):
            return "B"
        return "A"

    if isinstance(raw_side, int):
        return "B" if raw_side == 1 else "A"

    if isinstance(raw_side, str):
        s = raw_side.strip().upper()
        if s in ("B", "BUY", "BUYER", "1", "BID"):
            return "B"
        return "A"

    # Default conservativo: seller
    return "A"


class DatabentoAdapter(BaseAdapter):
    """Adapter per feed Databento trades live.

    Eredita da BaseAdapter:
    - Queue thread-safe + batch writer
    - Auto-reconnect con backoff esponenziale
    - Validazione + quarantena automatica
    - Metriche per monitoring
    """

    def __init__(
        self,
        config: dict[str, Any],
        db: DatabaseManager,
        telegram: Any = None,
    ) -> None:
        db_cfg = config.get("databento", {})

        super().__init__(
            adapter_name="databento",
            source_type="DATABENTO",
            target_table="trades_live",
            config=config,
            db=db,
            telegram=telegram,
        )

        # Config
        self.api_key: str = db_cfg.get("api_key", "")
        self.symbol: str = db_cfg.get("symbol", "ES.c.0")
        self.dataset: str = db_cfg.get("dataset", "GLBX.MDP3")
        self.schema: str = db_cfg.get("schema", "trades")
        self.filter_irregular: bool = db_cfg.get("filter_irregular", False)

        # Stato
        self._client: Any = None

        # B2: Lock per serializzare _write_to_db (stop() e _batch_writer_loop
        # girano su thread diversi e condividono la stessa connessione DuckDB)
        self._write_lock = threading.Lock()

        self._ensure_table()

    def _ensure_table(self) -> None:
        """Crea la tabella trades_live e indici se non esistono."""
        # Step 1: crea tabella
        try:
            self.db.execute_write("""
                CREATE TABLE IF NOT EXISTS trades_live (
                    ts_event TIMESTAMP,
                    ticker VARCHAR DEFAULT 'ES',
                    price DOUBLE,
                    size INTEGER,
                    side VARCHAR,
                    flags INTEGER DEFAULT 0,
                    sale_condition VARCHAR DEFAULT '',
                    source_type VARCHAR DEFAULT 'DATABENTO_STD',
                    freq_type VARCHAR DEFAULT 'TICK',
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception as e:
            self._log.error(f"Failed to create trades_live table: {e}")
            return

        # Step 2: indice temporale (non-unique, sempre sicuro)
        try:
            self.db.execute_write(
                "CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades_live(ts_event DESC)"
            )
        except Exception:
            pass  # indice gia' esistente

        # Step 3: indice UNIQUE per deduplicazione
        # Se esistono duplicati (da run precedenti senza indice), la creazione
        # fallisce silenziosamente — non e' critico per il funzionamento.
        try:
            self.db.execute_write(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_unique "
                "ON trades_live(ts_event, ticker, price, size, side)"
            )
        except Exception as e:
            # Duplicati presenti in dati storici — non blocca l'operativita'
            self._log.debug(f"UNIQUE index on trades_live skipped (duplicates exist): {e}")

    # ============================================================
    # Metodi astratti implementati
    # ============================================================

    def connect(self) -> None:
        """Connette a Databento Live API.

        IMPORTANTE: chiudere il client precedente prima di riconnettersi
        per evitare di saturare il connection limit di Databento.
        """
        import databento as db

        if not self.api_key:
            raise ConnectionError("databento.api_key non configurato")

        # Chiudi client precedente se esiste (evita connection limit)
        if self._client is not None:
            try:
                self._client.stop()
            except Exception:
                pass
            self._client = None
            time.sleep(2)  # attendi che la connessione si chiuda sul server

        self._log.info(
            f"Connecting to Databento: dataset={self.dataset}, "
            f"symbol={self.symbol}, schema={self.schema}"
        )

        self._client = db.Live(key=self.api_key)

        # stype_in: "continuous" per ES.c.0, "raw_symbol" per ESM5
        stype = "continuous" if ".c." in self.symbol else "raw_symbol"
        self._client.subscribe(
            dataset=self.dataset,
            schema=self.schema,
            symbols=[self.symbol],
            stype_in=stype,
        )
        self._log.info("Databento connected and subscribed")

    def listen(self) -> None:
        """Loop di ricezione trades da Databento.

        Databento client e' un iteratore: `for record in client`
        """
        if self._client is None:
            raise ConnectionError("Databento client not initialized")

        for record in self._client:
            if not self._running or self._shutdown_event.is_set():
                break

            # Filtra: solo TradeMsg (ignora SystemMsg, SymbolMappingMsg, etc.)
            rtype = type(record).__name__
            if rtype != "TradeMsg":
                continue

            parsed = self._parse_trade(record)
            if parsed is not None:
                self.process_message(parsed)

    def disconnect(self) -> None:
        """Chiude la connessione Databento.

        IMPORTANTE: chiama stop() per rilasciare il slot di connessione sul
        server Databento. Senza questo, il vecchio slot rimane occupato e il
        reconnect successivo riceve 'User has reached their open connection limit'.
        """
        if self._client is not None:
            try:
                self._client.stop()
            except Exception:
                pass
            self._client = None
            time.sleep(3)  # Attendi cleanup lato server Databento

    # ============================================================
    # Parsing trade Databento
    # ============================================================

    def _parse_trade(self, record: Any) -> dict[str, Any] | None:
        """Converte un record Databento in dict Gold Standard per il normalizer.

        Il record Databento ha attributi:
        - ts_event: int64 nanosecondi
        - price: int64 fixed-point o float
        - size: int
        - side: int (1=buyer, 2=seller)
        - flags: int (bitfield)
        - instrument_id: int
        - action: str (per MBO)
        - symbol: str (opzionale, dipende da subscription)
        """
        try:
            # Estrai attributi dal record Databento
            ts_raw = getattr(record, "ts_event", None)
            price_raw = getattr(record, "price", None)
            size_raw = getattr(record, "size", None)
            side_raw = getattr(record, "side", None)
            flags = getattr(record, "flags", 0)

            # Converti timestamp: nanosec -> UTC datetime
            timestamp_utc = _convert_timestamp(ts_raw)
            if timestamp_utc is None:
                self._log.debug(f"Invalid timestamp: {ts_raw}")
                return None

            # Converti prezzo: fixed-point -> float
            price_float = _convert_price(price_raw)
            if price_float is None:
                self._log.debug(f"Invalid price: {price_raw}")
                return None

            # Size: deve essere >= 1
            try:
                size = int(size_raw) if size_raw is not None else 0
            except (ValueError, TypeError):
                size = 0
            if size < 1:
                return None

            # Side: int/str -> 'B'/'A'
            side = _convert_side(side_raw)

            # Symbol -> Ticker Gold Standard
            symbol_raw = getattr(record, "symbol", "") or self.symbol
            ticker_root = _extract_ticker_root(symbol_raw)
            ticker = _FUTURES_ROOT_MAP.get(ticker_root, ticker_root)

            # Sale condition
            condition_raw = getattr(record, "trade_condition", None) or ""
            sale_condition = str(condition_raw).strip()

            # Filtro trade irregolari (opzionale)
            if self.filter_irregular and sale_condition.lower() not in _REGULAR_CONDITIONS:
                self._log.debug(f"Irregular trade filtered: condition={sale_condition}")
                return None

            # Costruisci record Gold Standard
            return {
                "ts_event": timestamp_utc,
                "ticker": ticker,
                "price": price_float,
                "size": size,
                "side": side,
                "flags": int(flags) if flags else 0,
                "sale_condition": sale_condition,
                "source_type": "DATABENTO_STD",
                "freq_type": "TICK",
                # B1: ingested_at esplicito (non affidarsi al DEFAULT)
                "ingested_at": datetime.now(timezone.utc),
            }

        except Exception as e:
            self._log.error(f"Failed to parse Databento trade: {e}")
            return None

    # ============================================================
    # Override _write_to_db per INSERT specifico trades_live
    # ============================================================

    def _write_to_db(self, df: Any) -> None:
        """Scrive il DataFrame in trades_live.

        Override per gestire le colonne specifiche di trades_live
        e aggiungere ingested_at.
        """
        # B1: ingested_at incluso esplicitamente
        expected_cols = [
            "ts_event", "ticker", "price", "size", "side",
            "flags", "sale_condition", "source_type", "freq_type",
            "ingested_at",
        ]
        # Filtra solo colonne presenti
        available = [c for c in expected_cols if c in df.columns]
        write_df = df[available].copy()

        # B2: Lock per evitare accessi concorrenti alla connessione DuckDB
        # (stop() chiama _flush_queue dal main thread, _batch_writer_loop
        # lo chiama dal suo thread -> stesso oggetto conn condiviso)
        with self._write_lock:
            conn = self.db.get_writer()
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.register("_trades_batch", write_df)
                # INSERT semplice senza ON CONFLICT: il UNIQUE INDEX DuckDB
                # richiedeva la sintassi ON CONFLICT(cols) DO NOTHING che
                # fallisce se l'index non esiste (es. dopo duplicati preesistenti).
                # I duplicati reali nei trade live sono rarissimi (timestamp nanosec
                # univoco); il feature builder gestisce dedup via _last_trade_ts.
                conn.execute(
                    f"INSERT INTO trades_live ({', '.join(available)}) "
                    f"SELECT * FROM _trades_batch"
                )
                conn.execute("COMMIT")
                conn.unregister("_trades_batch")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                    conn.unregister("_trades_batch")
                except Exception:
                    pass
                raise

    # ============================================================
    # Stats
    # ============================================================

    def get_stats(self) -> dict[str, Any]:
        """Estende stats con info Databento."""
        stats = super().get_stats()
        stats["symbol"] = self.symbol
        stats["schema"] = self.schema
        stats["dataset"] = self.dataset
        stats["filter_irregular"] = self.filter_irregular
        return stats
