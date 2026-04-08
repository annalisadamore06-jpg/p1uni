"""
P1-Lite Adapter - Riceve ranges e spot a 1Hz + snapshot MR/OR

Eredita da BaseAdapter. Implementa connect(), listen(), disconnect().
Batch writing, validazione, quarantena e reconnect gestiti da BaseAdapter.

NATURA IBRIDA DI P1-LITE:
  1. RANGE LIVE (ogni secondo): spot, high, low, VWAP, volume
     -> Tabella: intraday_ranges_stream
  2. SNAPSHOT MR/OR (eventi discreti): freeze dei range giornalieri
     -> Tabella: intraday_ranges_stream (con is_frozen=True)
     -> Anche: daily_ranges_snapshot (per Signal Engine / Level Validator)

FUSO ORARIO (CRUCIALE):
  P1-Lite usa timezone Europa/Zurigo (CET=UTC+1, CEST=UTC+2).
  I timestamp "naive" (senza timezone) DEVONO essere interpretati come Zurigo.
  MAI assumere UTC per P1-Lite!
  Il UniversalNormalizer con source='P1_LITE' gestisce questo automaticamente.

CONFIGURAZIONE (settings.yaml):
  p1lite:
    enabled: true
    url: "http://p1lite.local:8080/stream"  # o ws://
    frequency_hz: 1
    timezone: "Europe/Zurich"
    ticker: "ES"
    snapshot_events: ["morning_range_freeze", "overnight_range_freeze"]
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone, date
from typing import Any

from src.core.database import DatabaseManager
from src.ingestion.base_adapter import BaseAdapter

logger = logging.getLogger("p1uni.ingestion.p1")

# Eventi snapshot riconosciuti
DEFAULT_SNAPSHOT_EVENTS = {"morning_range_freeze", "overnight_range_freeze",
                           "mr_freeze", "or_freeze", "session_close"}


class P1LiteAdapter(BaseAdapter):
    """Adapter per feed P1-Lite: range live (1Hz) + snapshot MR/OR.

    Eredita da BaseAdapter:
    - Queue thread-safe + batch writer
    - Auto-reconnect con backoff esponenziale
    - Validazione + quarantena automatica
    - Metriche per monitoring

    LOGICA SPECIALE:
    - Distingue tra dati "live" (range ogni secondo) e "snapshot" (freeze MR/OR)
    - Gli snapshot vengono scritti anche in daily_ranges_snapshot
    - Tracking sessione: running high/low per la giornata
    """

    def __init__(
        self,
        config: dict[str, Any],
        db: DatabaseManager,
        telegram: Any = None,
    ) -> None:
        p1_cfg = config.get("p1lite", {})

        super().__init__(
            adapter_name="p1lite",
            source_type="P1_LITE",
            target_table="intraday_ranges_stream",
            config=config,
            db=db,
            telegram=telegram,
        )

        # Config
        self.p1_url: str = p1_cfg.get("url", "")
        self.frequency_hz: int = p1_cfg.get("frequency_hz", 1)
        self.ticker: str = p1_cfg.get("ticker", "ES")
        self.enabled: bool = p1_cfg.get("enabled", True)
        self.snapshot_events: set[str] = set(
            p1_cfg.get("snapshot_events", DEFAULT_SNAPSHOT_EVENTS)
        )

        # Stato sessione corrente
        self._session_date: date | None = None
        self._session_high: float = 0.0
        self._session_low: float = float("inf")
        self._session_vwap_sum: float = 0.0
        self._session_volume: int = 0
        self._session_tick_count: int = 0

        # Connessione
        self._conn: Any = None  # requests.Session o websocket

        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Crea le tabelle se non esistono."""
        try:
            self.db.execute_write("""
                CREATE TABLE IF NOT EXISTS intraday_ranges_stream (
                    ts_utc TIMESTAMP,
                    session_date DATE,
                    ticker VARCHAR DEFAULT 'ES',
                    source VARCHAR DEFAULT 'P1_LITE',
                    spot DOUBLE,
                    running_high DOUBLE,
                    running_low DOUBLE,
                    running_vwap DOUBLE,
                    volume_cumulative BIGINT,
                    mr1d DOUBLE,
                    mr1u DOUBLE,
                    or1d DOUBLE,
                    or1u DOUBLE,
                    is_frozen BOOLEAN DEFAULT FALSE
                )
            """)
            self.db.execute_write("""
                CREATE TABLE IF NOT EXISTS daily_ranges_snapshot (
                    session_date DATE,
                    ticker VARCHAR DEFAULT 'ES',
                    source VARCHAR DEFAULT 'P1_LITE',
                    mr1d DOUBLE,
                    mr1u DOUBLE,
                    mr2d DOUBLE,
                    mr2u DOUBLE,
                    or1d DOUBLE,
                    or1u DOUBLE,
                    or2d DOUBLE,
                    or2u DOUBLE,
                    vwap DOUBLE,
                    session_high DOUBLE,
                    session_low DOUBLE,
                    close_price DOUBLE,
                    is_final BOOLEAN DEFAULT FALSE,
                    frozen_at TIMESTAMP
                )
            """)
        except Exception as e:
            self._log.error(f"Failed to create P1-Lite tables: {e}")

    # ============================================================
    # Metodi astratti implementati
    # ============================================================

    def connect(self) -> None:
        """Stabilisce la connessione a P1-Lite.

        P1-Lite puo essere:
        - HTTP long-polling (requests.Session con stream=True)
        - WebSocket
        - REST polling ogni 1s
        L'implementazione dipende dall'interfaccia di P1-Lite.
        """
        if not self.enabled:
            raise ConnectionError("P1-Lite adapter disabled in config")

        if not self.p1_url:
            raise ConnectionError("p1lite.url non configurato in settings.yaml")

        self._log.info(f"Connecting to P1-Lite: {self.p1_url}")

        # Detect tipo connessione dall'URL
        if self.p1_url.startswith("ws://") or self.p1_url.startswith("wss://"):
            self._connect_websocket()
        else:
            self._connect_http()

        self._log.info("P1-Lite connected")

    def _connect_websocket(self) -> None:
        """Connessione WebSocket a P1-Lite."""
        import websocket
        self._conn = websocket.WebSocket()
        self._conn.settimeout(30)
        self._conn.connect(self.p1_url)
        self._conn_type = "ws"

    def _connect_http(self) -> None:
        """Connessione HTTP a P1-Lite (polling o SSE)."""
        import requests
        self._conn = requests.Session()
        self._conn_type = "http"

    def listen(self) -> None:
        """Loop di ricezione dati P1-Lite.

        Distingue automaticamente tra WebSocket (stream continuo)
        e HTTP (polling ogni frequency_hz).
        """
        if hasattr(self, "_conn_type") and self._conn_type == "ws":
            self._listen_ws()
        else:
            self._listen_http_poll()

    def _listen_ws(self) -> None:
        """Listen via WebSocket."""
        while self._running and not self._shutdown_event.is_set():
            try:
                raw_message = self._conn.recv()
                if raw_message:
                    self._handle_p1_message(raw_message)
            except Exception as e:
                if not self._running:
                    break
                raise ConnectionError(f"P1-Lite WS error: {e}") from e

    def _listen_http_poll(self) -> None:
        """Listen via HTTP polling."""
        poll_interval = 1.0 / self.frequency_hz

        while self._running and not self._shutdown_event.is_set():
            try:
                response = self._conn.get(self.p1_url, timeout=10)
                if response.status_code == 200:
                    self._handle_p1_message(response.text)
                else:
                    self._log.warning(f"P1-Lite HTTP {response.status_code}")
            except Exception as e:
                if not self._running:
                    break
                raise ConnectionError(f"P1-Lite HTTP error: {e}") from e

            self._shutdown_event.wait(poll_interval)

    def disconnect(self) -> None:
        """Chiude la connessione P1-Lite."""
        if self._conn is not None:
            try:
                if hasattr(self._conn, "close"):
                    self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ============================================================
    # Parsing messaggi P1-Lite
    # ============================================================

    def _handle_p1_message(self, raw_message: str) -> None:
        """Parsa e dispatcha un messaggio P1-Lite."""
        try:
            msg = json.loads(raw_message)
        except json.JSONDecodeError as e:
            self._log.warning(f"Malformed JSON from P1-Lite: {e}")
            return

        if not isinstance(msg, dict):
            return

        # Identifica tipo: LIVE o SNAPSHOT?
        is_snapshot = self._is_snapshot_event(msg)

        # Estrai e arricchisci payload
        record = self._parse_p1_payload(msg, is_snapshot)
        if record is None:
            return

        # Aggiorna stato sessione (running high/low)
        self._update_session_state(record)

        # Invia al batch writer (-> intraday_ranges_stream)
        self.process_message(record)

        # Se snapshot: scrivi anche in daily_ranges_snapshot
        if is_snapshot:
            self._handle_snapshot_event(record, msg)

    def _is_snapshot_event(self, msg: dict[str, Any]) -> bool:
        """Determina se il messaggio e' uno snapshot (freeze MR/OR).

        Riconosce:
        - Campo "is_frozen": true
        - Campo "event" in snapshot_events configurati
        - Campo "type" = "snapshot" o "freeze"
        """
        # Check esplicito is_frozen
        if msg.get("is_frozen") is True:
            return True

        # Check event type
        event = (msg.get("event") or msg.get("type") or "").lower()
        if event in self.snapshot_events:
            return True

        # Check campo frozen generico
        if msg.get("frozen") is True:
            return True

        return False

    def _parse_p1_payload(
        self,
        msg: dict[str, Any],
        is_snapshot: bool,
    ) -> dict[str, Any] | None:
        """Estrae campi rilevanti e prepara il record per il normalizer.

        Il record viene arricchito con:
        - ticker: forzato dal config (default "ES"), normalizzato
        - source: "P1_LITE"
        - is_frozen: True se snapshot
        - running_high/low: dal tracking sessione
        """
        # Estrai payload (potrebbe essere nested in "data")
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return None

        # Estrai spot/prezzo
        spot_raw = (
            data.get("spot") or data.get("price") or
            data.get("last_price") or data.get("close")
        )
        if spot_raw is None:
            self._log.debug("P1-Lite message without spot/price, skipping")
            return None

        # Timestamp: cerchiamo in vari campi
        ts_raw = (
            data.get("timestamp") or data.get("ts") or
            data.get("time") or data.get("datetime") or
            msg.get("timestamp") or msg.get("ts")
        )

        # Ticker: dal messaggio o forzato da config
        ticker_raw = data.get("ticker") or data.get("symbol") or msg.get("ticker") or self.ticker

        # Costruisci record grezzo per il normalizer
        record: dict[str, Any] = {
            "timestamp": ts_raw,
            "ticker": ticker_raw,
            "spot": spot_raw,
            "running_high": data.get("high") or data.get("running_high"),
            "running_low": data.get("low") or data.get("running_low"),
            "running_vwap": data.get("vwap") or data.get("running_vwap"),
            "volume_cumulative": data.get("volume") or data.get("volume_cumulative"),
            "mr1d": data.get("mr1d"),
            "mr1u": data.get("mr1u"),
            "or1d": data.get("or1d"),
            "or1u": data.get("or1u"),
            "is_frozen": is_snapshot,
        }

        # Campi extra per snapshot
        if is_snapshot:
            record["mr2d"] = data.get("mr2d")
            record["mr2u"] = data.get("mr2u")
            record["or2d"] = data.get("or2d")
            record["or2u"] = data.get("or2u")

        return record

    # ============================================================
    # Stato sessione (running high/low/VWAP)
    # ============================================================

    def _update_session_state(self, record: dict[str, Any]) -> None:
        """Aggiorna il tracking della sessione corrente.

        Traccia running high/low per la giornata. Reset a nuova sessione.
        """
        # Normalizza il timestamp per determinare la data sessione
        ts = self.normalizer.normalize(record, "P1_LITE").get("ts_utc")
        if ts is None:
            return

        session_date = ts.date()

        # Nuova sessione?
        if self._session_date != session_date:
            if self._session_date is not None:
                self._log.info(
                    f"Session {self._session_date} ended: "
                    f"H={self._session_high:.2f} L={self._session_low:.2f} "
                    f"Ticks={self._session_tick_count}"
                )
            self._session_date = session_date
            self._session_high = 0.0
            self._session_low = float("inf")
            self._session_vwap_sum = 0.0
            self._session_volume = 0
            self._session_tick_count = 0

        # Aggiorna high/low
        try:
            spot = float(record.get("spot", 0))
            if spot > 0:
                self._session_high = max(self._session_high, spot)
                self._session_low = min(self._session_low, spot)
                self._session_tick_count += 1

                # Arricchisci il record con running high/low se mancanti
                if record.get("running_high") is None:
                    record["running_high"] = self._session_high
                if record.get("running_low") is None:
                    record["running_low"] = self._session_low
        except (ValueError, TypeError):
            pass

    # ============================================================
    # Snapshot events -> daily_ranges_snapshot
    # ============================================================

    def _handle_snapshot_event(self, record: dict[str, Any], raw_msg: dict[str, Any]) -> None:
        """Gestisce un evento di congelamento MR/OR.

        Scrive in daily_ranges_snapshot per il Signal Engine / Level Validator.
        Invia alert Telegram opzionale.
        """
        event_type = (raw_msg.get("event") or raw_msg.get("type") or "snapshot").lower()

        self._log.info(
            f"SNAPSHOT EVENT: {event_type} | "
            f"MR=[{record.get('mr1d')}, {record.get('mr1u')}] "
            f"OR=[{record.get('or1d')}, {record.get('or1u')}] "
            f"Spot={record.get('spot')}"
        )

        # Scrivi in daily_ranges_snapshot
        try:
            # Normalizza per ottenere timestamp UTC
            normalized = self.normalizer.normalize(record, "P1_LITE")
            session_date = normalized.get("session_date") or (
                normalized["ts_utc"].date() if normalized.get("ts_utc") else None
            )

            if session_date is None:
                self._log.warning("Snapshot without session_date, skipping DB write")
                return

            self.db.execute_write(
                """INSERT INTO daily_ranges_snapshot
                   (session_date, ticker, source, mr1d, mr1u, mr2d, mr2u,
                    or1d, or1u, or2d, or2u, vwap, session_high, session_low,
                    frozen_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    session_date,
                    "ES",
                    "P1_LITE",
                    normalized.get("mr1d"),
                    normalized.get("mr1u"),
                    normalized.get("mr2d"),
                    normalized.get("mr2u"),
                    normalized.get("or1d"),
                    normalized.get("or1u"),
                    normalized.get("or2d"),
                    normalized.get("or2u"),
                    normalized.get("running_vwap"),
                    self._session_high if self._session_high > 0 else None,
                    self._session_low if self._session_low < float("inf") else None,
                    normalized.get("ts_utc"),
                ],
            )

            self._log.info(f"Snapshot written to daily_ranges_snapshot for {session_date}")

        except Exception as e:
            self._log.error(f"Failed to write snapshot: {e}")

        # Alert Telegram
        self._send_alert(
            f"MR/OR Frozen ({event_type}): "
            f"MR=[{record.get('mr1d')}, {record.get('mr1u')}] "
            f"OR=[{record.get('or1d')}, {record.get('or1u')}]",
            level="INFO",
        )

    # ============================================================
    # Override stats
    # ============================================================

    def get_stats(self) -> dict[str, Any]:
        """Estende stats con info sessione P1-Lite."""
        stats = super().get_stats()
        stats["session_date"] = str(self._session_date) if self._session_date else None
        stats["session_high"] = self._session_high if self._session_high > 0 else None
        stats["session_low"] = self._session_low if self._session_low < float("inf") else None
        stats["session_ticks"] = self._session_tick_count
        return stats
