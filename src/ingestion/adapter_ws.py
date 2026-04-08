"""
GexBot WebSocket Adapter - Riceve livelli GEX in tempo reale

Eredita da BaseAdapter. Implementa connect(), listen(), disconnect().
Il batch writing, la validazione, la quarantena e il reconnect
sono gestiti automaticamente da BaseAdapter.

FLUSSO:
  GexBot WS -> connect() -> listen() -> _parse_gex_message() -> process_message()
                                              |
                                    BaseAdapter._queue -> batch writer -> DB

CONFIGURAZIONE (settings.yaml):
  gexbot:
    ws_url: "wss://..."
    tickers: ["ES", "NQ", "SPY"]
    subscribe_action: "subscribe"
    ping_interval_sec: 25
    reconnect_alert_after_sec: 300  # alert critico dopo 5 min di disconnessione

PROTOCOLLO WS GexBot (tipico):
  -> Client invia: {"action": "subscribe", "tickers": ["ES", "NQ"]}
  <- Server invia: {"type": "gex", "ticker": "ES", "data": {...}}
  <- Server invia: {"type": "heartbeat"}
  <- Server invia: {"type": "greeks", "ticker": "ES", "data": {...}}
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.database import DatabaseManager
from src.ingestion.base_adapter import BaseAdapter

logger = logging.getLogger("p1uni.ingestion.ws")

# Tipi di messaggio che NON sono dati di mercato (ignorare o loggare)
_NON_DATA_TYPES = {"heartbeat", "pong", "status", "info", "welcome", "error", "ack"}


class GexBotWebSocketAdapter(BaseAdapter):
    """Adapter WebSocket per GexBot. Riceve livelli GEX e Greeks live.

    Eredita da BaseAdapter:
    - Queue thread-safe + batch writer (ogni 5s o 500 record)
    - Auto-reconnect con backoff esponenziale
    - Validazione + quarantena automatica
    - Metriche per monitoring

    Le sottoclassi non devono gestire nulla di questo.
    """

    def __init__(
        self,
        config: dict[str, Any],
        db: DatabaseManager,
        telegram: Any = None,
    ) -> None:
        gexbot_cfg = config.get("gexbot", {})

        super().__init__(
            adapter_name="gexbot_ws",
            source_type="WS",
            target_table="gex_summary",  # tabella primaria, greeks vanno in greeks_summary
            config=config,
            db=db,
            telegram=telegram,
        )

        # Config WS
        self.ws_url: str = gexbot_cfg.get("ws_url", "")
        self.tickers: list[str] = gexbot_cfg.get("tickers", ["ES"])
        self.subscribe_action: str = gexbot_cfg.get("subscribe_action", "subscribe")
        self.ping_interval: int = gexbot_cfg.get("ping_interval_sec", 25)
        self.reconnect_alert_sec: int = gexbot_cfg.get("reconnect_alert_after_sec", 300)

        # Stato connessione
        self._ws: Any = None  # websocket.WebSocket instance
        self._connected_since: datetime | None = None
        self._disconnected_since: datetime | None = None
        self._critical_alert_sent: bool = False

        # Cache GEX per feature_builder (aggiornamento atomico)
        base_dir = Path(config.get("_base_dir", "."))
        self._gex_cache_path = base_dir / "data" / "cache" / "gex_cache.json"

    # ============================================================
    # Metodi astratti implementati
    # ============================================================

    def connect(self) -> None:
        """Stabilisce la connessione WebSocket e invia subscribe.

        Usa websocket-client (sync) perche' BaseAdapter usa threading, non asyncio.
        """
        import websocket

        if not self.ws_url:
            raise ConnectionError("gexbot.ws_url non configurato in settings.yaml")

        self._log.info(f"Connecting to GexBot WS: {self.ws_url}")

        self._ws = websocket.WebSocket()
        self._ws.settimeout(self.ping_interval + 10)  # timeout > ping interval
        self._ws.connect(self.ws_url)

        # Subscribe ai ticker
        subscribe_msg = json.dumps({
            "action": self.subscribe_action,
            "tickers": self.tickers,
        })
        self._ws.send(subscribe_msg)
        self._log.info(f"Subscribed to tickers: {self.tickers}")

        self._connected_since = datetime.now(timezone.utc)
        self._disconnected_since = None
        self._critical_alert_sent = False

    def listen(self) -> None:
        """Loop di ricezione messaggi WS.

        Per ogni messaggio:
        - Se heartbeat/status: rispondi pong e logga
        - Se dati GEX/Greeks: parsa e passa a process_message()
        - Se errore JSON: logga e continua
        """
        last_ping = time.monotonic()

        while self._running and not self._shutdown_event.is_set():
            try:
                # Ping periodico per tenere viva la connessione
                now = time.monotonic()
                if now - last_ping >= self.ping_interval:
                    self._send_ping()
                    last_ping = now

                # Ricevi messaggio
                raw_message = self._ws.recv()
                if not raw_message:
                    continue

                self._handle_message(raw_message)

            except Exception as e:
                if not self._running:
                    break
                # Qualsiasi errore WS: esci da listen(), BaseAdapter fara' reconnect
                self._disconnected_since = datetime.now(timezone.utc)
                raise ConnectionError(f"WS recv error: {e}") from e

    def disconnect(self) -> None:
        """Chiude la connessione WebSocket."""
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._connected_since = None
        if self._disconnected_since is None:
            self._disconnected_since = datetime.now(timezone.utc)

    # ============================================================
    # Gestione messaggi
    # ============================================================

    def _handle_message(self, raw_message: str) -> None:
        """Parsa e dispatcha un messaggio WS."""
        # Parse JSON
        try:
            msg = json.loads(raw_message)
        except json.JSONDecodeError as e:
            self._log.warning(f"Malformed JSON from WS: {e} | raw={raw_message[:200]}")
            return

        if not isinstance(msg, dict):
            self._log.debug(f"Non-dict message ignored: {type(msg)}")
            return

        # Determina tipo messaggio
        msg_type = (msg.get("type") or msg.get("event") or "").lower()

        # Heartbeat / status: non sono dati
        if msg_type in _NON_DATA_TYPES:
            self._handle_non_data(msg, msg_type)
            return

        # Error dal server
        if msg_type == "error":
            self._log.error(f"GexBot error: {msg.get('message', msg)}")
            return

        # Dati di mercato: parsa e invia a process_message
        parsed = self._parse_gex_message(msg)
        if parsed is not None:
            self.process_message(parsed)

            # Aggiorna cache GEX per feature_builder
            self._update_gex_cache(parsed)

    def _handle_non_data(self, msg: dict[str, Any], msg_type: str) -> None:
        """Gestisce messaggi non-dati (heartbeat, status, etc.)."""
        if msg_type == "heartbeat":
            # Rispondi pong
            self._send_pong()
            self._log.debug("Heartbeat received, pong sent")
        elif msg_type in ("status", "info", "welcome"):
            self._log.info(f"WS {msg_type}: {msg.get('message', '')}")

    def _send_ping(self) -> None:
        """Invia ping per mantenere la connessione viva."""
        if self._ws is not None:
            try:
                self._ws.ping()
            except Exception:
                pass

    def _send_pong(self) -> None:
        """Rispondi a un heartbeat del server."""
        if self._ws is not None:
            try:
                self._ws.send(json.dumps({"action": "pong"}))
            except Exception:
                pass

    # ============================================================
    # Parsing messaggi GEX
    # ============================================================

    def _parse_gex_message(self, msg: dict[str, Any]) -> dict[str, Any] | None:
        """Estrae campi rilevanti da un messaggio GexBot.

        Il formato esatto dipende dal server GexBot. Gestisce 2 formati:

        Formato 1 (flat):
            {"ticker": "ES", "timestamp": "...", "call_gex": 5480, ...}

        Formato 2 (nested):
            {"type": "gex", "ticker": "ES", "data": {"call_gex": 5480, ...}}

        Ritorna un dict grezzo pronto per il UniversalNormalizer (via process_message).
        """
        # Formato nested: estrai "data"
        data = msg.get("data")
        if isinstance(data, dict):
            record = dict(data)  # shallow copy
            # Assicurati che ticker e timestamp siano nel record
            if "ticker" not in record and "ticker" in msg:
                record["ticker"] = msg["ticker"]
            if "timestamp" not in record and "timestamp" in msg:
                record["timestamp"] = msg["timestamp"]
        else:
            # Formato flat: usa il messaggio intero
            record = dict(msg)

        # Rimuovi campi di protocollo che non sono dati
        for key in ("type", "event", "action", "channel"):
            record.pop(key, None)

        # Deve avere almeno un ticker o un prezzo per essere utile
        has_ticker = any(k in record for k in ("ticker", "symbol", "asset", "instrument"))
        has_price = any(k in record for k in ("price", "spot", "last_price", "underlying"))

        if not has_ticker and not has_price:
            self._log.debug(f"Message without ticker/price, skipping: {list(record.keys())}")
            return None

        return record

    # ============================================================
    # Cache GEX per feature_builder
    # ============================================================

    def _update_gex_cache(self, record: dict[str, Any]) -> None:
        """Aggiorna gex_cache.json atomicamente per la pipeline ML.

        Il feature_builder legge questo file per calcolare gex_proximity.
        Scrittura atomica: scrivi su .tmp poi rinomina.
        """
        try:
            cache_data = {k: str(v) if isinstance(v, datetime) else v for k, v in record.items()}
            cache_data["_updated_at"] = datetime.now(timezone.utc).isoformat()
            cache_data["_source"] = "gexbot_ws"

            self._gex_cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._gex_cache_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(cache_data, indent=2, default=str))
            tmp_path.replace(self._gex_cache_path)
        except Exception as e:
            self._log.debug(f"GEX cache update failed: {e}")

    # ============================================================
    # Override: alert critico se disconnesso > 5 min
    # ============================================================

    def get_stats(self) -> dict[str, Any]:
        """Estende stats con info connessione WS."""
        stats = super().get_stats()
        stats["connected_since"] = (
            self._connected_since.isoformat() if self._connected_since else None
        )
        stats["disconnected_since"] = (
            self._disconnected_since.isoformat() if self._disconnected_since else None
        )

        # Alert critico se disconnesso > reconnect_alert_sec
        if self._disconnected_since is not None:
            down_sec = (datetime.now(timezone.utc) - self._disconnected_since).total_seconds()
            stats["downtime_sec"] = round(down_sec, 1)
            if down_sec > self.reconnect_alert_sec and not self._critical_alert_sent:
                self._critical_alert_sent = True
                self._send_alert(
                    f"GexBot WS DOWN da {down_sec/60:.0f} minuti! "
                    f"Livelli GEX NON aggiornati. Verificare connessione.",
                    level="ERROR",
                )

        return stats
