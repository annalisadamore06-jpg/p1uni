"""
NinjaTrader Bridge - Comunicazione bidirezionale con NinjaTrader 8 via ZMQ.

ARCHITETTURA:
  Python (Signal Engine)                  NinjaTrader 8 (C#)
         |                                      |
    [REQ socket] ----> tcp://5555 ----> [REP socket]  (comandi)
         |    JSON: {action, qty, sl, tp}        |
         |    <---- ACK/NACK ----                |
         |                                      |
    [SUB socket] <--- tcp://5556 <--- [PUB socket]  (eventi)
         |    JSON: {event, data}                |

MODALITA:
  - LIVE: ZMQ reale verso NT8
  - PAPER: Simulazione locale (nessun ZMQ, ordini riempiti istantaneamente)

SICUREZZA:
  - Ogni azione loggata
  - Timeout 5s su ACK
  - Retry 3x su timeout
  - Alert Telegram se disconnesso con posizione aperta
  - Reconnect esponenziale

CONFIGURAZIONE (settings.yaml):
  execution:
    bridge_host: "127.0.0.1"
    bridge_port: 5555           # comandi (REQ/REP)
    event_port: 5556            # eventi (PUB/SUB)
    ack_timeout_sec: 5
    max_retries: 3
    heartbeat_interval_sec: 30
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger("p1uni.execution.ninja_bridge")


# ============================================================
# Position state
# ============================================================

class PositionState:
    """Stato posizione corrente, thread-safe."""

    __slots__ = ("_lock", "side", "size", "entry_price", "unrealized_pnl",
                 "last_fill_time", "order_id")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.side: str = "FLAT"  # FLAT, LONG, SHORT
        self.size: int = 0
        self.entry_price: float = 0.0
        self.unrealized_pnl: float = 0.0
        self.last_fill_time: datetime | None = None
        self.order_id: str = ""

    def update(self, side: str, size: int, entry_price: float, order_id: str = "") -> None:
        with self._lock:
            self.side = side
            self.size = size
            self.entry_price = entry_price
            self.last_fill_time = datetime.now(timezone.utc)
            self.order_id = order_id

    def flatten(self) -> None:
        with self._lock:
            self.side = "FLAT"
            self.size = 0
            self.entry_price = 0.0
            self.unrealized_pnl = 0.0

    def get(self) -> dict[str, Any]:
        with self._lock:
            return {
                "side": self.side,
                "size": self.size,
                "entry_price": self.entry_price,
                "unrealized_pnl": self.unrealized_pnl,
                "last_fill_time": self.last_fill_time.isoformat() if self.last_fill_time else None,
                "order_id": self.order_id,
            }

    @property
    def is_flat(self) -> bool:
        with self._lock:
            return self.side == "FLAT"


# ============================================================
# NinjaTraderBridge
# ============================================================

class NinjaTraderBridge:
    """Ponte di comunicazione bidirezionale con NinjaTrader 8.

    Supporta modalita LIVE (ZMQ) e PAPER (simulazione locale).
    Thread-safe. Gestisce invio ordini, ascolto eventi, tracking posizione.
    """

    def __init__(self, config: dict[str, Any], telegram: Any = None) -> None:
        exec_cfg = config.get("execution", {})
        sys_cfg = config.get("system", {})

        self.host: str = exec_cfg.get("bridge_host", "127.0.0.1")
        self.cmd_port: int = int(exec_cfg.get("bridge_port", 5555))
        self.event_port: int = int(exec_cfg.get("event_port", 5556))
        self.ack_timeout_ms: int = int(exec_cfg.get("ack_timeout_sec", 5)) * 1000
        self.max_retries: int = int(exec_cfg.get("max_retries", 3))
        self.heartbeat_interval: int = int(exec_cfg.get("heartbeat_interval_sec", 30))

        self.paper_mode: bool = sys_cfg.get("mode", "paper") == "paper"
        self.telegram = telegram

        # Stato
        self.position = PositionState()
        self._connected: bool = False
        self._running: bool = False
        self._shutdown_event = threading.Event()

        # ZMQ (lazy init, solo in LIVE mode)
        self._zmq_context: Any = None
        self._cmd_socket: Any = None
        self._event_socket: Any = None
        self._cmd_lock = threading.Lock()  # serializza invio comandi

        # Event callbacks
        self._event_handlers: dict[str, list[Callable]] = {}

        # Paper mode: prezzo simulato
        self._paper_last_price: float = 0.0

        # Stats
        self.orders_sent: int = 0
        self.orders_filled: int = 0
        self.orders_rejected: int = 0
        self.orders_timed_out: int = 0

        mode_str = "PAPER" if self.paper_mode else "LIVE"
        logger.info(f"NinjaTraderBridge initialized in {mode_str} mode")

    # ============================================================
    # Lifecycle
    # ============================================================

    def start(self) -> None:
        """Avvia il bridge: connessione ZMQ (live) o init paper."""
        self._running = True

        if self.paper_mode:
            logger.info("[PAPER] Bridge started in simulation mode")
            self._connected = True
            return

        # LIVE: inizializza ZMQ
        self._connect_zmq()

        # Thread per ascolto eventi
        self._event_thread = threading.Thread(
            target=self._listen_events, name="nt8-events", daemon=True
        )
        self._event_thread.start()

        # Thread per heartbeat
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, name="nt8-heartbeat", daemon=True
        )
        self._hb_thread.start()

    def _connect_zmq(self) -> None:
        """Inizializza contesto e socket ZMQ."""
        try:
            import zmq
            self._zmq_context = zmq.Context()

            # Socket comandi (REQ)
            self._cmd_socket = self._zmq_context.socket(zmq.REQ)
            self._cmd_socket.setsockopt(zmq.RCVTIMEO, self.ack_timeout_ms)
            self._cmd_socket.setsockopt(zmq.SNDTIMEO, self.ack_timeout_ms)
            self._cmd_socket.setsockopt(zmq.LINGER, 0)
            cmd_addr = f"tcp://{self.host}:{self.cmd_port}"
            self._cmd_socket.connect(cmd_addr)

            # Socket eventi (SUB)
            self._event_socket = self._zmq_context.socket(zmq.SUB)
            self._event_socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1s poll
            self._event_socket.setsockopt_string(zmq.SUBSCRIBE, "")  # ricevi tutto
            event_addr = f"tcp://{self.host}:{self.event_port}"
            self._event_socket.connect(event_addr)

            self._connected = True
            logger.info(f"ZMQ connected: cmd={cmd_addr}, events={event_addr}")

        except Exception as e:
            self._connected = False
            logger.error(f"ZMQ connection failed: {e}")
            raise ConnectionError(f"ZMQ init failed: {e}") from e

    def stop(self) -> None:
        """Shutdown ordinato."""
        logger.info("Stopping NinjaTraderBridge...")
        self._running = False
        self._shutdown_event.set()
        self._close_zmq()
        logger.info("NinjaTraderBridge stopped")

    def _close_zmq(self) -> None:
        """Chiude socket e contesto ZMQ."""
        for sock in (self._cmd_socket, self._event_socket):
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
        if self._zmq_context is not None:
            try:
                self._zmq_context.term()
            except Exception:
                pass
        self._cmd_socket = None
        self._event_socket = None
        self._zmq_context = None
        self._connected = False

    # ============================================================
    # Invio Ordini
    # ============================================================

    def send_order(
        self,
        signal_type: str,
        size: int,
        sl: float,
        tp: float,
        price: float = 0.0,
    ) -> dict[str, Any]:
        """Invia un ordine a NinjaTrader.

        Args:
            signal_type: "LONG" o "SHORT"
            size: Numero contratti
            sl: Stop loss
            tp: Take profit
            price: Prezzo corrente (per paper mode e logging)

        Returns:
            Dict con risultato: {success, order_id, fill_price, message}
        """
        order_id = str(uuid.uuid4())[:8]
        action = "BUY" if signal_type == "LONG" else "SELL"

        msg = {
            "action": action,
            "qty": size,
            "sl": sl,
            "tp": tp,
            "price": price,
            "order_id": order_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            f"{'[PAPER] ' if self.paper_mode else ''}Sending order: "
            f"{action} x{size} SL={sl} TP={tp} price={price} id={order_id}"
        )
        self.orders_sent += 1

        if self.paper_mode:
            return self._paper_execute(msg)
        else:
            return self._zmq_send_with_retry(msg)

    def cancel_all(self) -> dict[str, Any]:
        """Invia comando per cancellare tutti gli ordini e chiudere posizioni."""
        msg = {
            "action": "FLATTEN",
            "order_id": str(uuid.uuid4())[:8],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        logger.warning(f"{'[PAPER] ' if self.paper_mode else ''}CANCEL ALL / FLATTEN")

        if self.paper_mode:
            self.position.flatten()
            return {"success": True, "message": "[PAPER] All positions flattened"}

        return self._zmq_send_with_retry(msg)

    # ============================================================
    # ZMQ send with retry
    # ============================================================

    def _zmq_send_with_retry(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Invia messaggio via ZMQ con retry esponenziale."""
        import zmq

        for attempt in range(1, self.max_retries + 1):
            try:
                with self._cmd_lock:
                    if self._cmd_socket is None:
                        return {"success": False, "message": "ZMQ not connected"}

                    # Invia
                    self._cmd_socket.send_json(msg)
                    logger.debug(f"ZMQ sent (attempt {attempt}): {msg['action']} id={msg['order_id']}")

                    # Attendi ACK
                    response = self._cmd_socket.recv_json()

                    if response.get("status") == "ACK":
                        fill_price = response.get("fill_price", msg.get("price", 0))
                        self._on_order_acked(msg, response)
                        return {
                            "success": True,
                            "order_id": msg["order_id"],
                            "fill_price": fill_price,
                            "message": "ACK received",
                        }
                    elif response.get("status") == "NACK":
                        reason = response.get("reason", "Unknown")
                        logger.warning(f"Order NACK: {reason}")
                        self.orders_rejected += 1
                        return {
                            "success": False,
                            "order_id": msg["order_id"],
                            "message": f"NACK: {reason}",
                        }

            except zmq.Again:
                # Timeout
                self.orders_timed_out += 1
                logger.warning(
                    f"ZMQ timeout (attempt {attempt}/{self.max_retries}). "
                    f"Recreating socket..."
                )
                self._reconnect_cmd_socket()
                if attempt < self.max_retries:
                    time.sleep(attempt)  # backoff

            except Exception as e:
                logger.error(f"ZMQ send error: {e}")
                self._reconnect_cmd_socket()
                if attempt < self.max_retries:
                    time.sleep(attempt)

        # Tutti i retry falliti
        error_msg = f"Order FAILED after {self.max_retries} retries"
        logger.error(error_msg)
        self._send_alert(
            f"ORDINE FALLITO: {msg['action']} x{msg.get('qty')} dopo {self.max_retries} tentativi!",
            "ERROR",
        )

        # Se abbiamo posizioni aperte e siamo disconnessi: CRITICO
        if not self.position.is_flat:
            self._send_alert(
                "DISCONNESSO DA NT8 CON POSIZIONE APERTA! Verificare manualmente!",
                "ERROR",
            )

        return {"success": False, "order_id": msg["order_id"], "message": error_msg}

    def _reconnect_cmd_socket(self) -> None:
        """Ricostruisci il socket REQ (necessario dopo timeout in ZMQ REQ)."""
        try:
            import zmq
            if self._cmd_socket is not None:
                self._cmd_socket.close()
            self._cmd_socket = self._zmq_context.socket(zmq.REQ)
            self._cmd_socket.setsockopt(zmq.RCVTIMEO, self.ack_timeout_ms)
            self._cmd_socket.setsockopt(zmq.SNDTIMEO, self.ack_timeout_ms)
            self._cmd_socket.setsockopt(zmq.LINGER, 0)
            self._cmd_socket.connect(f"tcp://{self.host}:{self.cmd_port}")
        except Exception as e:
            logger.error(f"CMD socket reconnect failed: {e}")
            self._cmd_socket = None

    def _on_order_acked(self, msg: dict[str, Any], response: dict[str, Any]) -> None:
        """Aggiorna stato dopo ACK."""
        action = msg.get("action", "")
        fill_price = response.get("fill_price", msg.get("price", 0))

        if action in ("BUY", "SELL"):
            side = "LONG" if action == "BUY" else "SHORT"
            self.position.update(side, msg.get("qty", 1), fill_price, msg.get("order_id", ""))
            self.orders_filled += 1
            logger.info(f"Order FILLED: {side} x{msg.get('qty')} @ {fill_price}")

        elif action == "FLATTEN":
            self.position.flatten()
            logger.info("Position FLATTENED")

    # ============================================================
    # Paper Mode execution
    # ============================================================

    def _paper_execute(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Simula l'esecuzione in paper mode."""
        action = msg.get("action", "")
        price = msg.get("price", self._paper_last_price)
        qty = msg.get("qty", 1)
        order_id = msg.get("order_id", "")

        if action in ("BUY", "SELL"):
            side = "LONG" if action == "BUY" else "SHORT"
            self.position.update(side, qty, price, order_id)
            self.orders_filled += 1

            logger.info(f"[PAPER] Order FILLED: {side} x{qty} @ {price}")

            # Genera evento finto
            self._dispatch_event("ORDER_FILLED", {
                "order_id": order_id,
                "side": side,
                "qty": qty,
                "fill_price": price,
            })

            return {
                "success": True,
                "order_id": order_id,
                "fill_price": price,
                "message": "[PAPER] Filled instantly",
            }

        elif action == "FLATTEN":
            self.position.flatten()
            self._dispatch_event("POSITION_UPDATED", {"side": "FLAT", "size": 0})
            return {"success": True, "message": "[PAPER] Flattened"}

        return {"success": False, "message": f"Unknown action: {action}"}

    def set_paper_price(self, price: float) -> None:
        """Aggiorna il prezzo simulato per paper mode."""
        self._paper_last_price = price

    # ============================================================
    # Event listener (LIVE mode)
    # ============================================================

    def _listen_events(self) -> None:
        """Thread: ascolta eventi da NT8 via SUB socket."""
        logger.info("Event listener started")

        while self._running and not self._shutdown_event.is_set():
            try:
                if self._event_socket is None:
                    time.sleep(1)
                    continue

                msg = self._event_socket.recv_json()
                self._handle_event(msg)

            except Exception:
                # Timeout recv (zmq.Again) o errore — continua
                continue

        logger.info("Event listener stopped")

    def _handle_event(self, event_data: dict[str, Any]) -> None:
        """Processa un evento ricevuto da NT8."""
        event_type = event_data.get("event", "UNKNOWN")

        logger.info(f"NT8 event: {event_type} -> {event_data}")

        if event_type == "ORDER_FILLED":
            side = event_data.get("side", "")
            fill_price = float(event_data.get("fill_price", 0))
            qty = int(event_data.get("qty", 0))
            order_id = event_data.get("order_id", "")
            if side and fill_price > 0:
                self.position.update(side, qty, fill_price, order_id)
                self.orders_filled += 1

        elif event_type == "ORDER_REJECTED":
            self.orders_rejected += 1
            reason = event_data.get("reason", "")
            logger.warning(f"Order REJECTED by NT8: {reason}")

        elif event_type == "POSITION_UPDATED":
            side = event_data.get("side", "FLAT")
            size = int(event_data.get("size", 0))
            price = float(event_data.get("entry_price", 0))
            if side == "FLAT" or size == 0:
                self.position.flatten()
            else:
                self.position.update(side, size, price)

        elif event_type == "ACCOUNT_STATUS":
            logger.info(f"Account status: {event_data}")

        # Dispatch ai handler registrati
        self._dispatch_event(event_type, event_data)

    def _dispatch_event(self, event_type: str, data: dict[str, Any]) -> None:
        """Notifica tutti gli handler registrati per questo evento."""
        handlers = self._event_handlers.get(event_type, [])
        for handler in handlers:
            try:
                handler(data)
            except Exception as e:
                logger.error(f"Event handler error ({event_type}): {e}")

    def register_event_handler(self, event_type: str, handler: Callable) -> None:
        """Registra un callback per un tipo di evento NT8."""
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)

    # ============================================================
    # Heartbeat
    # ============================================================

    def _heartbeat_loop(self) -> None:
        """Thread: invia heartbeat periodico."""
        while self._running and not self._shutdown_event.is_set():
            self._shutdown_event.wait(self.heartbeat_interval)
            if not self._running:
                break

            try:
                msg = {
                    "action": "HEARTBEAT",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                with self._cmd_lock:
                    if self._cmd_socket is not None:
                        self._cmd_socket.send_json(msg)
                        self._cmd_socket.recv_json()
                        self._connected = True
            except Exception:
                if self._connected:
                    logger.warning("NT8 heartbeat failed — connection may be lost")
                    self._connected = False

    # ============================================================
    # Helpers
    # ============================================================

    def is_connected(self) -> bool:
        """True se la connessione a NT8 e' attiva."""
        if self.paper_mode:
            return True
        return self._connected

    def get_position(self) -> dict[str, Any]:
        """Ritorna lo stato posizione corrente (thread-safe)."""
        return self.position.get()

    def _send_alert(self, message: str, level: str = "WARNING") -> None:
        if self.telegram is not None:
            try:
                self.telegram.send_alert(message, level)
            except Exception:
                pass

    def get_status(self) -> dict[str, Any]:
        """Snapshot per monitoring."""
        return {
            "connected": self.is_connected(),
            "paper_mode": self.paper_mode,
            "position": self.position.get(),
            "orders_sent": self.orders_sent,
            "orders_filled": self.orders_filled,
            "orders_rejected": self.orders_rejected,
            "orders_timed_out": self.orders_timed_out,
        }

    def close(self) -> None:
        """Alias per stop()."""
        self.stop()
