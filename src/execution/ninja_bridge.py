"""
NinjaTrader Bridge — TCP Server compatibile con MLAutoStrategy_P1.cs

ARCHITETTURA (CORRETTA):
  Python (TCP SERVER)  <─── NT8 (TCP CLIENT) connects
         |                        |
  binds 127.0.0.1:5555           connects to 5555
  sends JSON lines               reads JSON lines
                                 executes trades
                                 sends events back

PROTOCOLLO (JSON newline-delimited):
  Python → NT8:
    {"type": "HEARTBEAT", "timestamp": "..."}
    {"side": "LONG",  "confidence": 0.65, "signal_id": "abc123"}
    {"side": "SHORT", "confidence": 0.65, "signal_id": "def456"}
    {"side": "FLATTEN","signal_id": "ghi789"}

  NT8 → Python (opzionale):
    {"type": "FILL",  "side": "LONG",  "qty": 1, "price": 5450.0}
    {"type": "FLAT",  "pnl": 12.5}

NOTES:
  - Fire-and-forget: Python sends signal, non attende ACK
  - NT8 gestisce SL/TP dai livelli P1-Lite/GEX interni
  - L'unico dato critico che NT8 accetta da Python: side + confidence + signal_id

PAPER MODE:
  Simula localmente senza ZMQ/TCP.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger("p1uni.execution.ninja_bridge")


# ============================================================
# Position state (thread-safe)
# ============================================================

class PositionState:
    """Stato posizione corrente, thread-safe."""

    __slots__ = ("_lock", "side", "size", "entry_price", "unrealized_pnl",
                 "last_fill_time", "order_id")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.side: str = "FLAT"
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
    """
    Ponte Python ↔ NinjaTrader 8.

    LIVE mode: TCP server che attende la connessione del NinjaScript.
    Invia segnali come JSON newline-delimited (fire-and-forget).

    PAPER mode: Simulazione locale, nessun TCP.
    """

    def __init__(self, config: dict[str, Any], telegram: Any = None) -> None:
        exec_cfg = config.get("execution", {})
        sys_cfg = config.get("system", {})

        self.host: str = exec_cfg.get("bridge_host", "127.0.0.1")
        self.cmd_port: int = int(exec_cfg.get("bridge_port", 5555))
        self.heartbeat_interval: int = int(exec_cfg.get("heartbeat_interval_sec", 25))

        self.paper_mode: bool = sys_cfg.get("mode", "paper") == "paper"
        self.telegram = telegram

        # Stato
        self.position = PositionState()
        self._connected: bool = False
        self._running: bool = False
        self._shutdown_event = threading.Event()

        # TCP server / client
        self._server_sock: socket.socket | None = None
        self._client_sock: socket.socket | None = None
        self._client_lock = threading.Lock()

        # Event callbacks
        self._event_handlers: dict[str, list[Callable]] = {}

        # Paper mode: prezzo simulato
        self._paper_last_price: float = 0.0

        # Stats
        self.orders_sent: int = 0
        self.orders_filled: int = 0
        self.orders_rejected: int = 0

        # Heartbeat health tracking
        self._last_heartbeat_ok_ts: float = 0.0
        self._last_heartbeat_fail_ts: float = 0.0
        self._heartbeat_failures_total: int = 0

        mode_str = "PAPER" if self.paper_mode else "LIVE"
        logger.info(f"NinjaTraderBridge initialized in {mode_str} mode")

    # ============================================================
    # Lifecycle
    # ============================================================

    def start(self) -> None:
        """Avvia il bridge: TCP server (live) o init paper."""
        self._running = True

        if self.paper_mode:
            logger.info("[PAPER] Bridge started in simulation mode")
            self._connected = True
            return

        # LIVE: TCP server che attende NT8
        try:
            self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_sock.bind((self.host, self.cmd_port))
            self._server_sock.listen(1)
            self._server_sock.settimeout(5.0)  # timeout per accept loop
            logger.info(f"TCP Server bound on {self.host}:{self.cmd_port} — waiting for NT8 connection...")
        except Exception as e:
            logger.error(f"TCP server bind failed on {self.host}:{self.cmd_port}: {e}")
            raise

        # Thread: accetta connessioni NT8
        threading.Thread(
            target=self._accept_loop, name="nt8-accept", daemon=True
        ).start()

        # Thread: heartbeat periodico
        threading.Thread(
            target=self._heartbeat_loop, name="nt8-heartbeat", daemon=True
        ).start()

    def stop(self) -> None:
        """Shutdown ordinato."""
        logger.info("Stopping NinjaTraderBridge...")
        self._running = False
        self._shutdown_event.set()
        with self._client_lock:
            if self._client_sock is not None:
                try:
                    self._client_sock.close()
                except Exception:
                    pass
                self._client_sock = None
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except Exception:
                pass
            self._server_sock = None
        self._connected = False
        logger.info("NinjaTraderBridge stopped")

    # ============================================================
    # TCP Server: accept loop
    # ============================================================

    def _accept_loop(self) -> None:
        """Thread: accetta connessioni TCP da NT8."""
        while self._running:
            try:
                if self._server_sock is None:
                    break
                try:
                    conn, addr = self._server_sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    # Server socket chiuso
                    break

                # Configura la connessione client
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                conn.settimeout(None)  # blocking per recv

                with self._client_lock:
                    # Chiudi vecchia connessione se presente
                    if self._client_sock is not None:
                        try:
                            self._client_sock.close()
                        except Exception:
                            pass
                    self._client_sock = conn
                    self._connected = True

                logger.info(f"NT8 connected from {addr[0]}:{addr[1]}")
                self._send_alert("✅ NinjaTrader 8 connesso a P1UNI", "INFO")

                # Thread: ricevi eventi da NT8
                threading.Thread(
                    target=self._recv_loop, args=(conn,), name="nt8-recv", daemon=True
                ).start()

            except Exception as e:
                if self._running:
                    logger.error(f"Accept loop error: {e}")
                time.sleep(1)

    # ============================================================
    # TCP receive loop (events from NT8)
    # ============================================================

    def _recv_loop(self, conn: socket.socket) -> None:
        """Thread: ricevi eventi da NT8 sulla connessione attiva."""
        buf = b""
        while self._running:
            try:
                data = conn.recv(4096)
                if not data:
                    # Connessione chiusa da NT8
                    logger.warning("NT8 disconnected (connection closed)")
                    break
                buf += data
                # Parse newline-delimited JSON
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if line:
                        try:
                            event = json.loads(line.decode("utf-8", errors="ignore"))
                            self._handle_nt8_event(event)
                        except json.JSONDecodeError:
                            logger.debug(f"NT8 non-JSON: {line[:100]}")
            except OSError:
                break
            except Exception as e:
                logger.debug(f"Recv error: {e}")
                break

        # Connessione persa
        with self._client_lock:
            if self._client_sock is conn:
                self._client_sock = None
                self._connected = False
        logger.warning("NT8 connection lost — waiting for reconnect...")
        self._send_alert("⚠️ NT8 disconnesso da P1UNI — attesa riconnessione", "WARNING")

    def _handle_nt8_event(self, event: dict[str, Any]) -> None:
        """Processa un evento ricevuto da NT8."""
        etype = event.get("type", "UNKNOWN")
        logger.info(f"NT8 event: {etype} → {event}")

        if etype == "FILL":
            side = event.get("side", "")
            qty = int(event.get("qty", 0))
            price = float(event.get("price", 0))
            order_id = event.get("order_id", "")
            if side and price > 0:
                self.position.update(side, qty, price, order_id)
                self.orders_filled += 1
                logger.info(f"Position updated: {side} x{qty} @ {price}")

        elif etype in ("FLAT", "FLATTEN"):
            pnl = float(event.get("pnl", 0))
            self.position.flatten()
            logger.info(f"Position FLAT (pnl={pnl:.2f})")

        elif etype in ("REJECTED", "ORDER_REJECTED"):
            self.orders_rejected += 1
            reason = event.get("reason", "unknown")
            logger.warning(f"Order rejected by NT8: {reason}")

        elif etype == "ACCOUNT":
            daily_pnl = event.get("daily_pnl", 0)
            logger.info(f"Account update: daily_pnl={daily_pnl}")

        # Dispatch ai handler registrati
        self._dispatch_event(etype, event)

    # ============================================================
    # Heartbeat
    # ============================================================

    def _heartbeat_loop(self) -> None:
        """Thread: invia HEARTBEAT a NT8 ogni heartbeat_interval secondi.

        Se NT8 non riceve heartbeat per 30s, disabilita il trading lato NT8.
        """
        while self._running and not self._shutdown_event.is_set():
            self._shutdown_event.wait(self.heartbeat_interval)
            if not self._running:
                break

            hb = {
                "type": "HEARTBEAT",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            ok = self._send_to_nt8(hb)
            if ok:
                self._last_heartbeat_ok_ts = time.time()
                if not self._connected:
                    logger.info("NT8 heartbeat OK — connection restored")
                self._connected = True
            else:
                self._last_heartbeat_fail_ts = time.time()
                self._heartbeat_failures_total += 1
                if self._connected:
                    logger.warning("NT8 heartbeat failed — connection may be lost")
                # Don't set _connected=False here: let _recv_loop handle it

    # ============================================================
    # Send to NT8
    # ============================================================

    def _send_to_nt8(self, msg: dict[str, Any]) -> bool:
        """Invia un messaggio JSON a NT8 (thread-safe, fire-and-forget).

        Aggiunge newline come delimitatore (NT8 usa readline).

        Returns:
            True se inviato con successo, False se NT8 non connesso.
        """
        try:
            data = (json.dumps(msg) + "\n").encode("utf-8")
            with self._client_lock:
                if self._client_sock is None:
                    return False
                self._client_sock.sendall(data)
            return True
        except Exception as e:
            logger.debug(f"NT8 send failed: {e}")
            with self._client_lock:
                self._client_sock = None
                self._connected = False
            return False

    # ============================================================
    # Ordini
    # ============================================================

    def send_order(
        self,
        signal_type: str,
        size: int,
        sl: float,
        tp: float,
        price: float = 0.0,
        raw_probability: float = 0.0,
        trail_pts: float = 0.0,
    ) -> dict[str, Any]:
        """Invia un segnale di trading a NinjaTrader.

        Args:
            signal_type: "LONG" o "SHORT"
            size: Numero contratti (usato in paper mode; NT8 gestisce la size internamente)
            sl: Stop loss (informativo; NT8 usa i suoi livelli P1-Lite)
            tp: Take profit (informativo; NT8 usa i suoi livelli P1-Lite)
            price: Prezzo corrente (per logging)
            raw_probability: avg_proba grezza dal modello (0-1). Usata come confidence per NT8.
            trail_pts: Trailing-stop distance in points (0 = no trailing, fixed TP/SL).
                Set by hedging layer R5 (Phase 4 optimized = 4.0pt). NT8 strategy
                must honor this hint when non-zero — see hedging_signals.py docstring.

        Returns:
            Dict con risultato: {success, order_id, fill_price, message}
        """
        order_id = str(uuid.uuid4())[:8]
        side = "LONG" if signal_type == "LONG" else "SHORT"

        # NT8 usa confidence come "forza del segnale" su scala 0.50-1.0.
        # raw_probability è avg_proba del modello (0-1), che per SHORT è < 0.5.
        # Convertiamo a "distanza simmetrica da 0.5" → scala 0.5-1.0 indipendente dalla direzione:
        #   abs(raw_proba - 0.5) + 0.5
        #   Es: raw=0.35 (SHORT WEAK) → 0.15+0.5 = 0.65 → sopra MIN_CONFIDENCE=0.60 ✅
        #   Es: raw=0.65 (LONG WEAK)  → 0.15+0.5 = 0.65 → sopra MIN_CONFIDENCE=0.60 ✅
        # Fallback: 0.62 (appena sopra soglia NT8)
        if raw_probability > 0:
            confidence_for_nt8 = abs(raw_probability - 0.5) + 0.5
        else:
            confidence_for_nt8 = 0.62

        # In paper mode, use _paper_last_price as fill price when price=0
        effective_price = price if price > 0 else self._paper_last_price

        msg = {
            "side": side,
            "confidence": round(confidence_for_nt8, 4),
            "signal_id": order_id,
            "qty": size,
            "sl_hint": round(sl, 2),   # informativo
            "tp_hint": round(tp, 2),   # informativo
            "trail_hint": round(float(trail_pts), 2),  # 0 = no trailing; R5 uses 4.0pt
            "entry_price": round(effective_price, 2),
        }

        logger.info(
            f"{'[PAPER] ' if self.paper_mode else ''}Sending signal: "
            f"{side} conf={confidence_for_nt8:.3f} price={price:.2f} id={order_id}"
        )
        self.orders_sent += 1

        if self.paper_mode:
            return self._paper_execute(msg)

        if not self._connected:
            logger.warning("NT8 not connected — signal queued/dropped")
            self._send_alert(
                f"⚠️ Segnale {side} DROPPATO: NT8 non connesso (strategy offline?)",
                "WARNING",
            )
            return {
                "success": False,
                "order_id": order_id,
                "message": "NT8 not connected (strategy offline or NinjaTrader not running)",
            }

        ok = self._send_to_nt8(msg)
        if ok:
            # Fire-and-forget: assume optimistic success
            # Position update will come via NT8 event (FILL) if strategy responds
            logger.info(f"Signal sent to NT8: {side} id={order_id}")
            return {
                "success": True,
                "order_id": order_id,
                "fill_price": price,
                "message": "Signal sent to NT8 (fire-and-forget)",
            }
        else:
            logger.error(f"Failed to send signal to NT8: {side} id={order_id}")
            self._send_alert(
                f"❌ Segnale NT8 FALLITO: {side} conf={confidence_for_nt8:.2f}",
                "ERROR",
            )
            return {
                "success": False,
                "order_id": order_id,
                "message": "Send to NT8 failed",
            }

    def cancel_all(self) -> dict[str, Any]:
        """Invia comando FLATTEN a NT8 (chiude tutte le posizioni)."""
        msg = {
            "side": "FLATTEN",
            "signal_id": str(uuid.uuid4())[:8],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.warning(f"{'[PAPER] ' if self.paper_mode else ''}FLATTEN ALL")

        if self.paper_mode:
            self.position.flatten()
            return {"success": True, "message": "[PAPER] All positions flattened"}

        ok = self._send_to_nt8(msg)
        if ok:
            return {"success": True, "message": "FLATTEN sent to NT8"}
        else:
            return {"success": False, "message": "FLATTEN send failed (NT8 not connected)"}

    # ============================================================
    # Paper Mode
    # ============================================================

    def _paper_execute(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Simula esecuzione in paper mode."""
        side = msg.get("side", "")
        price = msg.get("entry_price") or self._paper_last_price
        qty = int(msg.get("qty", 1))
        order_id = msg.get("signal_id", "")

        if side in ("LONG", "SHORT"):
            self.position.update(side, qty, price, order_id)
            self.orders_filled += 1
            logger.info(f"[PAPER] Signal 'filled': {side} x{qty} @ {price}")
            self._dispatch_event("ORDER_FILLED", {
                "side": side,
                "qty": qty,
                "fill_price": price,
                "order_id": order_id,
            })
            return {
                "success": True,
                "order_id": order_id,
                "fill_price": price,
                "message": "[PAPER] Filled instantly",
            }
        elif side == "FLATTEN":
            self.position.flatten()
            self._dispatch_event("POSITION_UPDATED", {"side": "FLAT", "size": 0})
            return {"success": True, "order_id": order_id, "message": "[PAPER] Flattened"}

        return {"success": False, "message": f"Unknown side: {side}"}

    def set_paper_price(self, price: float) -> None:
        """Aggiorna il prezzo simulato per paper mode."""
        self._paper_last_price = price

    # ============================================================
    # Events
    # ============================================================

    def _dispatch_event(self, event_type: str, data: dict[str, Any]) -> None:
        """Notifica handler registrati per un evento."""
        for handler in self._event_handlers.get(event_type, []):
            try:
                handler(data)
            except Exception as e:
                logger.error(f"Event handler error ({event_type}): {e}")

    def register_event_handler(self, event_type: str, handler: Callable) -> None:
        """Registra un callback per eventi NT8."""
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)

    # ============================================================
    # Helpers
    # ============================================================

    def is_connected(self) -> bool:
        """True se NT8 è connesso."""
        if self.paper_mode:
            return True
        return self._connected

    def is_healthy(self) -> bool:
        """True se heartbeat TCP è sano.

        Considera sano se l'ultimo heartbeat ACK (sendall success) è avvenuto
        entro 2x l'intervallo di heartbeat. In paper mode ritorna sempre True.
        """
        if self.paper_mode:
            return True
        if not self._connected:
            return False
        if self._last_heartbeat_ok_ts == 0.0:
            # Nessun heartbeat ancora inviato — non bloccante al boot
            return True
        age = time.time() - self._last_heartbeat_ok_ts
        return age <= (self.heartbeat_interval * 2)

    def get_heartbeat_status(self) -> dict[str, Any]:
        """Ritorna stato heartbeat per monitoring."""
        now = time.time()
        return {
            "connected": self._connected,
            "healthy": self.is_healthy(),
            "last_ok_age_sec": (
                round(now - self._last_heartbeat_ok_ts, 1)
                if self._last_heartbeat_ok_ts else None
            ),
            "last_fail_age_sec": (
                round(now - self._last_heartbeat_fail_ts, 1)
                if self._last_heartbeat_fail_ts else None
            ),
            "failures_total": self._heartbeat_failures_total,
            "interval_sec": self.heartbeat_interval,
        }

    def get_position(self) -> dict[str, Any]:
        """Ritorna stato posizione corrente."""
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
            "mode": "PAPER" if self.paper_mode else "LIVE",
            "paper_mode": self.paper_mode,   # alias booleano per backward compat
            "connected": self.is_connected(),
            "server_port": self.cmd_port,
            "position": self.position.get(),
            "orders": {
                "sent": self.orders_sent,
                "filled": self.orders_filled,
                "rejected": self.orders_rejected,
            },
        }
