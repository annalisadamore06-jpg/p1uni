"""
Test per NinjaTraderBridge - paper mode, ordini, eventi, posizione.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.execution.ninja_bridge import NinjaTraderBridge, PositionState


def _make_config(paper: bool = True) -> dict[str, Any]:
    return {
        "system": {"mode": "paper" if paper else "live"},
        "execution": {
            "bridge_host": "127.0.0.1",
            "bridge_port": 5555,
            "event_port": 5556,
            "ack_timeout_sec": 5,
            "max_retries": 3,
            "heartbeat_interval_sec": 30,
        },
    }


# ============================================================
# Test: PositionState
# ============================================================

class TestPositionState:

    def test_initial_flat(self) -> None:
        ps = PositionState()
        assert ps.is_flat is True
        assert ps.get()["side"] == "FLAT"

    def test_update_long(self) -> None:
        ps = PositionState()
        ps.update("LONG", 2, 5420.0, "order-1")
        assert ps.is_flat is False
        state = ps.get()
        assert state["side"] == "LONG"
        assert state["size"] == 2
        assert state["entry_price"] == 5420.0

    def test_flatten(self) -> None:
        ps = PositionState()
        ps.update("SHORT", 1, 5430.0)
        ps.flatten()
        assert ps.is_flat is True
        assert ps.get()["size"] == 0


# ============================================================
# Test: Paper Mode
# ============================================================

class TestPaperMode:

    def test_paper_mode_detected(self) -> None:
        bridge = NinjaTraderBridge(_make_config(paper=True))
        assert bridge.paper_mode is True

    def test_paper_send_order_buy(self) -> None:
        """Paper BUY: ordine riempito istantaneamente."""
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()

        result = bridge.send_order("LONG", 2, 5400.0, 5450.0, price=5420.0)
        assert result["success"] is True
        assert "[PAPER]" in result["message"]
        assert result["fill_price"] == 5420.0

        pos = bridge.get_position()
        assert pos["side"] == "LONG"
        assert pos["size"] == 2
        assert pos["entry_price"] == 5420.0

    def test_paper_send_order_sell(self) -> None:
        """Paper SELL: ordine riempito."""
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()

        result = bridge.send_order("SHORT", 1, 5450.0, 5400.0, price=5430.0)
        assert result["success"] is True
        assert pos_state(bridge, "SHORT")

    def test_paper_cancel_all(self) -> None:
        """Paper FLATTEN: posizione azzerata."""
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()

        bridge.send_order("LONG", 2, 5400.0, 5450.0, price=5420.0)
        assert not bridge.position.is_flat

        result = bridge.cancel_all()
        assert result["success"] is True
        assert bridge.position.is_flat

    def test_paper_no_zmq(self) -> None:
        """Paper mode non inizializza ZMQ."""
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()
        assert bridge._zmq_context is None
        assert bridge._cmd_socket is None

    def test_paper_is_connected(self) -> None:
        """Paper mode e' sempre 'connesso'."""
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()
        assert bridge.is_connected() is True

    def test_paper_set_price(self) -> None:
        """set_paper_price aggiorna il prezzo simulato."""
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()
        bridge.set_paper_price(5500.0)

        result = bridge.send_order("LONG", 1, 5480.0, 5520.0)
        assert result["fill_price"] == 5500.0


# ============================================================
# Test: Event handling
# ============================================================

class TestEventHandling:

    def test_order_filled_updates_position(self) -> None:
        """Evento ORDER_FILLED aggiorna la posizione."""
        bridge = NinjaTraderBridge(_make_config(paper=True))

        bridge._handle_event({
            "event": "ORDER_FILLED",
            "side": "LONG",
            "fill_price": 5420.0,
            "qty": 2,
            "order_id": "abc",
        })
        pos = bridge.get_position()
        assert pos["side"] == "LONG"
        assert pos["entry_price"] == 5420.0

    def test_order_rejected_counted(self) -> None:
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge._handle_event({
            "event": "ORDER_REJECTED",
            "reason": "Insufficient margin",
        })
        assert bridge.orders_rejected == 1

    def test_position_flatten_event(self) -> None:
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.position.update("LONG", 1, 5420.0)

        bridge._handle_event({
            "event": "POSITION_UPDATED",
            "side": "FLAT",
            "size": 0,
        })
        assert bridge.position.is_flat

    def test_callback_registered_and_called(self) -> None:
        """Callback registrato viene chiamato su evento."""
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()

        events_received: list[dict] = []
        bridge.register_event_handler("ORDER_FILLED", lambda data: events_received.append(data))

        # Paper order genera evento ORDER_FILLED
        bridge.send_order("LONG", 1, 5400.0, 5450.0, price=5420.0)

        assert len(events_received) == 1
        assert events_received[0]["side"] == "LONG"


# ============================================================
# Test: Stats
# ============================================================

class TestStats:

    def test_order_counters(self) -> None:
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()

        bridge.send_order("LONG", 1, 5400.0, 5450.0, price=5420.0)
        bridge.send_order("SHORT", 1, 5450.0, 5400.0, price=5430.0)

        assert bridge.orders_sent == 2
        assert bridge.orders_filled == 2

    def test_get_status(self) -> None:
        bridge = NinjaTraderBridge(_make_config(paper=True))
        bridge.start()
        bridge.send_order("LONG", 1, 5400.0, 5450.0, price=5420.0)

        status = bridge.get_status()
        assert status["connected"] is True
        assert status["paper_mode"] is True
        assert status["orders_sent"] == 1
        assert status["orders_filled"] == 1
        assert status["position"]["side"] == "LONG"


# ============================================================
# Test: Live mode (senza ZMQ reale)
# ============================================================

class TestLiveMode:

    def test_live_mode_detected(self) -> None:
        bridge = NinjaTraderBridge(_make_config(paper=False))
        assert bridge.paper_mode is False

    def test_live_not_connected_without_zmq(self) -> None:
        """Live mode senza ZMQ non e' connesso."""
        bridge = NinjaTraderBridge(_make_config(paper=False))
        assert bridge.is_connected() is False

    def test_live_send_without_connection_fails(self) -> None:
        """Invio senza connessione ZMQ ritorna failure."""
        bridge = NinjaTraderBridge(_make_config(paper=False))
        result = bridge.send_order("LONG", 1, 5400.0, 5450.0, price=5420.0)
        assert result["success"] is False


# ============================================================
# Test: Disconnection alerts
# ============================================================

class TestDisconnectionAlerts:

    def test_alert_on_failed_order_with_position(self) -> None:
        """Se ordine fallisce con posizione aperta -> alert Telegram."""
        telegram = MagicMock()
        bridge = NinjaTraderBridge(_make_config(paper=False), telegram=telegram)

        # Simula posizione aperta
        bridge.position.update("LONG", 1, 5420.0)

        # Invio fallisce (no ZMQ)
        bridge.send_order("SHORT", 1, 5450.0, 5400.0, price=5430.0)

        # Telegram alert dovrebbe essere stato chiamato
        assert telegram.send_alert.called


# ============================================================
# Helpers
# ============================================================

def pos_state(bridge: NinjaTraderBridge, expected_side: str) -> bool:
    return bridge.get_position()["side"] == expected_side
