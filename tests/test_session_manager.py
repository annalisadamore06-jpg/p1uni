"""
Test per SessionManager - transizioni di fase, freeze events, riavvio a caldo.
"""

from __future__ import annotations

from datetime import datetime, time as dtime, timezone, date, timedelta
from typing import Any
from unittest.mock import MagicMock, patch
import pandas as pd

import pytest

from src.execution.session_manager import SessionManager, MarketPhase


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton prima di ogni test."""
    SessionManager.reset()
    yield
    SessionManager.reset()


def _make_config() -> dict[str, Any]:
    return {
        "database": {"path": ":memory:", "threads": 1, "memory_limit": "256MB"},
    }


def _make_db_mock() -> MagicMock:
    db = MagicMock()
    db.execute_read.return_value = pd.DataFrame()
    return db


def _utc(year: int, month: int, day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


# ============================================================
# Test: Risoluzione fasi temporali
# ============================================================

class TestPhaseResolution:

    def test_night_early(self) -> None:
        """00:30 UTC -> NIGHT_EARLY."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        phase = sm.get_current_phase(_utc(2026, 4, 8, 0, 30))
        assert phase == MarketPhase.NIGHT_EARLY

    def test_night_full(self) -> None:
        """03:00 UTC -> NIGHT_FULL."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        phase = sm.get_current_phase(_utc(2026, 4, 8, 3, 0))
        assert phase == MarketPhase.NIGHT_FULL

    def test_morning_eu(self) -> None:
        """10:00 UTC -> MORNING_EU."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        phase = sm.get_current_phase(_utc(2026, 4, 8, 10, 0))
        assert phase == MarketPhase.MORNING_EU

    def test_afternoon_us(self) -> None:
        """16:00 UTC -> AFTERNOON_US."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        phase = sm.get_current_phase(_utc(2026, 4, 8, 16, 0))
        assert phase == MarketPhase.AFTERNOON_US

    def test_halt(self) -> None:
        """22:00 UTC -> HALT."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        phase = sm.get_current_phase(_utc(2026, 4, 8, 22, 0))
        assert phase == MarketPhase.HALT

    def test_midnight_halt(self) -> None:
        """00:00 UTC -> HALT (prima di 00:01)."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        phase = sm.get_current_phase(_utc(2026, 4, 8, 0, 0))
        assert phase == MarketPhase.HALT


# ============================================================
# Test: Transizioni critiche
# ============================================================

class TestPhaseTransitions:

    def test_0859_to_0900_fires_freeze_mr(self) -> None:
        """Transizione 08:59 -> 09:00: deve triggerare freeze MR."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)

        # 08:59: siamo in NIGHT_FULL
        sm.update(_utc(2026, 4, 8, 8, 59))
        assert sm._current_phase == MarketPhase.NIGHT_FULL

        # 09:00: transizione a MORNING_EU -> freeze MR
        sm.update(_utc(2026, 4, 8, 9, 0))
        assert sm._current_phase == MarketPhase.MORNING_EU
        assert "freeze_mr" in sm._events_fired_today

    def test_1529_to_1530_fires_freeze_or(self) -> None:
        """Transizione 15:29 -> 15:30: deve triggerare freeze OR."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)

        sm.update(_utc(2026, 4, 8, 15, 29))
        assert sm._current_phase == MarketPhase.MORNING_EU

        sm.update(_utc(2026, 4, 8, 15, 30))
        assert sm._current_phase == MarketPhase.AFTERNOON_US
        assert "freeze_or" in sm._events_fired_today

    def test_2155_fires_halt(self) -> None:
        """21:55 UTC -> HALT, trading bloccato."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)

        sm.update(_utc(2026, 4, 8, 21, 54))
        assert sm._current_phase == MarketPhase.AFTERNOON_US
        assert sm.is_trading_allowed() is True

        sm.update(_utc(2026, 4, 8, 21, 55))
        assert sm._current_phase == MarketPhase.HALT
        assert sm.is_trading_allowed() is False

    def test_freeze_mr_not_fired_twice(self) -> None:
        """freeze MR non deve scattare due volte nello stesso giorno."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)

        sm.update(_utc(2026, 4, 8, 9, 0))
        assert "freeze_mr" in sm._events_fired_today

        # Simula un secondo update alle 09:01
        sm.update(_utc(2026, 4, 8, 9, 1))
        # Il set contiene ancora freeze_mr ma non e' stato chiamato di nuovo
        # (verificato dal fatto che il set non cresce)
        assert sm._events_fired_today == {"freeze_mr"}


# ============================================================
# Test: Weekend
# ============================================================

class TestWeekend:

    def test_saturday_morning_is_weekend(self) -> None:
        """Sabato 10:00 UTC -> WEEKEND."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        # 2026-04-11 = Sabato
        phase = sm.get_current_phase(_utc(2026, 4, 11, 10, 0))
        assert phase == MarketPhase.WEEKEND

    def test_sunday_afternoon_is_weekend(self) -> None:
        """Domenica 15:00 UTC -> WEEKEND."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        # 2026-04-12 = Domenica
        phase = sm.get_current_phase(_utc(2026, 4, 12, 15, 0))
        assert phase == MarketPhase.WEEKEND

    def test_sunday_2200_not_weekend(self) -> None:
        """Domenica 22:00 UTC -> NON weekend (mercato apre)."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        # 2026-04-12 = Domenica
        phase = sm.get_current_phase(_utc(2026, 4, 12, 22, 0))
        assert phase != MarketPhase.WEEKEND

    def test_friday_afternoon_not_weekend(self) -> None:
        """Venerdi 16:00 UTC -> NON weekend."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        # 2026-04-10 = Venerdi
        phase = sm.get_current_phase(_utc(2026, 4, 10, 16, 0))
        assert phase == MarketPhase.AFTERNOON_US


# ============================================================
# Test: Active levels per fase
# ============================================================

class TestActiveLevels:

    def test_morning_levels(self) -> None:
        """MORNING_EU: MR_FROZEN + RANGE_LIVE."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 8, 10, 0))
        levels = sm.get_active_levels()
        assert "MR_FROZEN" in levels
        assert "RANGE_LIVE" in levels

    def test_afternoon_levels(self) -> None:
        """AFTERNOON_US: OR_FROZEN + GEX_LIVE + RANGE_LIVE."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 8, 16, 0))
        levels = sm.get_active_levels()
        assert "OR_FROZEN" in levels
        assert "GEX_LIVE" in levels
        assert "RANGE_LIVE" in levels

    def test_halt_no_levels(self) -> None:
        """HALT: nessun livello attivo."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 8, 22, 0))
        levels = sm.get_active_levels()
        assert levels == []


# ============================================================
# Test: Riavvio a caldo
# ============================================================

class TestWarmRestart:

    def test_restart_at_noon_loads_morning_phase(self) -> None:
        """Riavvio a 12:00: deve essere MORNING_EU immediatamente."""
        db = _make_db_mock()
        # Simula che nel DB ci siano livelli MR frozen per oggi
        db.execute_read.return_value = pd.DataFrame([{
            "mr1d": 5380.0, "mr1u": 5460.0,
            "mr2d": None, "mr2u": None,
            "or1d": 5405.0, "or1u": 5435.0,
            "or2d": None, "or2u": None,
            "vwap": 5422.0,
            "session_high": 5445.0,
            "session_low": 5400.0,
            "is_final": False,
            "frozen_at": _utc(2026, 4, 8, 9, 0),
        }])

        with patch("src.execution.session_manager.datetime") as mock_dt:
            mock_dt.now.return_value = _utc(2026, 4, 8, 12, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            # Il singleton usa datetime.now() in __init__
            sm = SessionManager(_make_config(), db)

        # Deve essere MORNING_EU
        phase = sm.get_current_phase(_utc(2026, 4, 8, 12, 0))
        assert phase == MarketPhase.MORNING_EU

    def test_restart_loads_mr_frozen(self) -> None:
        """Riavvio: deve caricare i livelli MR frozen dal DB."""
        db = _make_db_mock()
        db.execute_read.return_value = pd.DataFrame([{
            "mr1d": 5380.0, "mr1u": 5460.0,
            "mr2d": None, "mr2u": None,
            "or1d": None, "or1u": None,
            "or2d": None, "or2u": None,
            "vwap": 5422.0,
            "session_high": None,
            "session_low": None,
            "is_final": False,
            "frozen_at": _utc(2026, 4, 8, 9, 0),
        }])

        sm = SessionManager(_make_config(), db)
        frozen = sm.get_frozen_levels()
        assert frozen["mr_frozen"].get("mr1d") == 5380.0
        assert frozen["mr_frozen"].get("mr1u") == 5460.0


# ============================================================
# Test: Callbacks
# ============================================================

class TestCallbacks:

    def test_freeze_callback_called(self) -> None:
        """Callback registrato deve essere chiamato su freeze."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)

        events_received: list[str] = []

        def on_freeze(event_name: str, levels: dict) -> None:
            events_received.append(event_name)

        sm.register_freeze_callback(on_freeze)
        sm.update(_utc(2026, 4, 8, 9, 0))  # freeze MR

        assert "freeze_mr" in events_received


# ============================================================
# Test: Trading control
# ============================================================

class TestTradingControl:

    def test_trading_blocked_in_halt(self) -> None:
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 8, 22, 0))
        assert sm.is_trading_allowed() is False

    def test_trading_blocked_in_weekend(self) -> None:
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 11, 10, 0))  # Sabato
        assert sm.is_trading_allowed() is False

    def test_trading_allowed_morning(self) -> None:
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 8, 10, 0))
        assert sm.is_trading_allowed() is True

    def test_max_trades_enforced(self) -> None:
        """Dopo max trades per fase, trading bloccato."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 8, 3, 0))  # NIGHT_FULL, max 2

        sm.record_trade()
        sm.record_trade()
        assert sm.is_trading_allowed() is False

    def test_trades_reset_on_phase_change(self) -> None:
        """Cambio fase resetta contatore trades."""
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)

        sm.update(_utc(2026, 4, 8, 3, 0))  # NIGHT_FULL
        sm.record_trade()
        sm.record_trade()
        assert sm.is_trading_allowed() is False

        sm.update(_utc(2026, 4, 8, 9, 0))  # MORNING_EU
        assert sm.is_trading_allowed() is True  # reset


# ============================================================
# Test: Status
# ============================================================

class TestStatus:

    def test_get_status(self) -> None:
        db = _make_db_mock()
        sm = SessionManager(_make_config(), db)
        sm.update(_utc(2026, 4, 8, 10, 0))

        status = sm.get_status()
        assert status["current_phase"] == "morning_eu"
        assert status["trading_allowed"] is True
        assert "MR_FROZEN" in status["active_levels"]
        assert isinstance(status["events_fired_today"], list)
