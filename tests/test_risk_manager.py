"""
Test per RiskManager - circuit breakers, dynamic sizing, pre-flight, persistenza.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.execution.risk_manager import RiskManager


def _make_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "_base_dir": str(tmp_path),
        "risk": {
            "max_daily_loss_pts": 40,
            "max_consec_losses": 3,
            "max_daily_trades": 10,
            "profit_lock_pts": 60,
            "profit_lock_size_factor": 0.5,
            "vol_extreme_threshold": 0.001,
            "base_size": 2,
            "max_size": 5,
            "avg_vol": 0.0004,
            "min_rr_ratio": 1.5,
            "max_exposure_contracts": 3,
            "consec_loss_cooldown_sec": 3600,
            "reset_hour_utc": 22,
        },
    }


# ============================================================
# Test: Circuit Breakers
# ============================================================

class TestCircuitBreakers:

    def test_initial_not_blocked(self, tmp_path: Path) -> None:
        """Inizialmente nessun blocco."""
        rm = RiskManager(_make_config(tmp_path))
        blocked, reason = rm.is_trading_blocked()
        assert blocked is False

    def test_daily_drawdown_blocks(self, tmp_path: Path) -> None:
        """Perdita >= 40 pts blocca il trading."""
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(-20)
        blocked, _ = rm.is_trading_blocked()
        assert blocked is False

        rm.record_trade_result(-21)  # totale -41
        blocked, reason = rm.is_trading_blocked()
        assert blocked is True
        assert "drawdown" in reason.lower() or "Daily" in reason

    def test_consecutive_losses_blocks(self, tmp_path: Path) -> None:
        """3 perdite consecutive bloccano con cooldown."""
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(-2)
        rm.record_trade_result(-2)
        rm.record_trade_result(-2)  # 3 consecutive

        blocked, reason = rm.is_trading_blocked()
        assert blocked is True
        assert "consecutive" in reason.lower() or "Consecutive" in reason

    def test_win_resets_consecutive_losses(self, tmp_path: Path) -> None:
        """Un win resetta la streak di perdite."""
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(-2)
        rm.record_trade_result(-2)
        assert rm.consecutive_losses == 2

        rm.record_trade_result(5)  # win!
        assert rm.consecutive_losses == 0

    def test_max_daily_trades_blocks(self, tmp_path: Path) -> None:
        """Dopo 10 trade, bloccato."""
        rm = RiskManager(_make_config(tmp_path))
        for _ in range(10):
            rm.record_trade_result(1)  # piccoli win

        blocked, reason = rm.is_trading_blocked()
        assert blocked is True
        assert "daily trades" in reason.lower() or "Max daily" in reason

    def test_daily_lock_persists(self, tmp_path: Path) -> None:
        """Una volta bloccato, resta bloccato."""
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(-45)  # supera -40

        blocked1, _ = rm.is_trading_blocked()
        assert blocked1 is True

        # Anche dopo un check successivo
        blocked2, _ = rm.is_trading_blocked()
        assert blocked2 is True


# ============================================================
# Test: Position Size Dinamica
# ============================================================

class TestPositionSizing:

    def test_base_size_normal_confidence(self, tmp_path: Path) -> None:
        """Confidence 0.65-0.75: size = base * 1.0 = 2."""
        rm = RiskManager(_make_config(tmp_path))
        size = rm.calculate_position_size(0.70)
        assert size == 2  # base=2 * conf=1.0

    def test_high_confidence_increases(self, tmp_path: Path) -> None:
        """Confidence > 0.85: size = base * 1.5."""
        rm = RiskManager(_make_config(tmp_path))
        size = rm.calculate_position_size(0.90)
        assert size == 3  # base=2 * 1.5 = 3

    def test_low_confidence_decreases(self, tmp_path: Path) -> None:
        """Confidence < 0.55: size = base * 0.5."""
        rm = RiskManager(_make_config(tmp_path))
        size = rm.calculate_position_size(0.50)
        assert size == 1  # base=2 * 0.5 = 1, min=1

    def test_double_vol_halves_size(self, tmp_path: Path) -> None:
        """Volatilita doppia dimezza la size."""
        rm = RiskManager(_make_config(tmp_path))
        # avg_vol=0.0004, current_vol=0.0008 -> factor=0.5
        size = rm.calculate_position_size(0.70, current_vol=0.0008)
        assert size == 1  # base=2 * conf=1.0 * vol=0.5 = 1

    def test_half_vol_doubles_size(self, tmp_path: Path) -> None:
        """Volatilita meta raddoppia la size (capped a 2.0x)."""
        rm = RiskManager(_make_config(tmp_path))
        # avg_vol=0.0004, current_vol=0.0002 -> factor=2.0
        size = rm.calculate_position_size(0.70, current_vol=0.0002)
        assert size == 4  # base=2 * conf=1.0 * vol=2.0 = 4

    def test_extreme_vol_returns_zero(self, tmp_path: Path) -> None:
        """Volatilita estrema (3x threshold) -> size = 0."""
        rm = RiskManager(_make_config(tmp_path))
        extreme_vol = rm.vol_extreme_threshold * 3.1  # > 3x
        size = rm.calculate_position_size(0.70, current_vol=extreme_vol)
        assert size == 0

    def test_max_size_cap(self, tmp_path: Path) -> None:
        """Size non supera mai max_size."""
        rm = RiskManager(_make_config(tmp_path))
        # Confidence altissima + vol bassissima -> size grande
        size = rm.calculate_position_size(0.95, current_vol=0.0001)
        assert size <= rm.max_size

    def test_blocked_returns_zero(self, tmp_path: Path) -> None:
        """Se trading bloccato, size = 0."""
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(-45)  # blocca
        size = rm.calculate_position_size(0.90)
        assert size == 0

    def test_profit_lock_reduces_size(self, tmp_path: Path) -> None:
        """In profitto >= profit_lock_pts, size ridotta a 50%."""
        rm = RiskManager(_make_config(tmp_path))
        rm.daily_pnl_pts = 65  # > profit_lock=60
        size = rm.calculate_position_size(0.70)
        # base=2 * conf=1.0 * profit_lock=0.5 = 1
        assert size == 1


# ============================================================
# Test: Pre-Flight Check
# ============================================================

class TestPreFlightCheck:

    def test_good_rr_ratio_passes(self, tmp_path: Path) -> None:
        """R/R >= 1.5 passa."""
        rm = RiskManager(_make_config(tmp_path))
        valid, reason = rm.validate_order("LONG", 5420.0, 5410.0, 5440.0)
        # risk=10, reward=20, R/R=2.0
        assert valid is True

    def test_bad_rr_ratio_fails(self, tmp_path: Path) -> None:
        """R/R < 1.5 rifiutato."""
        rm = RiskManager(_make_config(tmp_path))
        valid, reason = rm.validate_order("LONG", 5420.0, 5410.0, 5425.0)
        # risk=10, reward=5, R/R=0.5
        assert valid is False
        assert "R/R" in reason

    def test_max_exposure_blocks(self, tmp_path: Path) -> None:
        """Troppi contratti aperti bloccano."""
        rm = RiskManager(_make_config(tmp_path))
        rm.register_open_position("LONG", 5420, 1)
        rm.register_open_position("SHORT", 5430, 1)
        rm.register_open_position("LONG", 5425, 1)  # 3 = max

        valid, reason = rm.validate_order("SHORT", 5440.0, 5450.0, 5420.0)
        assert valid is False
        assert "exposure" in reason.lower()

    def test_duplicate_side_blocked(self, tmp_path: Path) -> None:
        """Secondo LONG con LONG gia aperto bloccato."""
        rm = RiskManager(_make_config(tmp_path))
        rm.register_open_position("LONG", 5420, 1)

        valid, reason = rm.validate_order("LONG", 5430.0, 5420.0, 5450.0)
        assert valid is False
        assert "Duplicate" in reason

    def test_opposite_side_allowed(self, tmp_path: Path) -> None:
        """SHORT con LONG aperto e' consentito."""
        rm = RiskManager(_make_config(tmp_path))
        rm.register_open_position("LONG", 5420, 1)

        valid, reason = rm.validate_order("SHORT", 5440.0, 5450.0, 5420.0)
        assert valid is True

    def test_zero_risk_blocked(self, tmp_path: Path) -> None:
        """SL == Entry -> risk=0 -> rifiutato."""
        rm = RiskManager(_make_config(tmp_path))
        valid, reason = rm.validate_order("LONG", 5420.0, 5420.0, 5440.0)
        assert valid is False


# ============================================================
# Test: Persistenza
# ============================================================

class TestPersistence:

    def test_state_saved_on_trade(self, tmp_path: Path) -> None:
        """risk_state.json creato dopo un trade."""
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(-5)

        state_file = tmp_path / "data" / "risk_state.json"
        assert state_file.exists()

        data = json.loads(state_file.read_text())
        assert data["daily_pnl_pts"] == -5
        assert data["total_trades_today"] == 1

    def test_state_restored_on_restart(self, tmp_path: Path) -> None:
        """Stato ripristinato al riavvio (stesso giorno)."""
        # Crea stato
        rm1 = RiskManager(_make_config(tmp_path))
        rm1.record_trade_result(-10)
        rm1.record_trade_result(-5)
        rm1.record_trade_result(8)

        # "Riavvia" il risk manager
        rm2 = RiskManager(_make_config(tmp_path))
        assert rm2.daily_pnl_pts == -7  # -10 -5 +8
        assert rm2.total_trades_today == 3
        assert rm2.consecutive_losses == 0  # l'ultimo e' un win

    def test_state_not_restored_next_day(self, tmp_path: Path) -> None:
        """Stato di ieri ignorato (contatori a zero)."""
        # Scrivi stato con data di ieri
        state_file = tmp_path / "data" / "risk_state.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        state_file.write_text(json.dumps({
            "date": yesterday,
            "daily_pnl_pts": -30,
            "total_trades_today": 8,
            "consecutive_losses": 2,
            "winning_trades": 3,
            "losing_trades": 5,
            "daily_peak_pnl_pts": 10,
            "daily_locked": True,
            "lock_reason": "test",
        }))

        rm = RiskManager(_make_config(tmp_path))
        assert rm.daily_pnl_pts == 0  # reset!
        assert rm.total_trades_today == 0
        assert rm._daily_locked is False


# ============================================================
# Test: Daily Reset
# ============================================================

class TestDailyReset:

    def test_reset_clears_all(self, tmp_path: Path) -> None:
        """reset_daily() azzera tutto."""
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(-30)
        rm.record_trade_result(-5)
        rm.record_trade_result(-5)  # 3 consec losses
        rm.register_open_position("LONG", 5420, 1)

        rm.reset_daily()

        assert rm.daily_pnl_pts == 0
        assert rm.consecutive_losses == 0
        assert rm.total_trades_today == 0
        assert rm._daily_locked is False
        assert len(rm._open_positions) == 0

        blocked, _ = rm.is_trading_blocked()
        assert blocked is False


# ============================================================
# Test: Force Close
# ============================================================

class TestForceClose:

    def test_force_close_locks_and_clears(self, tmp_path: Path) -> None:
        """force_close_all() chiude tutto e blocca."""
        rm = RiskManager(_make_config(tmp_path))
        rm.register_open_position("LONG", 5420, 2)
        rm.register_open_position("SHORT", 5440, 1)

        rm.force_close_all("Emergency: system shutdown")

        assert len(rm._open_positions) == 0
        assert rm._daily_locked is True
        blocked, reason = rm.is_trading_blocked()
        assert blocked is True
        assert "FORCE CLOSE" in reason


# ============================================================
# Test: Status
# ============================================================

class TestStatus:

    def test_status_complete(self, tmp_path: Path) -> None:
        rm = RiskManager(_make_config(tmp_path))
        rm.record_trade_result(10)
        rm.record_trade_result(-3)

        status = rm.get_status()
        assert status["daily_pnl_pts"] == 7
        assert status["daily_pnl_usd"] == 350  # 7 * $50
        assert status["total_trades"] == 2
        assert status["trades_remaining"] == 8
        assert status["win_rate"] == 0.5
        assert status["open_positions"] == 0
        assert "remaining_loss_budget_pts" in status
