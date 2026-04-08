"""
Test per SignalEngine - pipeline 6 step, level validation, cooldown, audit.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import numpy as np
import pytest

from src.execution.signal_engine import SignalEngine, DecisionRecord
from src.execution.session_manager import MarketPhase


# ============================================================
# Mock factory
# ============================================================

def _make_session_mgr(phase: MarketPhase = MarketPhase.MORNING_EU, trading: bool = True) -> MagicMock:
    sm = MagicMock()
    sm.get_current_phase.return_value = phase
    sm.is_trading_allowed.return_value = trading
    sm.get_active_levels.return_value = ["MR_FROZEN", "RANGE_LIVE"]
    sm.get_min_confidence.return_value = 0.60
    sm.update.return_value = phase
    sm.record_trade.return_value = None
    return sm


def _make_feature_builder(valid: bool = True, spot: float = 5420.0) -> MagicMock:
    fb = MagicMock()
    features = {name: 0.001 for name in [
        "return_1min", "return_5min", "vol_1min", "vol_5min",
        "ofi_buy", "ofi_sell", "ofi_net", "ofi_ratio",
        "trade_buy_volume", "trade_sell_volume", "trade_imbalance",
        "add_buy_count", "add_sell_count",
        "cancel_buy_count", "cancel_sell_count",
        "gex_proximity", "low_vol_regime",
    ]}
    features["vol_5min"] = 0.0004
    features["range_position"] = 0.5
    features["gex_proximity"] = 0.3
    fb.build_feature_vector.return_value = (np.zeros((1, 17)), features, valid)
    fb._last_spot = spot
    return fb


def _make_ensemble(signal: str = "LONG", confidence: float = 0.72, mode: str = "FULL") -> MagicMock:
    ens = MagicMock()
    ens.predict.return_value = {
        "signal": signal,
        "confidence": confidence,
        "mode": mode,
        "votes": {"bounce": signal, "regime": signal, "volatility": signal},
        "probabilities": {"bounce": confidence, "regime": confidence, "volatility": 1 - confidence},
    }
    return ens


def _make_risk_mgr(blocked: bool = False, size: int = 2) -> MagicMock:
    rm = MagicMock()
    rm.is_trading_blocked.return_value = (blocked, "blocked reason" if blocked else "")
    rm.calculate_position_size.return_value = size
    rm.validate_order.return_value = (True, "OK")
    rm.register_open_position.return_value = None
    return rm


def _make_bridge(success: bool = True) -> MagicMock:
    br = MagicMock()
    br.send_order.return_value = {
        "success": success,
        "order_id": "test-123",
        "fill_price": 5420.0,
        "message": "OK" if success else "Failed",
    }
    br.position = MagicMock()
    br.position.is_flat = True
    br.get_position.return_value = {"side": "FLAT", "size": 0}
    br.cancel_all.return_value = {"success": True}
    return br


def _make_config() -> dict[str, Any]:
    return {
        "signal_engine": {
            "min_confidence_long": 0.60,
            "min_confidence_short": 0.60,
            "cooldown_same_direction_sec": 0,  # disabled per test
            "sl_atr_multiplier": 2.0,
            "tp_atr_multiplier": 3.0,
            "default_sl_pts": 8.0,
            "default_tp_pts": 12.0,
        },
    }


def _make_engine(**kwargs: Any) -> SignalEngine:
    """Crea un SignalEngine con tutti i mock."""
    defaults = {
        "config": _make_config(),
        "session_manager": _make_session_mgr(),
        "feature_builder": _make_feature_builder(),
        "ensemble": _make_ensemble(),
        "risk_manager": _make_risk_mgr(),
        "ninja_bridge": _make_bridge(),
    }
    defaults.update(kwargs)
    return SignalEngine(**defaults)


# ============================================================
# Test: Step 1 - Session Check
# ============================================================

class TestSessionCheck:

    def test_halt_blocks(self) -> None:
        """HALT phase blocca il segnale allo step 1."""
        sm = _make_session_mgr(MarketPhase.HALT, trading=False)
        engine = _make_engine(session_manager=sm)

        result = engine.on_tick()
        assert result is not None
        assert result["step_reached"] == 1
        assert result["action_taken"] == "SKIP"

    def test_weekend_blocks(self) -> None:
        """WEEKEND blocca allo step 1."""
        sm = _make_session_mgr(MarketPhase.WEEKEND, trading=False)
        engine = _make_engine(session_manager=sm)

        result = engine.on_tick()
        assert result["step_reached"] == 1

    def test_morning_allows(self) -> None:
        """MORNING_EU consente il trading."""
        engine = _make_engine()  # default = MORNING_EU, trading=True
        result = engine.on_tick()
        assert result["step_reached"] > 1  # passa almeno step 1


# ============================================================
# Test: Step 2 - Features
# ============================================================

class TestFeatureCheck:

    def test_no_features_blocks(self) -> None:
        """Feature builder None -> step 2 block."""
        fb = MagicMock()
        fb.build_feature_vector.return_value = None
        fb._last_spot = None
        engine = _make_engine(feature_builder=fb)

        result = engine.on_tick()
        assert result["step_reached"] == 2
        assert "None" in result["reason"] or "insufficient" in result["reason"].lower()

    def test_degraded_features_blocks(self) -> None:
        """Features non valide (troppo stale) -> step 2 block."""
        fb = _make_feature_builder(valid=False)
        engine = _make_engine(feature_builder=fb)

        result = engine.on_tick()
        assert result["step_reached"] == 2
        assert "degraded" in result["reason"].lower() or "stale" in result["reason"].lower()


# ============================================================
# Test: Step 3 - ML Prediction
# ============================================================

class TestMLPrediction:

    def test_neutral_signal_skip(self) -> None:
        """ML signal NEUTRAL -> skip."""
        ens = _make_ensemble(signal="NEUTRAL", confidence=0.50)
        engine = _make_engine(ensemble=ens)

        result = engine.on_tick()
        assert result["step_reached"] == 3
        assert "NEUTRAL" in result["reason"]

    def test_low_confidence_skip(self) -> None:
        """Confidence sotto soglia -> skip."""
        ens = _make_ensemble(signal="LONG", confidence=0.45)
        engine = _make_engine(ensemble=ens)

        result = engine.on_tick()
        assert result["step_reached"] == 3
        assert "Confidence" in result["reason"]

    def test_degraded_1_blocks(self) -> None:
        """DEGRADED_1 mode -> blocca (troppo rischioso)."""
        ens = _make_ensemble(signal="LONG", confidence=0.80, mode="DEGRADED_1")
        engine = _make_engine(ensemble=ens)

        result = engine.on_tick()
        assert result["step_reached"] == 3
        assert "DEGRADED_1" in result["reason"]

    def test_night_higher_confidence_needed(self) -> None:
        """In NIGHT_FULL, confidence minima e' 0.70 (non 0.60)."""
        sm = _make_session_mgr(MarketPhase.NIGHT_FULL, trading=True)
        sm.get_min_confidence.return_value = 0.70

        ens = _make_ensemble(signal="LONG", confidence=0.65)
        engine = _make_engine(session_manager=sm, ensemble=ens)

        result = engine.on_tick()
        assert result["step_reached"] == 3
        assert "Confidence" in result["reason"]


# ============================================================
# Test: Step 4 - Level Validation
# ============================================================

class TestLevelValidation:

    def test_long_at_range_top_blocked(self) -> None:
        """LONG quando range_position > 0.95 -> rifiutato."""
        fb = _make_feature_builder()
        fb.build_feature_vector.return_value[1]["range_position"] = 0.98

        engine = _make_engine(feature_builder=fb)

        result = engine.on_tick()
        assert result["step_reached"] == 4
        assert "top of range" in result["reason"].lower()

    def test_short_at_range_bottom_blocked(self) -> None:
        """SHORT quando range_position < 0.05 -> rifiutato."""
        fb = _make_feature_builder()
        fb.build_feature_vector.return_value[1]["range_position"] = 0.02

        ens = _make_ensemble(signal="SHORT", confidence=0.72)
        engine = _make_engine(feature_builder=fb, ensemble=ens)

        result = engine.on_tick()
        assert result["step_reached"] == 4
        assert "bottom of range" in result["reason"].lower()

    def test_normal_range_passes(self) -> None:
        """Range position 0.5 -> passa level validation."""
        engine = _make_engine()  # default range_position = 0.5
        result = engine.on_tick()
        assert result["step_reached"] > 4  # passa step 4


# ============================================================
# Test: Step 5 - Risk Check
# ============================================================

class TestRiskCheck:

    def test_risk_blocked(self) -> None:
        """Risk manager bloccato -> step 5 block."""
        rm = _make_risk_mgr(blocked=True)
        engine = _make_engine(risk_manager=rm)

        result = engine.on_tick()
        assert result["step_reached"] == 5
        assert result["action_taken"] == "BLOCKED"

    def test_size_zero_blocked(self) -> None:
        """Position size = 0 -> blocked."""
        rm = _make_risk_mgr(size=0)
        engine = _make_engine(risk_manager=rm)

        result = engine.on_tick()
        assert result["step_reached"] == 5
        assert "size = 0" in result["reason"].lower() or "Position" in result["reason"]

    def test_existing_position_no_pyramid(self) -> None:
        """Con posizione LONG aperta, no nuovo LONG (no pyramiding)."""
        br = _make_bridge()
        br.position.is_flat = False
        br.get_position.return_value = {"side": "LONG", "size": 2}

        ens = _make_ensemble(signal="LONG", confidence=0.72)
        engine = _make_engine(ninja_bridge=br, ensemble=ens)

        result = engine.on_tick()
        assert result["step_reached"] == 5
        assert "pyramiding" in result["reason"].lower()


# ============================================================
# Test: Step 6 - Execution
# ============================================================

class TestExecution:

    def test_full_pipeline_success(self) -> None:
        """Pipeline completa: segnale valido -> ordine eseguito."""
        engine = _make_engine()
        result = engine.on_tick()

        assert result is not None
        assert result["step_reached"] == 6
        assert result["action_taken"] == "EXECUTE"
        assert result["signal"] == "LONG"
        assert result["size"] > 0
        assert engine.orders_executed == 1

    def test_order_failure(self) -> None:
        """Ordine che fallisce al bridge -> FAILED."""
        br = _make_bridge(success=False)
        engine = _make_engine(ninja_bridge=br)

        result = engine.on_tick()
        assert result["step_reached"] == 6
        assert result["action_taken"] == "FAILED"

    def test_sl_tp_calculated(self) -> None:
        """SL e TP devono essere calcolati."""
        engine = _make_engine()
        result = engine.on_tick()

        assert result["sl"] > 0  # SL calcolato
        assert result["tp"] > 0  # TP calcolato


# ============================================================
# Test: Anti-whipsaw cooldown
# ============================================================

class TestCooldown:

    def test_cooldown_blocks_same_direction(self) -> None:
        """Dopo un trade LONG, cooldown blocca il prossimo LONG."""
        config = _make_config()
        config["signal_engine"]["cooldown_same_direction_sec"] = 300

        engine = _make_engine(config=config)

        # Primo trade
        result1 = engine.on_tick()
        assert result1["action_taken"] == "EXECUTE"

        # Reset bridge per permettere secondo trade
        engine.bridge.position.is_flat = True

        # Secondo trade (stesso LONG, entro cooldown)
        result2 = engine.on_tick()
        assert result2["step_reached"] == 5
        assert "Cooldown" in result2["reason"]

    def test_no_cooldown_opposite_direction(self) -> None:
        """Dopo LONG, un SHORT non e' bloccato dal cooldown."""
        config = _make_config()
        config["signal_engine"]["cooldown_same_direction_sec"] = 300

        engine = _make_engine(config=config)

        # Trade LONG
        engine.on_tick()

        # Cambia segnale a SHORT
        engine.ensemble.predict.return_value["signal"] = "SHORT"
        engine.bridge.position.is_flat = True

        result = engine.on_tick()
        # Non dovrebbe essere bloccato dal cooldown (direzione diversa)
        assert result["step_reached"] >= 5


# ============================================================
# Test: SL/TP Calculation
# ============================================================

class TestSLTP:

    def test_long_sl_below_entry(self) -> None:
        """LONG: SL deve essere sotto il prezzo."""
        engine = _make_engine()
        sl, tp = engine._calculate_stop_loss("LONG", 5420.0, 0.0004)
        assert sl < 5420.0
        assert tp > 5420.0

    def test_short_sl_above_entry(self) -> None:
        """SHORT: SL deve essere sopra il prezzo."""
        engine = _make_engine()
        sl, tp = engine._calculate_stop_loss("SHORT", 5420.0, 0.0004)
        assert sl > 5420.0
        assert tp < 5420.0

    def test_default_sl_minimum(self) -> None:
        """SL non deve essere meno di default_sl_pts."""
        engine = _make_engine()
        sl, tp = engine._calculate_stop_loss("LONG", 5420.0, 0.00001)  # vol bassissima
        assert 5420.0 - sl >= engine.default_sl_pts


# ============================================================
# Test: Audit trail
# ============================================================

class TestAudit:

    def test_decisions_recorded(self) -> None:
        """Ogni tick registra una decisione."""
        engine = _make_engine()
        engine.on_tick()
        engine.on_tick()
        engine.on_tick()

        decisions = engine.get_recent_decisions(10)
        assert len(decisions) == 3

    def test_decision_record_complete(self) -> None:
        """DecisionRecord ha tutti i campi."""
        rec = DecisionRecord()
        d = rec.to_dict()
        assert "timestamp" in d
        assert "step_reached" in d
        assert "signal" in d
        assert "action_taken" in d

    def test_stats(self) -> None:
        engine = _make_engine()
        engine.on_tick()
        status = engine.get_status()
        assert status["ticks_processed"] == 1
        assert status["orders_executed"] == 1
