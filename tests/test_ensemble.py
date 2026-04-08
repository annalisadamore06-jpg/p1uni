"""
Test per MLEnsemble - degraded mode, NaN check, voting, velocita.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.ml.model_ensemble import (
    MLEnsemble,
    _ModelSlot,
    _vote_majority,
    _vote_weighted,
    _vote_unanimous,
)


# ============================================================
# Helpers
# ============================================================

def _make_config(tmp_path: Path, voting: str = "weighted") -> dict[str, Any]:
    models_dir = tmp_path / "ml_models"
    models_dir.mkdir(exist_ok=True)
    return {
        "_base_dir": str(tmp_path),
        "models": {
            "dir": str(models_dir),
            "bounce_file": "bounce_prob.joblib",
            "regime_file": "regime_persistence.joblib",
            "volatility_file": "volatility_30min.joblib",
            "scaler_file": "scaler.joblib",
            "bounce_weight": 0.50,
            "regime_weight": 0.30,
            "volatility_weight": 0.20,
            "min_confidence": 0.60,
            "voting_strategy": voting,
        },
    }


class _FakeModel:
    """Mock XGBoost model that returns fixed probabilities."""

    def __init__(self, p_class1: float = 0.7) -> None:
        self._p = p_class1

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        return np.array([[1 - self._p, self._p]])


def _create_fake_model_files(tmp_path: Path, bounce_p: float = 0.7, regime_p: float = 0.6, vol_p: float = 0.3) -> None:
    """Crea file .joblib finti con modelli mock."""
    import joblib
    models_dir = tmp_path / "ml_models"
    models_dir.mkdir(exist_ok=True)
    joblib.dump(_FakeModel(bounce_p), models_dir / "bounce_prob.joblib")
    joblib.dump(_FakeModel(regime_p), models_dir / "regime_persistence.joblib")
    joblib.dump(_FakeModel(vol_p), models_dir / "volatility_30min.joblib")
    # Scaler passthrough
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    # Fit su dati dummy (17 feature)
    dummy = np.zeros((2, 17))
    dummy[0] = 1.0
    scaler.fit(dummy)
    joblib.dump(scaler, models_dir / "scaler.joblib")


# ============================================================
# Test: Voting strategies (pure functions)
# ============================================================

class TestVotingStrategies:

    def test_majority_long(self) -> None:
        """2 LONG, 1 SHORT -> LONG."""
        votes = {"A": "LONG", "B": "LONG", "C": "SHORT"}
        assert _vote_majority(votes) == "LONG"

    def test_majority_short(self) -> None:
        """2 SHORT, 1 LONG -> SHORT."""
        votes = {"A": "SHORT", "B": "SHORT", "C": "LONG"}
        assert _vote_majority(votes) == "SHORT"

    def test_majority_stalemate(self) -> None:
        """A=LONG, B=SHORT, C=NEUTRAL -> NEUTRAL (no majority)."""
        votes = {"A": "LONG", "B": "SHORT", "C": "NEUTRAL"}
        assert _vote_majority(votes) == "NEUTRAL"

    def test_unanimous_all_long(self) -> None:
        """Tutti LONG -> LONG."""
        votes = {"A": "LONG", "B": "LONG", "C": "LONG"}
        assert _vote_unanimous(votes) == "LONG"

    def test_unanimous_disagreement(self) -> None:
        """Qualsiasi disaccordo -> NEUTRAL."""
        votes = {"A": "LONG", "B": "LONG", "C": "SHORT"}
        assert _vote_unanimous(votes) == "NEUTRAL"

    def test_weighted_high_prob(self) -> None:
        """Probabilita alte con pesi -> LONG."""
        probs = {"A": 0.8, "B": 0.7, "C": 0.6}
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}
        signal, conf = _vote_weighted(probs, weights)
        assert signal == "LONG"
        assert conf > 0.5

    def test_weighted_low_prob(self) -> None:
        """Probabilita basse con pesi -> SHORT."""
        probs = {"A": 0.2, "B": 0.3, "C": 0.4}
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}
        signal, conf = _vote_weighted(probs, weights)
        assert signal == "SHORT"

    def test_weighted_none_probs_neutral(self) -> None:
        """Tutte prob None -> NEUTRAL."""
        probs = {"A": None, "B": None, "C": None}
        weights = {"A": 0.5, "B": 0.3, "C": 0.2}
        signal, conf = _vote_weighted(probs, weights)
        assert signal == "NEUTRAL"
        assert conf == 0.0


# ============================================================
# Test: Model loading & degraded mode
# ============================================================

class TestModelLoading:

    def test_full_mode_all_models(self, tmp_path: Path) -> None:
        """3/3 modelli OK -> FULL mode."""
        _create_fake_model_files(tmp_path)
        ens = MLEnsemble(_make_config(tmp_path))
        assert ens._mode == "FULL"
        assert ens.is_operational() is True

    def test_degraded_2_missing_one(self, tmp_path: Path) -> None:
        """2/3 modelli OK -> DEGRADED_2 mode."""
        _create_fake_model_files(tmp_path)
        # Rimuovi un modello
        (tmp_path / "ml_models" / "regime_persistence.joblib").unlink()

        ens = MLEnsemble(_make_config(tmp_path))
        assert ens._mode == "DEGRADED_2"
        assert ens.is_operational() is True

        # Peso regime redistribuito
        alive_weights = [s.weight for s in ens._models.values() if s.status == "OK"]
        assert sum(alive_weights) == pytest.approx(1.0, abs=0.01)

    def test_halted_no_models(self, tmp_path: Path) -> None:
        """0/3 modelli -> HALTED."""
        ens = MLEnsemble(_make_config(tmp_path))
        assert ens._mode == "HALTED"
        assert ens.is_operational() is False

    def test_halted_predict_returns_none(self, tmp_path: Path) -> None:
        """HALTED mode: predict() ritorna None."""
        ens = MLEnsemble(_make_config(tmp_path))
        result = ens.predict({"spot": 5420})
        assert result is None


# ============================================================
# Test: NaN check
# ============================================================

class TestNaNCheck:

    def test_nan_in_features_returns_neutral(self, tmp_path: Path) -> None:
        """Feature con NaN -> NEUTRAL forzato."""
        _create_fake_model_files(tmp_path)
        ens = MLEnsemble(_make_config(tmp_path))

        # Mock feature builder che ritorna NaN
        mock_fb = MagicMock()
        mock_fb.build.return_value = {"return_1min": float("nan"), "return_5min": 0.001}
        ens.feature_builder = mock_fb

        result = ens.predict()
        assert result is not None
        assert result["signal"] == "NEUTRAL"
        assert "NaN" in result.get("note", "")

    def test_inf_in_features_returns_neutral(self, tmp_path: Path) -> None:
        """Feature con Inf -> NEUTRAL."""
        _create_fake_model_files(tmp_path)
        ens = MLEnsemble(_make_config(tmp_path))

        mock_fb = MagicMock()
        mock_fb.build.return_value = {"return_1min": float("inf"), "vol_1min": 0.001}
        ens.feature_builder = mock_fb

        result = ens.predict()
        assert result is not None
        assert result["signal"] == "NEUTRAL"

    def test_clean_features_pass(self, tmp_path: Path) -> None:
        """Feature pulite passano."""
        features = np.array([[0.001, 0.002, 0.003, 0.004,
                              1000, 800, 200, 0.1,
                              1000, 800, 0.1,
                              50, 40, 0, 0,
                              0.5, 0]])
        assert not MLEnsemble._has_nan(features)


# ============================================================
# Test: Predict end-to-end
# ============================================================

class TestPredictEndToEnd:

    def test_predict_with_mock_features(self, tmp_path: Path) -> None:
        """Predict con feature dict fornite direttamente."""
        _create_fake_model_files(tmp_path, bounce_p=0.78, regime_p=0.65, vol_p=0.30)
        ens = MLEnsemble(_make_config(tmp_path))

        # Fornisci feature come dict (senza feature_builder)
        features = {
            "return_1min": 0.001, "return_5min": 0.002,
            "vol_1min": 0.003, "vol_5min": 0.004,
            "ofi_buy": 1000, "ofi_sell": 800,
            "ofi_net": 200, "ofi_ratio": 0.1,
            "trade_buy_volume": 1000, "trade_sell_volume": 800,
            "trade_imbalance": 0.1,
            "add_buy_count": 50, "add_sell_count": 40,
            "cancel_buy_count": 0, "cancel_sell_count": 0,
            "gex_proximity": 0.5, "low_vol_regime": 0,
        }
        result = ens.predict(features)

        assert result is not None
        assert result["signal"] in ("LONG", "SHORT", "NEUTRAL")
        assert 0 <= result["confidence"] <= 1.0
        assert result["mode"] == "FULL"
        assert "bounce" in result["votes"]
        assert "regime" in result["votes"]
        assert "volatility" in result["votes"]
        assert result["elapsed_ms"] >= 0

    def test_predict_long_signal(self, tmp_path: Path) -> None:
        """Modelli con alta bounce prob -> LONG."""
        _create_fake_model_files(tmp_path, bounce_p=0.85, regime_p=0.75, vol_p=0.20)
        ens = MLEnsemble(_make_config(tmp_path))

        features = {f: 0.001 for f in [
            "return_1min", "return_5min", "vol_1min", "vol_5min",
            "ofi_buy", "ofi_sell", "ofi_net", "ofi_ratio",
            "trade_buy_volume", "trade_sell_volume", "trade_imbalance",
            "add_buy_count", "add_sell_count",
            "cancel_buy_count", "cancel_sell_count",
            "gex_proximity", "low_vol_regime",
        ]}
        result = ens.predict(features)

        assert result is not None
        assert result["signal"] == "LONG"
        assert result["confidence"] > 0.5

    def test_predict_short_signal(self, tmp_path: Path) -> None:
        """Modelli con bassa bounce prob -> SHORT."""
        _create_fake_model_files(tmp_path, bounce_p=0.20, regime_p=0.30, vol_p=0.80)
        ens = MLEnsemble(_make_config(tmp_path))

        features = {f: 0.001 for f in [
            "return_1min", "return_5min", "vol_1min", "vol_5min",
            "ofi_buy", "ofi_sell", "ofi_net", "ofi_ratio",
            "trade_buy_volume", "trade_sell_volume", "trade_imbalance",
            "add_buy_count", "add_sell_count",
            "cancel_buy_count", "cancel_sell_count",
            "gex_proximity", "low_vol_regime",
        ]}
        result = ens.predict(features)

        assert result is not None
        assert result["signal"] == "SHORT"

    def test_degraded_mode_still_predicts(self, tmp_path: Path) -> None:
        """In DEGRADED_2 mode, predict() funziona con 2 modelli."""
        _create_fake_model_files(tmp_path)
        (tmp_path / "ml_models" / "volatility_30min.joblib").unlink()

        ens = MLEnsemble(_make_config(tmp_path))
        assert ens._mode == "DEGRADED_2"

        features = {f: 0.001 for f in [
            "return_1min", "return_5min", "vol_1min", "vol_5min",
            "ofi_buy", "ofi_sell", "ofi_net", "ofi_ratio",
            "trade_buy_volume", "trade_sell_volume", "trade_imbalance",
            "add_buy_count", "add_sell_count",
            "cancel_buy_count", "cancel_sell_count",
            "gex_proximity", "low_vol_regime",
        ]}
        result = ens.predict(features)
        assert result is not None
        assert result["mode"] == "DEGRADED_2"
        assert result["probabilities"]["volatility"] is None  # mancante


# ============================================================
# Test: Performance
# ============================================================

class TestPerformance:

    def test_inference_under_50ms(self, tmp_path: Path) -> None:
        """L'inferenza completa (feature + 3 modelli) deve completare in < 50ms."""
        _create_fake_model_files(tmp_path)
        ens = MLEnsemble(_make_config(tmp_path))

        features = {f: 0.001 for f in [
            "return_1min", "return_5min", "vol_1min", "vol_5min",
            "ofi_buy", "ofi_sell", "ofi_net", "ofi_ratio",
            "trade_buy_volume", "trade_sell_volume", "trade_imbalance",
            "add_buy_count", "add_sell_count",
            "cancel_buy_count", "cancel_sell_count",
            "gex_proximity", "low_vol_regime",
        ]}

        t0 = time.perf_counter()
        for _ in range(100):
            result = ens.predict(features)
        elapsed_avg = (time.perf_counter() - t0) / 100 * 1000

        assert elapsed_avg < 50, f"Average inference time {elapsed_avg:.1f}ms > 50ms"


# ============================================================
# Test: Voting strategy config
# ============================================================

class TestVotingConfig:

    def test_unanimous_strategy(self, tmp_path: Path) -> None:
        """Voting strategy=unanimous: solo se tutti concordano."""
        _create_fake_model_files(tmp_path, bounce_p=0.8, regime_p=0.8, vol_p=0.2)
        ens = MLEnsemble(_make_config(tmp_path, voting="unanimous"))
        assert ens.voting_strategy == "unanimous"

    def test_majority_strategy(self, tmp_path: Path) -> None:
        _create_fake_model_files(tmp_path)
        ens = MLEnsemble(_make_config(tmp_path, voting="majority"))
        assert ens.voting_strategy == "majority"


# ============================================================
# Test: Status
# ============================================================

class TestStatus:

    def test_get_status(self, tmp_path: Path) -> None:
        _create_fake_model_files(tmp_path)
        ens = MLEnsemble(_make_config(tmp_path))
        status = ens.get_status()

        assert status["mode"] == "FULL"
        assert "bounce" in status["models"]
        assert status["models"]["bounce"]["status"] == "OK"
        assert status["scaler_loaded"] is True
