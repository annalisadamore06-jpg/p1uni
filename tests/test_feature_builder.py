"""
Test per FeatureBuilder - ordine feature, rolling buffers, missing data, boundaries.
"""

from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from src.ml.feature_builder import (
    FeatureBuilder,
    RollingBuffer,
    FEATURE_NAMES,
    NEUTRAL_DEFAULTS,
)


# ============================================================
# Helpers
# ============================================================

def _make_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "_base_dir": str(tmp_path),
        "execution": {"feature_window_min": 60},
        "ml": {"stale_threshold_sec": 5.0},
        "gex_threshold_levels": [5400, 5500],
        "database": {"path": ":memory:", "threads": 1, "memory_limit": "256MB"},
    }


def _make_trades(n: int = 100, base_price: float = 5420.0) -> list[dict[str, Any]]:
    """Genera N trade finti."""
    trades = []
    for i in range(n):
        trades.append({
            "price": base_price + (i % 5) * 0.25,
            "size": 1 + (i % 10),
            "side": "B" if i % 3 != 0 else "A",
        })
    return trades


def _make_db_with_trades(n: int = 100) -> MagicMock:
    """DB mock che ritorna N trade."""
    db = MagicMock()
    trades = _make_trades(n)
    df = pd.DataFrame(trades)
    df["ts_event"] = [datetime.now(timezone.utc) - timedelta(seconds=n - i) for i in range(n)]
    db.execute_read.return_value = df
    return db


# ============================================================
# Test: RollingBuffer
# ============================================================

class TestRollingBuffer:

    def test_mean(self) -> None:
        buf = RollingBuffer(10)
        for v in [1, 2, 3, 4, 5]:
            buf.append(v)
        assert buf.mean() == 3.0

    def test_std(self) -> None:
        buf = RollingBuffer(100)
        for v in [2, 4, 4, 4, 5, 5, 7, 9]:
            buf.append(v)
        assert abs(buf.std() - 2.0) < 0.1  # std ~= 2.0

    def test_maxlen_evicts(self) -> None:
        buf = RollingBuffer(3)
        for v in [1, 2, 3, 4, 5]:
            buf.append(v)
        assert buf.count() == 3
        assert buf.mean() == 4.0  # [3, 4, 5]

    def test_empty(self) -> None:
        buf = RollingBuffer(10)
        assert buf.mean() == 0.0
        assert buf.std() == 0.0
        assert buf.last() is None

    def test_nan_ignored(self) -> None:
        """NaN non deve entrare nel buffer."""
        buf = RollingBuffer(10)
        buf.append(1.0)
        buf.append(float("nan"))
        buf.append(3.0)
        assert buf.count() == 2
        assert buf.mean() == 2.0

    def test_inf_ignored(self) -> None:
        buf = RollingBuffer(10)
        buf.append(1.0)
        buf.append(float("inf"))
        assert buf.count() == 1


# ============================================================
# Test: Feature order
# ============================================================

class TestFeatureOrder:

    def test_feature_names_no_duplicates(self) -> None:
        """Nessun duplicato in FEATURE_NAMES."""
        assert len(FEATURE_NAMES) == len(set(FEATURE_NAMES))

    def test_build_feature_vector_order(self, tmp_path: Path) -> None:
        """L'array numpy deve avere lo stesso ordine di FEATURE_NAMES."""
        db = _make_db_with_trades(100)
        fb = FeatureBuilder(_make_config(tmp_path), db)

        # Popola buffers
        trades = _make_trades(100)
        fb._update_rolling_buffers(trades)

        result = fb.build_feature_vector()
        assert result is not None

        array, debug_dict, is_valid = result
        assert array.shape == (1, len(FEATURE_NAMES))

        # Ogni feature nel dict deve matchare l'array
        for i, name in enumerate(FEATURE_NAMES):
            assert name in debug_dict, f"Missing feature: {name}"
            # Il valore nel dict deve corrispondere all'array
            assert abs(array[0, i] - debug_dict[name]) < 1e-10, \
                f"Mismatch for {name}: array={array[0, i]}, dict={debug_dict[name]}"

    def test_load_custom_feature_order(self, tmp_path: Path) -> None:
        """Se feature_names.json esiste, usare quello."""
        models_dir = tmp_path / "ml_models"
        models_dir.mkdir(exist_ok=True)
        custom = ["feat_a", "feat_b", "feat_c"]
        (models_dir / "feature_names.json").write_text(json.dumps(custom))

        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)

        assert fb._feature_names == custom

    def test_validate_feature_order(self, tmp_path: Path) -> None:
        """validate_feature_order() deve matchare."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)

        assert fb.validate_feature_order(FEATURE_NAMES) is True
        assert fb.validate_feature_order(["wrong", "order"]) is False


# ============================================================
# Test: Missing data (fill forward)
# ============================================================

class TestMissingData:

    def test_gex_missing_uses_default(self, tmp_path: Path) -> None:
        """Se GEX non disponibile, gex_proximity usa default (0.0)."""
        db = _make_db_with_trades(100)
        config = _make_config(tmp_path)
        config["gex_threshold_levels"] = []  # nessun livello GEX

        fb = FeatureBuilder(config, db)
        trades = _make_trades(50)
        fb._update_rolling_buffers(trades)

        features = fb.build()
        assert features is not None
        assert features["gex_proximity"] == 0.0

    def test_no_nan_in_output(self, tmp_path: Path) -> None:
        """L'output non deve MAI contenere NaN."""
        db = _make_db_with_trades(100)
        fb = FeatureBuilder(_make_config(tmp_path), db)
        trades = _make_trades(50)
        fb._update_rolling_buffers(trades)

        features = fb.build()
        assert features is not None
        for name, val in features.items():
            assert not math.isnan(val), f"NaN in feature {name}"
            assert not math.isinf(val), f"Inf in feature {name}"

    def test_no_nan_in_vector(self, tmp_path: Path) -> None:
        """Il vettore numpy non deve contenere NaN."""
        db = _make_db_with_trades(100)
        fb = FeatureBuilder(_make_config(tmp_path), db)
        trades = _make_trades(50)
        fb._update_rolling_buffers(trades)

        result = fb.build_feature_vector()
        assert result is not None
        array, _, _ = result
        assert not np.any(np.isnan(array)), "NaN in feature vector!"
        assert not np.any(np.isinf(array)), "Inf in feature vector!"

    def test_insufficient_trades_returns_none(self, tmp_path: Path) -> None:
        """Meno di 5 trade -> build() ritorna None."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)

        # Solo 3 trade
        fb._update_rolling_buffers(_make_trades(3))
        result = fb.build()
        assert result is None


# ============================================================
# Test: Feature derivate - boundary conditions
# ============================================================

class TestFeatureDerivatives:

    def test_spot_on_zero_gamma(self, tmp_path: Path) -> None:
        """Spot esattamente su Zero Gamma -> spot_vs_zero_gamma = 0.0."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5400.0

        gex = {"zero_gamma": 5400.0}
        assert fb._calc_spot_vs_zero_gamma(gex) == 0.0

    def test_spot_above_zero_gamma(self, tmp_path: Path) -> None:
        """Spot sopra Zero Gamma -> positivo."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5450.0

        gex = {"zero_gamma": 5400.0}
        result = fb._calc_spot_vs_zero_gamma(gex)
        assert result > 0

    def test_spot_below_zero_gamma(self, tmp_path: Path) -> None:
        """Spot sotto Zero Gamma -> negativo."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5350.0

        gex = {"zero_gamma": 5400.0}
        result = fb._calc_spot_vs_zero_gamma(gex)
        assert result < 0

    def test_range_position_at_low(self, tmp_path: Path) -> None:
        """Prezzo al minimo del range -> 0.0."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5400.0

        result = fb._calc_range_position({"running_high": 5450.0, "running_low": 5400.0})
        assert result == pytest.approx(0.0)

    def test_range_position_at_high(self, tmp_path: Path) -> None:
        """Prezzo al massimo del range -> 1.0."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5450.0

        result = fb._calc_range_position({"running_high": 5450.0, "running_low": 5400.0})
        assert result == pytest.approx(1.0)

    def test_range_position_breakout_above(self, tmp_path: Path) -> None:
        """Prezzo sopra il range -> > 1.0."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5500.0

        result = fb._calc_range_position({"running_high": 5450.0, "running_low": 5400.0})
        assert result > 1.0

    def test_range_position_breakout_below(self, tmp_path: Path) -> None:
        """Prezzo sotto il range -> < 0.0."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5350.0

        result = fb._calc_range_position({"running_high": 5450.0, "running_low": 5400.0})
        assert result < 0.0

    def test_gex_proximity_close(self, tmp_path: Path) -> None:
        """Spot vicino a un livello GEX -> gex_proximity alta."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5401.0  # 1 punto da zero_gamma

        gex = {"zero_gamma": 5400.0}
        result = fb._calc_gex_proximity(gex)
        assert result > 0.3  # 1/(1+1) = 0.5

    def test_gex_proximity_far(self, tmp_path: Path) -> None:
        """Spot lontano da tutti i livelli -> gex_proximity bassa."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        fb._last_spot = 5000.0  # 400 punti da tutto

        gex = {"zero_gamma": 5400.0}
        result = fb._calc_gex_proximity(gex)
        assert result < 0.01

    def test_delta_aggression_buyers(self, tmp_path: Path) -> None:
        """Solo buyer -> aggression positiva."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        for _ in range(10):
            fb._aggression_buf.append(5.0)  # buyer
        assert fb._calc_delta_aggression() > 0

    def test_delta_aggression_sellers(self, tmp_path: Path) -> None:
        """Solo seller -> aggression negativa."""
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)
        for _ in range(10):
            fb._aggression_buf.append(-5.0)  # seller
        assert fb._calc_delta_aggression() < 0


# ============================================================
# Test: Status
# ============================================================

class TestStatus:

    def test_get_status(self, tmp_path: Path) -> None:
        db = MagicMock()
        db.execute_read.return_value = pd.DataFrame()
        fb = FeatureBuilder(_make_config(tmp_path), db)

        status = fb.get_status()
        assert status["feature_count"] == len(FEATURE_NAMES)
        assert "buffer_sizes" in status
        assert "last_spot" in status
