"""
Feature Builder - Trasforma dati live in vettore feature per il modello ML.

CRITICO: se le feature calcolate qui non corrispondono ESATTAMENTE a quelle
usate durante il training, il modello fa previsioni casuali (GIGO).

ARCHITETTURA:
  DB live (trades, gex, ranges) -> build_feature_vector(now)
      -> 1. Recupera ultimi dati validi (GEX, Spot, Range, Trades)
      -> 2. Calcola feature derivate (distanze, ratios, aggressioni)
      -> 3. Gestisci missing data (fill forward o default)
      -> 4. Costruisci numpy array nell'ordine ESATTO del training
      -> 5. Return (array, debug_dict, is_valid)

FEATURE LIST (17 feature dalla pipeline trades + GEX):
  L'ordine DEVE corrispondere a FEATURE_NAMES. Se il training usa un ordine
  diverso, aggiornare FEATURE_NAMES o caricare da feature_names.json.

MISSING DATA STRATEGY:
  - "last_valid": usa l'ultimo valore noto (fill forward)
  - "neutral_default": usa valori neutri (0 per differenze, 0.5 per posizioni)
  - Logga WARNING se un dato e' "stale" (> stale_threshold_sec)

ROLLING STATS:
  Buffer circolari (collections.deque) per medie mobili efficienti.
  Non ricalcola tutto da zero ogni volta.
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from src.core.database import DatabaseManager

logger = logging.getLogger("p1uni.ml.feature_builder")


# ============================================================
# Feature names — ordine ESATTO del training
# ============================================================
# Se il training salva un feature_names.json, caricarlo con load_feature_order().
# Altrimenti usa questo ordine hardcoded (da HANDOVER_QWEN.md).

FEATURE_NAMES: list[str] = [
    "return_1min",
    "return_5min",
    "vol_1min",
    "vol_5min",
    "ofi_buy",
    "ofi_sell",
    "ofi_net",
    "ofi_ratio",
    "trade_buy_volume",
    "trade_sell_volume",
    "trade_imbalance",
    "add_buy_count",
    "add_sell_count",
    "cancel_buy_count",
    "cancel_sell_count",
    "gex_proximity",
    "low_vol_regime",
]

# Defaults neutri per ogni feature (quando dato mancante)
NEUTRAL_DEFAULTS: dict[str, float] = {
    "return_1min": 0.0,
    "return_5min": 0.0,
    "vol_1min": 0.0003,      # volatilita media storica approssimata
    "vol_5min": 0.0004,
    "ofi_buy": 0.0,
    "ofi_sell": 0.0,
    "ofi_net": 0.0,
    "ofi_ratio": 0.0,
    "trade_buy_volume": 0.0,
    "trade_sell_volume": 0.0,
    "trade_imbalance": 0.0,
    "add_buy_count": 0.0,
    "add_sell_count": 0.0,
    "cancel_buy_count": 0.0,  # placeholder (non disponibile da trades)
    "cancel_sell_count": 0.0,
    "gex_proximity": 0.0,
    "low_vol_regime": 0.0,
    # Feature estese (se training le usa)
    "spot_vs_zero_gamma": 0.0,
    "zero_charm": 0.0,
    "minutes_since_open": 0.0,
    "range_position": 0.5,    # centro del range
    "delta_volume_aggression": 0.0,
}


# ============================================================
# Rolling Buffer per statistiche mobili
# ============================================================

class RollingBuffer:
    """Buffer circolare efficiente per calcolo rolling stats.

    Usa deque con maxlen per O(1) append e O(n) stats.
    Molto piu efficiente di ricalcolare da DataFrame ogni volta.
    """
    __slots__ = ("_buf", "_name")

    def __init__(self, maxlen: int, name: str = "") -> None:
        self._buf: deque[float] = deque(maxlen=maxlen)
        self._name = name

    def append(self, value: float) -> None:
        """Aggiungi un valore (scarta il piu vecchio se pieno)."""
        if not math.isnan(value) and not math.isinf(value):
            self._buf.append(value)

    def mean(self) -> float:
        """Media. Ritorna 0.0 se vuoto."""
        if not self._buf:
            return 0.0
        return sum(self._buf) / len(self._buf)

    def std(self) -> float:
        """Deviazione standard. Ritorna 0.0 se < 2 valori."""
        n = len(self._buf)
        if n < 2:
            return 0.0
        m = self.mean()
        var = sum((x - m) ** 2 for x in self._buf) / (n - 1)
        return math.sqrt(var)

    def sum(self) -> float:
        return sum(self._buf)

    def count(self) -> int:
        return len(self._buf)

    def last(self) -> float | None:
        return self._buf[-1] if self._buf else None

    def clear(self) -> None:
        self._buf.clear()


# ============================================================
# FeatureBuilder
# ============================================================

class FeatureBuilder:
    """Costruisce il vettore feature per il modello ML in tempo reale.

    IMPORTANT: FEATURE_NAMES definisce l'ordine esatto delle colonne.
    Se il training usa un ordine diverso, caricare da feature_names.json.
    """

    # Class-level per compatibilita con model_ensemble.py
    FEATURE_NAMES = FEATURE_NAMES

    def __init__(
        self,
        config: dict[str, Any],
        db: DatabaseManager,
        session_manager: Any = None,
    ) -> None:
        self.config = config
        self.db = db
        self.session_manager = session_manager

        exec_cfg = config.get("execution", {})
        self.window_min: int = int(exec_cfg.get("feature_window_min", 60))
        self.stale_threshold_sec: float = float(config.get("ml", {}).get("stale_threshold_sec", 10.0))

        # Feature order: carica da file se esiste, altrimenti usa default
        self._feature_names: list[str] = list(FEATURE_NAMES)
        self._load_feature_order(config)

        # GEX cache path
        base_dir = Path(config.get("_base_dir", "."))
        self._gex_cache_path = base_dir / "data" / "cache" / "gex_cache.json"

        # Rolling buffers per calcoli efficienti
        # 60 trade per ~1 min, 300 per ~5 min (a ~5 trade/sec per ES)
        self._returns_1m = RollingBuffer(60, "returns_1m")
        self._returns_5m = RollingBuffer(300, "returns_5m")
        self._buy_sizes = RollingBuffer(300, "buy_sizes")
        self._sell_sizes = RollingBuffer(300, "sell_sizes")
        self._buy_counts = RollingBuffer(300, "buy_counts")
        self._sell_counts = RollingBuffer(300, "sell_counts")
        self._aggression_buf = RollingBuffer(60, "aggression_60s")

        # Last valid values (fill forward per dati mancanti)
        self._last_valid: dict[str, tuple[float, float]] = {}  # {name: (value, timestamp_mono)}

        # Stato
        self._last_spot: float | None = None
        self._stale_count: int = 0

    def _load_feature_order(self, config: dict[str, Any]) -> None:
        """Carica l'ordine delle feature da feature_names.json se esiste."""
        base_dir = Path(config.get("_base_dir", "."))
        fn_path = base_dir / "ml_models" / "feature_names.json"
        if fn_path.exists():
            try:
                loaded = json.loads(fn_path.read_text())
                if isinstance(loaded, list) and len(loaded) > 0:
                    self._feature_names = loaded
                    logger.info(f"Feature order loaded from {fn_path}: {len(loaded)} features")
                    return
            except Exception as e:
                logger.warning(f"Failed to load feature_names.json: {e}")

        logger.info(f"Using default feature order: {len(self._feature_names)} features")

    # ============================================================
    # build() — entry point principale
    # ============================================================

    def build(self) -> dict[str, float] | None:
        """Costruisce il vettore feature dagli ultimi dati nel DB.

        Returns:
            Dict {feature_name: value} con tutte le feature, o None se dati insufficienti.
            Il dict rispetta FEATURE_NAMES come chiavi.
        """
        try:
            now = datetime.now(timezone.utc)

            # 1. Carica dati recenti dal DB
            trades_data = self._load_recent_trades(now)
            gex_data = self._load_gex_data()
            range_data = self._load_range_data(now)

            # 2. Aggiorna rolling buffers con i nuovi trade
            if trades_data is not None:
                self._update_rolling_buffers(trades_data)
            else:
                logger.debug("No recent trades for feature building")

            # 3. Calcola ogni feature
            features: dict[str, float] = {}
            stale_features: list[str] = []

            # Returns & Volatility (dai rolling buffer)
            features["return_1min"] = self._returns_1m.mean()
            features["return_5min"] = self._returns_5m.mean()
            features["vol_1min"] = self._returns_1m.std()
            features["vol_5min"] = self._returns_5m.std()

            # Order Flow Imbalance
            ofi_buy = self._buy_sizes.sum()
            ofi_sell = self._sell_sizes.sum()
            ofi_net = ofi_buy - ofi_sell
            ofi_denom = ofi_buy + ofi_sell + 1.0  # +1 evita div/0
            features["ofi_buy"] = ofi_buy
            features["ofi_sell"] = ofi_sell
            features["ofi_net"] = ofi_net
            features["ofi_ratio"] = ofi_net / ofi_denom

            # Trade volumes (proxy = ofi)
            features["trade_buy_volume"] = ofi_buy
            features["trade_sell_volume"] = ofi_sell
            features["trade_imbalance"] = ofi_net / ofi_denom

            # Trade counts
            features["add_buy_count"] = float(self._buy_counts.sum())
            features["add_sell_count"] = float(self._sell_counts.sum())

            # Placeholder (non disponibili da trades schema)
            features["cancel_buy_count"] = 0.0
            features["cancel_sell_count"] = 0.0

            # GEX Proximity
            features["gex_proximity"] = self._calc_gex_proximity(gex_data)

            # Low Vol Regime
            features["low_vol_regime"] = 1.0 if features["vol_5min"] < 0.0003 else 0.0

            # --- Feature estese (se nel feature order) ---
            if "spot_vs_zero_gamma" in self._feature_names:
                features["spot_vs_zero_gamma"] = self._calc_spot_vs_zero_gamma(gex_data)

            if "zero_charm" in self._feature_names:
                features["zero_charm"] = self._get_with_fallback("zero_charm", gex_data, 0.0)

            if "minutes_since_open" in self._feature_names:
                features["minutes_since_open"] = self._calc_minutes_since_open(now)

            if "range_position" in self._feature_names:
                features["range_position"] = self._calc_range_position(range_data)

            if "delta_volume_aggression" in self._feature_names:
                features["delta_volume_aggression"] = self._calc_delta_aggression()

            # 4. Applica fill-forward per valori mancanti
            mono_now = time.monotonic()
            for name in self._feature_names:
                val = features.get(name)

                if val is None or math.isnan(val) or math.isinf(val):
                    # Prova last valid
                    cached = self._last_valid.get(name)
                    if cached is not None:
                        old_val, old_ts = cached
                        age_sec = mono_now - old_ts
                        if age_sec > self.stale_threshold_sec:
                            stale_features.append(f"{name} ({age_sec:.0f}s stale)")
                        features[name] = old_val
                    else:
                        # Neutral default
                        features[name] = NEUTRAL_DEFAULTS.get(name, 0.0)
                else:
                    # Aggiorna cache last valid
                    self._last_valid[name] = (val, mono_now)

            # 5. Sanity check: nessun NaN/Inf
            for name, val in features.items():
                if math.isnan(val) or math.isinf(val):
                    features[name] = NEUTRAL_DEFAULTS.get(name, 0.0)

            # Log stale warnings
            if stale_features:
                self._stale_count += 1
                if self._stale_count <= 5 or self._stale_count % 100 == 0:
                    logger.warning(f"Stale features ({len(stale_features)}): {stale_features[:5]}")

            # Verifica minima: servono almeno trade data
            if self._returns_1m.count() < 5 and self._buy_sizes.count() < 5:
                logger.debug("Insufficient data for features (< 5 trades)")
                return None

            return features

        except Exception as e:
            logger.error(f"Feature build error: {e}")
            return None

    def build_feature_vector(
        self,
        current_time: datetime | None = None,
    ) -> tuple[np.ndarray, dict[str, float], bool] | None:
        """Costruisce il vettore numpy nell'ordine esatto del training.

        Returns:
            Tupla (array 1D, debug_dict, is_valid) o None se fallisce.
            is_valid = False se troppe feature sono stale/default.
        """
        features = self.build()
        if features is None:
            return None

        # Costruisci array nell'ordine ESATTO di _feature_names
        values = [features.get(name, NEUTRAL_DEFAULTS.get(name, 0.0)) for name in self._feature_names]
        array = np.array([values], dtype=np.float64)

        # Conta feature che usano il default (proxy per "validita")
        mono_now = time.monotonic()
        n_stale = 0
        for name in self._feature_names:
            cached = self._last_valid.get(name)
            if cached is not None:
                _, ts = cached
                if mono_now - ts > self.stale_threshold_sec:
                    n_stale += 1

        # is_valid: max 30% stale ammesso
        max_stale = len(self._feature_names) * 0.3
        is_valid = n_stale <= max_stale

        if not is_valid:
            logger.warning(
                f"Feature vector DEGRADED: {n_stale}/{len(self._feature_names)} stale "
                f"(threshold: {max_stale:.0f})"
            )

        return array, features, is_valid

    # ============================================================
    # Data loading dal DB
    # ============================================================

    def _load_recent_trades(self, now: datetime) -> list[dict[str, Any]] | None:
        """Carica gli ultimi N minuti di trade dal DB (read-only)."""
        try:
            cutoff = now - timedelta(minutes=self.window_min)
            df = self.db.execute_read(
                "SELECT ts_event, price, size, side FROM trades_live "
                "WHERE ts_event > ? ORDER BY ts_event",
                [cutoff],
            )
            if df.empty:
                return None
            return df.to_dict("records")
        except Exception as e:
            logger.error(f"Failed to load trades: {e}")
            return None

    def _load_gex_data(self) -> dict[str, Any]:
        """Carica gli ultimi livelli GEX dalla cache o dal DB."""
        # Prima: cache file (piu fresco, aggiornato dall'adapter_ws)
        try:
            if self._gex_cache_path.exists():
                data = json.loads(self._gex_cache_path.read_text())
                return data
        except Exception:
            pass

        # Fallback: DB (ultimo record gex_summary)
        try:
            df = self.db.execute_read(
                """SELECT spot, zero_gamma, call_wall_vol, call_wall_oi,
                          put_wall_vol, put_wall_oi, net_gex_vol, net_gex_oi
                   FROM gex_summary
                   WHERE ticker = 'ES'
                   ORDER BY ts_utc DESC LIMIT 1"""
            )
            if not df.empty:
                return df.iloc[0].to_dict()
        except Exception:
            pass

        return {}

    def _load_range_data(self, now: datetime) -> dict[str, Any]:
        """Carica gli ultimi range (running high/low) dal DB."""
        try:
            df = self.db.execute_read(
                """SELECT running_high, running_low, running_vwap, spot,
                          mr1d, mr1u, or1d, or1u
                   FROM intraday_ranges_stream
                   WHERE session_date = ? AND ticker = 'ES'
                   ORDER BY ts_utc DESC LIMIT 1""",
                [now.date()],
            )
            if not df.empty:
                return df.iloc[0].to_dict()
        except Exception:
            pass
        return {}

    # ============================================================
    # Rolling buffer update
    # ============================================================

    def _update_rolling_buffers(self, trades: list[dict[str, Any]]) -> None:
        """Aggiorna i buffer circolari con i nuovi trade."""
        for trade in trades:
            price = trade.get("price")
            size = trade.get("size", 0)
            side = trade.get("side", "")

            if price is None or price <= 0:
                continue

            # Returns (pct change)
            if self._last_spot is not None and self._last_spot > 0:
                pct = (price - self._last_spot) / self._last_spot
                self._returns_1m.append(pct)
                self._returns_5m.append(pct)
            self._last_spot = price

            # Buy/sell sizes
            if side == "B":
                self._buy_sizes.append(float(size))
                self._buy_counts.append(1.0)
            elif side == "A":
                self._sell_sizes.append(float(size))
                self._sell_counts.append(1.0)

            # Aggression buffer (buy - sell per trade)
            if side == "B":
                self._aggression_buf.append(float(size))
            else:
                self._aggression_buf.append(-float(size))

    # ============================================================
    # Feature derivate
    # ============================================================

    def _calc_gex_proximity(self, gex_data: dict[str, Any]) -> float:
        """Prossimita al livello GEX piu vicino.

        Formula: 1 / (1 + min_distance_to_any_gex_level)
        """
        spot = self._last_spot
        if spot is None or spot <= 0:
            return 0.0

        levels: list[float] = []
        for key in ("zero_gamma", "call_wall_vol", "call_wall_oi",
                     "put_wall_vol", "put_wall_oi"):
            val = gex_data.get(key)
            if val is not None:
                try:
                    f = float(val)
                    if f > 0 and not math.isnan(f) and not math.isinf(f):
                        levels.append(f)
                except (ValueError, TypeError):
                    pass

        if not levels:
            # Fallback a config statico
            static = self.config.get("gex_threshold_levels", [])
            levels = [float(x) for x in static if float(x) > 0]

        if not levels:
            return 0.0

        min_dist = min(abs(spot - level) for level in levels)
        return 1.0 / (1.0 + min_dist)

    def _calc_spot_vs_zero_gamma(self, gex_data: dict[str, Any]) -> float:
        """Distanza percentuale tra spot e Zero Gamma.

        Positivo = spot sopra zero gamma (bullish territory)
        Negativo = spot sotto zero gamma (bearish territory)
        """
        spot = self._last_spot
        zg = gex_data.get("zero_gamma")
        if spot is None or spot <= 0 or zg is None:
            return 0.0
        try:
            zg_f = float(zg)
            if zg_f <= 0 or math.isnan(zg_f):
                return 0.0
            return (spot - zg_f) / zg_f
        except (ValueError, TypeError):
            return 0.0

    def _calc_minutes_since_open(self, now: datetime) -> float:
        """Minuti dall'apertura della sessione corrente.

        Usa session_manager se disponibile, altrimenti approssima:
        - US session open: 14:30 UTC (09:30 ET)
        - ES futures open: 23:00 UTC (dom) o 00:00 UTC
        """
        if self.session_manager is not None:
            try:
                phase = self.session_manager.get_current_phase(now)
                from src.execution.session_manager import MarketPhase
                # Reference open per fase
                opens = {
                    MarketPhase.MORNING_EU: 9 * 60,    # 09:00 UTC
                    MarketPhase.AFTERNOON_US: 15 * 60 + 30,  # 15:30 UTC
                    MarketPhase.NIGHT_FULL: 2 * 60 + 15,     # 02:15 UTC
                    MarketPhase.NIGHT_EARLY: 0,
                }
                open_min = opens.get(phase, 0)
                now_min = now.hour * 60 + now.minute
                diff = now_min - open_min
                return max(0.0, float(diff))
            except Exception:
                pass

        # Fallback: minuti da 14:30 UTC (US open)
        now_min = now.hour * 60 + now.minute
        us_open = 14 * 60 + 30
        return max(0.0, float(now_min - us_open))

    def _calc_range_position(self, range_data: dict[str, Any]) -> float:
        """Posizione del prezzo nel range corrente.

        0.0 = al minimo (running_low)
        1.0 = al massimo (running_high)
        > 1.0 = breakout sopra
        < 0.0 = breakout sotto
        """
        spot = self._last_spot
        if spot is None or spot <= 0:
            return 0.5  # neutro

        high = range_data.get("running_high")
        low = range_data.get("running_low")

        if high is None or low is None:
            return 0.5

        try:
            h = float(high)
            l = float(low)
        except (ValueError, TypeError):
            return 0.5

        if h <= l or h <= 0 or l <= 0:
            return 0.5

        return (spot - l) / (h - l)

    def _calc_delta_aggression(self) -> float:
        """(Vol Ask - Vol Bid) / (Vol Ask + Vol Bid) su finestra 60s.

        Positivo = buyer dominant. Negativo = seller dominant.
        """
        total = self._aggression_buf.sum()
        n = self._aggression_buf.count()
        if n == 0:
            return 0.0
        abs_total = sum(abs(x) for x in self._aggression_buf._buf)
        if abs_total == 0:
            return 0.0
        return total / abs_total

    def _get_with_fallback(self, name: str, data: dict[str, Any], default: float) -> float:
        """Estrai un valore dal dict, con fallback a default."""
        val = data.get(name)
        if val is None:
            return default
        try:
            f = float(val)
            return f if not math.isnan(f) and not math.isinf(f) else default
        except (ValueError, TypeError):
            return default

    # ============================================================
    # Utilities
    # ============================================================

    def validate_feature_order(self, expected_names: list[str]) -> bool:
        """Verifica che l'ordine delle feature corrisponda a quello atteso.

        Chiamare all'avvio per confrontare con feature_names.json del training.
        """
        if self._feature_names != expected_names:
            logger.error(
                f"FEATURE ORDER MISMATCH!\n"
                f"  Expected: {expected_names}\n"
                f"  Got:      {self._feature_names}"
            )
            return False
        logger.info(f"Feature order validated: {len(self._feature_names)} features match")
        return True

    def get_status(self) -> dict[str, Any]:
        """Stato per monitoring."""
        return {
            "feature_count": len(self._feature_names),
            "feature_names": list(self._feature_names),
            "buffer_sizes": {
                "returns_1m": self._returns_1m.count(),
                "returns_5m": self._returns_5m.count(),
                "buy_sizes": self._buy_sizes.count(),
                "sell_sizes": self._sell_sizes.count(),
            },
            "last_spot": self._last_spot,
            "stale_count": self._stale_count,
            "last_valid_ages": {
                name: round(time.monotonic() - ts, 1)
                for name, (_, ts) in self._last_valid.items()
            },
        }
