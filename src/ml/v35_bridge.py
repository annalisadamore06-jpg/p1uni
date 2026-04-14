"""
V3.5 Bridge — Integra il motore V35ProductionEngine nel sistema P1UNI.

Il V3.5 usa 222 feature GREEN calcolate da GEX/Greeks/Orderflow (dal DB gold),
NON dai trade live Databento. Questo bridge:
1. Legge le ultime feature GEX dal DB gold (ml_gold.duckdb) in read-only
2. Calcola le feature derivate (spot_vs_zero_gamma, wall_spread, etc.)
3. Aggiunge feature temporali (minutes_since_open, hour_utc, etc.)
4. Passa il vettore al V35ProductionEngine con i 4 Risk Gates
5. Ritorna il segnale nel formato atteso dal SignalEngine

NOTA: il DB gold e' separato dal DB live (p1uni_live.duckdb).
Il gold viene letto in read-only, il live viene scritto dai trade Databento.
"""

from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

logger = logging.getLogger("p1uni.ml.v35_bridge")


class V35Bridge:
    """Bridge tra P1UNI e il motore V3.5 certificato.

    Carica l'engine V3.5, legge feature dal DB gold, produce segnali.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        models_dir = Path(config.get("_base_dir", ".")) / "ml_models" / "v35_production"

        # Carica feature names
        feat_path = models_dir / "feature_names_v35_production.json"
        with open(feat_path) as f:
            self.feature_names: list[str] = json.load(f)
        logger.info(f"V3.5 Bridge: {len(self.feature_names)} features loaded")

        # Carica engine V3.5
        from ml_models.v35_production.live_inference_v35 import V35ProductionEngine
        self.engine = V35ProductionEngine(models_dir=models_dir, enable_gates=True)
        self.engine.load()

        # DB gold path (read-only per le feature GEX)
        self.gold_db_path: str = config.get("database", {}).get(
            "gold_path",
            r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
        )

        # Cache ultimo vettore feature
        self._last_features: dict[str, float] = {}
        self._last_update: float = 0

    def predict(self, current_time: datetime | None = None) -> dict[str, Any] | None:
        """Genera un segnale usando il motore V3.5.

        1. Legge le ultime feature dal DB gold
        2. Calcola feature derivate e temporali
        3. Passa al V35ProductionEngine (con 4 Risk Gates)
        4. Ritorna nel formato atteso dal SignalEngine

        Returns:
            Dict compatibile con SignalEngine.on_tick() o None se impossibile.
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        t0 = time.perf_counter()

        # 1. Costruisci vettore feature
        features = self._build_v35_features(current_time)
        if features is None:
            return None

        # 2. Aggiungi feature temporali
        features["hour_utc"] = float(current_time.hour)
        features["minutes_since_open"] = self._calc_minutes_since_open(current_time)
        features["minutes_to_close"] = max(0, 22 * 60 - (current_time.hour * 60 + current_time.minute))

        # Aggiungi VIX per il gate (da feature o default)
        if "vix" not in features or features.get("vix", 0) == 0:
            features["vix"] = 18.0  # default conservativo

        # 3. Fill missing con 0
        full_features = {name: features.get(name, 0.0) for name in self.feature_names}

        # 4. Predict con V3.5 engine
        timestamp_str = current_time.isoformat()
        signal = self.engine.predict(full_features, timestamp=timestamp_str)

        elapsed_ms = (time.perf_counter() - t0) * 1000

        # 5. Converti nel formato P1UNI SignalEngine
        direction_map = {"UP": "LONG", "DOWN": "SHORT", "FLAT": "NEUTRAL"}
        result = {
            "signal": direction_map.get(signal.direction, "NEUTRAL"),
            "confidence": signal.confidence,
            "mode": "V35_FULL" if not signal.blocked else "V35_BLOCKED",
            "votes": {
                "seed42": "LONG" if signal.raw_probability > 0.5 else "SHORT",
                "agreement": signal.model_agreement,
            },
            "probabilities": {
                "raw": signal.raw_probability,
                "confidence": signal.confidence,
            },
            "signal_class": signal.signal_class.value,
            "position_size_fraction": signal.position_size,
            "blocked": signal.blocked,
            "block_reason": signal.block_reason,
            "gate_results": [
                {"gate": g.gate_name, "action": g.action, "reason": g.reason}
                for g in signal.gate_results
            ],
            "elapsed_ms": round(elapsed_ms, 1),
        }

        if signal.blocked:
            result["signal"] = "NEUTRAL"

        logger.info(
            f"V3.5: {result['signal']} conf={signal.confidence:.3f} "
            f"class={signal.signal_class.value} blocked={signal.blocked} "
            f"[{elapsed_ms:.1f}ms]"
        )

        return result

    def _build_v35_features(self, now: datetime) -> dict[str, float] | None:
        """Legge le ultime feature dal DB gold (read-only).

        Unisce dati da: gex_summary, greeks_summary, orderflow.
        """
        conn = None
        try:
            conn = duckdb.connect(self.gold_db_path, read_only=True)
            features: dict[str, float] = {}

            # GEX summary (ultimo record ES)
            try:
                gex = conn.execute("""
                    SELECT zero_gamma, call_wall_oi, put_wall_oi, net_gex_oi,
                           delta_rr, spot
                    FROM gex_summary
                    WHERE ticker='ES' AND hub='classic'
                    ORDER BY ts_utc DESC LIMIT 1
                """).fetchdf()
                if not gex.empty:
                    for col in gex.columns:
                        val = gex.iloc[0][col]
                        if val is not None and str(val) != "nan":
                            features[col] = float(val)
            except Exception as e:
                logger.debug(f"GEX query: {e}")

            # Greeks summary (ultimo per ogni tipo)
            try:
                for greek_type in ("delta", "gamma", "vanna", "charm"):
                    gr = conn.execute(f"""
                        SELECT major_positive, major_negative, major_long_gamma, major_short_gamma
                        FROM greeks_summary
                        WHERE ticker='ES' AND greek_type='{greek_type}'
                        ORDER BY ts_utc DESC LIMIT 1
                    """).fetchdf()
                    if not gr.empty:
                        prefix = f"zero_{greek_type}_" if greek_type != "delta" else ""
                        for col in gr.columns:
                            val = gr.iloc[0][col]
                            if val is not None and str(val) != "nan":
                                features[f"{prefix}{col}"] = float(val)
            except Exception as e:
                logger.debug(f"Greeks query: {e}")

            # Orderflow (ultimo record ES)
            try:
                of = conn.execute("""
                    SELECT * FROM orderflow
                    WHERE ticker='ES'
                    ORDER BY ts_utc DESC LIMIT 1
                """).fetchdf()
                if not of.empty:
                    for col in of.columns:
                        if col not in ("ts_utc", "source", "ticker"):
                            val = of.iloc[0][col]
                            if val is not None and str(val) != "nan":
                                features[col] = float(val)
            except Exception as e:
                logger.debug(f"Orderflow query: {e}")

            # Feature derivate
            spot = features.get("spot", 0)
            zg = features.get("zero_gamma", 0)
            cw = features.get("call_wall_oi", 0)
            pw = features.get("put_wall_oi", 0)

            if spot > 0 and zg > 0:
                features["spot_vs_zero_gamma"] = (spot - zg) / zg
            if spot > 0 and cw > 0:
                features["spot_vs_call_wall"] = (spot - cw) / cw
            if spot > 0 and pw > 0:
                features["spot_vs_put_wall"] = (spot - pw) / pw
            if cw > 0 and pw > 0:
                features["wall_spread"] = cw - pw

            if features:
                self._last_features = features
                self._last_update = time.monotonic()
                return features

            # Fallback a cache
            if self._last_features and (time.monotonic() - self._last_update) < 300:
                logger.warning("Using cached features (DB read failed)")
                return self._last_features

            logger.warning("No features available from DB gold")
            return None

        except Exception as e:
            logger.error(f"V3.5 feature build error: {e}")
            if self._last_features:
                return self._last_features
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    @staticmethod
    def _calc_minutes_since_open(now: datetime) -> float:
        """Minuti dall'apertura sessione US (14:30 UTC = 09:30 ET)."""
        now_min = now.hour * 60 + now.minute
        us_open = 14 * 60 + 30
        return max(0.0, float(now_min - us_open))

    def get_status(self) -> dict[str, Any]:
        return {
            "engine": "V3.5_PRODUCTION",
            "features_count": len(self.feature_names),
            "models_loaded": len(self.engine.models),
            "gates_enabled": self.engine.enable_gates,
            "emergency_halt": self.engine.emergency_halt,
            "last_features_count": len(self._last_features),
        }

    def is_operational(self) -> bool:
        return self.engine._loaded and len(self.engine.models) == 3
