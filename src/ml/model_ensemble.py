"""
ML Ensemble - Cervello decisionale: carica 3 XGBoost, vota, decide.

ARCHITETTURA:
  market_data -> FeatureBuilder.build() -> feature_vector (17 feature)
      -> NaN check
      -> Model A predict_proba()  \
      -> Model B predict_proba()   |-> VotingEngine -> {signal, confidence, votes}
      -> Model C predict_proba()  /

MODELLI (da HANDOVER_QWEN.md):
  A: bounce_prob.joblib     — Probabilita rimbalzo prezzo (peso 50%)
  B: regime_persistence.joblib — Stabilita regime mercato (peso 30%)
  C: volatility_30min.joblib — Probabilita alta volatilita (peso 20%, INVERTITO)

FORMULA ENSEMBLE:
  overall_conf = bounce * 0.50 + regime * 0.30 + (1 - vol) * 0.20
  Direzione: LONG se bounce_prob > 0.55, SHORT altrimenti

FALLBACK DEGRADATO:
  - 3/3 modelli OK -> FULL mode
  - 2/3 modelli OK -> DEGRADED mode (ricalcola pesi, alert WARNING)
  - 1/3 o 0/3 -> HALTED mode (nessun segnale, alert CRITICO)

VOTING STRATEGIES (config):
  - "majority": classe scelta da >= 2 modelli
  - "weighted": media ponderata delle probabilita
  - "unanimous": segnale solo se tutti concordano

OUTPUT:
  {
      "signal": "LONG" | "SHORT" | "NEUTRAL",
      "confidence": 0.72,
      "votes": {"bounce": "LONG", "regime": "LONG", "volatility": "SHORT"},
      "probabilities": {"bounce": 0.78, "regime": 0.65, "volatility": 0.30},
      "mode": "FULL",
      "timestamp": datetime
  }
"""

from __future__ import annotations

import logging
import math
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger("p1uni.ml.ensemble")


# ============================================================
# Model wrapper: carica un singolo modello con fallback
# ============================================================

class _ModelSlot:
    """Wrapper per un singolo modello XGBoost.

    Gestisce caricamento, fallback e status.
    """
    __slots__ = ("name", "path", "weight", "original_weight", "model", "status", "load_time_ms", "inverted")

    def __init__(self, name: str, path: Path, weight: float, inverted: bool = False) -> None:
        self.name = name
        self.path = path
        self.weight = weight
        self.original_weight = weight  # preserve for mode recalculation
        self.inverted = inverted  # True per volatilita (alta vol = cattivo)
        self.model: Any = None
        self.status: str = "NOT_LOADED"
        self.load_time_ms: float = 0.0

    def load(self) -> bool:
        """Carica il modello da file. Ritorna True se successo."""
        if not self.path.exists():
            self.status = "MISSING"
            logger.warning(f"Model {self.name} MISSING: {self.path}")
            return False

        try:
            import joblib
            t0 = time.perf_counter()
            self.model = joblib.load(self.path)
            self.load_time_ms = (time.perf_counter() - t0) * 1000
            self.status = "OK"
            logger.info(f"Model {self.name} loaded in {self.load_time_ms:.1f}ms from {self.path}")
            return True
        except Exception as e:
            self.status = f"ERROR: {e}"
            logger.error(f"Model {self.name} LOAD ERROR: {e}")
            return False

    def predict_proba(self, features: np.ndarray) -> float | None:
        """Esegue predict_proba e ritorna P(class=1).

        Returns:
            Probabilita classe positiva (0.0-1.0), o None se modello non caricato.
        """
        if self.model is None:
            return None
        try:
            proba = self.model.predict_proba(features)
            p = float(proba[0, 1])  # P(class=1)
            # Se invertito (volatilita): alta prob = cattivo, inverti
            if self.inverted:
                p = 1.0 - p
            return p
        except Exception as e:
            logger.error(f"Model {self.name} predict error: {e}")
            return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "weight": self.weight,
            "inverted": self.inverted,
            "load_time_ms": self.load_time_ms,
        }


# ============================================================
# Voting strategies
# ============================================================

def _vote_majority(votes: dict[str, str]) -> str:
    """Classe scelta da >= 2 modelli. Stallo -> NEUTRAL."""
    counts: dict[str, int] = {}
    for v in votes.values():
        counts[v] = counts.get(v, 0) + 1

    # Cerca maggioranza
    for signal, count in sorted(counts.items(), key=lambda x: -x[1]):
        if count >= 2:
            return signal

    return "NEUTRAL"


def _vote_weighted(probabilities: dict[str, float], weights: dict[str, float]) -> tuple[str, float]:
    """Media ponderata delle probabilita. Ritorna (signal, confidence)."""
    total_weight = sum(weights.get(name, 0) for name in probabilities if probabilities[name] is not None)
    if total_weight <= 0:
        return "NEUTRAL", 0.0

    weighted_sum = 0.0
    for name, prob in probabilities.items():
        if prob is not None:
            w = weights.get(name, 0)
            weighted_sum += prob * w

    confidence = weighted_sum / total_weight

    if confidence > 0.55:
        return "LONG", confidence
    elif confidence < 0.45:
        return "SHORT", 1.0 - confidence
    else:
        return "NEUTRAL", confidence


def _vote_unanimous(votes: dict[str, str]) -> str:
    """Segnale solo se TUTTI i modelli concordano."""
    unique = set(votes.values())
    if len(unique) == 1:
        return unique.pop()
    return "NEUTRAL"


_VOTING_STRATEGIES = {
    "majority": "majority",
    "weighted": "weighted",
    "unanimous": "unanimous",
}


# ============================================================
# MLEnsemble
# ============================================================

class MLEnsemble:
    """Ensemble di 3 modelli XGBoost per generare segnali di trading.

    Carica i modelli all'init, supporta modalita degradata,
    3 strategie di voto, check NaN sulle feature.
    Thread-safe (lock sul caricamento).
    """

    def __init__(self, config: dict[str, Any], feature_builder: Any = None) -> None:
        """
        Args:
            config: Configurazione globale (settings.yaml)
            feature_builder: Istanza FeatureBuilder per costruire le feature.
                             Se None, predict() riceve feature gia pronte.
        """
        models_cfg = config.get("models", {})
        models_dir = Path(models_cfg.get("dir", "ml_models"))

        # Risolvi path relativo
        base_dir = Path(config.get("_base_dir", "."))
        if not models_dir.is_absolute():
            models_dir = base_dir / models_dir

        self.feature_builder = feature_builder
        self.voting_strategy: str = models_cfg.get("voting_strategy", "weighted")
        self.min_confidence: float = float(models_cfg.get("min_confidence", 0.60))

        # Lock per caricamento thread-safe
        self._load_lock = threading.Lock()

        # Scaler
        self._scaler: Any = None
        scaler_path = models_dir / models_cfg.get("scaler_file", "scaler.joblib")
        self._load_scaler(scaler_path)

        # I 3 modelli
        self._models: dict[str, _ModelSlot] = {
            "bounce": _ModelSlot(
                name="bounce",
                path=models_dir / models_cfg.get("bounce_file", "bounce_prob.joblib"),
                weight=float(models_cfg.get("bounce_weight", 0.50)),
                inverted=False,
            ),
            "regime": _ModelSlot(
                name="regime",
                path=models_dir / models_cfg.get("regime_file", "regime_persistence.joblib"),
                weight=float(models_cfg.get("regime_weight", 0.30)),
                inverted=False,
            ),
            "volatility": _ModelSlot(
                name="volatility",
                path=models_dir / models_cfg.get("volatility_file", "volatility_30min.joblib"),
                weight=float(models_cfg.get("volatility_weight", 0.20)),
                inverted=True,  # alta volatilita = cattivo
            ),
        }

        # Carica modelli
        self._load_all_models()

        # Determina mode
        self._mode = self._determine_mode()
        logger.info(f"MLEnsemble initialized: mode={self._mode}, strategy={self.voting_strategy}")

    def _load_scaler(self, path: Path) -> None:
        """Carica lo scaler (StandardScaler o passthrough)."""
        if path.exists():
            try:
                import joblib
                self._scaler = joblib.load(path)
                logger.info(f"Scaler loaded from {path}")
            except Exception as e:
                logger.warning(f"Scaler load failed: {e} (using passthrough)")

    def _load_all_models(self) -> None:
        """Carica tutti i modelli con lock."""
        with self._load_lock:
            for slot in self._models.values():
                slot.load()

    def _determine_mode(self) -> str:
        """Determina la modalita operativa.

        FULL: 3/3 modelli OK
        DEGRADED_2: 2/3 modelli OK (ricalcola pesi)
        DEGRADED_1: 1/3 modelli OK (singolo modello, molto rischioso)
        HALTED: 0 modelli OK (nessun segnale)
        """
        ok_count = sum(1 for s in self._models.values() if s.status == "OK")

        # Restore original weights before recalculating
        for s in self._models.values():
            s.weight = s.original_weight

        if ok_count == 3:
            return "FULL"
        elif ok_count == 2:
            # Ricalcola pesi: distribuisci il peso del mancante tra i sani
            missing = [s for s in self._models.values() if s.status != "OK"]
            alive = [s for s in self._models.values() if s.status == "OK"]
            redistributed = missing[0].weight / len(alive)
            for s in alive:
                s.weight += redistributed
            logger.warning(
                f"DEGRADED MODE: {missing[0].name} missing. "
                f"Weights redistributed: {[(s.name, s.weight) for s in alive]}"
            )
            return f"DEGRADED_2"
        elif ok_count == 1:
            logger.error("DEGRADED_1: Only 1 model available. Signals unreliable.")
            return "DEGRADED_1"
        else:
            logger.critical("HALTED: No models loaded. Trading impossible.")
            return "HALTED"

    # ============================================================
    # Predict
    # ============================================================

    def predict(self, market_data: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Genera una predizione dall'ensemble.

        Args:
            market_data: Dati di mercato grezzi (passati al FeatureBuilder).
                         Se None e feature_builder disponibile, usa build() senza args.

        Returns:
            Dict strutturato con signal/confidence/votes/probabilities/mode,
            o None se impossibile (HALTED, NaN, errore).
        """
        if self._mode == "HALTED":
            logger.warning("Predict called but mode=HALTED (no models)")
            return None

        t0 = time.perf_counter()

        # 1. Costruisci feature vector
        features = self._build_features(market_data)
        if features is None:
            return None

        # 2. Check NaN nel feature vector
        if self._has_nan(features):
            logger.warning("Feature vector contains NaN — returning NEUTRAL")
            return self._make_result("NEUTRAL", 0.0, {}, {}, "NaN in features")

        # 3. Scala features
        scaled = self._scale_features(features)

        # 4. Inferenza su ogni modello sano
        probabilities: dict[str, float | None] = {}
        votes: dict[str, str] = {}

        for name, slot in self._models.items():
            if slot.status != "OK":
                probabilities[name] = None
                continue

            prob = slot.predict_proba(scaled)
            probabilities[name] = prob

            if prob is not None:
                if prob > 0.55:
                    votes[name] = "LONG"
                elif prob < 0.45:
                    votes[name] = "SHORT"
                else:
                    votes[name] = "NEUTRAL"

        # 5. Applica voting strategy
        signal, confidence = self._apply_voting(votes, probabilities)

        elapsed_ms = (time.perf_counter() - t0) * 1000

        logger.info(
            f"Prediction: {signal} conf={confidence:.3f} mode={self._mode} "
            f"votes={votes} probs={probabilities} [{elapsed_ms:.1f}ms]"
        )

        return self._make_result(signal, confidence, votes, probabilities, elapsed_ms=elapsed_ms)

    # ============================================================
    # Feature building
    # ============================================================

    def _build_features(self, market_data: dict[str, Any] | None) -> np.ndarray | None:
        """Costruisce il feature vector tramite FeatureBuilder.

        Se feature_builder non e' disponibile, assume che market_data
        sia gia' un dict di feature (chiave -> valore float).
        """
        try:
            if self.feature_builder is not None:
                # FeatureBuilder.build() ritorna dict[str, float] o None
                feature_dict = self.feature_builder.build()
                if feature_dict is None:
                    logger.warning("FeatureBuilder returned None (insufficient data)")
                    return None
            elif market_data is not None:
                feature_dict = market_data
            else:
                logger.warning("No feature_builder and no market_data")
                return None

            # Converti dict -> numpy array (ordine fisso)
            from src.ml.feature_builder import FeatureBuilder
            feature_names = FeatureBuilder.FEATURE_NAMES
            values = [float(feature_dict.get(name, 0.0)) for name in feature_names]
            return np.array([values])

        except Exception as e:
            logger.error(f"Feature building error: {e}")
            return None

    @staticmethod
    def _has_nan(features: np.ndarray) -> bool:
        """Controlla se il feature vector contiene NaN o Inf."""
        return bool(np.any(np.isnan(features)) or np.any(np.isinf(features)))

    def _scale_features(self, features: np.ndarray) -> np.ndarray:
        """Scala le feature con lo scaler (o passthrough)."""
        if self._scaler is not None:
            try:
                return self._scaler.transform(features)
            except Exception as e:
                logger.warning(f"Scaler failed: {e} (using raw features)")
        return features

    # ============================================================
    # Voting
    # ============================================================

    def _apply_voting(
        self,
        votes: dict[str, str],
        probabilities: dict[str, float | None],
    ) -> tuple[str, float]:
        """Applica la strategia di voto configurata.

        Returns:
            (signal: str, confidence: float)
        """
        if not votes:
            return "NEUTRAL", 0.0

        if self.voting_strategy == "unanimous":
            signal = _vote_unanimous(votes)
            # Confidence: media delle probabilita dei modelli concordanti
            probs = [p for p in probabilities.values() if p is not None]
            confidence = sum(probs) / len(probs) if probs else 0.0
            return signal, confidence

        elif self.voting_strategy == "majority":
            signal = _vote_majority(votes)
            probs = [p for p in probabilities.values() if p is not None]
            confidence = sum(probs) / len(probs) if probs else 0.0
            return signal, confidence

        else:
            # Default: weighted (formula originale)
            weights = {name: slot.weight for name, slot in self._models.items()}
            return _vote_weighted(probabilities, weights)

    # ============================================================
    # Result builder
    # ============================================================

    def _make_result(
        self,
        signal: str,
        confidence: float,
        votes: dict[str, str],
        probabilities: dict[str, float | None],
        note: str = "",
        elapsed_ms: float = 0.0,
    ) -> dict[str, Any]:
        """Costruisce il dizionario di output strutturato."""
        return {
            "signal": signal,
            "confidence": round(confidence, 4),
            "votes": dict(votes),
            "probabilities": {k: round(v, 4) if v is not None else None for k, v in probabilities.items()},
            "mode": self._mode,
            "voting_strategy": self.voting_strategy,
            "timestamp": datetime.now(timezone.utc),
            "elapsed_ms": round(elapsed_ms, 1),
            "note": note,
        }

    # ============================================================
    # Status per monitoring
    # ============================================================

    def get_status(self) -> dict[str, Any]:
        """Stato salute dei modelli per monitoring/dashboard."""
        return {
            "mode": self._mode,
            "voting_strategy": self.voting_strategy,
            "min_confidence": self.min_confidence,
            "models": {name: slot.to_dict() for name, slot in self._models.items()},
            "scaler_loaded": self._scaler is not None,
        }

    def is_operational(self) -> bool:
        """True se almeno 2 modelli sono OK (FULL o DEGRADED_2)."""
        return self._mode in ("FULL", "DEGRADED_2")
