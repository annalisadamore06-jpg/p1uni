"""
Signal Engine - Coordinatore centrale che chiude il cerchio.

Non fa calcoli pesanti (li delega), ma orchestra la decisione di trading
in una pipeline sequenziale a 6 step. Ogni step puo bloccare il segnale.

PIPELINE (on_tick):
  1. SESSION CHECK   -> session_manager.is_trading_allowed()
  2. BUILD FEATURES  -> feature_builder.build_feature_vector()
  3. ML PREDICTION   -> ensemble.predict(features)
  4. LEVEL VALIDATION -> _validate_level_confluence(signal, price, levels)
  5. RISK CHECK      -> risk_manager.is_trading_blocked() + calculate_position_size()
  6. EXECUTE ORDER   -> ninja_bridge.send_order() + risk_manager.validate_order()

Ogni step logga la decisione (anche se negativa) per audit.

ANTI-WHIPSAW:
  Cooldown configurabile: dopo un trade, attendi N secondi prima di
  prenderne un altro nella stessa direzione.

NO PYRAMIDING:
  Se c'e' gia una posizione aperta, non inviare nuovi ordini
  (tranne FLATTEN per chiudere).

CONFIGURAZIONE (settings.yaml):
  signal_engine:
    min_confidence_long: 0.60
    min_confidence_short: 0.60
    cooldown_same_direction_sec: 300
    sl_atr_multiplier: 2.0
    tp_atr_multiplier: 3.0
    default_sl_pts: 8.0
    default_tp_pts: 12.0
    max_stale_features_pct: 0.30
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from src.execution.session_manager import SessionManager, MarketPhase

logger = logging.getLogger("p1uni.execution.signal_engine")


# ============================================================
# Decision record (audit trail)
# ============================================================

class DecisionRecord:
    """Registra ogni decisione per audit e analisi."""

    __slots__ = (
        "timestamp", "step_reached", "signal", "confidence", "phase",
        "action_taken", "reason", "spot", "size", "sl", "tp",
        "elapsed_ms", "details",
    )

    def __init__(self) -> None:
        self.timestamp: datetime = datetime.now(timezone.utc)
        self.step_reached: int = 0       # 1-6, quanti step completati
        self.signal: str = "NONE"
        self.confidence: float = 0.0
        self.phase: str = ""
        self.action_taken: str = "SKIP"  # SKIP, EXECUTE, BLOCKED
        self.reason: str = ""
        self.spot: float = 0.0
        self.size: int = 0
        self.sl: float = 0.0
        self.tp: float = 0.0
        self.elapsed_ms: float = 0.0
        self.details: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "step_reached": self.step_reached,
            "signal": self.signal,
            "confidence": self.confidence,
            "phase": self.phase,
            "action_taken": self.action_taken,
            "reason": self.reason,
            "spot": self.spot,
            "size": self.size,
            "sl": self.sl,
            "tp": self.tp,
            "elapsed_ms": self.elapsed_ms,
            "details": self.details,
        }


# ============================================================
# SignalEngine
# ============================================================

class SignalEngine:
    """Coordinatore centrale: orchestra la pipeline decisionale a 6 step.

    Riceve componenti gia inizializzati e li coordina.
    Non fa calcoli pesanti, solo logica di orchestrazione.
    """

    def __init__(
        self,
        config: dict[str, Any],
        session_manager: SessionManager,
        feature_builder: Any,       # FeatureBuilder
        ensemble: Any,              # MLEnsemble (legacy) or V35Bridge
        risk_manager: Any,          # RiskManager
        ninja_bridge: Any,          # NinjaTraderBridge
        level_validator: Any = None,  # LevelValidator (opzionale)
        telegram: Any = None,
        v35_bridge: Any = None,     # V35Bridge (se presente, usa V3.5 invece di legacy)
    ) -> None:
        self.config = config
        self.session_mgr = session_manager
        self.feature_builder = feature_builder
        self.ensemble = ensemble
        self.v35_bridge = v35_bridge  # Se non None, usa V3.5 per le predizioni
        self.risk_mgr = risk_manager
        self.bridge = ninja_bridge
        self.level_validator = level_validator
        self.telegram = telegram

        # Config signal engine
        se_cfg = config.get("signal_engine", {})
        self.min_conf_long: float = float(se_cfg.get("min_confidence_long", 0.60))
        self.min_conf_short: float = float(se_cfg.get("min_confidence_short", 0.60))
        self.cooldown_sec: float = float(se_cfg.get("cooldown_same_direction_sec", 300))
        self.sl_atr_mult: float = float(se_cfg.get("sl_atr_multiplier", 2.0))
        self.tp_atr_mult: float = float(se_cfg.get("tp_atr_multiplier", 3.0))
        self.default_sl_pts: float = float(se_cfg.get("default_sl_pts", 8.0))
        self.default_tp_pts: float = float(se_cfg.get("default_tp_pts", 12.0))
        self.max_stale_pct: float = float(se_cfg.get("max_stale_features_pct", 0.30))

        # Anti-whipsaw: {direction: last_trade_monotonic_time}
        self._last_trade_time: dict[str, float] = {}

        # Audit trail (ultime N decisioni)
        self._decisions: list[dict[str, Any]] = []
        self._max_decisions: int = 500

        # Stats
        self.ticks_processed: int = 0
        self.signals_generated: int = 0
        self.orders_executed: int = 0
        self.orders_blocked: int = 0

    # ============================================================
    # on_tick — entry point principale
    # ============================================================

    def on_tick(self, market_data: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Esegue la pipeline decisionale a 6 step.

        Chiamato dal main loop ogni bar_interval (5 min) o ogni tick.

        Args:
            market_data: Dati di mercato correnti (opzionale, per feature builder).

        Returns:
            DecisionRecord.to_dict() con la decisione presa (anche se SKIP).
            None solo in caso di errore critico.
        """
        t0 = time.perf_counter()
        self.ticks_processed += 1
        now = datetime.now(timezone.utc)
        rec = DecisionRecord()
        rec.timestamp = now

        try:
            # === STEP 1: SESSION CHECK ===
            phase = self.session_mgr.get_current_phase(now)
            rec.phase = phase.value
            self.session_mgr.update(now)

            if not self.session_mgr.is_trading_allowed():
                rec.step_reached = 1
                rec.reason = f"Trading not allowed in phase {phase.value}"
                return self._finalize(rec, t0)

            active_levels = self.session_mgr.get_active_levels()
            phase_min_conf = self.session_mgr.get_min_confidence()
            rec.step_reached = 1

            # === STEP 2 + 3: BUILD FEATURES & ML PREDICTION ===
            # Se V3.5 bridge disponibile, usa quello (feature + predict in un unico step)
            if self.v35_bridge is not None:
                prediction = self.v35_bridge.predict(now)
                if prediction is None:
                    rec.step_reached = 3
                    rec.reason = "V3.5 Bridge returned None (no features)"
                    return self._finalize(rec, t0)

                # BUG#16 fix: spot precedenza: Databento > GEX json > 0
                # V35Bridge popola features["spot"] dal gexbot_latest.json
                feature_dict_temp = prediction.get("features", {})
                spot = 0.0
                if self.feature_builder is not None and getattr(self.feature_builder, '_last_spot', None):
                    spot = float(self.feature_builder._last_spot)
                if spot == 0.0:
                    spot = float(feature_dict_temp.get("spot", 0.0))
                rec.spot = spot
                rec.step_reached = 3

                # V3.5 gestisce i gate internamente — check blocked
                if prediction.get("blocked"):
                    rec.step_reached = 3
                    rec.signal = prediction.get("signal", "NEUTRAL")
                    rec.confidence = prediction.get("confidence", 0)
                    rec.reason = f"V3.5 GATE BLOCKED: {prediction.get('block_reason', '')}"
                    rec.details["gate_results"] = prediction.get("gate_results", [])
                    return self._finalize(rec, t0)

                # Initialize feature_dict for V3.5 path (used in later steps)
                feature_dict = prediction.get("features", {})

            else:
                # Legacy path: feature_builder + ensemble separati
                result = self.feature_builder.build_feature_vector(now)
                if result is None:
                    rec.step_reached = 2
                    rec.reason = "Feature builder returned None (insufficient data)"
                    return self._finalize(rec, t0)

                feature_array, feature_dict, is_valid = result
                if not is_valid:
                    rec.step_reached = 2
                    rec.reason = "Features degraded (too many stale values)"
                    rec.details["is_valid"] = False
                    return self._finalize(rec, t0)

                spot = feature_dict.get("ofi_buy", 0)
                if self.feature_builder._last_spot is not None:
                    spot = self.feature_builder._last_spot
                rec.spot = spot
                rec.step_reached = 2

                prediction = self.ensemble.predict(feature_dict)
            if prediction is None:
                rec.step_reached = 3
                rec.reason = "Ensemble returned None (HALTED or error)"
                return self._finalize(rec, t0)

            signal = prediction["signal"]
            confidence = prediction["confidence"]
            mode = prediction["mode"]
            rec.signal = signal
            rec.confidence = confidence
            rec.details["ml_mode"] = mode
            rec.details["votes"] = prediction.get("votes", {})

            # NEUTRAL = nessun trade
            if signal == "NEUTRAL":
                rec.step_reached = 3
                rec.reason = "ML signal is NEUTRAL"
                return self._finalize(rec, t0)

            # Confidence minima (piu alta della fase + soglia per direzione)
            min_conf = max(
                phase_min_conf,
                self.min_conf_long if signal == "LONG" else self.min_conf_short,
            )
            if confidence < min_conf:
                rec.step_reached = 3
                rec.reason = (
                    f"Confidence {confidence:.3f} < min {min_conf:.3f} "
                    f"(phase={phase.value})"
                )
                return self._finalize(rec, t0)

            # DEGRADED_1 mode: blocca (troppo rischioso con 1 solo modello)
            if mode == "DEGRADED_1":
                rec.step_reached = 3
                rec.reason = "Ensemble in DEGRADED_1 mode (only 1 model)"
                return self._finalize(rec, t0)

            rec.step_reached = 3
            self.signals_generated += 1

            # === STEP 4: LEVEL VALIDATION ===
            level_ok, level_reason = self._validate_level_confluence(
                signal, spot, active_levels, feature_dict
            )
            if not level_ok:
                rec.step_reached = 4
                rec.reason = f"Level validation failed: {level_reason}"
                return self._finalize(rec, t0)
            rec.step_reached = 4

            # === STEP 5: RISK CHECK ===
            blocked, block_reason = self.risk_mgr.is_trading_blocked()
            if blocked:
                rec.step_reached = 5
                rec.reason = f"Risk blocked: {block_reason}"
                rec.action_taken = "BLOCKED"
                self.orders_blocked += 1
                return self._finalize(rec, t0)

            # Anti-whipsaw cooldown
            if self._in_cooldown(signal):
                rec.step_reached = 5
                rec.reason = f"Cooldown active for {signal} ({self.cooldown_sec}s)"
                return self._finalize(rec, t0)

            # Posizione gia aperta? No pyramiding.
            if not self.bridge.position.is_flat:
                existing_side = self.bridge.get_position()["side"]
                if existing_side == signal:
                    rec.step_reached = 5
                    rec.reason = f"Already have {existing_side} position (no pyramiding)"
                    return self._finalize(rec, t0)
                # Se direzione opposta: prima chiudi, poi eventualmente riapri
                # Per semplicita: solo chiudi (flatten)
                logger.info(f"Reversing position: {existing_side} -> {signal}")
                self.bridge.cancel_all()

            # Calculate SL/TP
            vol = feature_dict.get("vol_5min", 0.0004)
            sl, tp = self._calculate_stop_loss(signal, spot, vol)
            rec.sl = sl
            rec.tp = tp

            # Position size dinamica
            size = self.risk_mgr.calculate_position_size(confidence, vol)
            if size <= 0:
                rec.step_reached = 5
                rec.reason = "Position size = 0 (risk too high)"
                return self._finalize(rec, t0)
            rec.size = size

            # Pre-flight check
            order_ok, order_reason = self.risk_mgr.validate_order(signal, spot, sl, tp)
            if not order_ok:
                rec.step_reached = 5
                rec.reason = f"Pre-flight failed: {order_reason}"
                rec.action_taken = "BLOCKED"
                self.orders_blocked += 1
                return self._finalize(rec, t0)

            rec.step_reached = 5

            # === STEP 6: EXECUTE ORDER ===
            # Passa raw_probability (avg_proba 0-1) al bridge per NT8 confidence check
            raw_prob = prediction.get("probabilities", {}).get("raw", 0.0)
            order_result = self.bridge.send_order(
                signal, size, sl, tp, price=spot, raw_probability=raw_prob
            )

            if order_result["success"]:
                rec.step_reached = 6
                rec.action_taken = "EXECUTE"
                rec.details["order_id"] = order_result.get("order_id", "")
                rec.details["fill_price"] = order_result.get("fill_price", 0)
                self.orders_executed += 1

                # Registra nel risk manager
                self.risk_mgr.register_open_position(signal, spot, size)
                self.session_mgr.record_trade()

                # Anti-whipsaw: registra tempo
                self._last_trade_time[signal] = time.monotonic()

                # Telegram
                self._send_signal_alert(signal, confidence, spot, size, sl, tp, phase)

                logger.info(
                    f"ORDER EXECUTED: {signal} x{size} @ {spot:.2f} "
                    f"SL={sl:.2f} TP={tp:.2f} conf={confidence:.3f}"
                )
            else:
                rec.step_reached = 6
                rec.action_taken = "FAILED"
                rec.reason = f"Order failed: {order_result.get('message', '')}"
                self.orders_blocked += 1

            return self._finalize(rec, t0)

        except Exception as e:
            logger.error(f"Signal engine error: {e}", exc_info=True)
            rec.reason = f"Exception: {e}"
            return self._finalize(rec, t0)

    # ============================================================
    # Level Validation
    # ============================================================

    def _validate_level_confluence(
        self,
        signal: str,
        spot: float,
        active_levels: list[str],
        features: dict[str, float],
    ) -> tuple[bool, str]:
        """Verifica che il segnale sia coerente con i livelli attivi.

        Regole:
        - LONG sotto resistenza GEX forte (zero gamma wall) -> SKIP
        - SHORT sopra supporto forte -> SKIP
        - Se level_validator presente, delega a lui
        - Altrimenti usa logica semplificata basata su feature

        Returns:
            (ok: bool, reason: str)
        """
        # Se abbiamo il LevelValidator dal signal_engine originale, usalo
        if self.level_validator is not None:
            try:
                ok = self.level_validator.check(spot, signal)
                if not ok:
                    return False, f"LevelValidator rejected {signal} at {spot:.2f}"
            except Exception as e:
                logger.warning(f"LevelValidator error: {e} (allowing trade)")

        # Check basato su feature: gex_proximity
        gex_prox = features.get("gex_proximity", 0)
        if gex_prox > 0.8:
            # Spot molto vicino a un livello GEX: cautela
            logger.info(f"Spot near GEX level (proximity={gex_prox:.2f})")
            # Non blocchiamo, ma logghiamo. Il risk_manager ridurra la size.

        # Check range_position
        range_pos = features.get("range_position", 0.5)
        if signal == "LONG" and range_pos > 0.95:
            return False, f"LONG at top of range (position={range_pos:.2f})"
        if signal == "SHORT" and range_pos < 0.05:
            return False, f"SHORT at bottom of range (position={range_pos:.2f})"

        # Check spot_vs_zero_gamma
        spot_zg = features.get("spot_vs_zero_gamma", 0)
        if signal == "LONG" and spot_zg < -0.02:
            # Spot significativamente sotto zero gamma: territorio bearish
            # Non blocchiamo automaticamente ma logghiamo
            logger.info(f"LONG in bearish territory (spot_vs_zg={spot_zg:.4f})")

        return True, "OK"

    # ============================================================
    # Stop Loss / Take Profit
    # ============================================================

    def _calculate_stop_loss(
        self,
        side: str,
        price: float,
        volatility: float,
    ) -> tuple[float, float]:
        """Calcola SL e TP dinamici basati su volatilita.

        Formula:
        - SL = price -/+ (sl_atr_mult * vol * price) o default_sl_pts
        - TP = price +/- (tp_atr_mult * vol * price) o default_tp_pts

        Usa il maggiore tra ATR-based e default per sicurezza.
        """
        # ATR-based (volatilita come proxy)
        if volatility > 0 and price > 0:
            atr_estimate = volatility * price  # ATR approssimato
            sl_dist = max(atr_estimate * self.sl_atr_mult, self.default_sl_pts)
            tp_dist = max(atr_estimate * self.tp_atr_mult, self.default_tp_pts)
        else:
            sl_dist = self.default_sl_pts
            tp_dist = self.default_tp_pts

        if side == "LONG":
            sl = price - sl_dist
            tp = price + tp_dist
        else:  # SHORT
            sl = price + sl_dist
            tp = price - tp_dist

        return round(sl, 2), round(tp, 2)

    # ============================================================
    # Anti-whipsaw cooldown
    # ============================================================

    def _in_cooldown(self, signal: str) -> bool:
        """True se siamo nel periodo di cooldown per questa direzione."""
        if self.cooldown_sec <= 0:
            return False

        last_time = self._last_trade_time.get(signal)
        if last_time is None:
            return False

        elapsed = time.monotonic() - last_time
        return elapsed < self.cooldown_sec

    # ============================================================
    # Telegram alert
    # ============================================================

    def _send_signal_alert(
        self,
        signal: str, confidence: float, spot: float,
        size: int, sl: float, tp: float, phase: MarketPhase,
    ) -> None:
        """Invia alert Telegram per ordine eseguito."""
        if self.telegram is not None:
            try:
                self.telegram.send_signal(signal, confidence, spot, phase.value)
            except Exception:
                pass

    # ============================================================
    # Finalize decision & audit
    # ============================================================

    def _finalize(self, rec: DecisionRecord, t0: float) -> dict[str, Any]:
        """Finalizza la decisione: logga, registra audit trail."""
        rec.elapsed_ms = (time.perf_counter() - t0) * 1000

        # SEMPRE INFO per le decisioni (serve per monitoring)
        logger.info(
            f"Decision: {rec.action_taken} | signal={rec.signal} conf={rec.confidence:.3f} "
            f"phase={rec.phase} step={rec.step_reached}/6 | {rec.reason} "
            f"[{rec.elapsed_ms:.1f}ms]"
        )

        result = rec.to_dict()
        self._decisions.append(result)
        if len(self._decisions) > self._max_decisions:
            self._decisions = self._decisions[-self._max_decisions:]

        return result

    # ============================================================
    # Status / audit
    # ============================================================

    def get_status(self) -> dict[str, Any]:
        """Snapshot per monitoring."""
        return {
            "ticks_processed": self.ticks_processed,
            "signals_generated": self.signals_generated,
            "orders_executed": self.orders_executed,
            "orders_blocked": self.orders_blocked,
            "cooldown_sec": self.cooldown_sec,
            "last_decision": self._decisions[-1] if self._decisions else None,
        }

    def get_recent_decisions(self, n: int = 20) -> list[dict[str, Any]]:
        """Ultime N decisioni per dashboard/debug."""
        return self._decisions[-n:]
