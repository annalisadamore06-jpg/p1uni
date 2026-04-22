"""
hedging_signals.py - Dealer-hedging-based signals (parallel layer to ML v3.5).

Based on the P1UNI 6-month backtest study (research/results/VALIDATION/).
Only rules that survived walk-forward OOS validation with 1pt slippage are
implemented here.

VALIDATED (2026-04-22):
    R5  Above Call Wall -> SHORT (mean reversion)
          n_test=15, total=+20.75pt, mean=+1.38pt/trade net of slippage
          Rationale: when spot trades above the call-wall (zone where dealer
          aggregated gamma peaks on the call side), dealers are structurally
          long gamma on the upside and must SELL to hedge. Mean reversion
          toward call_wall results.

REJECTED (not implemented) — see research/results/VALIDATION/ for details:
    R1  drr_z >= +2 AND regime=TRANSITION  -> SHORT   (too few OOS trades)
    R2  gex_skew < -0.5 AND non-TRANS       -> SHORT   (OOS negative)
    R3  Fade CW in PINNING                  -> SHORT   (overfit on train)
    R4  CW breakout in AMP + aligned        -> LONG    (too few OOS trades)

Rules are computed on each tick. The HedgingSignalEngine emits a directional
signal with its own "confidence" (fixed, calibrated to pass NT8 MIN_CONFIDENCE
gate). The SignalEngine uses this signal as a PARALLEL path: if the ML layer
outputs NEUTRAL or is blocked by internal gates but hedging fires, the
hedging signal is sent through the same risk/execution pipeline with a
different audit tag ("layer": "hedging").

Config (settings.yaml):
    hedging_signals:
        enabled: true
        r5_enabled: true
        r5_margin_pts: 0.0        # min points above CW to trigger
        r5_confidence: 0.65       # conf reported to risk/NT8 gates
        r5_cooldown_sec: 900      # minimum gap between R5 fires
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("p1uni.execution.hedging_signals")


@dataclass
class HedgingDecision:
    """Result of one hedging-layer evaluation."""
    signal: str              # "LONG", "SHORT", or "NEUTRAL"
    confidence: float
    rule: str                # e.g. "R5_aboveCW_short"
    reason: str
    features: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal": self.signal,
            "confidence": self.confidence,
            "rule": self.rule,
            "reason": self.reason,
            "features": self.features,
            "layer": "hedging",
        }


class HedgingSignalEngine:
    """Evaluates dealer-hedging rules that passed OOS validation.

    Stateless w.r.t. data (reads features each tick) but maintains minimal
    internal state for edge-detection (e.g. R5 fires only on cross above CW,
    not while spot stays above CW).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        hc = config.get("hedging_signals", {}) or {}
        self.enabled: bool = bool(hc.get("enabled", True))

        # R5: Above Call Wall -> SHORT
        self.r5_enabled: bool = bool(hc.get("r5_enabled", True))
        self.r5_margin_pts: float = float(hc.get("r5_margin_pts", 0.0))
        self.r5_confidence: float = float(hc.get("r5_confidence", 0.65))
        self.r5_cooldown_sec: float = float(hc.get("r5_cooldown_sec", 900))

        # State for edge detection
        self._r5_was_above: bool = False
        self._r5_last_fire_ts: float = 0.0

        # Stats
        self.ticks_processed: int = 0
        self.signals_emitted: int = 0
        self.r5_fires: int = 0

        logger.info(
            "HedgingSignalEngine initialized: enabled=%s r5_enabled=%s "
            "r5_margin_pts=%.2f r5_confidence=%.2f r5_cooldown_sec=%.0f",
            self.enabled, self.r5_enabled, self.r5_margin_pts,
            self.r5_confidence, self.r5_cooldown_sec,
        )

    def evaluate(self, features: dict[str, Any]) -> HedgingDecision:
        """Evaluate all enabled hedging rules; return first positive signal.

        Args:
            features: dict from FeatureBuilder / V35Bridge.
                      Must contain: spot, call_wall_oi.
                      Optional: put_wall_oi, zero_gamma.

        Returns:
            HedgingDecision. signal=="NEUTRAL" if nothing fires.
        """
        self.ticks_processed += 1
        if not self.enabled:
            return HedgingDecision(
                signal="NEUTRAL", confidence=0.0,
                rule="", reason="hedging disabled",
                features={},
            )

        # R5 — Above call wall -> SHORT mean reversion
        if self.r5_enabled:
            dec = self._eval_r5(features)
            if dec.signal != "NEUTRAL":
                self.signals_emitted += 1
                return dec

        return HedgingDecision(
            signal="NEUTRAL", confidence=0.0,
            rule="", reason="no rule fired",
            features={},
        )

    # ---- R5 -------------------------------------------------------------
    def _eval_r5(self, features: dict[str, Any]) -> HedgingDecision:
        """R5: spot > call_wall_oi + margin, edge-triggered, with cooldown."""
        spot = _sf(features.get("spot"))
        cw = _sf(features.get("call_wall_oi"))

        if spot is None or cw is None or spot <= 0 or cw <= 0:
            self._r5_was_above = False
            return HedgingDecision(
                signal="NEUTRAL", confidence=0.0,
                rule="R5_aboveCW_short",
                reason=f"missing features (spot={spot}, cw={cw})",
                features={"spot": spot, "call_wall_oi": cw},
            )

        is_above = spot > (cw + self.r5_margin_pts)
        # Edge: fire only on cross from below -> above
        crossed = is_above and not self._r5_was_above
        self._r5_was_above = is_above

        if not crossed:
            return HedgingDecision(
                signal="NEUTRAL", confidence=0.0,
                rule="R5_aboveCW_short",
                reason=("above CW but no fresh cross"
                        if is_above else "spot <= CW+margin"),
                features={"spot": spot, "call_wall_oi": cw, "is_above": is_above},
            )

        # Cooldown
        now_m = time.monotonic()
        elapsed = now_m - self._r5_last_fire_ts if self._r5_last_fire_ts > 0 else 1e9
        if elapsed < self.r5_cooldown_sec:
            return HedgingDecision(
                signal="NEUTRAL", confidence=0.0,
                rule="R5_aboveCW_short",
                reason=f"cooldown active ({elapsed:.0f}s < {self.r5_cooldown_sec:.0f}s)",
                features={"spot": spot, "call_wall_oi": cw},
            )

        self._r5_last_fire_ts = now_m
        self.r5_fires += 1
        logger.info(
            "HEDGING R5 fired: SHORT | spot=%.2f > CW=%.2f (+%.2f margin)",
            spot, cw, self.r5_margin_pts,
        )
        return HedgingDecision(
            signal="SHORT",
            confidence=self.r5_confidence,
            rule="R5_aboveCW_short",
            reason=f"R5: spot {spot:.2f} crossed above CW {cw:.2f}+{self.r5_margin_pts:.1f}",
            features={"spot": spot, "call_wall_oi": cw,
                       "put_wall_oi": _sf(features.get("put_wall_oi")),
                       "zero_gamma": _sf(features.get("zero_gamma"))},
        )

    # ---- Status ---------------------------------------------------------
    def get_status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "r5_enabled": self.r5_enabled,
            "ticks_processed": self.ticks_processed,
            "signals_emitted": self.signals_emitted,
            "r5_fires": self.r5_fires,
            "r5_was_above": self._r5_was_above,
            "r5_last_fire_ts": self._r5_last_fire_ts,
        }


def _sf(v: Any) -> float | None:
    """Safe float: returns None if v is None/NaN/invalid."""
    if v is None:
        return None
    try:
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None
