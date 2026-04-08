"""
=====================================================================
V3.5 PRODUCTION — PAPER TRADING MONITOR
=====================================================================

Run this during Giorni 1-3 del phased rollout.
Connects to the inference engine and logs every signal decision,
including gate blocks, for verification before going live.

Usage:
    python paper_trading_monitor.py

Outputs:
    - Console: real-time signal log with gate status
    - paper_trading_log.jsonl: append-only trade log for analysis
    - paper_trading_summary.json: daily P&L and gate statistics

=====================================================================
"""

import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from live_inference_v35 import (
    V35ProductionEngine, LiveFeatureBuilder, DriftMonitor,
    SignalClass
)

LOG_DIR = Path("D:/ml_ensemble_training/production_models/paper_logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "paper_trading.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("paper")


class PaperTradingSession:
    """
    Simulates live trading without real orders.
    Logs every signal, gate decision, and hypothetical P&L.
    """

    TP_POINTS = 15.0
    SL_POINTS = 4.0
    POINT_VALUE = 50.0  # ES futures: $50 per point

    def __init__(self):
        self.engine = V35ProductionEngine(enable_gates=True)
        self.builder = LiveFeatureBuilder()
        self.monitor = DriftMonitor()

        self.trade_log: List[Dict] = []
        self.gate_blocks: Dict[str, int] = {
            "TIME_GATE": 0, "SIZE_GATE": 0,
            "VIX_GATE": 0, "CONSENSUS_GATE": 0,
        }
        self.signals_generated = 0
        self.trades_taken = 0
        self.trades_blocked = 0
        self.session_start = datetime.now(timezone.utc)

        # Load engine
        self.engine.load()
        log.info("Paper Trading Session started")
        log.info("  Model: V3.5 PRODUCTION (222 GREEN features, 3 seeds)")
        log.info("  Gates: TIME, SIZE, VIX, CONSENSUS — all ACTIVE")
        log.info("  TP=%d pts, SL=%d pts, R/R=%.2f:1",
                 self.TP_POINTS, self.SL_POINTS,
                 self.TP_POINTS / self.SL_POINTS)

    def process_bar(self, bar: Dict) -> Dict:
        """
        Process a single 1-minute bar through the full pipeline.

        Args:
            bar: Dict with market data (spot, zero_gamma, vix, etc.)

        Returns:
            Dict with signal details and gate audit trail
        """
        timestamp = bar.get("timestamp", datetime.now(timezone.utc).isoformat())

        # Build features
        features = self.builder.update(bar)

        # Generate signal
        signal = self.engine.predict(features, timestamp)
        self.signals_generated += 1

        # Build log entry
        entry = {
            "timestamp": timestamp,
            "spot": bar.get("spot", 0),
            "vix": bar.get("vix", 0),
            "hour_utc": bar.get("hour_utc", 0),
            "minutes_since_open": bar.get("minutes_since_open", 0),
            "direction": signal.direction,
            "confidence": signal.confidence,
            "signal_class": signal.signal_class.value,
            "position_size": signal.position_size,
            "original_size": signal.original_size,
            "model_agreement": signal.model_agreement,
            "blocked": signal.blocked,
            "block_reason": signal.block_reason,
            "gates": [],
        }

        # Log gate details
        for gate in signal.gate_results:
            gate_entry = {
                "name": gate.gate_name,
                "passed": gate.passed,
                "action": gate.action,
                "multiplier": gate.size_multiplier,
                "reason": gate.reason,
            }
            entry["gates"].append(gate_entry)
            if not gate.passed:
                self.gate_blocks[gate.gate_name] = self.gate_blocks.get(gate.gate_name, 0) + 1

        # Track trade vs block
        if signal.blocked:
            self.trades_blocked += 1
            log.info("BLOCKED | %s | %s conf=%.2f | %s",
                     signal.direction, signal.signal_class.value,
                     signal.confidence, signal.block_reason)
        elif signal.signal_class != SignalClass.NO_TRADE:
            self.trades_taken += 1
            size_note = ""
            if signal.position_size < signal.original_size:
                size_note = f" (reduced from {signal.original_size:.0%})"
            log.info("SIGNAL  | %s | %s conf=%.2f | size=%.0f%%%s | agree=%.0f%%",
                     signal.direction, signal.signal_class.value,
                     signal.confidence, signal.position_size * 100,
                     size_note, signal.model_agreement * 100)

        # Append to trade log
        self.trade_log.append(entry)

        # Write to JSONL file (append-only)
        with open(LOG_DIR / "paper_trading_log.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        return entry

    def get_session_summary(self) -> Dict:
        """Get summary statistics for the current session."""
        elapsed = (datetime.now(timezone.utc) - self.session_start).total_seconds()

        # Count by signal class
        class_counts = {}
        for entry in self.trade_log:
            cls = entry["signal_class"]
            class_counts[cls] = class_counts.get(cls, 0) + 1

        summary = {
            "session_start": self.session_start.isoformat(),
            "elapsed_seconds": elapsed,
            "signals_generated": self.signals_generated,
            "trades_taken": self.trades_taken,
            "trades_blocked": self.trades_blocked,
            "gate_blocks": dict(self.gate_blocks),
            "signal_classes": class_counts,
            "drift_status": self.monitor.check_drift(),
        }

        return summary

    def print_summary(self):
        """Print a formatted session summary."""
        s = self.get_session_summary()
        elapsed_min = s["elapsed_seconds"] / 60

        log.info("")
        log.info("=" * 60)
        log.info("  PAPER TRADING SESSION SUMMARY")
        log.info("=" * 60)
        log.info("  Duration:       %.1f minutes", elapsed_min)
        log.info("  Bars processed: %d", s["signals_generated"])
        log.info("  Trades taken:   %d", s["trades_taken"])
        log.info("  Trades blocked: %d", s["trades_blocked"])
        log.info("")
        log.info("  GATE BLOCK COUNTS:")
        for gate, count in s["gate_blocks"].items():
            log.info("    %-18s %d blocks", gate, count)
        log.info("")
        log.info("  SIGNAL CLASS DISTRIBUTION:")
        for cls, count in sorted(s["signal_classes"].items()):
            log.info("    %-18s %d", cls, count)
        log.info("")
        log.info("  Drift Monitor: %s", s["drift_status"]["status"])
        log.info("=" * 60)

        # Save summary
        summary_path = LOG_DIR / "paper_trading_summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2, default=str)
        log.info("  Summary saved: %s", summary_path)


# ══════════════════════════════════════════════════════════════════
# DEMO: Simulate a trading session with synthetic bars
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    session = PaperTradingSession()

    # Simulate bars across different hours to verify gates
    test_scenarios = [
        # (hour_utc, mso, vix, description)
        (5, 300, 16.5, "Dead hour — should be BLOCKED"),
        (14, 5, 16.5, "Opening noise — should be SIZE REDUCED"),
        (15, 60, 16.5, "Core RTH — should PASS all gates"),
        (16, 120, 16.5, "Golden hour — should PASS all gates"),
        (18, 240, 16.5, "Power hour — should PASS all gates"),
        (16, 120, 42.0, "VIX spike — should be BLOCKED"),
        (22, 480, 16.5, "Post-close — should be BLOCKED"),
        (23, 540, 16.5, "Late night — should be BLOCKED"),
        (19, 300, 16.5, "Afternoon — should PASS all gates"),
        (20, 360, 16.5, "Pre-close — should PASS all gates"),
    ]

    print("\n" + "=" * 70)
    print("  PAPER TRADING DEMO — Gate Verification")
    print("=" * 70)

    for hour, mso, vix, desc in test_scenarios:
        bar = {
            "spot": 5800.0, "zero_gamma": 5750.0, "call_wall_oi": 100000,
            "put_wall_oi": 80000, "net_gex_oi": 5000000, "delta_rr": 0.05,
            "regime": 1, "call_wall_strike": 5900, "put_wall_strike": 5700,
            "wall_spread": 200, "spot_vs_call_wall": -100, "spot_vs_put_wall": 100,
            "spot_vs_zero_gamma": 50, "minutes_since_open": mso,
            "minutes_to_close": max(0, 480 - mso),
            "mr1d": 5760, "mr1u": 5840, "mr2d": 5720, "mr2u": 5880,
            "or1d": 5740, "or1u": 5860, "vix": vix, "skew": 140,
            "hour_utc": hour, "day_of_week": 2,
            "zero_major_long_gamma": 1000, "zero_major_short_gamma": 500,
            "one_major_long_gamma": 800, "one_major_short_gamma": 400,
            "zero_major_call": 600, "zero_major_put": 300,
            "one_major_call": 500, "one_major_put": 250,
            "dex_flow": 0.3, "gex_flow": 0.4,
            "greek_major_long": 1200, "greek_major_short": 600,
            "iv_r1": 15, "iv_straddle": 20, "point_r1": 50, "mdvs": 30, "odvs": 25,
            "timestamp": f"2026-04-08T{hour:02d}:00:00Z",
        }
        print(f"\n  Scenario: {desc}")
        entry = session.process_bar(bar)

        # Show gate results
        for g in entry["gates"]:
            status = "PASS" if g["passed"] else "BLOCK"
            print(f"    [{status}] {g['name']}: {g['reason']}")

    session.print_summary()
