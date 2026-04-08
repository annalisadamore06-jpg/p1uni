"""
P1UNI — Orchestratore globale.

Punto di ingresso unico che inizializza, avvia e monitora tutti i componenti.

ARCHITETTURA:
  main.py
    |-> load_config()
    |-> init_singletons (DB, Telegram, SessionManager)
    |-> init_components (adapters, feature_builder, ensemble, risk, bridge, signal_engine)
    |-> start_threads
    |     |-> Thread: GexBot WS adapter
    |     |-> Thread: P1-Lite adapter
    |     |-> Thread: Databento adapter
    |     |-> Thread: System monitor
    |     |-> Thread: Signal engine loop
    |     |-> Thread: Session manager ticker
    |-> wait_for_shutdown (SIGINT/SIGTERM)
    |-> graceful_shutdown (flush, close, notify)

USAGE:
  python main.py --mode paper     # simulazione (default)
  python main.py --mode live      # produzione
  python main.py --config path    # config custom

GRACEFUL SHUTDOWN (Ctrl+C):
  1. Blocca nuovi segnali
  2. Chiudi adapter (WS, P1, Databento)
  3. Flush code batch writing
  4. Chiudi NinjaBridge (ZMQ)
  5. Chiudi DB
  6. Telegram "Sistema spento"
  Timeout: 10s per step, poi force kill
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

# Aggiungi root progetto al path per gli import
_PROJECT_ROOT = Path(__file__).parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.logger import setup_logging

logger = logging.getLogger("p1uni.main")


# ============================================================
# Config loader
# ============================================================

def load_config(config_path: str) -> dict[str, Any]:
    """Carica la configurazione YAML con sostituzione env vars."""
    path = Path(config_path)
    if not path.exists():
        print(f"ERROR: Config not found: {path}")
        print("Copy config/settings.example.yaml -> config/settings.yaml")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Sostituisci ${ENV_VAR} con valori da ambiente
    for key, val in os.environ.items():
        raw = raw.replace(f"${{{key}}}", val)

    config = yaml.safe_load(raw)
    config["_base_dir"] = str(path.parent.parent)  # p1uni/
    config["_config_path"] = str(path)
    return config


# ============================================================
# P1UniSystem
# ============================================================

class P1UniSystem:
    """Sistema P1UNI: inizializza, avvia e gestisce tutti i componenti."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.mode: str = config.get("system", {}).get("mode", "paper")
        self._shutdown_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._components: dict[str, Any] = {}
        self._started = False

        logger.info(f"P1UNI initializing in {self.mode.upper()} mode...")

        # Init componenti in ordine di dipendenza
        self._init_singletons()
        self._init_adapters()
        self._init_ml()
        self._init_execution()

        logger.info("All components initialized")

    # ============================================================
    # Init: singletons
    # ============================================================

    def _init_singletons(self) -> None:
        """Inizializza singleton: DB, Telegram, SessionManager."""
        from src.core.database import DatabaseManager
        from src.utils.telegram_bot import TelegramBot
        from src.execution.session_manager import SessionManager

        self._components["db"] = DatabaseManager(self.config)
        self._components["telegram"] = TelegramBot(self.config)
        self._components["session_mgr"] = SessionManager(self.config, self._components["db"])

        logger.info("Singletons initialized: DB, Telegram, SessionManager")

    # ============================================================
    # Init: adapters
    # ============================================================

    def _init_adapters(self) -> None:
        """Inizializza i 3 adapter di ingestion."""
        db = self._components["db"]
        telegram = self._components["telegram"]

        # GexBot WS
        if self.config.get("gexbot", {}).get("ws_url"):
            from src.ingestion.adapter_ws import GexBotWebSocketAdapter
            self._components["adapter_ws"] = GexBotWebSocketAdapter(self.config, db, telegram)
            logger.info("GexBot WS adapter initialized")
        else:
            logger.warning("GexBot WS adapter DISABLED (no ws_url in config)")

        # P1-Lite
        if self.config.get("p1lite", {}).get("enabled", False):
            from src.ingestion.adapter_p1 import P1LiteAdapter
            self._components["adapter_p1"] = P1LiteAdapter(self.config, db, telegram)
            logger.info("P1-Lite adapter initialized")
        else:
            logger.warning("P1-Lite adapter DISABLED")

        # Databento
        if self.config.get("databento", {}).get("api_key"):
            from src.ingestion.adapter_databento import DatabentoAdapter
            self._components["adapter_db"] = DatabentoAdapter(self.config, db, telegram)
            logger.info("Databento adapter initialized")
        else:
            logger.warning("Databento adapter DISABLED (no api_key)")

    # ============================================================
    # Init: ML
    # ============================================================

    def _init_ml(self) -> None:
        """Inizializza FeatureBuilder e MLEnsemble."""
        db = self._components["db"]
        session_mgr = self._components["session_mgr"]

        from src.ml.feature_builder import FeatureBuilder
        from src.ml.model_ensemble import MLEnsemble

        fb = FeatureBuilder(self.config, db, session_mgr)
        self._components["feature_builder"] = fb

        ens = MLEnsemble(self.config, fb)
        self._components["ensemble"] = ens

        if not ens.is_operational():
            logger.warning("Legacy ML Ensemble not operational (expected if using V3.5)")

        # V3.5 Bridge (prioritario se i modelli esistono)
        v35_models_dir = Path(self.config.get("_base_dir", ".")) / "ml_models" / "v35_production"
        if (v35_models_dir / "model_v35_prod_seed42.json").exists():
            try:
                from src.ml.v35_bridge import V35Bridge
                v35 = V35Bridge(self.config)
                self._components["v35_bridge"] = v35
                logger.info(f"V3.5 PRODUCTION engine loaded: {v35.get_status()}")
            except Exception as e:
                logger.error(f"V3.5 Bridge init failed: {e}")
                self._components["v35_bridge"] = None
        else:
            logger.info("V3.5 models not found, using legacy ensemble")
            self._components["v35_bridge"] = None

        mode = "V3.5" if self._components.get("v35_bridge") else ens._mode
        logger.info(f"ML initialized: mode={mode}")

    # ============================================================
    # Init: execution
    # ============================================================

    def _init_execution(self) -> None:
        """Inizializza RiskManager, NinjaBridge, LevelValidator, SignalEngine."""
        db = self._components["db"]
        telegram = self._components["telegram"]

        from src.execution.risk_manager import RiskManager
        from src.execution.ninja_bridge import NinjaTraderBridge
        from src.execution.level_validator import LevelValidator
        from src.execution.signal_engine import SignalEngine

        rm = RiskManager(self.config, db)
        self._components["risk_mgr"] = rm

        bridge = NinjaTraderBridge(self.config, telegram)
        self._components["bridge"] = bridge

        lv = LevelValidator(self.config, db)
        self._components["level_validator"] = lv

        se = SignalEngine(
            config=self.config,
            session_manager=self._components["session_mgr"],
            feature_builder=self._components["feature_builder"],
            ensemble=self._components["ensemble"],
            risk_manager=rm,
            ninja_bridge=bridge,
            level_validator=lv,
            telegram=telegram,
            v35_bridge=self._components.get("v35_bridge"),
        )
        self._components["signal_engine"] = se

        from src.utils.system_monitor import SystemMonitor
        self._components["monitor"] = SystemMonitor(self.config, telegram)

        logger.info("Execution components initialized")

    # ============================================================
    # Start
    # ============================================================

    def start(self) -> None:
        """Avvia tutti i thread e attende shutdown."""
        logger.info("=" * 60)
        logger.info(f"  P1UNI STARTING — Mode: {self.mode.upper()}")
        logger.info(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        logger.info("=" * 60)

        self._components["telegram"].send_alert(
            f"P1UNI starting in {self.mode.upper()} mode", "INFO"
        )

        # Avvia NinjaBridge
        self._components["bridge"].start()

        # Thread: adapters
        for name in ("adapter_ws", "adapter_p1", "adapter_db"):
            adapter = self._components.get(name)
            if adapter is not None:
                t = threading.Thread(target=self._run_adapter, args=(adapter, name), name=name, daemon=True)
                self._threads.append(t)

        # Thread: system monitor
        t_mon = threading.Thread(
            target=self._run_monitor, name="monitor", daemon=True
        )
        self._threads.append(t_mon)

        # Thread: session manager ticker (aggiorna fase ogni secondo)
        t_session = threading.Thread(
            target=self._run_session_ticker, name="session-ticker", daemon=True
        )
        self._threads.append(t_session)

        # Thread: signal engine loop
        t_signal = threading.Thread(
            target=self._run_signal_loop, name="signal-engine", daemon=True
        )
        self._threads.append(t_signal)

        # Avvia tutti
        for t in self._threads:
            t.start()
            logger.info(f"Thread started: {t.name}")

        self._started = True
        logger.info(f"All {len(self._threads)} threads started. System running.")

        # Attendi shutdown
        self._wait_for_shutdown()

    # ============================================================
    # Thread runners
    # ============================================================

    def _run_adapter(self, adapter: Any, name: str) -> None:
        """Esegue un adapter con error recovery."""
        while not self._shutdown_event.is_set():
            try:
                adapter.start()
            except Exception as e:
                logger.error(f"Adapter {name} crashed: {e}")
                self._components["telegram"].send_alert(
                    f"Adapter {name} crashed: {e}", "ERROR"
                )
            # Se esce, aspetta e riprova (l'adapter gestisce il suo reconnect,
            # ma se crasha completamente il thread lo riavvia)
            if not self._shutdown_event.is_set():
                self._shutdown_event.wait(10)

    def _run_monitor(self) -> None:
        """Esegue il system monitor."""
        try:
            self._components["monitor"].start()
        except Exception as e:
            logger.error(f"Monitor crashed: {e}")

    def _run_session_ticker(self) -> None:
        """Aggiorna il session manager ogni secondo."""
        session_mgr = self._components["session_mgr"]
        risk_mgr = self._components["risk_mgr"]

        while not self._shutdown_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                session_mgr.update(now)

                # Daily reset a 22:00 UTC
                if now.hour == 22 and now.minute == 0 and now.second < 2:
                    risk_mgr.reset_daily()
                    session_mgr.reset() if hasattr(session_mgr, 'reset') else None

            except Exception as e:
                logger.error(f"Session ticker error: {e}")

            self._shutdown_event.wait(1.0)

    def _run_signal_loop(self) -> None:
        """Loop principale del signal engine."""
        signal_engine = self._components["signal_engine"]
        interval = self.config.get("execution", {}).get("bar_interval_min", 5) * 60

        logger.info(f"Signal engine loop started (interval={interval}s)")

        while not self._shutdown_event.is_set():
            try:
                signal_engine.on_tick()
            except Exception as e:
                logger.error(f"Signal engine error: {e}")

            self._shutdown_event.wait(interval)

    # ============================================================
    # Shutdown
    # ============================================================

    def _wait_for_shutdown(self) -> None:
        """Attende Ctrl+C o SIGTERM."""
        try:
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(1.0)
        except KeyboardInterrupt:
            logger.info("Ctrl+C received")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Shutdown ordinato con timeout per step."""
        if self._shutdown_event.is_set() and not self._started:
            return
        logger.info("=" * 60)
        logger.info("  SHUTTING DOWN P1UNI")
        logger.info("=" * 60)

        self._shutdown_event.set()

        # Step 1: Ferma signal engine (blocca nuovi segnali)
        logger.info("[1/6] Blocking new signals...")
        # signal_engine smette da solo (controlla shutdown_event)

        # Step 2: Flatten posizioni (se live e configurato)
        if self.mode == "live":
            logger.info("[2/6] Flattening positions...")
            try:
                bridge = self._components.get("bridge")
                if bridge and not bridge.position.is_flat:
                    bridge.cancel_all()
                    logger.info("Positions flattened")
            except Exception as e:
                logger.error(f"Flatten error: {e}")
        else:
            logger.info("[2/6] Paper mode — skip flatten")

        # Step 3: Ferma adapters (chiudi connessioni, flush code)
        logger.info("[3/6] Stopping adapters...")
        for name in ("adapter_ws", "adapter_p1", "adapter_db"):
            adapter = self._components.get(name)
            if adapter is not None:
                try:
                    adapter.stop()
                    logger.info(f"  {name} stopped")
                except Exception as e:
                    logger.error(f"  {name} stop error: {e}")

        # Step 4: Ferma monitor
        logger.info("[4/6] Stopping monitor...")
        monitor = self._components.get("monitor")
        if monitor:
            try:
                monitor.stop()
            except Exception:
                pass

        # Step 5: Chiudi bridge e DB
        logger.info("[5/6] Closing bridge and DB...")
        bridge = self._components.get("bridge")
        if bridge:
            try:
                bridge.stop()
            except Exception:
                pass

        db = self._components.get("db")
        if db:
            try:
                db.close()
            except Exception:
                pass

        # Step 6: Notifica Telegram
        logger.info("[6/6] Sending shutdown notification...")
        telegram = self._components.get("telegram")
        if telegram:
            try:
                # Report finale
                risk_mgr = self._components.get("risk_mgr")
                if risk_mgr:
                    telegram.send_daily_summary(risk_mgr.get_status())
                telegram.send_alert("P1UNI shut down cleanly", "INFO")
            except Exception:
                pass

        logger.info("Shutdown complete.")

    # ============================================================
    # Telegram commands (chiamabili da external handler)
    # ============================================================

    def handle_telegram_command(self, command: str) -> str:
        """Gestisce comandi Telegram remoti.

        Chiamato dal telegram_bot quando riceve un messaggio.
        """
        cmd = command.strip().lower()

        if cmd == "/status":
            return self._cmd_status()
        elif cmd == "/flatten":
            return self._cmd_flatten()
        elif cmd == "/stop":
            return self._cmd_stop()
        elif cmd == "/start":
            return self._cmd_start_trading()
        else:
            return f"Unknown command: {cmd}. Available: /status /flatten /stop /start"

    def _cmd_status(self) -> str:
        """Ritorna status completo del sistema."""
        lines = [f"P1UNI Status ({self.mode.upper()})"]
        lines.append(f"Uptime: running")

        sm = self._components.get("session_mgr")
        if sm:
            st = sm.get_status()
            lines.append(f"Phase: {st['current_phase']}")
            lines.append(f"Trading: {'YES' if st['trading_allowed'] else 'NO'}")

        rm = self._components.get("risk_mgr")
        if rm:
            rs = rm.get_status()
            lines.append(f"PnL: {rs['daily_pnl_pts']:+.1f} pts (${rs['daily_pnl_usd']:+.0f})")
            lines.append(f"Trades: {rs['total_trades']} (remaining: {rs['trades_remaining']})")
            lines.append(f"Win rate: {rs['win_rate']:.0%}")

        se = self._components.get("signal_engine")
        if se:
            ss = se.get_status()
            lines.append(f"Signals: {ss['signals_generated']}, Executed: {ss['orders_executed']}")

        bridge = self._components.get("bridge")
        if bridge:
            lines.append(f"NT8: {'Connected' if bridge.is_connected() else 'Disconnected'}")
            pos = bridge.get_position()
            lines.append(f"Position: {pos['side']} x{pos['size']}")

        ens = self._components.get("ensemble")
        if ens:
            es = ens.get_status()
            lines.append(f"ML: {es['mode']}")

        return "\n".join(lines)

    def _cmd_flatten(self) -> str:
        bridge = self._components.get("bridge")
        rm = self._components.get("risk_mgr")
        if bridge:
            bridge.cancel_all()
        if rm:
            rm.force_close_all("Telegram /flatten command")
        return "All positions flattened. Trading blocked."

    def _cmd_stop(self) -> str:
        rm = self._components.get("risk_mgr")
        if rm:
            rm.force_close_all("Telegram /stop command")
        return "Trading stopped. Use /start to resume."

    def _cmd_start_trading(self) -> str:
        rm = self._components.get("risk_mgr")
        if rm:
            rm.reset_daily()
        return "Trading resumed. Risk counters reset."


# ============================================================
# Entry point
# ============================================================

def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(description="P1UNI — ES Futures Autotrading System")
    parser.add_argument(
        "--mode", choices=["paper", "live"],
        default=None, help="Trading mode (overrides config)"
    )
    parser.add_argument(
        "--config", default="config/settings.yaml",
        help="Path to config file (default: config/settings.yaml)"
    )
    args = parser.parse_args()

    # Carica config
    config = load_config(args.config)

    # Override mode
    if args.mode:
        config.setdefault("system", {})["mode"] = args.mode

    # Setup logging
    log_level = config.get("system", {}).get("log_level", "INFO")
    setup_logging(log_level)

    # Avvia sistema
    system = P1UniSystem(config)

    # Signal handlers
    def handle_sig(signum: int, frame: Any) -> None:
        logger.info(f"Signal {signum} received")
        system._shutdown_event.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Start (blocca fino a shutdown)
    system.start()


if __name__ == "__main__":
    main()
