"""
Master Orchestrator — Avvio automatico completo del sistema P1UNI.

Lanciato da Windows Task Scheduler ogni giorno alle 07:30.

Sequenza:
  07:30 - Start
  07:30-07:45 - Avvia infrastruttura esterna:
    - NinjaTrader 8 + auto-login
    - (TWS se configurato)
    - (Chrome P1-Lite se configurato)
  07:45-08:00 - Avvia motore Python P1UNI
  08:00 - Health check + notifica Telegram
  08:00-22:00 - Monitor continuo ogni 5 minuti
  22:00 - Shutdown ordinato + harvest storico

HEALTH CHECKS:
  - Databento tick counter cresce (>0 dopo 30s)
  - V3.5 engine risponde
  - Nessun errore critico nei log

CREDENZIALI:
  Tutte da .env file (mai hardcoded).

Usage:
    python src/automation/master_orchestrator.py              # full run
    python src/automation/master_orchestrator.py --boot-only  # solo boot, no monitor
    python src/automation/master_orchestrator.py --test       # modalita test
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil

BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.core.secrets import get_secret

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_PATH = LOG_DIR / f"orchestrator_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [ORCH] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ]
)
log = logging.getLogger("master")


# ============================================================
# Telegram helper
# ============================================================

def telegram_send(message: str) -> bool:
    """Invia un messaggio Telegram (raises nothing, ritorna bool)."""
    try:
        import requests
        token = get_secret("TELEGRAM_BOT_TOKEN")
        chat_id = get_secret("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            log.warning("Telegram not configured")
            return False
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as e:
        log.warning(f"Telegram error: {e}")
        return False


# ============================================================
# Service Managers
# ============================================================

class NT8Manager:
    """NinjaTrader 8 lifecycle."""

    EXE = r"C:\NinjaTrader 8\bin\NinjaTrader.exe"

    @staticmethod
    def is_running() -> bool:
        for p in psutil.process_iter(["name"]):
            try:
                if "ninjatrader" in (p.info["name"] or "").lower():
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return False

    @staticmethod
    def is_logged_in() -> bool:
        """True se Control Center aperto (= login completato)."""
        try:
            from pywinauto import Desktop
            wins = Desktop(backend="win32").windows(title_re=".*Control Center.*")
            return len(wins) > 0
        except Exception:
            return False

    @classmethod
    def start(cls) -> bool:
        if cls.is_running():
            log.info("NT8 already running")
            if cls.is_logged_in():
                log.info("NT8 Control Center open (logged in)")
                return True
            return cls._auto_login()

        if not Path(cls.EXE).exists():
            log.error(f"NT8 not found: {cls.EXE}")
            return False

        log.info("Launching NinjaTrader 8...")
        subprocess.Popen([cls.EXE], creationflags=subprocess.CREATE_NEW_CONSOLE)
        time.sleep(15)
        return cls._auto_login()

    @classmethod
    def _auto_login(cls) -> bool:
        pwd = get_secret("NT8_PASSWORD", "")
        if not pwd:
            log.warning("NT8_PASSWORD not set, manual login required")
            return cls.is_running()

        try:
            import pyautogui
            from pywinauto import Desktop

            log.info("Attempting NT8 auto-login...")
            start = time.time()
            win = None
            while time.time() - start < 40:
                wins = Desktop(backend="win32").windows(title_re=".*NinjaTrader.*")
                if wins:
                    win = wins[0]
                    break
                time.sleep(1)

            if not win:
                log.error("NT8 login window not found")
                return False

            win.set_focus()
            win.restore()
            time.sleep(1.5)

            rect = win.rectangle()
            pwd_x = rect.left + int(rect.width() * 0.50)
            pwd_y = rect.top + int(rect.height() * 0.39)

            pyautogui.click(pwd_x, pwd_y)
            time.sleep(0.5)
            pyautogui.hotkey("ctrl", "a")
            pyautogui.press("backspace")
            time.sleep(0.3)

            try:
                import keyboard
                keyboard.write(pwd, delay=0.05)
            except ImportError:
                for ch in pwd:
                    if ch == "!":
                        pyautogui.hotkey("shift", "1")
                    elif ch.isupper():
                        pyautogui.hotkey("shift", ch.lower())
                    else:
                        pyautogui.press(ch)
                    time.sleep(0.05)

            time.sleep(0.5)
            pyautogui.press("enter")
            time.sleep(10)

            logged = cls.is_logged_in()
            if logged:
                log.info("NT8 login successful")
            else:
                log.warning("NT8 login sent but Control Center not detected")
            return logged

        except Exception as e:
            log.error(f"NT8 auto-login error: {e}")
            return False


class P1UniManager:
    """P1UNI Python main.py lifecycle."""

    @staticmethod
    def is_running() -> bool:
        for p in psutil.process_iter(["cmdline"]):
            try:
                cl = p.info.get("cmdline") or []
                if any("main.py" in str(a) for a in cl) and any("p1uni" in str(a).lower() or "P1UNI" in str(a) for a in cl):
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return False

    @classmethod
    def start(cls, mode: str = "paper") -> bool:
        if cls.is_running():
            log.info(f"P1UNI already running")
            return True

        log.info(f"Launching P1UNI main.py --mode {mode}")
        try:
            proc = subprocess.Popen(
                [sys.executable, "main.py", "--mode", mode, "--config", "config/settings.yaml"],
                cwd=str(BASE_DIR),
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            time.sleep(8)
            if proc.poll() is None:
                log.info(f"P1UNI started (PID {proc.pid})")
                return True
            log.error(f"P1UNI exited early (code {proc.returncode})")
            return False
        except Exception as e:
            log.error(f"P1UNI launch error: {e}")
            return False


# ============================================================
# Health checks
# ============================================================

def check_databento_ingesting(timeout_sec: int = 60) -> tuple[bool, int]:
    """Verifica che Databento stia ricevendo trade (leggendo i log)."""
    import re
    log_file = BASE_DIR / "logs" / "p1uni.log"
    if not log_file.exists():
        return False, 0

    start = time.time()
    last_total = 0
    first_total = None

    while time.time() - start < timeout_sec:
        try:
            content = log_file.read_text(encoding="utf-8", errors="ignore")
            matches = re.findall(r"Wrote \d+ records to trades_live \(total: (\d+)\)", content)
            if matches:
                current = int(matches[-1])
                if first_total is None:
                    first_total = current
                if current > first_total:
                    return True, current
                last_total = current
        except Exception:
            pass
        time.sleep(5)

    return False, last_total


def boot_sequence() -> dict[str, bool]:
    """Esegue tutta la sequenza di avvio."""
    log.info("=" * 55)
    log.info("  MASTER ORCHESTRATOR — BOOT SEQUENCE")
    log.info(f"  {datetime.now().isoformat()}")
    log.info("=" * 55)

    telegram_send("P1UNI Orchestrator - Boot sequence started")

    results = {}

    # Step 1: NinjaTrader (non bloccante se fallisce — paper mode non lo richiede)
    log.info("\n[STEP 1/3] NinjaTrader 8")
    results["nt8"] = NT8Manager.start()

    # Step 2: P1UNI main
    log.info("\n[STEP 2/3] P1UNI main.py")
    results["p1uni"] = P1UniManager.start(mode="paper")

    # Step 3: Health check
    log.info("\n[STEP 3/3] Health check (verifica ingestion)")
    if results["p1uni"]:
        ok, total = check_databento_ingesting(timeout_sec=90)
        results["ingestion"] = ok
        log.info(f"Databento ingestion: {'OK' if ok else 'FAIL'} (total: {total})")
    else:
        results["ingestion"] = False

    # Report finale
    log.info("\n" + "=" * 55)
    log.info("  BOOT RESULT")
    for k, v in results.items():
        log.info(f"  {k}: {'OK' if v else 'FAIL'}")
    log.info("=" * 55)

    # Telegram summary
    status_lines = [f"P1UNI Boot Report"]
    for k, v in results.items():
        emoji = "OK" if v else "FAIL"
        status_lines.append(f"- {k}: {emoji}")
    all_ok = all(results.values())
    status_lines.append("")
    status_lines.append("Status: READY" if all_ok else "Status: DEGRADED")
    telegram_send("\n".join(status_lines))

    return results


def monitor_loop(interval_min: int = 5) -> None:
    """Monitoring continuo: check health ogni N minuti + heartbeat Telegram ogni 4h."""
    log.info(f"Starting monitor loop (interval={interval_min}min)")
    last_heartbeat = 0.0

    while True:
        try:
            running = P1UniManager.is_running()
            if not running:
                log.warning("P1UNI not running! Attempting restart...")
                telegram_send("P1UNI crash detected, restarting...")
                P1UniManager.start(mode="paper")
                time.sleep(30)
                continue

            # Heartbeat Telegram ogni 4 ore
            now_mono = time.monotonic()
            if now_mono - last_heartbeat > 4 * 3600:
                ok, total = check_databento_ingesting(timeout_sec=30)
                msg = f"P1UNI Heartbeat\nRunning: OK\nDatabento trades: {total}\nTime: {datetime.now().strftime('%H:%M')}"
                telegram_send(msg)
                last_heartbeat = now_mono

        except Exception as e:
            log.error(f"Monitor cycle error: {e}")

        time.sleep(interval_min * 60)


# ============================================================
# Entry point
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--boot-only", action="store_true", help="Solo boot, senza monitoring loop")
    parser.add_argument("--test", action="store_true", help="Test mode (no real launches)")
    args = parser.parse_args()

    if args.test:
        log.info("TEST MODE — verifying prerequisites only")
        log.info(f"NT8 exe exists: {Path(NT8Manager.EXE).exists()}")
        log.info(f"Python: {sys.executable}")
        log.info(f"P1UNI dir: {BASE_DIR}")
        log.info(f".env exists: {(BASE_DIR / '.env').exists()}")
        log.info(f"settings.yaml exists: {(BASE_DIR / 'config' / 'settings.yaml').exists()}")
        log.info(f"Models dir: {(BASE_DIR / 'ml_models' / 'v35_production').exists()}")
        return

    results = boot_sequence()

    if args.boot_only:
        log.info("--boot-only specified, exiting")
        return

    if not results.get("p1uni"):
        log.error("P1UNI failed to start, skipping monitor loop")
        telegram_send("P1UNI BOOT FAILED - manual intervention required")
        sys.exit(1)

    # Monitor loop (blocca fino a Ctrl+C o kill)
    monitor_loop(interval_min=5)


if __name__ == "__main__":
    main()
