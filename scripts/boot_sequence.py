"""
Boot Sequence — Avvia TUTTI i software esterni in ordine, poi lancia P1UNI.

Sequenza:
  1. NinjaTrader 8 (se non attivo) + auto-login
  2. Verifica Databento API + GexBot API
  3. Morning readiness check
  4. Lancia main.py --mode paper

Usage:
  python scripts/boot_sequence.py              # paper mode (default)
  python scripts/boot_sequence.py --mode live   # live mode (ATTENZIONE!)
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [BOOT] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent.parent / "logs" / "boot_sequence.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("boot")

P1UNI_DIR = str(Path(__file__).parent.parent)
NT8_EXE = r"C:\NinjaTrader 8\bin\NinjaTrader.exe"


def is_process_running(name: str) -> bool:
    for proc in psutil.process_iter(["name"]):
        try:
            if name.lower() in (proc.info["name"] or "").lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False


def step_1_ninjatrader():
    """Avvia NinjaTrader se non attivo."""
    log.info("STEP 1: NinjaTrader 8")

    if is_process_running("NinjaTrader"):
        log.info("  NT8 gia' in esecuzione. OK.")
        return True

    if not Path(NT8_EXE).exists():
        log.warning(f"  NT8 non trovato: {NT8_EXE}")
        return False

    log.info("  Avvio NinjaTrader...")
    subprocess.Popen([NT8_EXE], creationflags=subprocess.CREATE_NEW_CONSOLE)
    time.sleep(15)

    # Auto-login se password disponibile
    nt8_pwd = os.environ.get("NT8_PASSWORD", "")
    if nt8_pwd:
        log.info("  Tentativo auto-login...")
        try:
            import pyautogui
            from pywinauto import Desktop

            wins = Desktop(backend="win32").windows(title_re=".*NinjaTrader.*")
            if wins:
                win = wins[0]
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
                    keyboard.write(nt8_pwd, delay=0.05)
                except ImportError:
                    for ch in nt8_pwd:
                        if ch == "!":
                            pyautogui.hotkey("shift", "1")
                        elif ch.isupper():
                            pyautogui.hotkey("shift", ch.lower())
                        else:
                            pyautogui.press(ch)
                        time.sleep(0.05)

                time.sleep(0.5)
                pyautogui.press("enter")
                log.info("  Login inviato. Attendo 10s...")
                time.sleep(10)
                log.info("  NT8 auto-login completato.")
        except Exception as e:
            log.warning(f"  Auto-login fallito: {e} (login manuale richiesto)")
    else:
        log.info("  NT8_PASSWORD non impostata. Login manuale richiesto.")

    return is_process_running("NinjaTrader")


def step_2_api_checks():
    """Verifica connettivita' API."""
    log.info("STEP 2: API Checks")
    ok = True

    # Load secrets
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.core.secrets import get_secret

    # Databento
    try:
        import databento as db
        db.Historical(key=get_secret("DATABENTO_API_KEY", required=True))
        log.info("  Databento API: OK")
    except Exception as e:
        log.error(f"  Databento API: FAIL ({e})")
        ok = False

    # GexBot
    try:
        import requests
        gex_key = get_secret("GEXBOT_API_KEY", required=True)
        resp = requests.get(
            "https://api.gexbot.com/negotiate",
            headers={"Authorization": f"Basic {gex_key}"},
            timeout=10
        )
        if resp.status_code == 200:
            log.info(f"  GexBot API: OK (prefix={resp.json().get('prefix')})")
        else:
            log.warning(f"  GexBot API: Status {resp.status_code}")
    except Exception as e:
        log.warning(f"  GexBot API: {e}")

    return ok


def step_3_morning_check():
    """Esegui morning readiness check."""
    log.info("STEP 3: Morning Readiness Check")
    try:
        result = subprocess.run(
            [sys.executable, "scripts/morning_check.py"],
            cwd=P1UNI_DIR, capture_output=True, text=True, timeout=30
        )
        if "READY FOR TRADING" in result.stdout:
            log.info("  READY FOR TRADING")
            return True
        else:
            log.warning("  Issues detected. Check MORNING_READINESS_REPORT.md")
            return True  # non bloccante
    except Exception as e:
        log.warning(f"  Morning check failed: {e}")
        return True


def step_4_launch_p1uni(mode: str = "paper"):
    """Lancia il sistema P1UNI."""
    log.info(f"STEP 4: Launching P1UNI --mode {mode}")

    # Verifica che non sia gia' in esecuzione
    for proc in psutil.process_iter(["cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if any("main.py" in arg for arg in cmdline) and any("P1UNI" in arg for arg in cmdline):
                log.info("  P1UNI gia' in esecuzione. Skip.")
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    cmd = [sys.executable, "main.py", "--mode", mode, "--config", "config/settings.yaml"]
    log.info(f"  Command: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd, cwd=P1UNI_DIR,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )
    time.sleep(5)

    if proc.poll() is None:
        log.info(f"  P1UNI avviato (PID: {proc.pid})")
        return True
    else:
        log.error(f"  P1UNI terminato subito (exit: {proc.returncode})")
        return False


def main():
    parser = argparse.ArgumentParser(description="P1UNI Boot Sequence")
    parser.add_argument("--mode", default="paper", choices=["paper", "live"])
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("  P1UNI BOOT SEQUENCE")
    log.info(f"  Mode: {args.mode.upper()}")
    log.info("=" * 55)

    Path(P1UNI_DIR, "logs").mkdir(exist_ok=True)

    results = {}
    results["NT8"] = step_1_ninjatrader()
    results["APIs"] = step_2_api_checks()
    results["Morning"] = step_3_morning_check()
    results["P1UNI"] = step_4_launch_p1uni(args.mode)

    log.info("")
    log.info("=" * 55)
    log.info("  BOOT SEQUENCE COMPLETE")
    for name, ok in results.items():
        log.info(f"  {name}: {'OK' if ok else 'FAIL'}")
    log.info("=" * 55)

    all_ok = all(results.values())
    if not all_ok:
        log.warning("Some components failed. Check logs.")

    # Telegram
    try:
        import requests
        from src.core.secrets import get_secret as _get_secret
        msg = "P1UNI Boot: " + ", ".join(f"{k}={'OK' if v else 'FAIL'}" for k, v in results.items())
        requests.post(
            f"https://api.telegram.org/bot{_get_secret('TELEGRAM_BOT_TOKEN', '')}/sendMessage",
            json={"chat_id": _get_secret("TELEGRAM_CHAT_ID", ""), "text": msg}, timeout=10
        )
    except Exception:
        pass


if __name__ == "__main__":
    main()
