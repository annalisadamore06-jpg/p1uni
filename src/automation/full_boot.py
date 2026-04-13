"""
Full Boot — Avvia TUTTO in sequenza: TWS -> P1-Lite -> NinjaTrader -> P1UNI.

Questo e' il singolo script che il Task Scheduler chiama alle 07:30.
Gestisce ogni componente con retry, health check e Telegram alerts.

Sequenza:
  1. TWS via IBC (60s attesa)
  2. P1-Lite su Chrome (http://127.0.0.1:8060)
  3. NinjaTrader 8 + auto-login + verifica finestre
  4. P1UNI main.py --mode paper
  5. Verifica ingestion Databento
  6. Telegram report

Ogni step ha retry e fallback. Se un componente fallisce, il sistema
continua con gli altri (degraded mode) e avvisa via Telegram.
"""

from __future__ import annotations

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

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [BOOT] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "full_boot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("boot")


# ============================================================
# Telegram
# ============================================================

def tg(msg: str) -> None:
    try:
        import requests
        token = get_secret("TELEGRAM_BOT_TOKEN")
        chat = get_secret("TELEGRAM_CHAT_ID")
        if token and chat:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat, "text": msg}, timeout=10)
    except Exception:
        pass


# ============================================================
# Helpers
# ============================================================

def is_running(name: str) -> bool:
    for p in psutil.process_iter(["name"]):
        try:
            if name.lower() in (p.info["name"] or "").lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False


def wait_for_process(name: str, timeout: int = 60) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if is_running(name):
            return True
        time.sleep(2)
    return False


# ============================================================
# STEP 1: TWS via IBC
# ============================================================

def boot_tws() -> bool:
    """Avvia TWS tramite IBC auto-login."""
    log.info("="*50)
    log.info("STEP 1: TWS via IBC")

    # Check se gia' attivo
    for proc_name in ("tws", "ibgateway", "javaw"):
        if is_running(proc_name):
            log.info(f"  TWS gia' attivo ({proc_name})")
            return True

    ibc_bat = Path(r"C:\IBC\StartTWS.bat")
    if not ibc_bat.exists():
        log.error(f"  IBC non trovato: {ibc_bat}")
        return False

    log.info("  Avvio IBC/TWS...")
    try:
        subprocess.Popen(
            [str(ibc_bat)],
            cwd=r"C:\IBC",
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
    except Exception as e:
        log.error(f"  Errore avvio IBC: {e}")
        return False

    log.info("  Attendo avvio TWS (max 90s)...")
    ok = wait_for_process("javaw", timeout=90) or wait_for_process("tws", timeout=10)
    if ok:
        log.info("  TWS avviato. Attendo stabilizzazione (30s)...")
        time.sleep(30)
        log.info("  TWS OK")
    else:
        log.error("  TWS non rilevato dopo timeout")
    return ok


# ============================================================
# STEP 2: P1-Lite su Chrome
# ============================================================

def boot_p1lite() -> bool:
    """Apri Chrome su P1-Lite."""
    log.info("="*50)
    log.info("STEP 2: P1-Lite (Chrome)")

    P1_URL = "http://127.0.0.1:8060"
    P1_DIR = Path(r"C:\Users\annal\Desktop\p1-lite")
    CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

    # Check se collector gia' gira
    collector_running = False
    for p in psutil.process_iter(["cmdline"]):
        try:
            cl = " ".join(p.info.get("cmdline") or [])
            if "collector_lite" in cl:
                collector_running = True
                break
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    # 1. Avvia collector_lite.py (si connette a TWS)
    if not collector_running:
        if (P1_DIR / "collector_lite.py").exists():
            log.info("  Avvio P1-Lite collector_lite.py...")
            subprocess.Popen(
                [sys.executable, "collector_lite.py"],
                cwd=str(P1_DIR),
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            time.sleep(8)
            log.info("  Collector avviato")
        else:
            log.error(f"  collector_lite.py non trovato in {P1_DIR}")
            return False
    else:
        log.info("  P1-Lite collector gia' attivo")

    # 2. Avvia dashboard app.py (porta 8060)
    try:
        import requests as req
        r = req.get(P1_URL, timeout=3)
        if r.status_code == 200:
            log.info(f"  P1-Lite dashboard gia' attivo su {P1_URL}")
        else:
            raise Exception("not 200")
    except Exception:
        log.info("  Avvio P1-Lite dashboard (app.py)...")
        if (P1_DIR / "app.py").exists():
            subprocess.Popen(
                [sys.executable, "app.py"],
                cwd=str(P1_DIR),
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            time.sleep(5)
        else:
            log.warning(f"  app.py non trovato in {P1_DIR}")

    # 3. Apri Chrome
    try:
        import requests as req
        r = req.get(P1_URL, timeout=3)
        if r.status_code == 200:
            log.info(f"  P1-Lite raggiungibile su {P1_URL}")
            if Path(CHROME).exists():
                subprocess.Popen([CHROME, P1_URL])
            else:
                os.startfile(P1_URL)
            time.sleep(3)
            log.info("  Chrome aperto su P1-Lite")
            return True
    except Exception:
        log.warning(f"  P1-Lite non raggiungibile su {P1_URL}")

    return collector_running


# ============================================================
# STEP 3: NinjaTrader 8
# ============================================================

def boot_nt8() -> bool:
    """Avvia NT8, auto-login, verifica finestre."""
    log.info("="*50)
    log.info("STEP 3: NinjaTrader 8")

    NT8_EXE = r"C:\NinjaTrader 8\bin\NinjaTrader.exe"

    # Check finestre NT8 (usa UIA backend + PID per trovare finestre WPF)
    try:
        from pywinauto import Application
        nt_pids = []
        for p in psutil.process_iter(["name", "pid"]):
            try:
                if "ninja" in (p.info["name"] or "").lower():
                    nt_pids.append(p.info["pid"])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        for pid in nt_pids:
            try:
                app = Application(backend="uia").connect(process=pid)
                wins = app.windows()
                titles = [w.window_text() for w in wins if w.window_text()]
                has_cc = any("Control Center" in t for t in titles)
                has_chart = any("Chart" in t for t in titles)

                # Se ha Chart = e' loggato (il chart non appare senza login)
                if has_cc or has_chart:
                    log.info(f"  NT8 LOGGATO (PID {pid}): CC={has_cc} Chart={has_chart}")
                    log.info(f"  Finestre: {titles}")
                    return True
            except Exception:
                pass

        log.info(f"  NT8 running (PID {nt_pids}) ma nessun Control Center trovato")
    except Exception:
        pass

    # Se non running, avvia
    if not is_running("NinjaTrader"):
        if not Path(NT8_EXE).exists():
            log.error(f"  NT8 non trovato: {NT8_EXE}")
            return False

        log.info("  Avvio NinjaTrader...")
        subprocess.Popen([NT8_EXE], creationflags=subprocess.CREATE_NEW_CONSOLE)
        log.info("  Attendo avvio (15s)...")
        time.sleep(15)

    # Auto-login
    pwd = get_secret("NT8_PASSWORD", "")
    if not pwd:
        log.warning("  NT8_PASSWORD non impostata — login manuale richiesto")
        return is_running("NinjaTrader")

    log.info("  Tentativo auto-login...")
    try:
        import pyautogui
        pyautogui.FAILSAFE = False  # disabilita fail-safe (mouse corner)
        from pywinauto import Desktop

        # Cerca finestra login
        start = time.time()
        win = None
        while time.time() - start < 40:
            wins = Desktop(backend="win32").windows(title_re=".*NinjaTrader.*")
            if wins:
                win = wins[0]
                break
            time.sleep(1)

        if not win:
            log.error("  Finestra NT8 non trovata")
            return False

        # Focus e click campo password
        try:
            win.set_focus()
            win.restore()
        except Exception:
            pass
        time.sleep(1.5)

        rect = win.rectangle()
        pwd_x = rect.left + int(rect.width() * 0.50)
        pwd_y = rect.top + int(rect.height() * 0.39)

        pyautogui.click(pwd_x, pwd_y)
        time.sleep(0.5)
        pyautogui.hotkey("ctrl", "a")
        pyautogui.press("backspace")
        time.sleep(0.3)

        # Digita password
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
        log.info("  Login inviato, attendo 15s...")
        time.sleep(15)

        # Verifica finestre (UIA + PID)
        from pywinauto import Application
        for p in psutil.process_iter(["name", "pid"]):
            try:
                if "ninja" in (p.info["name"] or "").lower():
                    app = Application(backend="uia").connect(process=p.info["pid"])
                    wins = app.windows()
                    titles = [w.window_text() for w in wins if w.window_text()]
                    has_cc = any("Control Center" in t for t in titles)
                    has_chart = any("Chart" in t for t in titles)
                    log.info(f"  Finestre: {titles}")
                    log.info(f"  Control Center: {'SI' if has_cc else 'NO'} | Chart: {'SI' if has_chart else 'NO'}")
                    if has_cc:
                        log.info("  NT8 login OK")
                        return True
            except Exception:
                pass
        log.warning("  NT8 login inviato ma Control Center non trovato")
        return is_running("NinjaTrader")

    except ImportError as e:
        log.error(f"  Dipendenza mancante: {e}")
        return is_running("NinjaTrader")
    except Exception as e:
        log.error(f"  Auto-login errore: {e}")
        return is_running("NinjaTrader")


# ============================================================
# STEP 4: P1UNI main.py
# ============================================================

def boot_p1uni(mode: str = "paper") -> bool:
    """Lancia P1UNI main.py."""
    log.info("="*50)
    log.info(f"STEP 4: P1UNI main.py --mode {mode}")

    # Check se gia' running (e non zombie)
    for p in psutil.process_iter(["cmdline", "pid"]):
        try:
            cl = p.info.get("cmdline") or []
            if any("main.py" in str(a) for a in cl) and any("P1UNI" in str(a) or "p1uni" in str(a).lower() for a in cl):
                # Check se zombie: log attivita' recente
                log_file = BASE_DIR / "logs" / "p1uni.log"
                if log_file.exists():
                    import re
                    content = log_file.read_text(encoding="utf-8", errors="ignore")
                    lines = content.strip().split("\n")
                    if lines:
                        m = re.search(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", lines[-1])
                        if m:
                            from datetime import timedelta
                            last_ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                            age = datetime.now(timezone.utc) - last_ts
                            if age < timedelta(minutes=10):
                                log.info(f"  P1UNI gia' running e sano (PID {p.info['pid']})")
                                return True
                            else:
                                log.warning(f"  P1UNI zombie (PID {p.info['pid']}, log stale {age}). Killing...")
                                p.kill()
                                time.sleep(3)
                else:
                    log.info(f"  P1UNI running PID {p.info['pid']}")
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    # Lancia
    log.info("  Avvio P1UNI...")
    try:
        proc = subprocess.Popen(
            [sys.executable, "main.py", "--mode", mode, "--config", "config/settings.yaml"],
            cwd=str(BASE_DIR),
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
        time.sleep(8)
        if proc.poll() is None:
            log.info(f"  P1UNI avviato (PID {proc.pid})")
            return True
        log.error(f"  P1UNI terminato subito (exit {proc.returncode})")
        return False
    except Exception as e:
        log.error(f"  Errore: {e}")
        return False


# ============================================================
# STEP 5: Verifica ingestion
# ============================================================

def verify_ingestion(timeout: int = 60) -> tuple[bool, int]:
    """Verifica che Databento stia ingestendo trade."""
    import re
    log.info("="*50)
    log.info("STEP 5: Verifica ingestion Databento")

    log_file = BASE_DIR / "logs" / "p1uni.log"
    if not log_file.exists():
        log.warning("  Log file non trovato")
        return False, 0

    start = time.time()
    first_total = None

    while time.time() - start < timeout:
        try:
            content = log_file.read_text(encoding="utf-8", errors="ignore")
            matches = re.findall(r"Wrote \d+ records to trades_live \(total: (\d+)\)", content)
            if matches:
                current = int(matches[-1])
                if first_total is None:
                    first_total = current
                if current > (first_total or 0):
                    log.info(f"  Databento OK: {current} trade nel DB")
                    return True, current
        except Exception:
            pass
        time.sleep(5)

    total = first_total or 0
    if total > 0:
        log.info(f"  Databento: {total} trade nel DB (ma non sta crescendo — mercato chiuso?)")
        return True, total

    log.warning(f"  Databento: nessun trade dopo {timeout}s")
    return False, 0


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="paper", choices=["paper", "live"])
    parser.add_argument("--skip-tws", action="store_true", help="Salta avvio TWS")
    parser.add_argument("--skip-p1", action="store_true", help="Salta P1-Lite")
    parser.add_argument("--skip-nt8", action="store_true", help="Salta NinjaTrader")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("  P1UNI FULL BOOT SEQUENCE")
    log.info(f"  Mode: {args.mode.upper()}")
    log.info(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    results = {}

    # Step 1: TWS
    if not args.skip_tws:
        results["TWS"] = boot_tws()
    else:
        results["TWS"] = "SKIP"
        log.info("STEP 1: TWS SKIPPED")

    # Step 2: P1-Lite
    if not args.skip_p1:
        results["P1-Lite"] = boot_p1lite()
    else:
        results["P1-Lite"] = "SKIP"
        log.info("STEP 2: P1-Lite SKIPPED")

    # Step 3: NinjaTrader
    if not args.skip_nt8:
        results["NT8"] = boot_nt8()
    else:
        results["NT8"] = "SKIP"
        log.info("STEP 3: NT8 SKIPPED")

    # Step 4: P1UNI
    results["P1UNI"] = boot_p1uni(args.mode)

    # Step 5: Verifica ingestion (30s)
    if results["P1UNI"]:
        ok, total = verify_ingestion(timeout=30)
        results["Ingestion"] = f"{total} trades" if ok else "FAIL"
    else:
        results["Ingestion"] = "SKIP (P1UNI not started)"

    # Report
    log.info("")
    log.info("=" * 55)
    log.info("  BOOT RESULTS")
    for k, v in results.items():
        status = "OK" if v is True else ("FAIL" if v is False else str(v))
        log.info(f"  {k:15s} {status}")
    log.info("=" * 55)

    # Telegram
    lines = ["P1UNI Boot Report"]
    for k, v in results.items():
        status = "OK" if v is True else ("FAIL" if v is False else str(v))
        lines.append(f"- {k}: {status}")
    all_critical_ok = results.get("P1UNI") is True
    lines.append("")
    lines.append("Status: READY" if all_critical_ok else "Status: DEGRADED - check logs")
    tg("\n".join(lines))

    log.info("Boot sequence complete.")


if __name__ == "__main__":
    main()
