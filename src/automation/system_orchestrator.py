"""
System Orchestrator — Supervisore Zero-Touch per tutto l'ecosistema.

Gira in loop ogni 30 secondi e controlla:
  1. TWS/IBC: se non attivo, lo avvia
  2. NinjaTrader 8: se non attivo, lo avvia + auto-login
  3. Chrome/P1-Lite: se non attivo e TWS e' pronto, lo apre
  4. Python adapters: verificati dal main.py (non qui)
  5. Dashboard testuale: stampa stato ogni ciclo

SCENARIO: "Accendo il PC alle 08:00, cosa succede?"
  08:00:00 - Orchestrator parte (da Task Scheduler)
  08:00:05 - Rileva: TWS assente -> avvia IBC
  08:01:05 - TWS pronto -> Rileva: NT8 assente -> avvia NT8 + autologin
  08:01:45 - NT8 pronto -> Rileva: P1-Lite assente -> apre Chrome
  08:02:00 - Avvia main.py (trading system)
  08:02:05 - Tutto UP. Dashboard mostra tutto verde.

CREDENZIALI:
  Tutte da variabili d'ambiente (mai hardcoded):
  - NT8_PASSWORD: password NinjaTrader
  - IBC_USER / IBC_PASSWORD: credenziali Interactive Brokers (se usato)

Basato sugli script testati in:
  - vibrant-spence/MasterWatchdog.py (logica process check)
  - vibrant-spence/ninja_autologin.py (NT8 auto-login)
  - infallible-jang/watchdog_launcher.py (anti-duplicate, anti-loop)
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil

logger = logging.getLogger("p1uni.automation.orchestrator")


# ============================================================
# Process checker utilities
# ============================================================

def is_process_running(name: str) -> bool:
    """Controlla se un processo con il nome dato e' in esecuzione."""
    name_lower = name.lower()
    for proc in psutil.process_iter(["name", "exe"]):
        try:
            pname = (proc.info["name"] or "").lower()
            if name_lower in pname:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False


def find_process_by_name(name: str) -> list[psutil.Process]:
    """Trova tutti i processi che matchano il nome."""
    results = []
    name_lower = name.lower()
    for proc in psutil.process_iter(["name", "pid", "exe", "cmdline"]):
        try:
            pname = (proc.info["name"] or "").lower()
            if name_lower in pname:
                results.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return results


def is_chrome_on_url(url_fragment: str) -> bool:
    """Controlla se Chrome ha una tab aperta con l'URL dato."""
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            if "chrome" not in (proc.info["name"] or "").lower():
                continue
            cmdline = proc.info.get("cmdline") or []
            if any(url_fragment in arg for arg in cmdline):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False


def get_system_metrics() -> dict[str, Any]:
    """Metriche sistema per dashboard."""
    cpu = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("C:\\")
    return {
        "cpu_pct": cpu,
        "ram_pct": mem.percent,
        "ram_used_gb": round(mem.used / (1024**3), 1),
        "ram_total_gb": round(mem.total / (1024**3), 1),
        "disk_free_gb": round(disk.free / (1024**3), 1),
    }


# ============================================================
# Component managers
# ============================================================

class TWS_Manager:
    """Gestisce Interactive Brokers TWS/Gateway via IBC."""

    def __init__(self, config: dict[str, Any]) -> None:
        ibc_cfg = config.get("ibc", {})
        self.ibc_path: str = ibc_cfg.get("path", r"C:\IBC\StartTWS.bat")
        self.ibc_ini: str = ibc_cfg.get("ini_path", r"C:\IBC\config.ini")
        self.process_names = ("tws", "ibgateway", "javaw")
        self.startup_wait_sec: int = ibc_cfg.get("startup_wait_sec", 60)
        self.enabled: bool = ibc_cfg.get("enabled", False)

    def is_running(self) -> bool:
        for name in self.process_names:
            if is_process_running(name):
                return True
        return False

    def start(self) -> bool:
        if not self.enabled:
            logger.info("IBC/TWS disabled in config")
            return True

        if self.is_running():
            logger.info("TWS gia' attivo, skip avvio")
            return True

        logger.info(f"Avvio TWS via IBC: {self.ibc_path}")
        try:
            if not Path(self.ibc_path).exists():
                logger.error(f"IBC non trovato: {self.ibc_path}")
                return False

            subprocess.Popen(
                [self.ibc_path],
                cwd=str(Path(self.ibc_path).parent),
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            logger.info(f"IBC avviato. Attendo {self.startup_wait_sec}s per inizializzazione...")
            time.sleep(self.startup_wait_sec)

            if self.is_running():
                logger.info("TWS avviato con successo")
                return True
            else:
                logger.error("TWS non rilevato dopo avvio IBC")
                return False

        except Exception as e:
            logger.error(f"Errore avvio TWS: {e}")
            return False


class NT8_Manager:
    """Gestisce NinjaTrader 8: avvio + auto-login.

    Basato su: vibrant-spence/ninja_autologin.py (testato e funzionante)
    """

    def __init__(self, config: dict[str, Any]) -> None:
        nt8_cfg = config.get("ninjatrader", {})
        self.exe_path: str = nt8_cfg.get("exe_path", r"C:\NinjaTrader 8\bin\NinjaTrader.exe")
        self.username: str = nt8_cfg.get("username", os.environ.get("NT8_USER", ""))
        self.password: str = nt8_cfg.get("password", os.environ.get("NT8_PASSWORD", ""))
        self.startup_wait_sec: int = nt8_cfg.get("startup_wait_sec", 15)
        self.login_timeout_sec: int = nt8_cfg.get("login_timeout_sec", 40)
        self.enabled: bool = nt8_cfg.get("enabled", True)

    def is_running(self) -> bool:
        return is_process_running("NinjaTrader")

    def is_control_center_open(self) -> bool:
        """Verifica che il Control Center sia aperto (= login completato)."""
        try:
            from pywinauto import Desktop
            wins = Desktop(backend="win32").windows(title_re=".*Control Center.*")
            return len(wins) > 0
        except Exception:
            return False

    def start(self) -> bool:
        """Avvia NT8 e esegui auto-login se necessario."""
        if not self.enabled:
            logger.info("NinjaTrader disabled in config")
            return True

        if self.is_running():
            if self.is_control_center_open():
                logger.info("NT8 gia' attivo e loggato (Control Center aperto)")
                return True
            else:
                logger.info("NT8 attivo ma non loggato — tento auto-login")
                return self._perform_login()

        # Avvia NT8
        logger.info(f"Avvio NinjaTrader: {self.exe_path}")
        if not Path(self.exe_path).exists():
            logger.error(f"NinjaTrader non trovato: {self.exe_path}")
            return False

        try:
            subprocess.Popen([self.exe_path], creationflags=subprocess.CREATE_NEW_CONSOLE)
            logger.info(f"NT8 avviato. Attendo {self.startup_wait_sec}s...")
            time.sleep(self.startup_wait_sec)
            return self._perform_login()
        except Exception as e:
            logger.error(f"Errore avvio NT8: {e}")
            return False

    def _perform_login(self) -> bool:
        """Auto-login su NT8 usando pyautogui.

        Logica da vibrant-spence/ninja_autologin.py (gia testato).
        Usa password da env var NT8_PASSWORD.
        """
        if not self.password:
            logger.warning("NT8_PASSWORD non impostata — login manuale richiesto")
            return False

        try:
            import pyautogui
            from pywinauto import Desktop

            logger.info("Ricerca finestra login NT8...")

            # Attendi finestra di login
            start = time.time()
            win = None
            while time.time() - start < self.login_timeout_sec:
                wins = Desktop(backend="win32").windows(title_re=".*NinjaTrader.*")
                if wins:
                    win = wins[0]
                    break
                time.sleep(1)

            if win is None:
                logger.error("Finestra NT8 non trovata entro timeout")
                return False

            # Porta in primo piano
            try:
                win.set_focus()
                win.restore()
            except Exception:
                pass
            time.sleep(1.5)

            # Coordinate campo password (relative alla finestra)
            rect = win.rectangle()
            pwd_x = rect.left + int(rect.width() * 0.50)
            pwd_y = rect.top + int(rect.height() * 0.39)

            # Click campo password
            pyautogui.click(pwd_x, pwd_y)
            time.sleep(0.5)
            pyautogui.hotkey("ctrl", "a")
            time.sleep(0.2)
            pyautogui.press("backspace")
            time.sleep(0.3)

            # Digita password (gestisce caratteri speciali)
            try:
                import keyboard
                keyboard.write(self.password, delay=0.05)
            except ImportError:
                for ch in self.password:
                    if ch == "!":
                        pyautogui.hotkey("shift", "1")
                    elif ch.isupper():
                        pyautogui.hotkey("shift", ch.lower())
                    else:
                        pyautogui.press(ch)
                    time.sleep(0.05)

            time.sleep(0.5)
            pyautogui.press("enter")
            time.sleep(8)

            if self.is_control_center_open():
                logger.info("NT8 login riuscito — Control Center aperto")
                return True
            else:
                logger.warning("NT8 login inviato ma Control Center non rilevato")
                return False

        except ImportError as e:
            logger.error(f"Dipendenza mancante per NT8 auto-login: {e}")
            logger.info("Installa: pip install pywinauto pyautogui keyboard")
            return False
        except Exception as e:
            logger.error(f"NT8 auto-login fallito: {e}")
            return False


class Chrome_P1Lite_Manager:
    """Gestisce Chrome con P1-Lite."""

    def __init__(self, config: dict[str, Any]) -> None:
        p1_cfg = config.get("p1lite", {})
        self.p1_url: str = p1_cfg.get("chrome_url", "http://127.0.0.1:8060")
        self.chrome_path: str = config.get("chrome", {}).get(
            "path",
            r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        )
        self.enabled: bool = p1_cfg.get("enabled", False)

    def is_running(self) -> bool:
        return is_chrome_on_url(self.p1_url) or is_chrome_on_url("8060")

    def start(self) -> bool:
        if not self.enabled:
            logger.info("P1-Lite/Chrome disabled in config")
            return True

        if self.is_running():
            logger.info(f"Chrome P1-Lite gia' aperto ({self.p1_url})")
            return True

        logger.info(f"Apertura Chrome su {self.p1_url}")
        try:
            chrome = Path(self.chrome_path)
            if chrome.exists():
                subprocess.Popen([str(chrome), self.p1_url])
            else:
                # Fallback: usa il browser di default
                os.startfile(self.p1_url)
            time.sleep(5)
            logger.info("Chrome P1-Lite avviato")
            return True
        except Exception as e:
            logger.error(f"Errore apertura Chrome: {e}")
            return False


class MainPy_Manager:
    """Gestisce il processo main.py di P1UNI."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.python_exe: str = config.get("automation", {}).get(
            "python_exe", sys.executable
        )
        self.p1uni_dir: str = config.get("_base_dir", r"C:\Users\annal\Desktop\P1UNI")
        self.main_script: str = str(Path(self.p1uni_dir) / "main.py")
        self.config_path: str = str(Path(self.p1uni_dir) / "config" / "settings.yaml")
        self.mode: str = config.get("system", {}).get("mode", "paper")
        self._process: subprocess.Popen | None = None

    def is_running(self) -> bool:
        """Controlla se main.py e' gia in esecuzione."""
        for proc in psutil.process_iter(["cmdline"]):
            try:
                cmdline = proc.info.get("cmdline") or []
                if any("main.py" in arg for arg in cmdline):
                    if any("P1UNI" in arg or "p1uni" in arg for arg in cmdline):
                        return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return False

    def start(self) -> bool:
        if self.is_running():
            logger.info("P1UNI main.py gia' in esecuzione")
            return True

        logger.info(f"Avvio P1UNI main.py --mode {self.mode}")
        try:
            self._process = subprocess.Popen(
                [self.python_exe, self.main_script, "--mode", self.mode, "--config", self.config_path],
                cwd=self.p1uni_dir,
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            time.sleep(3)
            if self._process.poll() is None:
                logger.info(f"P1UNI main.py avviato (PID: {self._process.pid})")
                return True
            else:
                logger.error(f"P1UNI main.py terminato subito (exit: {self._process.returncode})")
                return False
        except Exception as e:
            logger.error(f"Errore avvio main.py: {e}")
            return False


# ============================================================
# SystemOrchestrator
# ============================================================

class SystemOrchestrator:
    """Supervisore principale: controlla tutto ogni 30 secondi.

    Sequenza di avvio:
    1. TWS/IBC (se abilitato)
    2. NinjaTrader 8 + auto-login
    3. Chrome P1-Lite (se abilitato)
    4. P1UNI main.py

    Ogni ciclo: check status di tutto, riavvia se necessario.
    """

    def __init__(self, config: dict[str, Any], telegram: Any = None) -> None:
        self.config = config
        self.telegram = telegram
        self.check_interval: int = config.get("automation", {}).get("check_interval_sec", 30)

        # Component managers
        self.tws = TWS_Manager(config)
        self.nt8 = NT8_Manager(config)
        self.chrome = Chrome_P1Lite_Manager(config)
        self.main_py = MainPy_Manager(config)

        # Stato
        self._running: bool = False
        self._shutdown_event = threading.Event()
        self._startup_done: bool = False
        self._cycle_count: int = 0
        self._errors: dict[str, int] = {}

    def run(self) -> None:
        """Loop principale: avvio iniziale + monitoring continuo."""
        self._running = True
        logger.info("=" * 55)
        logger.info("  SYSTEM ORCHESTRATOR AVVIATO")
        logger.info(f"  Check interval: {self.check_interval}s")
        logger.info("=" * 55)

        # Avvio iniziale sequenziale
        self._initial_startup()

        # Loop di monitoring
        while self._running and not self._shutdown_event.is_set():
            try:
                self._cycle_count += 1
                self._check_and_repair()
                self._print_dashboard()
            except Exception as e:
                logger.error(f"Orchestrator cycle error: {e}")

            self._shutdown_event.wait(self.check_interval)

        logger.info("System Orchestrator fermato")

    def _initial_startup(self) -> None:
        """Sequenza di avvio ordinata."""
        logger.info("--- SEQUENZA DI AVVIO ---")

        # 1. TWS
        if self.tws.enabled:
            ok = self.tws.start()
            if not ok:
                self._alert("TWS/IBC non avviato! Verifica manualmente.", "ERROR")

        # 2. NinjaTrader
        if self.nt8.enabled:
            ok = self.nt8.start()
            if not ok:
                self._alert("NT8 non avviato o login fallito!", "WARNING")

        # 3. Chrome P1-Lite
        if self.chrome.enabled:
            self.chrome.start()

        # 4. P1UNI main.py
        ok = self.main_py.start()
        if ok:
            self._alert("P1UNI Sistema avviato correttamente", "INFO")
        else:
            self._alert("P1UNI main.py non avviato!", "ERROR")

        self._startup_done = True
        logger.info("--- AVVIO COMPLETATO ---")

    def _check_and_repair(self) -> None:
        """Controlla ogni componente e riavvia se necessario."""
        # TWS
        if self.tws.enabled and not self.tws.is_running():
            self._errors["tws"] = self._errors.get("tws", 0) + 1
            if self._errors["tws"] <= 3:
                logger.warning("TWS non rilevato — tentativo riavvio")
                self._alert("TWS non rilevato — tentativo riavvio", "WARNING")
                self.tws.start()

        # NT8
        if self.nt8.enabled and not self.nt8.is_running():
            self._errors["nt8"] = self._errors.get("nt8", 0) + 1
            if self._errors["nt8"] <= 3:
                logger.warning("NT8 non rilevato — tentativo riavvio")
                self._alert("NT8 non rilevato — tentativo riavvio", "WARNING")
                self.nt8.start()

        # Chrome P1-Lite
        if self.chrome.enabled and not self.chrome.is_running():
            self.chrome.start()

        # P1UNI main.py
        if not self.main_py.is_running():
            self._errors["main"] = self._errors.get("main", 0) + 1
            if self._errors["main"] <= 5:
                logger.warning("P1UNI main.py non rilevato — riavvio")
                self._alert("P1UNI main.py crashato — riavvio automatico", "WARNING")
                self.main_py.start()
            else:
                self._alert("P1UNI main.py crash ripetuto (>5 volte)! Intervento manuale richiesto.", "ERROR")

    def _print_dashboard(self) -> None:
        """Dashboard testuale nel terminale."""
        metrics = get_system_metrics()
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        status_lines = [
            f"╔══════════════════════════════════════════╗",
            f"║  P1UNI SYSTEM STATUS — {now}   ║",
            f"╠══════════════════════════════════════════╣",
            f"║  TWS/IBC:    {'[ON]' if self.tws.is_running() else '[OFF]' if self.tws.enabled else '[SKIP]':>8}  ║",
            f"║  NinjaTrader:{'[ON]' if self.nt8.is_running() else '[OFF]' if self.nt8.enabled else '[SKIP]':>8}  ║",
            f"║  P1-Lite:    {'[ON]' if self.chrome.is_running() else '[OFF]' if self.chrome.enabled else '[SKIP]':>8}  ║",
            f"║  P1UNI:      {'[ON]' if self.main_py.is_running() else '[OFF]':>8}  ║",
            f"╠══════════════════════════════════════════╣",
            f"║  CPU: {metrics['cpu_pct']:4.0f}%  RAM: {metrics['ram_pct']:4.0f}%  Disk: {metrics['disk_free_gb']:5.0f}GB  ║",
            f"║  Cycle: {self._cycle_count}  Errors: {sum(self._errors.values())}         ║",
            f"╚══════════════════════════════════════════╝",
        ]

        # Pulisci schermo e stampa (solo ogni 5 cicli per non spammare)
        if self._cycle_count % 5 == 0:
            for line in status_lines:
                logger.info(line)

    def _alert(self, message: str, level: str = "INFO") -> None:
        """Invia alert Telegram."""
        if self.telegram is not None:
            try:
                self.telegram.send_alert(message, level)
            except Exception:
                pass

    def stop(self) -> None:
        """Ferma l'orchestrator."""
        self._running = False
        self._shutdown_event.set()


# ============================================================
# Entry point standalone
# ============================================================

def main() -> None:
    """Avvia l'orchestrator come script standalone."""
    import yaml

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Carica config
    config_path = Path(__file__).parent.parent.parent / "config" / "settings.yaml"
    if config_path.exists():
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        config["_base_dir"] = str(config_path.parent.parent)
    else:
        logger.warning(f"Config non trovato: {config_path}, uso defaults")
        config = {"_base_dir": str(Path(__file__).parent.parent.parent)}

    # Telegram (opzionale)
    telegram = None
    try:
        from src.utils.telegram_bot import TelegramBot
        telegram = TelegramBot(config)
    except Exception:
        pass

    # Avvia
    orch = SystemOrchestrator(config, telegram)

    import signal
    def handle_sig(signum, frame):
        orch.stop()
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    orch.run()


if __name__ == "__main__":
    main()
