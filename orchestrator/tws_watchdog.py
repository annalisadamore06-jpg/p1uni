# -*- coding: utf-8 -*-
"""TWS watchdog - auto-restart TWS via IBC when port 7496 stops responding.

Design
------
- Probe TCP socket to 127.0.0.1:7496 every CHECK_INTERVAL seconds.
- If the probe fails FAIL_THRESHOLD consecutive times, relaunch TWS via IBC.
- IBC is blocking on first launch (waits for 2FA completion). Annalisa is
  expected to accept the IB phone / Entrust notification when it pops.
- Backs off after each restart to avoid tight relaunch loops while 2FA is
  pending or IBC is starting up.

Standalone usage (foreground, for testing):
    python orchestrator/tws_watchdog.py

As a background service, register with Task Scheduler (run from elevated):
    schtasks /Create /SC ONLOGON /TN P1UNI_TWSWatchdog ^
        /TR "pythonw.exe C:\\Users\\annal\\Desktop\\P1UNI\\orchestrator\\tws_watchdog.py" ^
        /RL LIMITED /F
"""
from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

# ------------------------------------------------------------------
# Config (override via env vars if needed)
# ------------------------------------------------------------------
TWS_HOST = os.environ.get("TWS_HOST", "127.0.0.1")
TWS_PORT = int(os.environ.get("TWS_PORT", "7496"))
CHECK_INTERVAL_SEC = int(os.environ.get("TWS_CHECK_INTERVAL", "30"))
FAIL_THRESHOLD = int(os.environ.get("TWS_FAIL_THRESHOLD", "3"))
RESTART_COOLDOWN_SEC = int(os.environ.get("TWS_RESTART_COOLDOWN", "300"))
IBC_START_BAT = os.environ.get("IBC_START_BAT", r"C:\IBC\StartTWS.bat")
LOG_PATH = os.environ.get(
    "TWS_WATCHDOG_LOG",
    str(Path(__file__).parent.parent / "logs" / "tws_watchdog.log"),
)

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("tws_watchdog")


# ------------------------------------------------------------------
# Probes
# ------------------------------------------------------------------
def probe_tws(host: str = TWS_HOST, port: int = TWS_PORT, timeout: float = 3.0) -> bool:
    """TCP connect probe. Returns True if TWS accepts the connection."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def launch_tws() -> subprocess.Popen:
    """Launch IBC StartTWS.bat detached."""
    if not os.path.exists(IBC_START_BAT):
        log.error("IBC batch missing: %s", IBC_START_BAT)
        raise FileNotFoundError(IBC_START_BAT)

    log.info("Launching TWS via IBC: %s", IBC_START_BAT)
    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP

    proc = subprocess.Popen(
        [IBC_START_BAT],
        cwd=os.path.dirname(IBC_START_BAT),
        creationflags=creationflags,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    log.info("IBC launched, pid=%s", proc.pid)
    return proc


# ------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------
def main() -> int:
    log.info("=" * 60)
    log.info("TWS watchdog starting")
    log.info(
        "  probe=%s:%d interval=%ds fail_threshold=%d cooldown=%ds",
        TWS_HOST, TWS_PORT, CHECK_INTERVAL_SEC, FAIL_THRESHOLD, RESTART_COOLDOWN_SEC,
    )
    log.info("  IBC batch: %s", IBC_START_BAT)
    log.info("=" * 60)

    consecutive_fails = 0
    last_restart_ts: float = 0.0

    while True:
        alive = probe_tws()
        now = time.time()

        if alive:
            if consecutive_fails > 0:
                log.info("TWS recovered after %d failed probes", consecutive_fails)
            consecutive_fails = 0
        else:
            consecutive_fails += 1
            log.warning("TWS probe FAILED (%d/%d)", consecutive_fails, FAIL_THRESHOLD)

            if consecutive_fails >= FAIL_THRESHOLD:
                since_last = now - last_restart_ts
                if since_last < RESTART_COOLDOWN_SEC:
                    log.info(
                        "cooldown active (%.0fs since last restart, need %ds) - waiting",
                        since_last, RESTART_COOLDOWN_SEC,
                    )
                else:
                    try:
                        launch_tws()
                        last_restart_ts = now
                        log.info(
                            "Restart issued. Waiting %ds before next probe cycle.",
                            RESTART_COOLDOWN_SEC,
                        )
                        time.sleep(RESTART_COOLDOWN_SEC)
                        consecutive_fails = 0
                        continue
                    except Exception as e:
                        log.error("Restart attempt failed: %s", e)

        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("Watchdog stopped by user")
        sys.exit(0)
