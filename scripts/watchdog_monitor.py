"""
Watchdog Monitor — Gira in background, controlla stato P1UNI ogni N minuti.

Ogni ciclo:
  1. Verifica che il processo main.py sia vivo
  2. Legge gli ultimi log (Databento writes, GexBot messages, signals)
  3. Query al DB (se sbloccabile) per contare trade recenti
  4. Invia snapshot via Telegram ogni INTERVAL_MIN
  5. Se trova problemi -> alert Telegram

Usage:
    python scripts/watchdog_monitor.py --interval 10  # check ogni 10 min
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psutil
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

BASE_DIR = Path(__file__).parent.parent
LOG_PATH = BASE_DIR / "logs" / "p1uni.log"
WATCHDOG_LOG = BASE_DIR / "logs" / "watchdog.log"

from src.core.secrets import get_secret
TELEGRAM_TOKEN = get_secret("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = get_secret("TELEGRAM_CHAT_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [WATCHDOG] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(WATCHDOG_LOG, encoding="utf-8"),
    ]
)
log = logging.getLogger("watchdog")


def is_p1uni_running() -> tuple[bool, int]:
    """True se main.py di P1UNI e' in esecuzione."""
    for proc in psutil.process_iter(["cmdline", "pid"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if any("main.py" in str(a) for a in cmdline) and any("P1UNI" in str(a) or "p1uni" in str(a).lower() for a in cmdline):
                return True, proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False, 0


def get_log_stats() -> dict:
    """Estrae statistiche dagli ultimi log."""
    if not LOG_PATH.exists():
        return {"error": "log file not found"}

    try:
        content = LOG_PATH.read_text(encoding="utf-8", errors="ignore")
        lines = content.split("\n")

        # Ultimi 1000 righe
        recent = lines[-1000:] if len(lines) > 1000 else lines

        # Databento writes (ultimo total)
        databento_total = 0
        databento_last_ts = None
        for line in reversed(recent):
            m = re.search(r"Wrote \d+ records to trades_live \(total: (\d+)\)", line)
            if m:
                databento_total = int(m.group(1))
                ts_m = re.search(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if ts_m:
                    databento_last_ts = ts_m.group(1)
                break

        # Databento errors recenti
        databento_errors = sum(1 for l in recent if "Authentication failed" in l or "Connection lost" in l)

        # GexBot joins e messages
        gexbot_joins = sum(1 for l in recent if "Joined blue_" in l)
        gexbot_connected = sum(1 for l in recent if "gexbot_ws] Connected. Listening" in l)

        # Signal engine decisions
        last_decision = None
        for line in reversed(recent):
            if "Decision:" in line:
                last_decision = line.split("Decision:")[-1].strip()[:100]
                break

        # Phase corrente
        phase = None
        for line in reversed(recent):
            m = re.search(r"phase=(\w+)", line)
            if m:
                phase = m.group(1)
                break

        return {
            "databento_total": databento_total,
            "databento_last": databento_last_ts,
            "databento_errors": databento_errors,
            "gexbot_joins": gexbot_joins,
            "gexbot_connected": gexbot_connected,
            "last_decision": last_decision,
            "phase": phase,
        }
    except Exception as e:
        return {"error": str(e)}


def send_telegram(message: str) -> bool:
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
        return resp.status_code == 200
    except Exception:
        return False


def build_report(stats: dict, running: bool, pid: int) -> str:
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [f"<b>P1UNI Watchdog — {now}</b>", ""]

    if running:
        lines.append(f"✅ P1UNI Running (PID {pid})")
    else:
        lines.append(f"❌ P1UNI NOT RUNNING")

    if "error" in stats:
        lines.append(f"⚠️ Log error: {stats['error']}")
    else:
        lines.append(f"📈 Databento: {stats.get('databento_total', 0):,} trades")
        if stats.get('databento_last'):
            lines.append(f"   Last write: {stats['databento_last']}")
        if stats.get('databento_errors', 0) > 0:
            lines.append(f"   ⚠️ {stats['databento_errors']} recent auth/conn errors")

        lines.append(f"📊 GexBot: {stats.get('gexbot_joins', 0)} joins, {stats.get('gexbot_connected', 0)} connections")

        if stats.get('phase'):
            lines.append(f"⏰ Phase: {stats['phase']}")

        if stats.get('last_decision'):
            lines.append(f"🧠 Last signal: {stats['last_decision'][:80]}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=10, help="Check interval in minutes")
    parser.add_argument("--telegram", action="store_true", help="Send Telegram on each cycle")
    args = parser.parse_args()

    log.info(f"Watchdog started, interval={args.interval} min, telegram={args.telegram}")

    while True:
        try:
            running, pid = is_p1uni_running()
            stats = get_log_stats()
            report = build_report(stats, running, pid)

            log.info(f"Check: running={running}, databento_total={stats.get('databento_total', 0)}")
            print(report)
            print()

            if args.telegram:
                send_telegram(report)

        except Exception as e:
            log.error(f"Watchdog cycle error: {e}")

        time.sleep(args.interval * 60)


if __name__ == "__main__":
    main()
