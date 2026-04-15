"""
P1UNI System Monitor — eseguito ogni 13 minuti da Task Scheduler.

Controlla:
  - Processi in esecuzione (P1UNI, p1-lite, gexbot_scraper)
  - Ultimi trade scritti in trades_live (DB)
  - Ultime decisioni del signal engine (dal log)
  - Errori recenti
  - Stato p1-lite (state.json, live_log CSV)

Invia un report Telegram riassuntivo.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ---- Paths ----------------------------------------------------------------
ROOT = Path(__file__).parent.parent
LOG_FILE = ROOT / "logs" / "p1uni.log"
ENV_FILE = ROOT / ".env"
P1LITE_DIR = Path(r"C:\Users\annal\Desktop\p1-lite")
P1LITE_STATE = P1LITE_DIR / "data" / "state.json"
P1LITE_DATA = P1LITE_DIR / "data"

# ---- Load .env ------------------------------------------------------------
def load_env() -> None:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip(); v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v

load_env()

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.environ.get("TELEGRAM_CHAT_ID", "")

# ---- Telegram sender ------------------------------------------------------
def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("[TELEGRAM] No token/chat configured, printing to stdout:\n" + text)
        return
    try:
        import urllib.request, urllib.parse
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT,
            "text": text,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[TELEGRAM] Send failed: {e}")

# ---- Process check --------------------------------------------------------
def get_python_processes() -> list[dict]:
    """Ritorna lista {pid, cmd} dei processi python.exe con commandline."""
    result = []
    try:
        out = subprocess.check_output(
            ["wmic", "process", "where", "name='python.exe'",
             "get", "processid,commandline"],
            encoding="utf-8", errors="ignore", timeout=10,
        )
        for line in out.splitlines():
            line = line.strip()
            if not line or "ProcessId" in line:
                continue
            # last token is PID
            parts = line.rsplit(None, 1)
            if len(parts) == 2:
                cmd, pid = parts
                try:
                    result.append({"pid": int(pid), "cmd": cmd.strip()})
                except ValueError:
                    pass
    except Exception:
        pass
    return result

def check_processes() -> dict:
    procs = get_python_processes()
    p1uni = next((p for p in procs if "main.py" in p["cmd"] and "--mode" in p["cmd"]), None)
    p1lite = next((p for p in procs if "watchdog.py" in p["cmd"] and "p1-lite" in p["cmd"]), None)
    gexbot = next((p for p in procs if "gexbot_scraper.py" in p["cmd"]), None)
    return {
        "p1uni":  p1uni,
        "p1lite": p1lite,
        "gexbot": gexbot,
    }

# ---- Log analysis ---------------------------------------------------------
def tail_log(n: int = 300) -> list[str]:
    """Ultime N righe del log P1UNI."""
    if not LOG_FILE.exists():
        return []
    try:
        lines = LOG_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
        return lines[-n:]
    except Exception:
        return []

def parse_recent_decisions(lines: list[str], max_decisions: int = 5) -> list[str]:
    """Estrai ultime N decisioni del signal engine."""
    decisions = [l for l in lines if "Decision:" in l and ("signal_engine" in l or "p1uni.execution" in l)]
    return decisions[-max_decisions:]

def parse_trade_writes(lines: list[str]) -> tuple[int, str]:
    """Ultimo write a trades_live: ritorna (total_count, timestamp)."""
    writes = [l for l in lines if "Wrote" in l and "trades_live" in l]
    if not writes:
        return 0, "mai"
    last = writes[-1]
    # [2026-04-14 12:06:58 UTC] ... Wrote 13 records to trades_live (total: 131)
    ts_match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)\]', last)
    total_match = re.search(r'\(total: (\d+)\)', last)
    ts = ts_match.group(1) if ts_match else "?"
    total = int(total_match.group(1)) if total_match else 0
    return total, ts

def count_recent_errors(lines: list[str]) -> int:
    """Conta errori negli ultimi 100 log lines."""
    recent = lines[-100:]
    return sum(1 for l in recent if "] [ERROR]" in l and "quarantine" not in l.lower())

def check_databento_status(lines: list[str]) -> str:
    """Stato connessione Databento."""
    for line in reversed(lines):
        if "CRAM authentication successful" in line:
            return "OK (CRAM auth)"
        if "Wrote" in line and "trades_live" in line:
            return "OK (writing data)"
        if "CRAM authentication error" in line:
            return "ERR (connection limit)"
        if "Databento connected and subscribed" in line:
            return "OK (connected)"
        if "Connection lost" in line and "databento" in line.lower():
            return "RECONNECTING"
    return "UNKNOWN"

def check_gexbot_status(lines: list[str]) -> str:
    """Stato GexBot WS."""
    joins = [l for l in lines if "Joined blue_" in l]
    if joins:
        ts_match = re.search(r'\[(\d{2}:\d{2}:\d{2} UTC)\]', joins[-1])
        ts = ts_match.group(1) if ts_match else "?"
        return f"OK (last join {ts})"
    return "UNKNOWN"

# ---- P1-Lite state --------------------------------------------------------
def check_p1lite() -> dict:
    result = {"state_age_min": None, "csv_age_min": None, "status": "?"}
    try:
        if P1LITE_STATE.exists():
            age_sec = time.time() - P1LITE_STATE.stat().st_mtime
            result["state_age_min"] = round(age_sec / 60, 1)

        today = datetime.now().strftime("%Y-%m-%d")
        csv_file = P1LITE_DATA / f"live_log_{today}.csv"
        if csv_file.exists():
            age_sec = time.time() - csv_file.stat().st_mtime
            result["csv_age_min"] = round(age_sec / 60, 1)

        if result["state_age_min"] is not None and result["state_age_min"] < 5:
            result["status"] = "OK"
        elif result["state_age_min"] is not None:
            result["status"] = f"STALE ({result['state_age_min']}min)"
        else:
            result["status"] = "NO DATA"
    except Exception as e:
        result["status"] = f"ERR: {e}"
    return result

# ---- Report builder -------------------------------------------------------
def build_report() -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = tail_log(400)

    procs = check_processes()
    p1uni_ok = "✅" if procs["p1uni"] else "❌"
    p1lite_ok = "✅" if procs["p1lite"] else "❌"
    gexbot_ok = "✅" if procs["gexbot"] else "⚠️"

    total_trades, last_write_ts = parse_trade_writes(lines)
    decisions = parse_recent_decisions(lines, 3)
    n_errors = count_recent_errors(lines)
    db_status = check_databento_status(lines)
    gex_status = check_gexbot_status(lines)
    p1lite_info = check_p1lite()

    # Format decisions
    decision_lines = []
    for d in decisions:
        # Estrai solo la parte Decision: ... [Xms]
        match = re.search(r'Decision: (.+?) \[[\d.]+ms\]', d)
        if match:
            decision_lines.append("  • " + match.group(1)[:80])

    p1uni_pid = procs["p1uni"]["pid"] if procs["p1uni"] else "N/A"
    mode_match = re.search(r'--mode (\w+)', procs["p1uni"]["cmd"]) if procs["p1uni"] else None
    mode = mode_match.group(1).upper() if mode_match else "?"

    report = (
        f"<b>📊 P1UNI STATUS REPORT</b>\n"
        f"<i>{now_str}</i>\n"
        f"\n"
        f"<b>Processi:</b>\n"
        f"  {p1uni_ok} P1UNI (PID {p1uni_pid}, mode={mode})\n"
        f"  {p1lite_ok} p1-lite watchdog\n"
        f"  {gexbot_ok} gexbot_scraper\n"
        f"\n"
        f"<b>Ingestion:</b>\n"
        f"  Databento: {db_status}\n"
        f"  Trades in DB: {total_trades} (ultimo: {last_write_ts})\n"
        f"  GexBot WS: {gex_status}\n"
        f"\n"
        f"<b>p1-lite:</b>\n"
        f"  state.json: {p1lite_info['state_age_min']} min fa\n"
        f"  live_log CSV: {p1lite_info['csv_age_min']} min fa\n"
        f"\n"
        f"<b>Signal Engine (ultime decisioni):</b>\n"
    )
    if decision_lines:
        report += "\n".join(decision_lines) + "\n"
    else:
        report += "  Nessuna decisione recente\n"

    if n_errors > 0:
        report += f"\n⚠️ <b>Errori recenti (ultimi 100 log):</b> {n_errors}"
    else:
        report += "\n✅ Nessun errore recente"

    return report

# ---- Main -----------------------------------------------------------------
if __name__ == "__main__":
    report = build_report()
    print(report)
    send_telegram(report)
    print("\n[DONE] Report inviato.")
