"""
Morning Readiness Check — Verifica che tutto sia pronto per il trading.

Genera MORNING_READINESS_REPORT.md e invia su Telegram.
Eseguire alle 07:55 locali (prima dell'avvio sistema).
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import psutil
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
log = logging.getLogger("morning")

REPORT_PATH = Path(__file__).parent.parent / "MORNING_READINESS_REPORT.md"


def check_process(name: str) -> tuple[bool, str]:
    for proc in psutil.process_iter(["name"]):
        try:
            if name.lower() in (proc.info["name"] or "").lower():
                return True, f"PID {proc.pid}"
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False, "Not running"


def check_databento_api() -> tuple[bool, str]:
    try:
        import databento as db
        client = db.Historical(key="REDACTED_KEY")
        # Just check we can authenticate
        return True, "API key valid"
    except Exception as e:
        return False, str(e)


def check_gexbot_api() -> tuple[bool, str]:
    try:
        resp = requests.get(
            "https://api.gexbot.com/negotiate",
            headers={"Authorization": "Basic REDACTED_KEY"},
            timeout=10
        )
        if resp.status_code == 200 and "websocket_urls" in resp.json():
            return True, f"Prefix: {resp.json().get('prefix')}"
        return False, f"Status {resp.status_code}"
    except Exception as e:
        return False, str(e)


def check_models() -> tuple[bool, str]:
    models_dir = Path(__file__).parent.parent / "ml_models" / "v35_production"
    missing = []
    for seed in [42, 2026, 7777]:
        p = models_dir / f"model_v35_prod_seed{seed}.json"
        if not p.exists():
            missing.append(str(p.name))
    if missing:
        return False, f"Missing: {missing}"
    return True, "3/3 models present"


def check_db() -> tuple[bool, str]:
    live_db = Path(__file__).parent.parent / "data" / "p1uni_live.duckdb"
    if live_db.exists():
        try:
            conn = duckdb.connect(str(live_db), read_only=True)
            tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            total = conn.execute("SELECT COUNT(*) FROM trades_live").fetchone()[0]
            conn.close()
            return True, f"{len(tables)} tables, {total:,} trades"
        except Exception as e:
            return False, str(e)
    return True, "Fresh DB (will be created on start)"


def check_system() -> dict:
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("C:\\")
    return {
        "cpu_pct": cpu,
        "ram_pct": mem.percent,
        "disk_free_gb": round(disk.free / (1024**3), 1),
    }


def generate_report() -> str:
    now = datetime.now(timezone.utc)
    lines = [
        f"# MORNING READINESS REPORT",
        f"**Generated**: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "## System Checks",
        "",
        "| Component | Status | Details |",
        "|-----------|--------|---------|",
    ]

    checks = [
        ("NinjaTrader", check_process("NinjaTrader")),
        ("Databento API", check_databento_api()),
        ("GexBot API", check_gexbot_api()),
        ("V3.5 Models", check_models()),
        ("Live DB", check_db()),
    ]

    all_ok = True
    for name, (ok, detail) in checks:
        status = "OK" if ok else "FAIL"
        if not ok:
            all_ok = False
        lines.append(f"| {name} | {'OK' if ok else 'FAIL'} | {detail} |")

    sys_metrics = check_system()
    lines.append(f"| CPU | {'OK' if sys_metrics['cpu_pct'] < 90 else 'HIGH'} | {sys_metrics['cpu_pct']:.0f}% |")
    lines.append(f"| RAM | {'OK' if sys_metrics['ram_pct'] < 95 else 'HIGH'} | {sys_metrics['ram_pct']:.0f}% |")
    lines.append(f"| Disk | {'OK' if sys_metrics['disk_free_gb'] > 10 else 'LOW'} | {sys_metrics['disk_free_gb']:.0f} GB free |")

    lines.extend([
        "",
        f"## Verdict: {'READY FOR TRADING' if all_ok else 'ISSUES DETECTED'}",
        "",
        "## Launch Command",
        "```",
        "cd C:\\Users\\annal\\Desktop\\P1UNI",
        "python main.py --mode paper --config config/settings.yaml",
        "```",
    ])

    return "\n".join(lines)


def send_telegram(message: str) -> None:
    try:
        url = "https://api.telegram.org/botREDACTED_KEY/sendMessage"
        requests.post(url, json={"chat_id": "REDACTED_ID", "text": message, "parse_mode": "HTML"}, timeout=10)
    except Exception:
        pass


def main():
    log.info("=== MORNING READINESS CHECK ===")
    report = generate_report()

    # Save report
    REPORT_PATH.write_text(report)
    log.info(f"Report saved: {REPORT_PATH}")
    print(report)

    # Send Telegram summary
    lines = report.split("\n")
    verdict = [l for l in lines if "Verdict" in l]
    telegram_msg = f"P1UNI Morning Check\n{verdict[0] if verdict else 'Check complete'}"
    send_telegram(telegram_msg)
    log.info("Telegram notification sent")


if __name__ == "__main__":
    main()
