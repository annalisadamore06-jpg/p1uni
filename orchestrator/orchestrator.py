"""P1 System Orchestrator — single source of truth for what must be alive.

Usage:
    python orchestrator.py                 # audit only (read-only report, default)
    python orchestrator.py --fix            # kill duplicates + launch missing ONCE
    python orchestrator.py --loop --fix     # supervise forever (60s cycle)
    python orchestrator.py --json           # machine-readable JSON output

Philosophy: this does NOT replace component-level watchdogs. It is the umbrella
that ensures exactly ONE instance of each required component is alive, in the
right dependency order, with fresh output files. Component-level internals
(reconnects, retries, crash handling) remain the responsibility of the component.

Design choices:
    * Read-only by default. `--fix` is opt-in to prevent accidental kills.
    * Dedup keeps the OLDEST process (longest-alive = most stable); newer
      duplicates are terminated.
    * Dependencies are resolved bottom-up; a missing dep blocks spawning.
    * Health = process alive AND (if defined) output file mtime < max_age_sec.
    * TWS health also verifies port 7496 LISTENING.
    * Logs: orchestrator/logs/orchestrator_YYYY-MM-DD.log
"""
from __future__ import annotations

import argparse
import json
import logging
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

try:
    import psutil
except ImportError:
    print("psutil missing. Install with: pip install psutil", file=sys.stderr)
    sys.exit(2)

from components import COMPONENTS, BY_NAME

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="%(asctime)s %(levelname)-7s %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_DIR / f"orchestrator_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("orch")


# ---------------- models ----------------

@dataclass
class Inspect:
    name: str
    category: str
    pids: List[int] = field(default_factory=list)
    alive: bool = False
    duplicate: bool = False
    stale: bool = False
    health_age_sec: Optional[float] = None
    port_ok: Optional[bool] = None
    reason: str = ""


# ---------------- helpers ----------------

def list_running() -> list[dict]:
    out = []
    for p in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
        try:
            info = p.info
            cmd = " ".join(info.get("cmdline") or []).lower()
            cwd = ""
            try:
                cwd = (p.cwd() or "").lower()
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass
            out.append({
                "pid": info["pid"],
                "name": (info["name"] or "").lower(),
                "cmdline": cmd,
                "cwd": cwd,
                "create_time": info.get("create_time") or 0.0,
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return out


def match(comp: dict, procs: list[dict]) -> list[dict]:
    cmd_needles = [n.lower() for n in comp.get("match_cmd", [])]
    name_whitelist = [n.lower() for n in comp.get("match_name", [])]
    cwd_needles = [n.lower() for n in comp.get("match_cwd_contains", [])]
    hits = []
    for p in procs:
        if name_whitelist and p["name"] not in name_whitelist:
            continue
        if cmd_needles and not all(n in p["cmdline"] for n in cmd_needles):
            continue
        if cwd_needles and not any(n in p["cwd"] for n in cwd_needles):
            continue
        # guard: if no filters at all, never match everything
        if not (cmd_needles or name_whitelist):
            continue
        hits.append(p)
    return hits


def port_listening(port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        return s.connect_ex(("127.0.0.1", port)) == 0
    finally:
        s.close()


def file_age(path: str) -> Optional[float]:
    p = Path(path)
    if not p.exists():
        return None
    return time.time() - p.stat().st_mtime


def inspect_component(comp: dict, procs: list[dict]) -> Inspect:
    r = Inspect(name=comp["name"], category=comp["category"])
    found = match(comp, procs)
    r.pids = sorted(p["pid"] for p in found)
    r.alive = bool(found)
    r.duplicate = len(found) > 1 and comp.get("singleton")

    if r.alive and comp.get("health_file"):
        age = file_age(comp["health_file"])
        r.health_age_sec = age
        if age is None:
            r.stale = True
            r.reason = f"health_file missing: {comp['health_file']}"
        elif age > comp["max_age_sec"]:
            r.stale = True
            r.reason = f"stale {age:.0f}s > {comp['max_age_sec']}s"

    if comp.get("port_check"):
        r.port_ok = port_listening(comp["port_check"])
        if not r.port_ok:
            r.alive = False
            r.reason = f"port {comp['port_check']} not listening"

    return r


def oldest_pid(pids: list[int]) -> int:
    if not pids:
        return -1
    ages = []
    for pid in pids:
        try:
            ages.append((psutil.Process(pid).create_time(), pid))
        except psutil.NoSuchProcess:
            pass
    if not ages:
        return pids[0]
    ages.sort()
    return ages[0][1]


def kill_pid(pid: int) -> bool:
    try:
        p = psutil.Process(pid)
        p.terminate()
        try:
            p.wait(5)
        except psutil.TimeoutExpired:
            p.kill()
        return True
    except psutil.NoSuchProcess:
        return True
    except Exception as e:
        log.error("kill %d failed: %s", pid, e)
        return False


def launch(comp: dict) -> Optional[int]:
    log.info("LAUNCH %s cwd=%s cmd=%s", comp["name"], comp["launch_cwd"], comp["launch_cmd"])
    try:
        creation = 0
        if sys.platform == "win32":
            creation = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        p = subprocess.Popen(
            comp["launch_cmd"],
            cwd=comp["launch_cwd"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=creation,
            close_fds=True,
        )
        return p.pid
    except Exception as e:
        log.error("launch %s failed: %s", comp["name"], e)
        return None


# ---------------- core ----------------

def audit() -> dict:
    procs = list_running()
    results = {c["name"]: inspect_component(c, procs) for c in COMPONENTS}
    return results


def fix(results: dict) -> None:
    """1) kill duplicates (keep oldest)  2) launch missing respecting deps."""
    # pass 1 — dedupe
    for name, r in results.items():
        if r.duplicate:
            keep = oldest_pid(r.pids)
            for pid in r.pids:
                if pid != keep:
                    log.warning("DUPLICATE %s pid=%d (keep=%d) => terminating", name, pid, keep)
                    kill_pid(pid)

    # pass 2 — launch missing in dependency order
    launched = set()
    MAX_PASSES = 3
    for _ in range(MAX_PASSES):
        progress = False
        for comp in COMPONENTS:
            name = comp["name"]
            if name in launched:
                continue
            r = results[name]
            deps_ok = all(results[d].alive and not results[d].stale for d in comp.get("depends_on", []))
            if r.alive and not r.stale:
                launched.add(name)
                continue
            if comp.get("external"):
                if not r.alive:
                    log.warning("EXTERNAL %s is DOWN — manual start required (%s)", name, comp["launch_cwd"])
                launched.add(name)
                continue
            if not deps_ok:
                continue
            pid = launch(comp)
            if pid is not None:
                launched.add(name)
                progress = True
                time.sleep(2)
        if not progress:
            break


def pretty(results: dict) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    lines = [f"\n=== P1 ORCHESTRATOR @ {now} ==="]
    groups: dict[str, list] = {}
    for r in results.values():
        groups.setdefault(r.category, []).append(r)
    order = ["TWS", "DATA", "BRIDGE", "EXEC", "UI"]
    for cat in order:
        if cat not in groups:
            continue
        lines.append(f"\n[{cat}]")
        for r in groups[cat]:
            if r.alive and not r.stale and not r.duplicate:
                icon = "OK  "
            elif r.duplicate:
                icon = "DUP "
            elif r.stale:
                icon = "STAL"
            else:
                icon = "DOWN"
            extra = []
            if r.pids:
                extra.append(f"pids={r.pids}")
            if r.health_age_sec is not None:
                extra.append(f"age={r.health_age_sec:.0f}s")
            if r.port_ok is False:
                extra.append("port=DOWN")
            if r.reason:
                extra.append(r.reason)
            lines.append(f"  [{icon}] {r.name:24s} {' '.join(extra)}")
    return "\n".join(lines)


def to_json(results: dict) -> str:
    def asdict(r: Inspect) -> dict:
        return {
            "name": r.name, "category": r.category, "pids": r.pids,
            "alive": r.alive, "duplicate": r.duplicate, "stale": r.stale,
            "health_age_sec": r.health_age_sec, "port_ok": r.port_ok,
            "reason": r.reason,
        }
    return json.dumps({k: asdict(v) for k, v in results.items()}, indent=2)


# ---------------- entry ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true", help="kill duplicates + launch missing")
    ap.add_argument("--loop", action="store_true", help="run forever (60s cycle)")
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--json", action="store_true", help="JSON output instead of text")
    args = ap.parse_args()

    def cycle():
        results = audit()
        if args.json:
            print(to_json(results))
        else:
            print(pretty(results))
        if args.fix:
            fix(results)

    cycle()
    if not args.loop:
        return

    while True:
        time.sleep(args.interval)
        try:
            cycle()
        except KeyboardInterrupt:
            log.info("interrupted — exiting loop")
            return
        except Exception as e:
            log.exception("cycle failed: %s", e)


if __name__ == "__main__":
    main()
