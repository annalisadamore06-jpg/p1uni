# -*- coding: utf-8 -*-
"""P1UNI Unified Watchdog - single supervisor for the whole trading stack.

Manages 8 services + scheduled jobs:
    1. TWS           (IBC autologin on port 7496)
    2. NinjaTrader   (manual launch, presence check only)
    3. p1uni_main    (signal engine + execution)
    4. gexbot_scraper (P1UNI REST GEX poll)
    5. collector_ws  (WEBSOCKET DB: GEXBot WebSocket collector)
    6. collector_lite (p1-lite range + snapshots)
    7. nt8_bridge_from_scraper (WEBSOCKET DB -> NT8 JSON)
    8. p1lite_dashboard (p1-lite Streamlit/Dash UI)

Scheduled jobs (one-shot per day, tracked separately from services):
    - nightly_harvest (Databento bulk download, 23:05 Zurich, 30min timeout)

Per service:
    - psutil-based identification (name + cmdline substring + optional cwd)
    - pre-launch dedup so we never spawn a duplicate
    - post-launch verification: PID must be alive + non-zombie after a settle
      window, otherwise we record the failure and back off via cooldown
    - auto-launch if DOWN (respecting dependencies + operating hours + cooldown)
    - singleton enforcement (keep oldest PID, terminate newer duplicates)
    - optional health file freshness check
    - optional TCP port check (TWS)

Per job:
    - daily trigger window in Zurich time
    - idempotent (state["jobs"][name]["last_run_date"] = today => skip)
    - singleton-by-PID (won't spawn a second one if previous is still running)
    - hard timeout: process is killed if it exceeds the configured limit

State:
    - P1UNI/data/watchdog_state.json    (last_restart per service, jobs, audit)
    - P1UNI/logs/watchdog.log           (rotating not needed, we append)
    - P1UNI/logs/<job_name>_<date>.log  (per-job stdout/stderr capture)

Usage:
    python watchdog.py                  # one audit + fix, then exit
    python watchdog.py --loop           # supervise forever (default 60s)
    python watchdog.py --loop --interval 30
    python watchdog.py --once --no-fix  # audit-only snapshot

Task Scheduler AtLogOn: see install_watchdog.ps1 (registers via watchdog.bat).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, time as dtime
from pathlib import Path
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    import psutil
except ImportError:
    print("psutil missing. Install with: pip install psutil", file=sys.stderr)
    sys.exit(2)

# ------------------------------------------------------------------
# Paths & constants
# ------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
P1UNI_DIR = ROOT.parent
LOG_PATH = P1UNI_DIR / "logs" / "watchdog.log"
STATE_PATH = P1UNI_DIR / "data" / "watchdog_state.json"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

PY = r"C:\Program Files\Python313\python.exe"
PYW = r"C:\Program Files\Python313\pythonw.exe"

IBC_TWS_LNK = r"C:\IBC\IBC (TWS).lnk"
NT8_EXE = r"C:\Program Files (x86)\NinjaTrader 8\bin64\NinjaTrader.exe"
BRIDGE_PORT = 5555      # ninja_bridge.py TCP server; NT8 connects as client

P1UNI = str(P1UNI_DIR)
P1LITE = r"C:\Users\annal\Desktop\p1-lite"
WSDB = r"C:\Users\annal\Desktop\WEBSOCKET DATABASE"

ZURICH = ZoneInfo("Europe/Zurich")

# Operating windows (Zurich time). CEST applies Mar-Oct; ZoneInfo handles DST.
OP_ALWAYS = "always"
OP_RTH = "rth"                  # 15:25 - 22:05 Mon-Fri
OP_MARKET_HOURS = "market"      # 09:00 - 22:05 Mon-Fri
OP_MARKET_DAYS = "weekday"      # any time Mon-Fri

DEFAULT_COOLDOWN = 300          # 5min between restarts of the same service
LAUNCH_SETTLE = 2               # seconds to wait after launch before next service
LAUNCH_VERIFY_DELAY = 3         # seconds before checking the freshly-spawned PID is alive
LAUNCH_VERIFY_FAIL_COOLDOWN = 120  # if verify fails, mark as restarted so we back off


# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("watchdog")


# ------------------------------------------------------------------
# Service definitions
# ------------------------------------------------------------------
@dataclass
class Service:
    name: str
    category: str                               # TWS | DATA | BRIDGE | EXEC | UI
    launch_cmd: List[str]
    launch_cwd: str
    match_cmd: List[str] = field(default_factory=list)   # ALL substrings must appear in cmdline (lowercased)
    match_name: List[str] = field(default_factory=list)  # process exe name whitelist
    match_cwd: List[str] = field(default_factory=list)   # cwd substring(s) any-match
    depends_on: List[str] = field(default_factory=list)
    port_check: Optional[int] = None
    health_file: Optional[str] = None
    max_age_sec: Optional[int] = None
    singleton: bool = True
    external: bool = False                       # cannot auto-launch reliably (NT8)
    operating_hours: str = OP_ALWAYS
    cooldown_sec: int = DEFAULT_COOLDOWN


SERVICES: List[Service] = [
    Service(
        name="TWS",
        category="TWS",
        match_cmd=["ibcalpha.ibc.ibctws"],
        match_name=["java.exe"],
        launch_cmd=["cmd", "/c", "start", "", IBC_TWS_LNK],
        launch_cwd=r"C:\IBC",
        port_check=7496,
        operating_hours=OP_ALWAYS,
        cooldown_sec=600,                       # IBC 2FA takes time
    ),
    Service(
        name="NinjaTrader",
        category="EXEC",
        match_cmd=[],
        match_name=["ninjatrader.exe"],
        launch_cmd=["cmd", "/c", "start", "", NT8_EXE],
        launch_cwd=r"C:\Program Files (x86)\NinjaTrader 8\bin64",
        external=True,                          # manual start (login prompt)
        port_check=BRIDGE_PORT,                 # bridge must be listening for NT8 to connect
        operating_hours=OP_RTH,                 # alert only during trading window 15:25-22:05
    ),
    Service(
        name="p1uni_main",
        category="EXEC",
        match_cmd=["main.py", "--mode", "paper"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cmd=[PYW, "main.py", "--mode", "paper", "--config", r"config\settings.yaml"],
        launch_cwd=P1UNI,
        depends_on=["TWS"],
        operating_hours=OP_ALWAYS,
    ),
    Service(
        name="gexbot_scraper",
        category="DATA",
        match_cmd=[r"scripts\gexbot_scraper.py"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cmd=[PYW, r"scripts\gexbot_scraper.py"],
        launch_cwd=P1UNI,
        health_file=str(Path(P1UNI) / "data" / "gexbot_latest.json"),
        max_age_sec=180,
        operating_hours=OP_MARKET_HOURS,
    ),
    Service(
        name="collector_ws",
        category="DATA",
        match_cmd=["collector.py", "--no-rth"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cmd=[PYW, "collector.py", "--no-rth"],
        launch_cwd=WSDB,
        operating_hours=OP_ALWAYS,
    ),
    Service(
        name="collector_lite",
        category="DATA",
        match_cmd=["collector_lite.py"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cmd=[PYW, "collector_lite.py"],
        launch_cwd=P1LITE,
        depends_on=["TWS"],
        health_file=str(Path(P1LITE) / "data" / "state.json"),
        max_age_sec=180,
        operating_hours=OP_MARKET_DAYS,  # Sat/Sun: market closed, no data expected
    ),
    Service(
        name="nt8_bridge_from_scraper",
        category="BRIDGE",
        match_cmd=["nt8_bridge_from_scraper.py"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cmd=[PYW, "nt8_bridge_from_scraper.py"],
        launch_cwd=WSDB,
        depends_on=["collector_ws"],
        health_file=str(Path(WSDB) / "nt8_live_enhanced.json"),
        max_age_sec=900,
        operating_hours=OP_MARKET_HOURS,
    ),
    Service(
        name="p1lite_dashboard",
        category="UI",
        match_cmd=["app.py"],
        match_name=["python.exe", "pythonw.exe"],
        match_cwd=["p1-lite"],
        launch_cmd=[PY, "app.py"],
        launch_cwd=P1LITE,
        operating_hours=OP_ALWAYS,
    ),
]

BY_NAME = {s.name: s for s in SERVICES}


# ------------------------------------------------------------------
# Operating hours
# ------------------------------------------------------------------
def _in_window(now: datetime, start: dtime, end: dtime) -> bool:
    return start <= now.timetz().replace(tzinfo=None) <= end


def should_be_up(svc: Service, now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(ZURICH)
    weekday = now.weekday() < 5
    t = now.time()
    if svc.operating_hours == OP_ALWAYS:
        return True
    if svc.operating_hours == OP_MARKET_DAYS:
        return weekday
    if svc.operating_hours == OP_MARKET_HOURS:
        return weekday and dtime(9, 0) <= t <= dtime(22, 5)
    if svc.operating_hours == OP_RTH:
        return weekday and dtime(15, 25) <= t <= dtime(22, 5)
    return True


# ------------------------------------------------------------------
# Process discovery
# ------------------------------------------------------------------
def snapshot_procs() -> List[dict]:
    out = []
    for p in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
        try:
            info = p.info
            cmd = " ".join(info.get("cmdline") or []).lower()
            try:
                cwd = (p.cwd() or "").lower()
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                cwd = ""
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


def match_service(svc: Service, procs: List[dict]) -> List[dict]:
    names = [n.lower() for n in svc.match_name]
    cmd_needles = [n.lower() for n in svc.match_cmd]
    cwd_needles = [n.lower() for n in svc.match_cwd]
    hits = []
    for p in procs:
        if names and p["name"] not in names:
            continue
        if cmd_needles and not all(n in p["cmdline"] for n in cmd_needles):
            continue
        if cwd_needles and not any(n in p["cwd"] for n in cwd_needles):
            continue
        # require at least one positive filter so we never match everything
        if not (names or cmd_needles):
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
    return (time.time() - p.stat().st_mtime) if p.exists() else None


# ------------------------------------------------------------------
# Inspection result
# ------------------------------------------------------------------
@dataclass
class Status:
    name: str
    category: str
    expected_up: bool
    alive: bool = False
    pids: List[int] = field(default_factory=list)
    duplicate: bool = False
    stale: bool = False
    port_ok: Optional[bool] = None
    health_age_sec: Optional[float] = None
    reason: str = ""
    action: str = ""                           # what the watchdog did this cycle


def inspect(svc: Service, procs: List[dict]) -> Status:
    st = Status(name=svc.name, category=svc.category, expected_up=should_be_up(svc))
    hits = match_service(svc, procs)
    st.pids = sorted(p["pid"] for p in hits)
    st.alive = bool(hits)
    st.duplicate = svc.singleton and len(hits) > 1

    if svc.port_check is not None:
        st.port_ok = port_listening(svc.port_check)
        if not st.port_ok:
            if not svc.external:
                # For managed services, port down means dead (e.g. TWS not yet ready).
                # For external services (NT8), port is informational: process may be alive
                # but bridge not in live mode — don't override the process match result.
                st.alive = False
            st.reason = f"port {svc.port_check} not listening"
        elif not st.alive:
            # Port is listening even though process match failed (e.g. TWS started
            # manually instead of via IBC). Treat port as primary liveness signal.
            st.alive = True
            st.reason = f"port {svc.port_check} listening (process not matched)"

    if st.alive and svc.health_file and svc.max_age_sec:
        age = file_age(svc.health_file)
        st.health_age_sec = age
        if age is None:
            st.stale = True
            st.reason = f"health_file missing: {svc.health_file}"
        elif age > svc.max_age_sec:
            st.stale = True
            st.reason = f"stale {age:.0f}s > {svc.max_age_sec}s"

    return st


# ------------------------------------------------------------------
# Actions: kill, launch
# ------------------------------------------------------------------
def oldest_pid(pids: List[int]) -> int:
    ages = []
    for pid in pids:
        try:
            ages.append((psutil.Process(pid).create_time(), pid))
        except psutil.NoSuchProcess:
            pass
    if not ages:
        return pids[0] if pids else -1
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


def pid_alive(pid: int) -> bool:
    """Returns True iff pid corresponds to a running, non-terminated process."""
    if pid is None or pid <= 0:
        return False
    try:
        p = psutil.Process(pid)
        if not p.is_running():
            return False
        # On Windows STATUS_ZOMBIE is rare but STATUS_DEAD/STOPPED can occur
        return p.status() not in (psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE, psutil.STATUS_STOPPED)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def launch_service(svc: Service) -> Optional[int]:
    """Spawn the service and return the immediate Popen PID (may be a wrapper)."""
    log.info("LAUNCH %s cwd=%s cmd=%s", svc.name, svc.launch_cwd, svc.launch_cmd)
    try:
        creation = 0
        if sys.platform == "win32":
            creation = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        p = subprocess.Popen(
            svc.launch_cmd,
            cwd=svc.launch_cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=creation,
            close_fds=True,
        )
        return p.pid
    except Exception as e:
        log.error("launch %s failed: %s", svc.name, e)
        return None


def verify_service_alive(svc: Service) -> Tuple[bool, List[int]]:
    """Re-snapshot processes and confirm svc is now alive via the same match logic.

    Used after launch to catch zombies / immediate crashes. Works for all launch
    styles (Python direct, cmd /c wrappers, .lnk shortcuts) because we look at the
    final process tree, not the Popen handle.
    """
    procs = snapshot_procs()
    hits = match_service(svc, procs)
    pids = sorted(p["pid"] for p in hits)
    alive = bool(hits)
    if alive and svc.port_check is not None:
        alive = port_listening(svc.port_check)
    return alive, pids


# ------------------------------------------------------------------
# State file
# ------------------------------------------------------------------
def _empty_state() -> dict:
    return {"last_restart": {}, "jobs": {}, "last_cycle": None}


def load_state() -> dict:
    if not STATE_PATH.exists():
        return _empty_state()
    try:
        s = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("state file corrupted (%s), starting fresh", e)
        return _empty_state()
    s.setdefault("last_restart", {})
    s.setdefault("jobs", {})
    s.setdefault("last_cycle", None)
    return s


def save_state(state: dict) -> None:
    try:
        STATE_PATH.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
    except Exception as e:
        log.error("save_state failed: %s", e)


# ------------------------------------------------------------------
# Scheduled jobs (one-shot per day, separate from long-running services)
# ------------------------------------------------------------------
@dataclass
class Job:
    name: str
    launch_cmd: List[str]
    launch_cwd: str
    trigger_hour: int                  # Zurich hour
    trigger_minute: int                # Zurich minute
    timeout_sec: int                   # hard kill after this many seconds
    weekday_only: bool = False         # if True, never run on Sat/Sun
    log_dir: str = str(P1UNI_DIR / "logs")


JOBS: List[Job] = [
    Job(
        name="nightly_harvest",
        launch_cmd=[PY, r"scripts\nightly_data_harvest.py", "--days", "2"],
        launch_cwd=str(P1UNI_DIR),
        trigger_hour=23,
        trigger_minute=5,
        timeout_sec=1800,              # 30 minutes hard cap
        weekday_only=False,            # also run on Sat morning to catch Fri data
    ),
]


def _today_zurich() -> str:
    return datetime.now(ZURICH).date().isoformat()


def _job_state(state: dict, name: str) -> dict:
    return state.setdefault("jobs", {}).setdefault(name, {})


def _spawn_job(job: Job) -> Optional[int]:
    """Spawn a job as a tracked subprocess with stdout/stderr captured to a log file."""
    log_path = Path(job.log_dir) / f"{job.name}_{_today_zurich()}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("JOB START %s -> %s", job.name, log_path)
    try:
        # Open log in append mode so re-runs in the same day extend the file
        fh = open(log_path, "ab", buffering=0)
        creation = 0
        if sys.platform == "win32":
            creation = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        p = subprocess.Popen(
            job.launch_cmd,
            cwd=job.launch_cwd,
            stdout=fh,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            creationflags=creation,
            close_fds=True,
        )
        return p.pid
    except Exception as e:
        log.error("JOB %s spawn failed: %s", job.name, e)
        return None


def run_jobs(state: dict) -> None:
    """Each cycle: poll any in-flight job (kill on timeout), then maybe trigger
    today's run if the scheduled window has been reached and we haven't run yet."""
    now = datetime.now(ZURICH)
    today = now.date().isoformat()

    for job in JOBS:
        js = _job_state(state, job.name)

        # 1) Poll an in-flight run for completion or timeout
        running_pid = js.get("pid")
        if running_pid:
            if pid_alive(running_pid):
                started = js.get("started_at_ts", time.time())
                elapsed = time.time() - started
                if elapsed > job.timeout_sec:
                    log.error("JOB %s TIMEOUT after %.0fs (limit %ds) — killing pid=%d",
                              job.name, elapsed, job.timeout_sec, running_pid)
                    kill_pid(running_pid)
                    js["pid"] = None
                    js["last_status"] = "timeout"
                    js["last_run_date"] = today        # do not retry today
                    js["finished_at"] = now.isoformat(timespec="seconds")
                else:
                    # still running — nothing to do, will be re-checked next cycle
                    log.info("JOB %s still running pid=%d elapsed=%.0fs",
                             job.name, running_pid, elapsed)
                    continue
            else:
                log.info("JOB %s finished pid=%d", job.name, running_pid)
                js["pid"] = None
                js["last_status"] = "finished"
                js["last_run_date"] = today
                js["finished_at"] = now.isoformat(timespec="seconds")

        # 2) Trigger if today's window reached and we haven't run yet
        if js.get("last_run_date") == today:
            continue
        if job.weekday_only and now.weekday() >= 5:
            continue
        scheduled = dtime(job.trigger_hour, job.trigger_minute)
        if now.time() < scheduled:
            continue
        # Avoid spawning if a previous PID is somehow still around (defense in depth)
        if running_pid and pid_alive(running_pid):
            log.warning("JOB %s skip-trigger: previous pid=%d still alive",
                        job.name, running_pid)
            continue

        pid = _spawn_job(job)
        if pid is None:
            js["last_status"] = "spawn_failed"
            js["last_run_date"] = today                # don't hammer-retry
        else:
            js["pid"] = pid
            js["started_at"] = now.isoformat(timespec="seconds")
            js["started_at_ts"] = time.time()
            js["last_status"] = "running"


# ------------------------------------------------------------------
# Supervisor
# ------------------------------------------------------------------
def supervise(state: dict, fix: bool) -> dict:
    procs = snapshot_procs()
    statuses = {svc.name: inspect(svc, procs) for svc in SERVICES}
    now_ts = time.time()

    # Pass 1: dedupe singletons (kill before launch so we never spawn into duplicates)
    if fix:
        for svc in SERVICES:
            st = statuses[svc.name]
            if st.duplicate:
                keep = oldest_pid(st.pids)
                killed = []
                for pid in st.pids:
                    if pid != keep and kill_pid(pid):
                        killed.append(pid)
                log.warning("DEDUP %s keep=%d killed=%s", svc.name, keep, killed)
                st.action = f"dedup keep={keep}"
                st.pids = [keep]
                st.duplicate = False

    # Pass 2: launch missing honoring deps + hours + cooldown
    just_launched: List[str] = []
    if fix:
        launched = set()
        for _ in range(3):                          # up to 3 passes for deps
            progress = False
            for svc in SERVICES:
                if svc.name in launched:
                    continue
                st = statuses[svc.name]

                if not st.expected_up:
                    st.action = st.action or "skip-hours"
                    launched.add(svc.name)
                    continue

                if st.alive and not st.stale:
                    launched.add(svc.name)
                    continue

                if svc.external:
                    if not st.alive:
                        port_info = ""
                        if svc.port_check is not None:
                            port_info = (f" | bridge port {svc.port_check} "
                                         f"{'OK' if st.port_ok else 'DOWN'}")
                        if st.expected_up:
                            log.error("EXTERNAL %s DOWN during trading hours%s — manual start required",
                                      svc.name, port_info)
                        else:
                            log.warning("EXTERNAL %s DOWN%s — manual start required",
                                        svc.name, port_info)
                        st.action = "external-down"
                    elif st.stale:
                        log.warning("EXTERNAL %s STALE (age=%.0fs) — bridge may not be connected to NT8",
                                    svc.name, st.health_age_sec or 0)
                        st.action = "external-stale"
                    launched.add(svc.name)
                    continue

                deps_ok = all(
                    statuses[d].alive and not statuses[d].stale
                    for d in svc.depends_on
                )
                if not deps_ok:
                    continue

                last = state["last_restart"].get(svc.name, 0.0)
                since = now_ts - last
                if since < svc.cooldown_sec:
                    st.action = f"cooldown ({int(svc.cooldown_sec - since)}s left)"
                    log.info("COOLDOWN %s %ds remaining", svc.name, int(svc.cooldown_sec - since))
                    launched.add(svc.name)
                    continue

                # PRE-LAUNCH safety re-check: confirm singleton is truly empty.
                # Guards against the race where a process spawned between snapshot
                # and now (e.g. by Task Scheduler) and we'd otherwise create a duplicate.
                if svc.singleton:
                    fresh_hits = match_service(svc, snapshot_procs())
                    if fresh_hits:
                        log.info("SKIP-LAUNCH %s already alive (race-detected pids=%s)",
                                 svc.name, [p["pid"] for p in fresh_hits])
                        st.alive = True
                        st.pids = sorted(p["pid"] for p in fresh_hits)
                        st.action = "race-detected, skipped launch"
                        launched.add(svc.name)
                        continue

                pid = launch_service(svc)
                if pid is None:
                    st.action = "launch_failed"
                    state["last_restart"][svc.name] = now_ts  # back off
                    launched.add(svc.name)
                    continue

                state["last_restart"][svc.name] = now_ts
                st.action = f"launched pid={pid}"
                just_launched.append(svc.name)
                launched.add(svc.name)
                progress = True
                time.sleep(LAUNCH_SETTLE)
            if not progress:
                break

    # Pass 3: post-launch verification — re-snapshot, confirm each launched service
    # is actually visible in the process tree (catches zombies, immediate crashes,
    # and wrapper-launched programs that did not start the real child).
    if fix and just_launched:
        time.sleep(LAUNCH_VERIFY_DELAY)
        for name in just_launched:
            svc = BY_NAME[name]
            alive, pids = verify_service_alive(svc)
            st = statuses[name]
            if alive:
                st.alive = True
                st.pids = pids
                st.action = (st.action or "") + f" -> verified alive pids={pids}"
                log.info("VERIFY %s OK pids=%s", name, pids)
            else:
                st.alive = False
                st.action = (st.action or "") + " -> verify FAILED (zombie or crashed)"
                log.error("VERIFY %s FAILED — process not visible after launch (cooldown applied)",
                          name)
                # last_restart already set above, so the configured cooldown
                # prevents a tight relaunch loop.

    # Run scheduled jobs (idempotent, singleton, timeout-bounded)
    if fix:
        try:
            run_jobs(state)
        except Exception as e:
            log.exception("run_jobs failed: %s", e)

    state["last_cycle"] = datetime.now(ZURICH).isoformat(timespec="seconds")
    state["statuses"] = {n: asdict(s) for n, s in statuses.items()}
    save_state(state)
    return statuses


# ------------------------------------------------------------------
# Reporting
# ------------------------------------------------------------------
def pretty(statuses: dict, state: Optional[dict] = None) -> str:
    now = datetime.now(ZURICH).strftime("%Y-%m-%d %H:%M:%S %Z")
    out = [f"=== P1UNI WATCHDOG @ {now} ==="]
    groups: dict = {}
    for st in statuses.values():
        groups.setdefault(st.category, []).append(st)
    for cat in ["TWS", "DATA", "BRIDGE", "EXEC", "UI"]:
        if cat not in groups:
            continue
        out.append(f"[{cat}]")
        for st in groups[cat]:
            if not st.expected_up:
                icon = "OFF "
            elif st.duplicate:
                icon = "DUP "
            elif st.stale:
                icon = "STAL"
            elif st.alive:
                icon = "OK  "
            else:
                icon = "DOWN"
            extras = []
            if st.pids:
                extras.append(f"pids={st.pids}")
            if st.health_age_sec is not None:
                extras.append(f"age={st.health_age_sec:.0f}s")
            if st.port_ok is False:
                extras.append("port=DOWN")
            if st.reason:
                extras.append(st.reason)
            if st.action:
                extras.append(f"act={st.action}")
            out.append(f"  [{icon}] {st.name:28s} {' '.join(extras)}")

    if state and state.get("jobs"):
        out.append("[JOBS]")
        for job in JOBS:
            js = state["jobs"].get(job.name, {})
            status = js.get("last_status", "never-run")
            last_date = js.get("last_run_date", "—")
            pid = js.get("pid") or "—"
            out.append(f"  [{status:10s}] {job.name:28s} last_run={last_date} pid={pid} "
                       f"trigger={job.trigger_hour:02d}:{job.trigger_minute:02d} "
                       f"timeout={job.timeout_sec}s")
    return "\n".join(out)


# ------------------------------------------------------------------
# Entry
# ------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="P1UNI Unified Watchdog")
    ap.add_argument("--loop", action="store_true", help="supervise forever")
    ap.add_argument("--once", action="store_true", help="one cycle then exit")
    ap.add_argument("--no-fix", action="store_true", help="audit only, do not kill/launch")
    ap.add_argument("--interval", type=int, default=60, help="seconds between cycles (loop mode)")
    args = ap.parse_args()

    fix = not args.no_fix
    log.info("=" * 70)
    log.info("P1UNI watchdog starting: loop=%s fix=%s interval=%ds services=%d",
             args.loop, fix, args.interval, len(SERVICES))
    log.info("=" * 70)

    state = load_state()

    def cycle():
        try:
            statuses = supervise(state, fix=fix)
            log.info("\n" + pretty(statuses, state))
        except Exception as e:
            log.exception("cycle failed: %s", e)

    cycle()
    if not args.loop or args.once:
        return 0

    while True:
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            log.info("watchdog stopped by user")
            return 0
        cycle()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("watchdog stopped by user")
        sys.exit(0)
