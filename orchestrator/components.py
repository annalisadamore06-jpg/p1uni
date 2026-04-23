"""Registry of every process that must be alive for automated trading.

Fields per component:
    name:            short id used in logs / reports
    match_cmd:       substring(s) that must ALL appear in the running cmdline to consider this
                     component "alive" (case-insensitive). Use enough specificity to distinguish
                     `main.py --mode paper` (P1UNI) from NT8 or other mains.
    launch_cwd:      working directory for the launch command
    launch_cmd:      exact command to start the component (list form, no shell). First item is
                     the executable, remaining are args. `{pyw}` / `{py}` placeholders are
                     substituted with pythonw.exe / python.exe paths.
    health_file:     path whose mtime we check for staleness (optional).
    max_age_sec:     health threshold. If `health_file` mtime older than this => STALE.
    depends_on:      names of other components that must be UP before this one.
    external:        True = we cannot auto-launch (manual check only — e.g. NinjaTrader UI).
    singleton:       True = exactly one instance. Duplicates get reported and (with --fix) killed
                     except the oldest.
    category:        TWS / IB / DATA / BRIDGE / EXEC / UI — used for grouping in reports.
"""
from __future__ import annotations

PY  = r"C:\Program Files\Python313\python.exe"
PYW = r"C:\Program Files\Python313\pythonw.exe"

TWS_EXE      = r"C:\Jts\tws.exe"
IBC_TWS_LNK  = r"C:\IBC\IBC (TWS).lnk"
NT8_EXE      = r"C:\Program Files (x86)\NinjaTrader 8\bin64\NinjaTrader.exe"

P1UNI_DIR    = r"C:\Users\annal\Desktop\P1UNI"
P1LITE_DIR   = r"C:\Users\annal\Desktop\p1-lite"
WSDB_DIR     = r"C:\Users\annal\Desktop\WEBSOCKET DATABASE"
MLDB_DIR     = r"C:\Users\annal\Desktop\ML DATABASE"

COMPONENTS = [
    # ---- TWS / IB ----
    dict(
        name="TWS",
        category="TWS",
        match_cmd=["ibcalpha.ibc.IbcTws"],
        launch_cwd=r"C:\IBC",
        launch_cmd=["cmd", "/c", "start", "", IBC_TWS_LNK],
        depends_on=[],
        external=False,
        singleton=True,
        port_check=7496,
    ),
    # ---- P1-LITE (ranges MR/OR + state) ----
    dict(
        name="p1lite_collector",
        category="DATA",
        match_cmd=["collector_lite.py"],
        match_name=["python.exe", "pythonw.exe"],  # ignore cmd.exe with the arg
        launch_cwd=P1LITE_DIR,
        launch_cmd=[PYW, "collector_lite.py"],
        health_file=rf"{P1LITE_DIR}\data\state.json",
        max_age_sec=120,
        depends_on=["TWS"],
        singleton=True,
    ),
    dict(
        name="p1lite_dashboard",
        category="UI",
        match_cmd=["app.py"],
        match_name=["python.exe", "pythonw.exe"],
        match_cwd_contains=[r"p1-lite"],  # distinguish from other app.py files
        launch_cwd=P1LITE_DIR,
        launch_cmd=[PY, "app.py"],
        depends_on=[],
        singleton=True,
    ),
    # ---- WEBSOCKET DATABASE (GEXBot stream + bridges) ----
    # NOTE: gexbot_snapshot.json only refreshes during RTH (15:30-22:00 CEST).
    # Outside RTH the file is stale by design. max_age_sec kept generous.
    dict(
        name="gexbot_ws_collector",
        category="DATA",
        match_cmd=["collector.py", "--no-rth"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cwd=WSDB_DIR,
        launch_cmd=[PYW, "collector.py", "--no-rth"],
        depends_on=[],
        singleton=True,
    ),
    dict(
        name="nt8_bridge_scraper",
        category="BRIDGE",
        match_cmd=["nt8_bridge_from_scraper.py"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cwd=WSDB_DIR,
        launch_cmd=[PYW, "nt8_bridge_from_scraper.py"],
        health_file=rf"{WSDB_DIR}\nt8_live_enhanced.json",
        max_age_sec=900,  # 15min — during RTH the bridge refreshes every 60s
        depends_on=["gexbot_ws_collector"],
        singleton=True,
    ),
    # ---- P1UNI (main ML + execution engine) ----
    dict(
        name="p1uni_main",
        category="EXEC",
        match_cmd=["main.py", "--mode", "paper"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cwd=P1UNI_DIR,
        launch_cmd=[PYW, "main.py", "--mode", "paper", "--config", r"config\settings.yaml"],
        depends_on=["TWS"],
        singleton=True,
    ),
    dict(
        name="p1uni_gexbot_scraper",
        category="DATA",
        match_cmd=[r"scripts\gexbot_scraper.py"],
        match_name=["python.exe", "pythonw.exe"],
        launch_cwd=P1UNI_DIR,
        launch_cmd=[PYW, r"scripts\gexbot_scraper.py"],
        health_file=rf"{P1UNI_DIR}\data\gexbot_latest.json",
        max_age_sec=180,
        depends_on=[],
        singleton=True,
    ),
    # ---- NT8 (execution platform, manual) ----
    dict(
        name="NinjaTrader",
        category="EXEC",
        match_cmd=[],  # name-only match (no cmdline for GUI/service)
        match_name=["ninjatrader.exe"],
        launch_cwd=r"C:\Program Files (x86)\NinjaTrader 8\bin64",
        launch_cmd=["cmd", "/c", "start", "", NT8_EXE],
        depends_on=[],
        external=True,
        singleton=True,
    ),
]

# Indexed lookup
BY_NAME = {c["name"]: c for c in COMPONENTS}
