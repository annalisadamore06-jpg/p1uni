# P1UNI Unified Watchdog

Single-file supervisor for the whole trading stack. Replaces the old
`orchestrator.py` + `components.py` + `tws_watchdog.py` split.

---

## Services (8)

| # | Name | Category | Auto-launch | Singleton | Deps | Operating hours |
|---|---|---|---|---|---|---|
| 1 | TWS | TWS | via IBC (.lnk) | ✓ | — | always |
| 2 | NinjaTrader | EXEC | manual (external) | ✓ | — | weekdays |
| 3 | p1uni_main | EXEC | ✓ | ✓ | TWS | always |
| 4 | gexbot_scraper | DATA | ✓ | ✓ | — | market (09:00–22:05) |
| 5 | collector_ws | DATA | ✓ | ✓ | — | always |
| 6 | collector_lite | DATA | ✓ | ✓ | TWS | always |
| 7 | nt8_bridge_from_scraper | BRIDGE | ✓ | ✓ | collector_ws | market |
| 8 | p1lite_dashboard | UI | ✓ | ✓ | — | always |

All times Zurich (CEST/CET, auto via `zoneinfo`).

---

## Usage

```bash
# One audit + fix, then exit
python watchdog.py

# Supervise forever (60s cycle)
python watchdog.py --loop

# Read-only snapshot (no kills, no launches)
python watchdog.py --once --no-fix

# Double-click launcher (same as --loop with auto-restart wrapper)
watchdog.bat
```

---

## What the watchdog does each cycle

1. **Snapshot** all Python/java/NT8 processes via `psutil`.
2. **Identify** each service by `match_name` + `match_cmd` + optional `match_cwd`.
3. **Check health**: alive? port listening (TWS)? health-file mtime fresh?
4. **Dedup**: if `singleton=True` and >1 PIDs, terminate all except the oldest.
5. **Launch missing**: respects `depends_on`, `operating_hours`, and per-service
   `cooldown_sec` (default 300s, TWS 600s to avoid IBC 2FA loops).
6. **Persist state** to `P1UNI/data/watchdog_state.json`.
7. **Log** human-readable status block to `P1UNI/logs/watchdog.log`.

---

## Status icons

| Icon | Meaning |
|---|---|
| `OK  ` | alive + fresh + singleton-respected |
| `DOWN` | no matching process found (or port/health failed) |
| `DUP ` | multiple PIDs, will dedupe next cycle |
| `STAL` | alive but health_file older than `max_age_sec` |
| `OFF ` | outside operating hours — intentionally not running |

---

## Task Scheduler

```powershell
powershell -ExecutionPolicy Bypass -File install_watchdog.ps1
```

Registers `P1UNI_Watchdog` with `AtLogOn` + `AtStartup` triggers. Old
`P1_Orchestrator` task is automatically removed.

---

## State file

`P1UNI/data/watchdog_state.json` — JSON with:

- `last_restart`: per-service timestamp of last launch (for cooldown)
- `last_cycle`: ISO timestamp of most recent audit
- `statuses`: full snapshot of every service on the last cycle

---

## Philosophy — what it does NOT do

- ❌ Does not auto-launch NinjaTrader (external, needs GUI login)
- ❌ Does not manage GEXBot API tokens (refresh is manual in `settings.yaml`)
- ❌ Does not send Telegram alerts (single-component watchdogs do that)
- ❌ Does not touch TWS authentication (IBC handles autologin / 2FA)
- ❌ Does not restart services that are still healthy

---

## Files

| File | Role |
|---|---|
| `watchdog.py` | Main supervisor |
| `watchdog.bat` | Double-click launcher + auto-restart wrapper |
| `install_watchdog.ps1` | Task Scheduler registration |
| `logs/` | Runtime logs (file `watchdog.log` appended) |

Nothing else lives here. If you ever need to add or remove a service,
edit the `SERVICES` list at the top of `watchdog.py`.
