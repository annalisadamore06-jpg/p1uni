# P1UNI Architecture

> Living document. Last updated: 2026-04-24.
> Owner: Annalisa. Target machine: FRACTAL (Windows 11).

---

## 1. What P1UNI is

**P1UNI is the unified trading system.** There is no "p1-lite project" and no
"WEBSOCKET DATABASE project" as separate products anymore — those were
historical repos whose code has been absorbed into P1UNI's surface:

- **Range engine** (was: p1-lite/collector_lite.py) — still lives under
  `C:\Users\annal\Desktop\p1-lite\` as a data-producer module; P1UNI treats
  that directory as the **range component workspace**, not as a peer system.
- **GexBot WS collector** (was: WEBSOCKET DATABASE/collector.py --no-rth) —
  same: it's a P1UNI component that happens to live in a sibling folder for
  legacy reasons.
- **Feature store + ML artifacts** (was: ML DATABASE/) — `ml_gold.duckdb` is
  **P1UNI's central database**; it just happens to sit on a separate disk
  folder so the 164 GB file doesn't bloat the repo.

Everything is orchestrated by P1UNI. `P1UNI/orchestrator/orchestrator.py`
supervises lifecycle for every component (range collector, GexBot WS, REST
scraper, main signal loop, dashboards, TWS via the new watchdog). If you're
looking at runtime behavior, only **P1UNI matters** — the other folders are
module directories, not independent systems.

P1UNI itself lives at `C:\Users\annal\Desktop\P1UNI\` and provides:
- `main.py` — orchestrator entry (Databento live, GexBot WS, signal engine, execution).
- `scripts/` — nightly harvest, consolidation, utilities.
- `src/ingestion/` — adapters (base, Databento, GexBot WS, P1-Lite).
- `orchestrator/` — process supervisor + TWS watchdog.
- `ml_models/` — V40, Virgin, Ensemble artifacts loaded by signal engine.

---

## 2. The 9 tickers (one universe, many feeds)

Source of truth: `config/settings.yaml:132-134 → tickers.valid`.

| Ticker | Asset class  | Databento dataset | GexBot | NT8 traded |
|--------|--------------|-------------------|--------|------------|
| ES     | CME future   | GLBX.MDP3         | yes    | **primary**|
| NQ     | CME future   | GLBX.MDP3         | yes    | no         |
| SPY    | NYSE ETF     | DBEQ.BASIC        | yes    | no         |
| QQQ    | Nasdaq ETF   | DBEQ.BASIC        | yes    | no         |
| IWM    | NYSE ETF     | DBEQ.BASIC        | yes    | no         |
| GLD    | NYSE ETF     | DBEQ.BASIC        | yes    | no         |
| TLT    | NYSE ETF     | DBEQ.BASIC        | yes    | no         |
| UVXY   | NYSE ETF     | DBEQ.BASIC        | yes    | no         |
| VIX    | CBOE index   | XCBO (opt-in)     | yes    | no         |

Trading is currently ES-only. The other 8 tickers feed **features** only
(intermarket context for the ML ensemble).

---

## 3. Data flow — where bytes come from, where they land

```
                       ┌───────────────────────────────┐
                       │   IB TWS (port 7496)           │
                       │   kept alive by P1UNI/         │
                       │   orchestrator/tws_watchdog.py │
                       └────┬──────────────────┬────────┘
                            │ ES/SPX options   │
                            ▼                  │
┌────────────────────────────────────────┐     │
│ RANGE COMPONENT (p1-lite/)              │     │
│ collector_lite.py  (pythonw)            │     │
│  - IB tick → SPX/VIX/STR/DVS            │     │
│  - computes R1/R2 via IV daily+straddle │     │
│  - snapshots_YYYY-MM-DD.csv   (append)  │     │
│  - data/state.json            (atomic)  │     │
│  - data/ninja/range_levels.csv (live)   │     │
└───────────┬─────────────────────────────┘     │
            │ range CSV (ES rails)                │
            ▼                                      │
┌────────────────────────────────────────┐       │
│ NinjaTrader 8                           │       │
│  - P1RangeLevelsES.cs  (draw lines)     │       │
│  - MLSignalIndicator.cs (reads ranges + │       │
│                          nt8_live.json) │       │
│  - MLAutoStrategy_P1.cs (executes)      │       │
└────────────────────────────────────────┘       │
                                                   │
  ┌───────────────────────────────────────────────┘
  │
  │  (meanwhile, 24/7:)
  │
┌─┴──────────────────────────┐   ┌──────────────────────────┐
│ GEXBOT WS COMPONENT         │   │ P1UNI/main.py --mode live │
│ WEBSOCKET DATABASE/         │   │  - databento Live stream  │
│   collector.py --no-rth     │   │    → ml_gold.trades_live  │
│                             │   │  - gexbot WS subscriber   │
│  → gexbot_ws.duckdb         │   │    → ml_gold.gex_summary /│
│                             │   │      greeks_summary /     │
│                             │   │      orderflow            │
│                             │   │  - signal engine loop     │
│                             │   │  - execution bridge → NT8 │
└───────┬─────────────┬───────┘   └───────┬──────────────────┘
        │             │                    │
┌───────▼───┐ ┌───────▼────────────┐ ┌────▼─────────────────┐
│ gexbot_ws │ │ P1UNI/scripts/     │ │  ml_gold.duckdb      │◀── live writes
│ .duckdb   │ │  gexbot_scraper.py │ │  (central store)     │
│           │ │   REST snapshots   │ │                      │
│           │ │  → gexbot_         │ │  70M trades_live     │
│           │ │    snapshots.      │ │  2.4B mbo_orders     │
│           │ │    duckdb          │ │  56M gex_summary     │
└───────────┘ └────────────────────┘ │  7.5M orderflow      │
                                     └────────┬─────────────┘
                                              │
         ╔════════════════════════════════════▼═══════════════════════════════╗
         ║  nightly_data_harvest.py --consolidate    (22:15 UTC, P1new_EOD)    ║
         ║                                                                     ║
         ║  1. Databento historical (9 tickers) → p1uni_history.duckdb        ║
         ║  2. consolidate_to_ml_gold.py:                                      ║
         ║     - p1uni_history  → ml_gold.historical_trades                   ║
         ║     - gexbot_snapshots → ml_gold.gexbot_*                           ║
         ║     - gexbot_ws       → ml_gold.gexbot_ws_*                         ║
         ║     - p1-lite snapshots_*.csv → ml_gold.p1lite_range_snapshots     ║
         ║     - p1-lite range_levels.csv → ml_gold.p1lite_range_live         ║
         ╚═════════════════════════════════════════════════════════════════════╝
```

Key invariant: **every feed writes to its own "near-store" in real time** and
the nightly consolidator fans everything into `ml_gold.duckdb` so the ML
training pipeline has a single gold source. Hot write path stays fast (no
cross-volume DB contention); the nightly step uses watermarks so re-runs are
idempotent.

---

## 4. ml_gold.duckdb schema (central store)

Location: `C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb` (~164 GB).

### 4.1 Pre-existing live tables
- `trades_live` — 70.8M rows, Databento live ES. Columns: `ts_event, ticker, price, size, side, flags, sale_condition, source_type, freq_type, ingested_at`. Watermark: `ts_event`.
- `mbo_orders` — 2.4B rows, Databento MBO book events. Watermark: `ts_event`.
- `gex_summary`, `greeks_summary`, `orderflow` — GexBot WS data.
- `settlement_ranges` — 527 rows, daily EOD range summary.
- `cross_asset_ohlcv`, `greeks_contracts`, `labels`, `ml_training_set`,
  `validation_report` — V3.5 feature store + ML artifacts.

### 4.2 New tables (added 2026-04-24 by `consolidate_to_ml_gold.py`)
- `historical_trades` — Databento nightly historical copy.
  Columns: `ts_event, ticker, price, size, side, flags, source, ingested_at`.
- `gexbot_<tablename>` — 1-for-1 mirror of `gexbot_snapshots.duckdb`.
- `gexbot_ws_<tablename>` — 1-for-1 mirror of `gexbot_ws.duckdb`.
- `p1lite_range_snapshots` — 09:00 MR + 15:30 OR snapshots.
  Columns: `timestamp, slot, date, base_label, base_value, spx_open, spread, iv_daily, iv_straddle, r1_up, r2_up, center, r2_dn, r1_dn, source, ingested_at`.
- `p1lite_range_live` — latest live row of `range_levels.csv`.
  Columns: `timestamp, session, mr1d/u, mr2d/u, or1d/u, or2d/u, live_r1_up/dn, live_r2_up/dn, settlement_signal, ingested_at`.

---

## 5. Trading pipeline

```
  Range CSV (ES rails) ──┐
                         ├─► MLSignalIndicator.cs ─► MLAutoStrategy_P1.cs ─► NT8 orders
  GEX / nt8_live.json ───┤                                      │
                         │                                      └─► Telegram
  ML ensemble (V40 +     │
    Virgin, ml_models/)  ┘
```

- **Range engine** (`p1-lite/collector_lite.py`): publishes `range_levels.csv` with MR (09:00 snapshot), OR (15:30 snapshot), and live R1/R2 every cycle (1s). 17-column schema (since 2026-04-24): `timestamp, session, mr1d, mr1u, mr2d, mr2u, or1d, or1u, or2d, or2u, live_range_up, live_range_dn, live_r1_up, live_r1_dn, live_r2_up, live_r2_dn, settlement_signal`.
- **NT8 indicator** reads the CSV + `nt8_live.json` (GEX regime) every ~1s.
- **NT8 strategy** combines rails + ML confidence (via `nt8_bridge.py` → `P1UNI/main.py` signal engine) to fire entries, trail stops, and respect risk gates (DLL $3,300, trailing DD $4,500).
- **Hedging layer (R5)** — parallel dealer-hedging strategy with independent risk budget (`settings.yaml:91-98`).

---

## 6. Operational schedule (Zurich time; EU DST on = UTC+2)

| Time        | Event                                                       | Owner                              |
|-------------|-------------------------------------------------------------|------------------------------------|
| 02:15       | NIGHT session starts (range collector resumes writes)       | p1-lite/collector_lite.py          |
| 08:59:58–09:00:08 | **Sacred window** — burst 2s cycles, skip VolTide + GEXBot | range collector + dashboards   |
| 09:00       | MR snapshot (VWAP base, 09:00 slot)                         | range collector                    |
| 15:29:58–15:30:08 | **Sacred window** — OR snapshot (SPX open base, RTH slot) | range collector                  |
| 15:25–22:05 | RTH — NT8 trading live                                      | NT8 + MLAutoStrategy               |
| 21:55       | FlatTime — NT8 closes positions                             | NT8 auto                           |
| 22:00       | CLOSED — range collector stops writes (hard stop 20:00 UTC) | range collector                    |
| 22:15       | `nightly_data_harvest.py --consolidate` starts              | P1new_EOD_Runner scheduled task    |
| 00:01       | GexBot WS collector runs 24/7 (no downtime)                 | orchestrator                       |
| 00:05       | Midnight reset — clear daily state, reopen for NIGHT        | P1new_MidnightReset                |

Sacred window enforcement: dashboards freeze callbacks via `state["sacred_window"]`; collector writes once at 09:00:00 / 15:30:00 sharp; NT8 ignores flicker because it reads `range_levels.csv` as a single atomic file.

---

## 7. Processes & how they're launched

All managed by `orchestrator/orchestrator.py` (dedup + relaunch by
`match_cmd + match_name + match_cwd`). TWS is managed by the separate
`orchestrator/tws_watchdog.py`.

| Process                      | Command                                                             | Owner                            |
|------------------------------|---------------------------------------------------------------------|----------------------------------|
| **TWS (IB)**                 | `C:\IBC\StartTWS.bat` (via watchdog)                                | `tws_watchdog.py` (auto)         |
| `collector_lite.py`          | `pythonw.exe collector_lite.py` in `C:\Users\annal\Desktop\p1-lite` | watchdog + orchestrator          |
| `main.py --mode live`        | `python main.py --mode live` in `C:\Users\annal\Desktop\P1UNI`      | orchestrator                     |
| `gexbot_scraper.py`          | `python scripts\gexbot_scraper.py`                                   | orchestrator                     |
| `WEBSOCKET/collector.py`     | `pythonw collector.py --no-rth`                                      | orchestrator                     |
| NinjaTrader 8                | `NinjaTrader.exe` (login via settings.yaml:159)                      | orchestrator                     |
| `nightly_data_harvest.py`    | `python scripts\nightly_data_harvest.py --consolidate`               | P1new_EOD_Runner (22:15)         |
| Dashboards (8054/8051/8055)  | `python dashboard.py` (3 isolated dashboards)                        | orchestrator                     |

---

## 8. TWS auto-restart (orchestrator/tws_watchdog.py)

**What it does:** probes TCP `127.0.0.1:7496` every 30 s. After 3 consecutive
failures (~90 s downtime), runs `C:\IBC\StartTWS.bat` to relaunch TWS. IBC
completes the login flow; Annalisa accepts the IB phone / Entrust 2FA prompt
when it arrives. Cooldown of 5 min between restarts prevents loops while 2FA
is pending or IBC is initializing.

**Config (via env vars):**
- `TWS_HOST` (default `127.0.0.1`)
- `TWS_PORT` (default `7496`)
- `TWS_CHECK_INTERVAL` (default `30` s)
- `TWS_FAIL_THRESHOLD` (default `3`)
- `TWS_RESTART_COOLDOWN` (default `300` s)
- `IBC_START_BAT` (default `C:\IBC\StartTWS.bat`)

**Log:** `C:\Users\annal\Desktop\P1UNI\logs\tws_watchdog.log`.

**Manual install as scheduled task** (one-time, from elevated prompt):
```cmd
schtasks /Create /SC ONLOGON /TN P1UNI_TWSWatchdog ^
  /TR "\"C:\Program Files\Python313\pythonw.exe\" C:\Users\annal\Desktop\P1UNI\orchestrator\tws_watchdog.py" ^
  /RL LIMITED /F
```

**Manual foreground test:**
```powershell
python C:\Users\annal\Desktop\P1UNI\orchestrator\tws_watchdog.py
```

**Stopping:** `taskkill /IM pythonw.exe /FI "WINDOWTITLE eq tws_watchdog*"` or
Task Scheduler → End Task.

---

## 9. Known issues / runbook

These are documented because some require human intervention (2FA,
credentials) that the orchestrator cannot automate.

### 9.1 TWS is down → range collector can't connect
**Symptom:** `p1-lite/logs/collector_lite.log` shows `Connecting to IB 127.0.0.1:7496 clientId=77` with no follow-up. `netstat -ano | grep 7496` returns nothing.

**Fix:**
- If `tws_watchdog.py` is running, nothing to do — it relaunches TWS within ~2 min of detecting downtime. Accept the 2FA push.
- If the watchdog is not running, start it:
  ```powershell
  Start-Process "C:\Program Files\Python313\pythonw.exe" -ArgumentList "C:\Users\annal\Desktop\P1UNI\orchestrator\tws_watchdog.py"
  ```
- After TWS is up, restart the range collector:
  ```powershell
  Start-Process -FilePath "C:\Program Files\Python313\pythonw.exe" `
    -ArgumentList "collector_lite.py" `
    -WorkingDirectory "C:\Users\annal\Desktop\p1-lite" -WindowStyle Hidden
  ```

### 9.2 GexBot WebSocket 401 Unauthorized (token expired)
**Symptom:** `P1UNI/logs/p1uni.log` shows `Handshake status 401 Unauthorized ... error="invalid_token"`.

**Root cause:** GexBot issues short-lived WS tokens, but the static API key in `config/settings.yaml:25` that exchanges for them is **not rotated automatically**.

**Fix:**
1. Login to GexBot dashboard.
2. Generate / copy a fresh `gexbot_custom_*` API key.
3. Edit `C:\Users\annal\Desktop\P1UNI\config\settings.yaml:25` → replace `gexbot.api_key` value.
4. Restart main.py:
   ```powershell
   Stop-Process -Id (Get-CimInstance Win32_Process -Filter "CommandLine LIKE '%main.py%mode%live%'").ProcessId -Force
   cd C:\Users\annal\Desktop\P1UNI
   Start-Process python -ArgumentList "main.py","--mode","live"
   ```
5. Tail `logs/p1uni.log` — look for `[classic] Connected` within ~2 min.

### 9.3 Databento live feed stale (trades_live not advancing)
**Symptom:** `ml_gold.duckdb.trades_live` max(ts_event) older than ~10 min during RTH.

**Root cause:** `src/ingestion/adapter_databento.py` listener hangs; `_running` flag stays `True` so subsequent `start()` calls see "already running" and no reconnect fires.

**Fix:** restart main.py (same procedure as §9.2 step 4). Check Databento API quota if persistent.

### 9.4 Range CSV stale / missing live R2
**Symptom:** `p1-lite/data/ninja/range_levels.csv` has only 13 columns (no `live_r1_up/dn`, `live_r2_up/dn`) or timestamp older than 5 s during market hours.

**Root cause (pre-2026-04-24):** `_write_range_levels_csv()` only wrote the outer R1 band. **Fixed** in `p1-lite` commit `da6e14e` — CSV now has 17 columns.

**Fix if stale:** restart range collector (§9.1).

### 9.5 p1-lite ranges not in ml_gold
**Symptom:** `SELECT COUNT(*) FROM p1lite_range_snapshots` in `ml_gold.duckdb` returns 0.

**Fix:** run consolidator manually:
```powershell
cd C:\Users\annal\Desktop\P1UNI
python scripts\consolidate_to_ml_gold.py
```
Idempotent (watermark on `timestamp`) — safe to rerun.

---

## 10. Health checks

One-liners (from `C:\Users\annal\Desktop\P1UNI`):

```powershell
# 1. Range CSV freshness + schema
python -c "import csv; r=next(csv.DictReader(open(r'C:\Users\annal\Desktop\p1-lite\data\ninja\range_levels.csv'))); print(r['timestamp'], list(r.keys()))"

# 2. ml_gold tables + freshness
python -c "import duckdb; con=duckdb.connect(r'C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb',read_only=True); [print(t[0]) for t in con.execute('SHOW TABLES').fetchall()]"

# 3. trades_live age
python -c "import duckdb; con=duckdb.connect(r'C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb',read_only=True); print('max(ts_event)=', con.execute('SELECT MAX(ts_event) FROM trades_live').fetchone()[0])"

# 4. TWS watchdog alive
Get-Content C:\Users\annal\Desktop\P1UNI\logs\tws_watchdog.log -Tail 5

# 5. Running processes
powershell "Get-CimInstance Win32_Process -Filter \"Name='python.exe' OR Name='pythonw.exe'\" | Select-Object ProcessId,CommandLine"
```

Target state:
1. timestamp within last 5 s during market hours, 17 columns;
2. tables include `trades_live`, `gex_summary`, `p1lite_range_snapshots`, `p1lite_range_live`, `historical_trades`;
3. `max(ts_event)` within last 10 min during RTH;
4. watchdog log shows recent probes (no stale "probe FAILED" streaks);
5. four python processes alive — main.py (live), collector.py (GexBot WS), gexbot_scraper.py, pythonw collector_lite.py — plus the TWS watchdog.

---

## 11. What's deliberately *not* automated

- **GexBot API key rotation** — static credential, manual refresh at the dashboard (§9.2). No refresh endpoint exposed by vendor.
- **`p1lite.enabled: false`** is intentional in `settings.yaml`. The push-mode WS adapter (`src/ingestion/adapter_p1.py`) expects a WS endpoint the range collector doesn't expose. Instead, the range collector writes files and the nightly consolidator pulls them in — simpler, no coupling between the live collector and the ingestion layer.
- **IB 2FA** — the TWS watchdog launches IBC, IBC completes the login headlessly using credentials in `C:\IBC\config.ini`, but the IB phone / Entrust push still needs a tap from Annalisa.
