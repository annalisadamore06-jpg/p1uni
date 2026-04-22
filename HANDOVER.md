# P1UNI — Handover

**Last updated:** 2026-04-22 (Phase 4 deep-optimization complete, R5 rewired)
**Repo:** `C:\Users\annal\Desktop\P1UNI`
**Runtime:** `python main.py --mode paper --config config/settings.yaml`

---

## TL;DR — state of play

- **ML v3.5 bridge** is the primary signal layer (3 models, DEGRADED_1 auto-fallback).
- **Dealer-hedging R5** is the parallel fallback layer. After 15 research studies across 3 phases, **R5 is the only rule with robust OOS edge** — and as of Phase 4 its parameters are now validated and shipped.
- Phase 4 deep optimization (2026-04-22) replaced R5's fixed TP=12 / SL=8 / buffer=0 with **trail=4 / SL=10 / buffer=10 / cooldown=60min**. TEST set: 13 trades, +36.5pt, Sharpe 0.24 (vs Phase 2 baseline Sharpe 0.16).
- Other studies (D1b deep, vol prediction, orderflow depth, IV smile) produced weak or unstable signals; **none deployed**. See `_PHASE4_REPORT.md` in `p1-clean` for full verdicts.

---

## What changed in this commit

### `src/execution/hedging_signals.py`
Phase 4 optimized R5 defaults:
- `r5_margin_pts`: 0.0 → **10.0** (entry buffer above CW)
- `r5_sl_pts`: NEW, default **10.0** (hard stop distance)
- `r5_trail_pts`: NEW, default **4.0** (trailing stop from best excursion)
- `r5_cooldown_sec`: 900 → **3600** (60min = 12 bars, as in Phase 4 grid)
- R5 decision now emits `r5_sl_pts`, `r5_trail_pts`, `r5_margin_pts` in `features` so downstream layers can honor them per-signal.

### `src/execution/signal_engine.py`
When the hedging layer takes over (ML rejected), the signal engine:
- Overrides SL with `hedging_sl_pts` (10pt instead of default 8pt).
- Leaves TP as-is (computed by `_calculate_stop_loss`, but it's now just a cap — the real exit is trailing).
- Carries `trail_pts` through to the bridge via a new kwarg.

### `src/execution/ninja_bridge.py`
`send_order(...)` now accepts `trail_pts: float = 0.0`. The outgoing NT8 message carries `trail_hint` in addition to `sl_hint`/`tp_hint`.

### `config/settings.yaml` (gitignored — LOCAL EDIT REQUIRED)
`settings.yaml` is `.gitignored`. The code defaults in `hedging_signals.py` are already Phase 4 values, so paper/live paths work without editing. **If you want to tune R5 parameters without a code change**, add this block to your local `config/settings.yaml` (under `signal_engine:`):

```yaml
hedging_signals:
  enabled: true
  r5_enabled: true
  r5_margin_pts: 10.0
  r5_sl_pts: 10.0
  r5_trail_pts: 4.0
  r5_confidence: 0.65
  r5_cooldown_sec: 3600
```

---

## ⚠️ REQUIRED NEXT: NT8 strategy update

The Python side now emits `trail_hint` (float, points) in every NT8 order message. **The current NT8 P1 strategy does not read this field.** Until the NT8 strategy is updated, R5 will fall back to whatever TP/SL discipline NT8 enforces internally (the old P1-Lite levels, not the new trailing stop).

**What the NT8 strategy needs to do when `trail_hint > 0`:**
1. On fill, arm a trailing stop at `fill_price ± trail_hint` (sign depends on side).
2. On each price update, if price improves against the stop, ratchet the stop by `trail_hint` points from the new best excursion.
3. Keep the hard stop (`sl_hint`) as the outer guardrail.
4. Ignore `tp_hint` when `trail_hint > 0` (trailing replaces fixed TP).

Until the NT8 strategy honors `trail_hint`, **the Phase 4 backtest edge will not be realized in paper/live trading.**

---

## Research state (recap)

All studies in `C:\Users\annal\Desktop\p1-clean\research\`:

| Phase | Study | Outcome | Shipped? |
|---|---|---|---|
| 1-2 | A1/A3/B1/C2/C2b/D1/D1b/E1/F1/VIX_LEAD/MULTIFACTOR | mixed, mostly weak | only R5 (Phase 2 params) |
| 3 | validation + re-test | R5 confirmed as sole survivor | yes |
| 4 | R5_OPT, D1b_DEEP, VOL_PRED, ORDERFLOW, IV_SMILE | **R5_OPT breakthrough**; others fail OOS | **R5 Phase 4 params shipped now** |

Detailed report: `C:\Users\annal\Desktop\p1-clean\_PHASE4_REPORT.md`.

---

## Known issues (unrelated to this change)

- `test_ninja_bridge.py`: 5 pre-existing failures (tests reference old attribute names: `_zmq_context`, `_handle_event`, `orders_sent`). Not introduced by this commit; tracked separately.
- `MORNING_READINESS_REPORT.md` flags Live DB file-lock (`pythonw.exe` PID 31416 holds `p1uni_live.duckdb`). Same pattern as p1-clean cutover — coordinate with p1new PROD lock.

---

## Quick launch

```bash
# Paper mode (safe)
python main.py --mode paper --config config/settings.yaml

# Verify hedging config loaded
python -c "from src.execution.hedging_signals import HedgingSignalEngine; \
  import yaml; cfg = yaml.safe_load(open('config/settings.yaml')); \
  e = HedgingSignalEngine(cfg); print(e.get_status())"
```

Expected output includes: `r5_margin_pts: 10.0`, `r5_sl_pts: 10.0`, `r5_trail_pts: 4.0`.
