"""
STUDY D1b DEEP - proper parameter grid for Δz delta_rr velocity
=================================================================

The raw UP-spike signal has p=0.0001 and -3.4pt @ 30m, -6.6pt @ 60m,
but Phase 3 backtest (TP=12/SL=8/24 bars) lost because the stops were
too tight for a 60-120min decay. Test the right parameters.

Compare three exit styles:
  (a) Fixed TP/SL
  (b) Time-only exit (close after N bars, no TP/SL)
  (c) Trailing stop

Walk-forward 70/30.
"""
from __future__ import annotations

import itertools
import json
import logging
import time
import warnings
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
TICKER = "ES"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\D1b_DEEP")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78
ROLLING_BARS = BARS_PER_RTH * 20
DELTA_LAG_BARS = 6
SLIPPAGE = 1.0
TRAIN_FRAC = 0.70
COOLDOWN_BARS = 12

TP_GRID = [8, 12, 15, 20, 25]
SL_GRID = [6, 8, 10, 12, 15]
HOLD_GRID = [12, 18, 24, 36, 48]     # 60/90/120/180/240 min
Z_GRID = [0.75, 1.0, 1.25, 1.5]
TRAIL_GRID = [4, 6, 8, 10]

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
                              logging.StreamHandler()])
log = logging.getLogger("D1bD")


def load_bars() -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high, MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close,
        AVG(delta_rr) AS delta_rr
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    con = duckdb.connect(DB_PATH, read_only=True,
                         config={"threads": "32", "memory_limit": "110GB"})
    df = con.execute(q).df()
    con.close()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    return df


def prep(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    df = df[df["delta_rr"].notna()].reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    m = df["delta_rr"].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
    s = df["delta_rr"].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
    df["z_drr"] = (df["delta_rr"] - m) / s.replace(0, np.nan)
    same = df["date"] == df["date"].shift(DELTA_LAG_BARS)
    df["delta_z"] = (df["z_drr"] - df["z_drr"].shift(DELTA_LAG_BARS)).where(same, np.nan)
    return df


def _simulate(df: pd.DataFrame, z_thr: float, mode: str,
              tp: float, sl: float, hold: int,
              trail: float = 0.0) -> pd.DataFrame:
    """mode in {'fixed','time','trail'}"""
    trades = []
    cooldown = 0
    n = len(df); i = 0
    sc = df["s_close"].values; so = df["s_open"].values
    sh = df["s_high"].values; sl_arr = df["s_low"].values
    dz = df["delta_z"].values
    dates = df["date"].values

    while i < n - 1:
        if cooldown > 0: cooldown -= 1; i += 1; continue
        if np.isnan(dz[i]):
            i += 1; continue
        if dz[i] >= z_thr:
            side = "SHORT"
        elif dz[i] <= -z_thr:
            side = "LONG"
        else:
            i += 1; continue
        ei = i + 1
        if ei >= n or dates[ei] != dates[i]:
            i += 1; continue
        ep = so[ei] + (SLIPPAGE if side == "LONG" else -SLIPPAGE)
        ed = dates[ei]
        tp_px = ep + (tp if side == "LONG" else -tp)
        sl_px = ep - (sl if side == "LONG" else -sl)
        best = ep  # for trailing
        xr = "TIMEOUT"; xp = ep; xi = ei
        for k in range(1, hold + 1):
            j = ei + k
            if j >= n: break
            if dates[j] != ed:
                xp = sc[j-1]; xi = j-1; xr = "EOD"; break
            hi = sh[j]; lo = sl_arr[j]; cl = sc[j]
            if mode == "fixed":
                if side == "LONG":
                    if lo <= sl_px: xp = sl_px; xr = "SL"; xi = j; break
                    if hi >= tp_px: xp = tp_px; xr = "TP"; xi = j; break
                else:
                    if hi >= sl_px: xp = sl_px; xr = "SL"; xi = j; break
                    if lo <= tp_px: xp = tp_px; xr = "TP"; xi = j; break
            elif mode == "trail":
                if side == "LONG":
                    if hi > best: best = hi
                    trail_px = best - trail
                    if lo <= sl_px: xp = sl_px; xr = "SL"; xi = j; break
                    if (best - ep) >= trail and lo <= trail_px:
                        xp = trail_px; xr = "TRAIL"; xi = j; break
                else:
                    if lo < best: best = lo
                    trail_px = best + trail
                    if hi >= sl_px: xp = sl_px; xr = "SL"; xi = j; break
                    if (ep - best) >= trail and hi >= trail_px:
                        xp = trail_px; xr = "TRAIL"; xi = j; break
            # mode == "time" has no TP/SL
            if k == hold:
                xp = cl; xi = j; xr = "TIMEOUT"
        pnl = (xp - ep) if side == "LONG" else (ep - xp)
        pnl -= SLIPPAGE
        trades.append({"date": ed, "side": side, "z": float(dz[i]),
                       "entry_px": ep, "exit_px": xp, "exit_reason": xr, "pnl_pt": pnl})
        cooldown = COOLDOWN_BARS
        i = xi + 1
    return pd.DataFrame(trades)


def _metrics(tr: pd.DataFrame) -> dict:
    if len(tr) == 0: return {"n": 0}
    p = tr["pnl_pt"]; eq = p.cumsum()
    dd = (eq.cummax() - eq).max() if len(eq) else 0.0
    return {
        "n": int(len(tr)), "total_pt": float(p.sum()), "mean_pt": float(p.mean()),
        "win_rate": float((p > 0).mean()),
        "sharpe_per_trade": float(p.mean()/p.std(ddof=0)) if p.std(ddof=0) > 0 else 0.0,
        "tp_rate": float((tr["exit_reason"] == "TP").mean()),
        "sl_rate": float((tr["exit_reason"] == "SL").mean()),
        "max_dd_pt": float(dd),
    }


def split_tt(df: pd.DataFrame):
    dates = sorted(df["date"].unique())
    split = int(len(dates) * TRAIN_FRAC)
    tr = df[df["date"].isin(set(dates[:split]))].reset_index(drop=True)
    te = df[df["date"].isin(set(dates[split:]))].reset_index(drop=True)
    log.info("Split: %d train days / %d test days", split, len(dates) - split)
    return tr, te


def grid_fixed(train):
    rows = []
    for tp, sl, hold, z in itertools.product(TP_GRID, SL_GRID, HOLD_GRID, Z_GRID):
        tr = _simulate(train, z, "fixed", tp, sl, hold)
        m = _metrics(tr); m.update({"tp": tp, "sl": sl, "hold": hold, "z": z, "mode": "fixed"})
        rows.append(m)
    return pd.DataFrame(rows)


def grid_time(train):
    rows = []
    for hold, z in itertools.product(HOLD_GRID, Z_GRID):
        tr = _simulate(train, z, "time", 0, 0, hold)
        m = _metrics(tr); m.update({"hold": hold, "z": z, "mode": "time"})
        rows.append(m)
    return pd.DataFrame(rows)


def grid_trail(train):
    rows = []
    for trail, sl, hold, z in itertools.product(TRAIL_GRID, SL_GRID, HOLD_GRID, Z_GRID):
        tr = _simulate(train, z, "trail", 0, sl, hold, trail=trail)
        m = _metrics(tr); m.update({"trail": trail, "sl": sl, "hold": hold, "z": z, "mode": "trail"})
        rows.append(m)
    return pd.DataFrame(rows)


def top5_retest(grid: pd.DataFrame, test: pd.DataFrame, mode: str, min_n: int = 20):
    t = grid[grid["n"] >= min_n].copy()
    if not len(t): return pd.DataFrame()
    t = t.sort_values("sharpe_per_trade", ascending=False).head(5)
    rows = []
    for _, r in t.iterrows():
        if mode == "fixed":
            tr = _simulate(test, r["z"], "fixed", r["tp"], r["sl"], int(r["hold"]))
            p = {"tp": r["tp"], "sl": r["sl"], "hold": int(r["hold"]), "z": r["z"]}
        elif mode == "time":
            tr = _simulate(test, r["z"], "time", 0, 0, int(r["hold"]))
            p = {"hold": int(r["hold"]), "z": r["z"]}
        else:
            tr = _simulate(test, r["z"], "trail", 0, r["sl"], int(r["hold"]), trail=r["trail"])
            p = {"trail": r["trail"], "sl": r["sl"], "hold": int(r["hold"]), "z": r["z"]}
        mt = _metrics(tr)
        rows.append({"mode": mode, **p,
                     "train_n": int(r["n"]), "train_sharpe": float(r["sharpe_per_trade"]),
                     "train_mean_pt": float(r["mean_pt"]), "train_total_pt": float(r["total_pt"]),
                     "test_n": mt.get("n", 0), "test_sharpe": mt.get("sharpe_per_trade", 0.0),
                     "test_mean_pt": mt.get("mean_pt", 0.0), "test_total_pt": mt.get("total_pt", 0.0),
                     "test_wr": mt.get("win_rate", 0.0), "test_max_dd": mt.get("max_dd_pt", 0.0)})
    return pd.DataFrame(rows)


def main():
    t0 = time.time()
    df = prep(load_bars())
    log.info("Loaded %d valid bars", len(df))
    train, test = split_tt(df)

    g_fixed = grid_fixed(train); g_fixed.to_csv(OUT_DIR / "grid_fixed_train.csv", index=False)
    g_time = grid_time(train);   g_time.to_csv(OUT_DIR / "grid_time_train.csv", index=False)
    g_trail = grid_trail(train); g_trail.to_csv(OUT_DIR / "grid_trail_train.csv", index=False)
    log.info("fixed combos: %d  time: %d  trail: %d", len(g_fixed), len(g_time), len(g_trail))

    r_fixed = top5_retest(g_fixed, test, "fixed", min_n=10)
    r_time = top5_retest(g_time, test, "time", min_n=20)
    r_trail = top5_retest(g_trail, test, "trail", min_n=10)

    for name, df in [("fixed", r_fixed), ("time", r_time), ("trail", r_trail)]:
        log.info("\n=== TOP-5 %s (TEST) ===\n%s", name.upper(), df.round(3).to_string(index=False))
        df.to_csv(OUT_DIR / f"top5_{name}_test.csv", index=False)

    summary = {}
    for name, df in [("fixed", r_fixed), ("time", r_time), ("trail", r_trail)]:
        if len(df):
            best = df.sort_values("test_sharpe", ascending=False).iloc[0].to_dict()
            summary[f"{name}_best_on_test"] = best
    with open(OUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    # plot test sharpe comparison
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, (name, df) in enumerate([("fixed", r_fixed), ("time", r_time), ("trail", r_trail)]):
        if len(df):
            ax.scatter([i]*len(df), df["test_sharpe"], label=name, s=100, alpha=0.7)
    ax.axhline(0, color="k", ls="--", alpha=0.4)
    ax.set_xticks([0, 1, 2]); ax.set_xticklabels(["fixed", "time", "trail"])
    ax.set_ylabel("TEST Sharpe/trade"); ax.set_title("D1b DEEP — top-5 per mode on TEST")
    ax.legend()
    plt.tight_layout(); plt.savefig(OUT_DIR / "d1b_deep_overview.png", dpi=120); plt.close()

    log.info("\n=== DONE in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
