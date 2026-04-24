"""
STUDY D1b - Δz delta_rr CHANGE (30min Velocity)
================================================

HYPOTHESIS
----------
D1 tests the LEVEL of z-scored delta_rr. D1b tests the VELOCITY:
a fast change in z over 30min (6 bars) = regime shift = tradable.

    Δz = z_dRR(t) - z_dRR(t - 6)

Thresholds:
    SPIKE_UP   = Δz >= +1.5  -> SHORT (call euphoria formation)
    SPIKE_DOWN = Δz <= -1.5  -> LONG  (put capitulation forming)

METHOD
------
1. Load 5-min ES bars with delta_rr from gex_summary (classic/gex_zero).
2. Rolling 20-day z-score of delta_rr.
3. Δz over 30min (6 bars).
4. Forward returns 5/15/30/60/120 min by Δz bucket.
5. Walk-forward 70/30 OOS backtest:
   - LONG on Δz <= -1.5, SHORT on Δz >= +1.5
   - TP=12, SL=8, max 24 bars, cooldown 12 bars.
6. Compare with D1 (level-based).
"""
from __future__ import annotations

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
from scipy import stats

warnings.filterwarnings("ignore")

DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
TICKER = "ES"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\D1b")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78
ROLLING_BARS = BARS_PER_RTH * 20
DELTA_LAG_BARS = 6              # 30min
Z_SPIKE = 1.5

TP_PTS = 12.0
SL_PTS = 8.0
SLIPPAGE = 1.0
MAX_HOLD_BARS = 24
COOLDOWN_BARS = 12
TRAIN_FRAC = 0.70

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
              logging.StreamHandler()],
)
log = logging.getLogger("D1b")


def load_bars() -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high,
        MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close,
        AVG(delta_rr) AS delta_rr,
        AVG(zero_gamma) AS zero_gamma
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    con = duckdb.connect(DB_PATH, read_only=True,
                         config={"threads": "32", "memory_limit": "110GB"})
    t0 = time.time()
    df = con.execute(q).df()
    con.close()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    log.info("Loaded %s bars in %.1fs", f"{len(df):,}", time.time() - t0)
    return df


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    df = df[df["delta_rr"].notna()].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    log.info("RTH valid bars: %s (%s -> %s)",
             f"{len(df):,}", df["ts_bar"].min(), df["ts_bar"].max())
    return df


def compute_zscore(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    m = df["delta_rr"].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
    s = df["delta_rr"].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
    df["z_drr"] = (df["delta_rr"] - m) / s.replace(0, np.nan)
    df["z_drr_lag6"] = df["z_drr"].shift(DELTA_LAG_BARS)
    # only keep Δz computed within same day
    same_day = df["date"] == df["date"].shift(DELTA_LAG_BARS)
    df["delta_z"] = (df["z_drr"] - df["z_drr_lag6"]).where(same_day, np.nan)
    df["spike"] = np.where(df["delta_z"] >= Z_SPIKE, "UP",
                   np.where(df["delta_z"] <= -Z_SPIKE, "DOWN", "NONE"))
    return df


def label_fwd(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for name, k in {"5m": 1, "15m": 3, "30m": 6, "60m": 12, "120m": 24}.items():
        fc = df["s_close"].shift(-k)
        same = df["date"].shift(-k) == df["date"]
        df[f"ret_fwd_{name}"] = (fc - df["s_close"]).where(same, np.nan)
    return df


def fwd_by_bucket(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for spike in ["UP", "DOWN", "NONE"]:
        d = df[df["spike"] == spike]
        for h in ["5m", "15m", "30m", "60m", "120m"]:
            r = d[f"ret_fwd_{h}"].dropna()
            if len(r) == 0: continue
            rows.append({
                "spike": spike, "horizon": h, "n": len(r),
                "mean_pts": r.mean(), "median_pts": r.median(),
                "std_pts": r.std(ddof=0), "wr_pos": (r > 0).mean(),
                "t_stat": stats.ttest_1samp(r, 0.0).statistic,
                "p_value": stats.ttest_1samp(r, 0.0).pvalue,
            })
    out = pd.DataFrame(rows)
    out.to_csv(OUT_DIR / "fwd_by_spike.csv", index=False)
    log.info("\n=== fwd returns by spike bucket ===\n%s", out.round(4).to_string(index=False))
    return out


def backtest_wf(df: pd.DataFrame) -> dict:
    df = df.copy().reset_index(drop=True)
    df["side"] = np.where(df["spike"] == "UP", "SHORT",
                  np.where(df["spike"] == "DOWN", "LONG", None))
    dates = sorted(df["date"].unique())
    split_idx = int(len(dates) * TRAIN_FRAC)
    train_d, test_d = set(dates[:split_idx]), set(dates[split_idx:])
    log.info("Walk-forward: %d train / %d test (split %s)", len(train_d), len(test_d), dates[split_idx])
    all_trades = []
    results = {}
    for label, dset in [("TRAIN", train_d), ("TEST", test_d)]:
        sub = df[df["date"].isin(dset)].reset_index(drop=True)
        tr = _run_trades(sub)
        tr["split"] = label
        all_trades.append(tr)
        results[label] = _summary(tr, label)
        log.info("%s: %s", label, json.dumps(results[label], default=str))
    tdf = pd.concat(all_trades, ignore_index=True)
    tdf.to_csv(OUT_DIR / "trades_all.csv", index=False)
    return results


def _run_trades(df: pd.DataFrame) -> pd.DataFrame:
    trades = []
    cooldown = 0
    n = len(df); i = 0
    while i < n - 1:
        if cooldown > 0: cooldown -= 1; i += 1; continue
        side = df.loc[i, "side"]
        if side is None or (isinstance(side, float) and np.isnan(side)):
            i += 1; continue
        ei = i + 1
        if ei >= n or df.loc[ei, "date"] != df.loc[i, "date"]:
            i += 1; continue
        ep = df.loc[ei, "s_open"] + (SLIPPAGE if side == "LONG" else -SLIPPAGE)
        ed = df.loc[ei, "date"]
        tp = ep + (TP_PTS if side == "LONG" else -TP_PTS)
        sl = ep - (SL_PTS if side == "LONG" else -SL_PTS)
        xr = "TIMEOUT"; xp = ep; xi = ei
        for k in range(1, MAX_HOLD_BARS + 1):
            j = ei + k
            if j >= n: break
            if df.loc[j, "date"] != ed:
                xp = df.loc[j-1, "s_close"]; xi = j-1; xr = "EOD"; break
            hi, lo = df.loc[j, "s_high"], df.loc[j, "s_low"]
            if side == "LONG":
                if lo <= sl: xp = sl; xr = "SL"; xi = j; break
                if hi >= tp: xp = tp; xr = "TP"; xi = j; break
            else:
                if hi >= sl: xp = sl; xr = "SL"; xi = j; break
                if lo <= tp: xp = tp; xr = "TP"; xi = j; break
            if k == MAX_HOLD_BARS:
                xp = df.loc[j, "s_close"]; xi = j; xr = "TIMEOUT"
        pnl = (xp - ep) if side == "LONG" else (ep - xp)
        pnl -= SLIPPAGE
        trades.append({
            "entry_ts": df.loc[ei, "ts_bar"], "date": ed,
            "spike": df.loc[i, "spike"], "delta_z": df.loc[i, "delta_z"],
            "side": side, "entry_px": ep, "exit_px": xp,
            "exit_reason": xr, "pnl_pt": pnl,
        })
        cooldown = COOLDOWN_BARS
        i = xi + 1
    return pd.DataFrame(trades)


def _summary(tr: pd.DataFrame, label: str) -> dict:
    if len(tr) == 0:
        return {"split": label, "n": 0}
    p = tr["pnl_pt"]; eq = p.cumsum()
    dd = (eq.cummax() - eq).max() if len(eq) else 0.0
    return {
        "split": label, "n": int(len(tr)),
        "total_pt": float(p.sum()), "mean_pt": float(p.mean()),
        "win_rate": float((p > 0).mean()),
        "sharpe_per_trade": float(p.mean()/p.std(ddof=0)) if p.std(ddof=0) > 0 else 0.0,
        "tp_rate": float((tr["exit_reason"] == "TP").mean()),
        "sl_rate": float((tr["exit_reason"] == "SL").mean()),
        "max_dd_pt": float(dd),
    }


def make_plots(df: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    df["delta_z"].dropna().hist(bins=80, ax=axes[0], color="tab:purple", alpha=0.7)
    axes[0].axvline(Z_SPIKE, color="r", ls="--"); axes[0].axvline(-Z_SPIKE, color="g", ls="--")
    axes[0].set_title("Δz delta_rr (30min) distribution"); axes[0].set_xlabel("Δz")
    tr = pd.read_csv(OUT_DIR / "trades_all.csv")
    test = tr[tr["split"] == "TEST"].copy()
    if len(test):
        test["cum"] = test["pnl_pt"].cumsum()
        axes[1].plot(range(len(test)), test["cum"], color="tab:blue")
        axes[1].set_title(f"D1b TEST equity ({len(test)} trades, {test['pnl_pt'].sum():.1f}pt)")
        axes[1].axhline(0, color="k", ls="--", alpha=0.5)
    plt.tight_layout(); plt.savefig(OUT_DIR / "d1b_overview.png", dpi=120); plt.close()


def main():
    t0 = time.time()
    df = load_bars()
    df = filter_rth(df)
    df = compute_zscore(df)
    df = label_fwd(df)
    df[["ts_bar", "s_close", "delta_rr", "z_drr", "delta_z", "spike"] + [c for c in df.columns if c.startswith("ret_fwd_")]].to_csv(OUT_DIR / "features_sample.csv", index=False)
    fwd_by_bucket(df)
    results = backtest_wf(df)
    with open(OUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    make_plots(df)
    log.info("\n=== DONE in %.1fs ===", time.time() - t0)
    log.info("Output: %s", OUT_DIR)


if __name__ == "__main__":
    main()
