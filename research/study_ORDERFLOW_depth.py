"""
STUDY ORDERFLOW DEPTH
======================

orderflow has 38 numeric cols for ES — never fully analyzed.
For each col, compute correlation with fwd returns at 15/30/60 min,
then take the top-5 by |corr| and run quintile + walk-forward backtest.
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
from scipy.stats import pearsonr, spearmanr

warnings.filterwarnings("ignore")

DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
TICKER = "ES"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\ORDERFLOW")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78
ROLLING_BARS = BARS_PER_RTH * 20
TRAIN_FRAC = 0.70
TP = 12.0; SL = 8.0; MAX_HOLD = 24; SLIPPAGE = 1.0; COOLDOWN = 12

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
                              logging.StreamHandler()])
log = logging.getLogger("OF")


def load_data() -> tuple[pd.DataFrame, list[str]]:
    con = duckdb.connect(DB_PATH, read_only=True,
                         config={"threads": "32", "memory_limit": "110GB"})
    cols = [r[0] for r in con.execute("DESCRIBE orderflow").fetchall()]
    numeric_cols = [c for c in cols if c not in ("ts_utc", "source", "ticker", "spot")]

    agg = ", ".join([f"AVG({c}) AS {c}" for c in numeric_cols])
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high, MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close,
        {agg}
    FROM orderflow
    WHERE ticker='{TICKER}'
    GROUP BY 1 ORDER BY 1
    """
    t0 = time.time()
    df = con.execute(q).df()
    con.close()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    log.info("Loaded %d 5-min bars in %.1fs", len(df), time.time() - t0)
    return df, numeric_cols


def prep(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    for k, name in [(3, "15m"), (6, "30m"), (12, "60m")]:
        fc = df["s_close"].shift(-k)
        same = df["date"].shift(-k) == df["date"]
        df[f"ret_fwd_{name}"] = (fc - df["s_close"]).where(same, np.nan)
    return df


def correlate_all(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    rows = []
    for col in numeric_cols:
        x = df[col].values
        # also z-score version
        m = pd.Series(x).rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
        s = pd.Series(x).rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
        z = ((pd.Series(x) - m) / s.replace(0, np.nan)).values
        for variant, vec in [("raw", x), ("z20d", z), ("abs", np.abs(x)), ("abs_z20d", np.abs(z))]:
            for hz in ["15m", "30m", "60m"]:
                y = df[f"ret_fwd_{hz}"].values
                ok = np.isfinite(vec) & np.isfinite(y)
                if ok.sum() < 500: continue
                r_p, p_p = pearsonr(vec[ok], y[ok])
                r_s, p_s = spearmanr(vec[ok], y[ok])
                rows.append({"col": col, "variant": variant, "horizon": hz,
                             "n": int(ok.sum()),
                             "pearson": r_p, "p_pearson": p_p,
                             "spearman": r_s, "p_spearman": p_s})
    out = pd.DataFrame(rows)
    out["abs_pearson"] = out["pearson"].abs()
    out = out.sort_values("abs_pearson", ascending=False).reset_index(drop=True)
    return out


def quintile_fwd(df: pd.DataFrame, col: str, variant: str, horizon: str) -> pd.DataFrame:
    x = df[col].values
    if variant == "abs": x = np.abs(x)
    if variant in ("z20d", "abs_z20d"):
        m = pd.Series(df[col]).rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
        s = pd.Series(df[col]).rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
        z = (pd.Series(df[col]) - m) / s.replace(0, np.nan)
        x = z.values if variant == "z20d" else np.abs(z.values)
    y = df[f"ret_fwd_{horizon}"].values
    ok = np.isfinite(x) & np.isfinite(y)
    if ok.sum() < 500: return pd.DataFrame()
    d = pd.DataFrame({"x": x[ok], "y": y[ok]})
    d["q"] = pd.qcut(d["x"], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
    g = d.groupby("q").agg(n=("y", "count"), mean=("y", "mean"),
                            median=("y", "median"), wr=("y", lambda s: (s > 0).mean()),
                            std=("y", "std")).reset_index()
    g["sharpe"] = g["mean"] / g["std"].replace(0, np.nan)
    g["col"] = col; g["variant"] = variant; g["horizon"] = horizon
    return g


def backtest_signal(df: pd.DataFrame, signal_bool: np.ndarray, side_arr) -> pd.DataFrame:
    """signal_bool indicates rows where trade fires. side_arr is "LONG" or "SHORT" per row."""
    trades = []
    cooldown = 0
    n = len(df); i = 0
    sc = df["s_close"].values; so = df["s_open"].values
    sh = df["s_high"].values; sl_arr = df["s_low"].values
    dates = df["date"].values
    while i < n - 1:
        if cooldown > 0: cooldown -= 1; i += 1; continue
        if not signal_bool[i]:
            i += 1; continue
        side = side_arr[i]
        ei = i + 1
        if ei >= n or dates[ei] != dates[i]:
            i += 1; continue
        ep = so[ei] + (SLIPPAGE if side == "LONG" else -SLIPPAGE)
        ed = dates[ei]
        tp_px = ep + (TP if side == "LONG" else -TP)
        sl_px = ep - (SL if side == "LONG" else -SL)
        xp = ep; xr = "TIMEOUT"; xi = ei
        for k in range(1, MAX_HOLD + 1):
            j = ei + k
            if j >= n: break
            if dates[j] != ed: xp = sc[j-1]; xi = j-1; xr = "EOD"; break
            hi = sh[j]; lo = sl_arr[j]
            if side == "LONG":
                if lo <= sl_px: xp = sl_px; xr = "SL"; xi = j; break
                if hi >= tp_px: xp = tp_px; xr = "TP"; xi = j; break
            else:
                if hi >= sl_px: xp = sl_px; xr = "SL"; xi = j; break
                if lo <= tp_px: xp = tp_px; xr = "TP"; xi = j; break
            if k == MAX_HOLD: xp = sc[j]; xi = j; xr = "TIMEOUT"
        pnl = (xp - ep) if side == "LONG" else (ep - xp)
        pnl -= SLIPPAGE
        trades.append({"date": ed, "side": side, "entry_px": ep, "exit_px": xp,
                       "exit_reason": xr, "pnl_pt": pnl})
        cooldown = COOLDOWN
        i = xi + 1
    return pd.DataFrame(trades)


def extreme_quintile_signal(df: pd.DataFrame, col: str, variant: str,
                            horizon: str, sign: int) -> tuple[np.ndarray, np.ndarray]:
    """Return boolean array where signal fires (top or bottom quintile),
    and side based on sign convention: sign=+1 -> top Q5 is LONG, sign=-1 -> top Q5 is SHORT.
    Uses rolling quintile (computed on TRAIN global threshold - simple version)."""
    x = df[col].values
    if variant == "abs": x = np.abs(x)
    if variant in ("z20d", "abs_z20d"):
        m = pd.Series(df[col]).rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
        s = pd.Series(df[col]).rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
        z = (pd.Series(df[col]) - m) / s.replace(0, np.nan)
        x = z.values if variant == "z20d" else np.abs(z.values)
    # Thresholds: lower 20%, upper 80%
    valid = x[np.isfinite(x)]
    if len(valid) < 100:
        return np.zeros(len(df), dtype=bool), np.array([None] * len(df))
    lo, hi = np.nanquantile(x, 0.20), np.nanquantile(x, 0.80)
    sig = np.zeros(len(df), dtype=bool)
    side = np.array([None] * len(df), dtype=object)
    top_mask = x >= hi
    bot_mask = x <= lo
    if sign > 0:
        side[top_mask] = "LONG"; side[bot_mask] = "SHORT"
    else:
        side[top_mask] = "SHORT"; side[bot_mask] = "LONG"
    sig[top_mask | bot_mask] = True
    return sig, side


def walk_forward(df: pd.DataFrame, col: str, variant: str, horizon: str, sign: int) -> dict:
    dates = sorted(df["date"].unique())
    split = int(len(dates) * TRAIN_FRAC)
    train = df[df["date"].isin(set(dates[:split]))].reset_index(drop=True)
    test = df[df["date"].isin(set(dates[split:]))].reset_index(drop=True)
    results = {}
    for label, sub in [("TRAIN", train), ("TEST", test)]:
        sig, side = extreme_quintile_signal(sub, col, variant, horizon, sign)
        tr = backtest_signal(sub, sig, side)
        if len(tr) == 0:
            results[label] = {"n": 0}
            continue
        p = tr["pnl_pt"]
        results[label] = {"n": int(len(tr)), "total_pt": float(p.sum()),
                          "mean_pt": float(p.mean()),
                          "win_rate": float((p > 0).mean()),
                          "sharpe_per_trade": float(p.mean()/p.std(ddof=0)) if p.std(ddof=0) > 0 else 0.0}
    return results


def main():
    t0 = time.time()
    df, numeric_cols = load_data()
    df = prep(df)
    log.info("Prep done: %d bars, %d numeric cols", len(df), len(numeric_cols))

    corr = correlate_all(df, numeric_cols)
    corr.to_csv(OUT_DIR / "all_correlations.csv", index=False)
    log.info("\n=== TOP-20 by |pearson| ===\n%s",
             corr.head(20).round(5).to_string(index=False))

    # take top-10 unique (col,variant,horizon) by |pearson|
    top = corr.head(10).copy()
    quintiles = []
    for _, r in top.iterrows():
        q = quintile_fwd(df, r["col"], r["variant"], r["horizon"])
        quintiles.append(q)
    Q = pd.concat(quintiles, ignore_index=True)
    Q.to_csv(OUT_DIR / "top10_quintiles.csv", index=False)
    log.info("\n=== TOP-10 QUINTILES (fwd ret by bucket) ===\n%s",
             Q.round(4).to_string(index=False))

    # Walk-forward backtest on top-5
    backtests = []
    for _, r in top.head(5).iterrows():
        # Determine sign from Q5 mean: if positive, top = LONG (sign=+1), else sign=-1
        qrows = quintile_fwd(df, r["col"], r["variant"], r["horizon"])
        if len(qrows) == 0 or "Q5" not in qrows["q"].astype(str).values:
            continue
        q5_mean = qrows[qrows["q"].astype(str) == "Q5"]["mean"].iloc[0]
        sign = 1 if q5_mean > 0 else -1
        res = walk_forward(df, r["col"], r["variant"], r["horizon"], sign)
        backtests.append({"col": r["col"], "variant": r["variant"], "horizon": r["horizon"],
                          "pearson": r["pearson"], "sign": sign,
                          **{f"{k}_{sk}": sv for k, v in res.items() for sk, sv in v.items()}})
    BT = pd.DataFrame(backtests)
    BT.to_csv(OUT_DIR / "top5_walkforward.csv", index=False)
    log.info("\n=== TOP-5 WALK-FORWARD ===\n%s",
             BT.round(4).to_string(index=False))

    # Plot
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    top15 = corr.head(15)
    axes[0].barh(range(len(top15)), top15["abs_pearson"], color="tab:purple")
    axes[0].set_yticks(range(len(top15)))
    axes[0].set_yticklabels([f"{r.col}/{r.variant}/{r.horizon}" for _, r in top15.iterrows()])
    axes[0].set_xlabel("|pearson|"); axes[0].set_title("Top 15 orderflow correlations")
    axes[0].invert_yaxis()

    if "TEST_sharpe_per_trade" in BT.columns and "TRAIN_sharpe_per_trade" in BT.columns:
        axes[1].scatter(BT["TRAIN_sharpe_per_trade"], BT["TEST_sharpe_per_trade"], s=80)
        for _, r in BT.iterrows():
            axes[1].annotate(f"{r['col'][:8]}/{r['variant']}",
                             (r["TRAIN_sharpe_per_trade"], r["TEST_sharpe_per_trade"]), fontsize=8)
        axes[1].axhline(0, color="k", ls="--"); axes[1].axvline(0, color="k", ls="--")
        axes[1].set_xlabel("TRAIN sharpe"); axes[1].set_ylabel("TEST sharpe")
        axes[1].set_title("Overfit check")
    plt.tight_layout(); plt.savefig(OUT_DIR / "orderflow_overview.png", dpi=120); plt.close()

    log.info("\n=== DONE in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
