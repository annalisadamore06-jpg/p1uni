"""
STUDY R5 OPTIMIZATION
======================

R5 = short ES when es_close > call_wall_oi + buffer (fade above CW).
Phase 2 validated this single rule. Now optimize ALL parameters
and test trailing / dynamic exits on OOS (last 30% of days).

GRID
----
TP:        [6, 8, 10, 12, 15, 20]
SL:        [4, 6, 8, 10, 12]
max_hold:  [6, 12, 18, 24, 36] bars (30/60/90/120/180 min)
buffer:    [0, 2, 5, 10]
cooldown:  [6, 12, 18, 24]

Selection: top-5 on TRAIN by Sharpe/trade (not total). Retest on TEST.

EXTRAS
------
- Trailing stop: instead of fixed TP, trail X pt off best favorable.
- Dynamic exit: close when close <= call_wall (signal invalidated).
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
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\R5_OPT")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
SLIPPAGE = 1.0
TRAIN_FRAC = 0.70

TP_GRID = [6, 8, 10, 12, 15, 20]
SL_GRID = [4, 6, 8, 10, 12]
HOLD_GRID = [6, 12, 18, 24, 36]   # bars
BUF_GRID = [0, 2, 5, 10]
CD_GRID = [6, 12, 18, 24]

TRAIL_GRID = [4, 6, 8, 10]        # trailing stop distance

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
              logging.StreamHandler()],
)
log = logging.getLogger("R5OPT")


def load_bars() -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high, MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close,
        AVG(call_wall_oi) AS call_wall,
        AVG(put_wall_oi)  AS put_wall,
        AVG(zero_gamma)   AS zero_gamma
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


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    df = df[df["call_wall"].notna() & df["put_wall"].notna()].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    return df


def _simulate(df: pd.DataFrame, tp: float, sl: float, hold: int,
              buf: float, cd: int, mode: str = "fixed",
              trail: float = 0.0, dynamic: bool = False) -> pd.DataFrame:
    """mode in {'fixed','trail','dyn'}. Short-only R5."""
    trades = []
    cooldown = 0
    n = len(df); i = 0
    sc = df["s_close"].values; so = df["s_open"].values
    sh = df["s_high"].values; sl_arr = df["s_low"].values
    cw = df["call_wall"].values
    dates = df["date"].values

    while i < n - 1:
        if cooldown > 0: cooldown -= 1; i += 1; continue
        if not (sc[i] > cw[i] + buf):
            i += 1; continue
        ei = i + 1
        if ei >= n or dates[ei] != dates[i]:
            i += 1; continue
        ep = so[ei] - SLIPPAGE   # SHORT entry
        ed = dates[ei]
        tp_px = ep - tp
        sl_px = ep + sl
        best_low = ep  # for trailing
        xr = "TIMEOUT"; xp = ep; xi = ei
        for k in range(1, hold + 1):
            j = ei + k
            if j >= n: break
            if dates[j] != ed:
                xp = sc[j-1]; xi = j-1; xr = "EOD"; break
            hi = sh[j]; lo = sl_arr[j]; cl = sc[j]
            # SL always active
            if hi >= sl_px:
                xp = sl_px; xr = "SL"; xi = j; break
            if mode == "fixed":
                if lo <= tp_px: xp = tp_px; xr = "TP"; xi = j; break
            elif mode == "trail":
                if lo < best_low: best_low = lo
                trail_px = best_low + trail
                # Trail only arms after best_low moved below ep by trail
                if (ep - best_low) >= trail and hi >= trail_px:
                    xp = trail_px; xr = "TRAIL"; xi = j; break
            if dynamic and cl <= cw[j]:
                xp = cl; xr = "DYN"; xi = j; break
            if k == hold:
                xp = cl; xi = j; xr = "TIMEOUT"
        pnl = ep - xp - SLIPPAGE   # short pnl
        trades.append({"date": ed, "entry_px": ep, "exit_px": xp,
                       "exit_reason": xr, "pnl_pt": pnl})
        cooldown = cd
        i = xi + 1
    return pd.DataFrame(trades)


def _metrics(tr: pd.DataFrame) -> dict:
    if len(tr) == 0: return {"n": 0}
    p = tr["pnl_pt"]
    eq = p.cumsum()
    dd = (eq.cummax() - eq).max() if len(eq) else 0.0
    return {
        "n": int(len(tr)),
        "total_pt": float(p.sum()),
        "mean_pt": float(p.mean()),
        "median_pt": float(p.median()),
        "win_rate": float((p > 0).mean()),
        "sharpe_per_trade": float(p.mean()/p.std(ddof=0)) if p.std(ddof=0) > 0 else 0.0,
        "tp_rate": float((tr["exit_reason"] == "TP").mean()),
        "sl_rate": float((tr["exit_reason"] == "SL").mean()),
        "max_dd_pt": float(dd),
    }


def split_train_test(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = sorted(df["date"].unique())
    split = int(len(dates) * TRAIN_FRAC)
    train = df[df["date"].isin(set(dates[:split]))].reset_index(drop=True)
    test = df[df["date"].isin(set(dates[split:]))].reset_index(drop=True)
    log.info("Split: %d train days, %d test days", split, len(dates) - split)
    return train, test


def grid_search_fixed(train: pd.DataFrame) -> pd.DataFrame:
    rows = []
    combos = list(itertools.product(TP_GRID, SL_GRID, HOLD_GRID, BUF_GRID, CD_GRID))
    log.info("Grid-search fixed TP/SL: %d combos", len(combos))
    for tp, sl, hold, buf, cd in combos:
        tr = _simulate(train, tp, sl, hold, buf, cd, mode="fixed")
        m = _metrics(tr)
        m.update({"tp": tp, "sl": sl, "hold": hold, "buf": buf, "cd": cd, "mode": "fixed"})
        rows.append(m)
    out = pd.DataFrame(rows)
    return out


def grid_search_trail(train: pd.DataFrame) -> pd.DataFrame:
    rows = []
    combos = list(itertools.product(TRAIL_GRID, SL_GRID, HOLD_GRID, BUF_GRID, CD_GRID))
    log.info("Grid-search trailing: %d combos", len(combos))
    for trail, sl, hold, buf, cd in combos:
        tr = _simulate(train, 0, sl, hold, buf, cd, mode="trail", trail=trail)
        m = _metrics(tr)
        m.update({"trail": trail, "sl": sl, "hold": hold, "buf": buf, "cd": cd, "mode": "trail"})
        rows.append(m)
    return pd.DataFrame(rows)


def grid_search_dynamic(train: pd.DataFrame) -> pd.DataFrame:
    rows = []
    combos = list(itertools.product(TP_GRID, SL_GRID, HOLD_GRID, BUF_GRID, CD_GRID))
    log.info("Grid-search dynamic-exit: %d combos", len(combos))
    for tp, sl, hold, buf, cd in combos:
        tr = _simulate(train, tp, sl, hold, buf, cd, mode="fixed", dynamic=True)
        m = _metrics(tr)
        m.update({"tp": tp, "sl": sl, "hold": hold, "buf": buf, "cd": cd, "mode": "fixed+dyn"})
        rows.append(m)
    return pd.DataFrame(rows)


def top_and_retest(train_grid: pd.DataFrame, test: pd.DataFrame, mode: str,
                   n_top: int = 5, min_n: int = 20) -> pd.DataFrame:
    """Rank top-n by sharpe/trade on TRAIN (with min n trades), retest on TEST."""
    t = train_grid[train_grid["n"] >= min_n].copy()
    if len(t) == 0:
        log.warning("No train rows with n >= %d for mode=%s", min_n, mode)
        return pd.DataFrame()
    t = t.sort_values("sharpe_per_trade", ascending=False).head(n_top)
    rows = []
    for _, r in t.iterrows():
        if mode == "fixed":
            tr_test = _simulate(test, r["tp"], r["sl"], int(r["hold"]), r["buf"], int(r["cd"]), mode="fixed")
            params = {"tp": r["tp"], "sl": r["sl"], "hold": int(r["hold"]), "buf": r["buf"], "cd": int(r["cd"])}
        elif mode == "trail":
            tr_test = _simulate(test, 0, r["sl"], int(r["hold"]), r["buf"], int(r["cd"]), mode="trail", trail=r["trail"])
            params = {"trail": r["trail"], "sl": r["sl"], "hold": int(r["hold"]), "buf": r["buf"], "cd": int(r["cd"])}
        else:  # fixed+dyn
            tr_test = _simulate(test, r["tp"], r["sl"], int(r["hold"]), r["buf"], int(r["cd"]), mode="fixed", dynamic=True)
            params = {"tp": r["tp"], "sl": r["sl"], "hold": int(r["hold"]), "buf": r["buf"], "cd": int(r["cd"])}
        m_test = _metrics(tr_test)
        rows.append({
            "mode": mode, **params,
            "train_n": int(r["n"]), "train_sharpe": float(r["sharpe_per_trade"]),
            "train_mean_pt": float(r["mean_pt"]), "train_total_pt": float(r["total_pt"]),
            "train_wr": float(r["win_rate"]),
            "test_n": m_test.get("n", 0),
            "test_sharpe": m_test.get("sharpe_per_trade", 0.0),
            "test_mean_pt": m_test.get("mean_pt", 0.0),
            "test_total_pt": m_test.get("total_pt", 0.0),
            "test_wr": m_test.get("win_rate", 0.0),
            "test_max_dd": m_test.get("max_dd_pt", 0.0),
        })
    return pd.DataFrame(rows)


def make_plots(retests: dict, top_fixed: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    for mode, df in retests.items():
        if len(df) == 0: continue
        axes[0].scatter(df["train_sharpe"], df["test_sharpe"], label=mode, s=80, alpha=0.7)
    axes[0].axhline(0, color="k", ls="--", alpha=0.4); axes[0].axvline(0, color="k", ls="--", alpha=0.4)
    axes[0].plot([-0.3, 0.5], [-0.3, 0.5], color="gray", ls=":")
    axes[0].set_xlabel("TRAIN Sharpe/trade"); axes[0].set_ylabel("TEST Sharpe/trade")
    axes[0].set_title("Overfit check: TRAIN vs TEST"); axes[0].legend()

    # Best config bar chart
    best_each = []
    for mode, df in retests.items():
        if len(df) == 0: continue
        best = df.sort_values("test_sharpe", ascending=False).iloc[0]
        best_each.append({"mode": mode, "test_sharpe": best["test_sharpe"], "test_total": best["test_total_pt"], "test_n": best["test_n"]})
    if best_each:
        be = pd.DataFrame(best_each)
        be.plot(x="mode", y=["test_sharpe"], kind="bar", ax=axes[1], color="tab:green", legend=False)
        axes[1].set_title("Best TEST Sharpe by exit mode")
        for i, v in enumerate(be["test_total"]):
            axes[1].text(i, 0.0, f"{v:+.1f}pt\nn={int(be['test_n'][i])}", ha="center")
    plt.tight_layout(); plt.savefig(OUT_DIR / "r5_opt_overview.png", dpi=120); plt.close()


def main():
    t0 = time.time()
    df = filter_rth(load_bars())
    log.info("Loaded %d bars (%s -> %s)", len(df), df["ts_bar"].min(), df["ts_bar"].max())
    train, test = split_train_test(df)

    # 1. Fixed TP/SL
    g_fixed = grid_search_fixed(train)
    g_fixed.to_csv(OUT_DIR / "grid_fixed_train.csv", index=False)
    r_fixed = top_and_retest(g_fixed, test, "fixed")
    r_fixed.to_csv(OUT_DIR / "top5_fixed_test.csv", index=False)
    log.info("\n=== TOP-5 FIXED (TEST) ===\n%s",
             r_fixed.round(3).to_string(index=False))

    # 2. Trailing stop
    g_trail = grid_search_trail(train)
    g_trail.to_csv(OUT_DIR / "grid_trail_train.csv", index=False)
    r_trail = top_and_retest(g_trail, test, "trail")
    r_trail.to_csv(OUT_DIR / "top5_trail_test.csv", index=False)
    log.info("\n=== TOP-5 TRAIL (TEST) ===\n%s",
             r_trail.round(3).to_string(index=False))

    # 3. Dynamic exit (close if s_close <= CW)
    g_dyn = grid_search_dynamic(train)
    g_dyn.to_csv(OUT_DIR / "grid_dyn_train.csv", index=False)
    r_dyn = top_and_retest(g_dyn, test, "fixed+dyn")
    r_dyn.to_csv(OUT_DIR / "top5_dyn_test.csv", index=False)
    log.info("\n=== TOP-5 DYN (TEST) ===\n%s",
             r_dyn.round(3).to_string(index=False))

    retests = {"fixed": r_fixed, "trail": r_trail, "fixed+dyn": r_dyn}
    # Aggregate
    all_top = pd.concat(list(retests.values()), ignore_index=True)
    all_top.to_csv(OUT_DIR / "all_top5.csv", index=False)

    summary = {}
    for mode, df in retests.items():
        if len(df) == 0: continue
        best = df.sort_values("test_sharpe", ascending=False).iloc[0].to_dict()
        summary[mode + "_best_on_test"] = best
    with open(OUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    make_plots(retests, r_fixed)
    log.info("\n=== DONE in %.1fs ===", time.time() - t0)
    log.info("Output: %s", OUT_DIR)


if __name__ == "__main__":
    main()
