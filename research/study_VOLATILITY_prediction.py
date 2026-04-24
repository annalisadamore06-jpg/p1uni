"""
STUDY VOLATILITY PREDICTION
============================

Hypothesis: gamma/skew data predicts realized VOLATILITY, not direction.
If true, size scaling / straddle exits become tractable.

Target: realized_vol_forward = std(log returns) over next 12/24/48 bars
(60/120/240 min).

Features:
  From gex_summary: z_net_gex, z_delta_rr, dist_cw, dist_pw, dist_zg,
                    zone_code, |delta_rr|
  From orderflow:   |gex_flow|, |dex_flow|, |cvr_flow|, |zero_vanna|,
                    |zero_charm|, |zero_net_dex|
  Time: hour, minute

Model: XGBoost GPU regressor, 3 seeds ensemble, walk-forward 70/30.
Metric: OOS R², pearson corr.

Then use predicted vol to gate R5: trade only when predicted vol
is in upper tercile (breakout regime) vs lower tercile (grind).
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
import xgboost as xgb
from scipy.stats import pearsonr
from sklearn.metrics import r2_score

warnings.filterwarnings("ignore")

DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
TICKER = "ES"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\VOL_PRED")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78
ROLLING_BARS = BARS_PER_RTH * 20
TRAIN_FRAC = 0.70
SEEDS = [42, 2026, 7777]
VOL_HORIZONS = [12, 24, 48]   # 60/120/240 min

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
                              logging.StreamHandler()])
log = logging.getLogger("VOL")


def load_data() -> pd.DataFrame:
    q_gex = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        LAST(spot ORDER BY ts_utc) AS s_close,
        MAX(spot) AS s_high, MIN(spot) AS s_low,
        AVG(zero_gamma) AS zero_gamma,
        AVG(call_wall_oi) AS call_wall,
        AVG(put_wall_oi) AS put_wall,
        AVG(net_gex_oi) AS net_gex,
        AVG(delta_rr) AS delta_rr
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    q_of = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        AVG(zero_vanna) AS zero_vanna,
        AVG(zero_charm) AS zero_charm,
        AVG(dex_flow) AS dex_flow,
        AVG(gex_flow) AS gex_flow,
        AVG(cvr_flow) AS cvr_flow,
        AVG(zero_net_dex) AS zero_net_dex
    FROM orderflow
    WHERE ticker='{TICKER}'
    GROUP BY 1 ORDER BY 1
    """
    con = duckdb.connect(DB_PATH, read_only=True,
                         config={"threads": "32", "memory_limit": "110GB"})
    t0 = time.time()
    g = con.execute(q_gex).df()
    of = con.execute(q_of).df()
    con.close()
    g["ts_bar"] = pd.to_datetime(g["ts_bar"], utc=True)
    of["ts_bar"] = pd.to_datetime(of["ts_bar"], utc=True)
    df = g.merge(of, on="ts_bar", how="left")
    log.info("Loaded gex %d, of %d, merged %d in %.1fs", len(g), len(of), len(df), time.time() - t0)
    return df


def features_and_target(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    # z-scores
    for c in ["net_gex", "delta_rr"]:
        m = df[c].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
        s = df[c].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
        df[f"z_{c}"] = (df[c] - m) / s.replace(0, np.nan)
    # zone + dist
    s = df["s_close"]; cw = df["call_wall"]; zg = df["zero_gamma"]; pw = df["put_wall"]
    conds = [s > cw, (s <= cw) & (s > zg), (s <= zg) & (s > pw), s <= pw]
    df["zone_code"] = np.select(conds, [0, 1, 2, 3], default=-1)
    df["dist_cw"] = cw - s; df["dist_pw"] = s - pw; df["dist_zg"] = zg - s
    df["abs_delta_rr"] = df["delta_rr"].abs()
    # absolute orderflow
    for c in ["gex_flow", "dex_flow", "cvr_flow", "zero_vanna", "zero_charm", "zero_net_dex"]:
        df[f"abs_{c}"] = df[c].abs()
    df["hour_of_day"] = ts.hour
    df["minute_of_day"] = ts.hour * 60 + ts.minute

    # Target: realized vol = std of log returns over next N bars, same-day only
    df["log_ret"] = np.log(df["s_close"] / df["s_close"].shift(1))
    lr = df["log_ret"].values
    for H in VOL_HORIZONS:
        # Forward H log returns starting at i+1: lr[i+1], lr[i+2], ..., lr[i+H]
        mat = np.full((len(df), H), np.nan)
        for k in range(H):
            mat[:-1-k, k] = lr[1+k:]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            std_fwd = np.nanstd(mat, axis=1)
        same_day = df["date"].shift(-H) == df["date"]
        df[f"rv_fwd_{H}"] = pd.Series(std_fwd, index=df.index).where(same_day, np.nan)
    return df


FEATURES = [
    "z_net_gex", "z_delta_rr", "abs_delta_rr",
    "dist_cw", "dist_pw", "dist_zg", "zone_code",
    "abs_gex_flow", "abs_dex_flow", "abs_cvr_flow",
    "abs_zero_vanna", "abs_zero_charm", "abs_zero_net_dex",
    "hour_of_day", "minute_of_day",
]


def train_eval(train: pd.DataFrame, test: pd.DataFrame, target: str) -> dict:
    tr = train.dropna(subset=[target]).reset_index(drop=True)
    te = test.dropna(subset=[target]).reset_index(drop=True)
    X_tr = tr[FEATURES].values.astype("f4"); y_tr = tr[target].values.astype("f4")
    X_te = te[FEATURES].values.astype("f4"); y_te = te[target].values.astype("f4")
    mask_tr = np.isfinite(X_tr).all(axis=1) & np.isfinite(y_tr)
    mask_te = np.isfinite(X_te).all(axis=1) & np.isfinite(y_te)
    X_tr, y_tr = X_tr[mask_tr], y_tr[mask_tr]
    X_te, y_te = X_te[mask_te], y_te[mask_te]
    log.info("  cleaned: train %d, test %d", len(X_tr), len(X_te))
    preds = []; imps = []
    for seed in SEEDS:
        m = xgb.XGBRegressor(device="cuda", tree_method="hist",
                             n_estimators=400, max_depth=6, learning_rate=0.05,
                             subsample=0.85, colsample_bytree=0.85,
                             reg_lambda=1.0, reg_alpha=0.1,
                             random_state=seed, verbosity=0)
        m.fit(X_tr, y_tr)
        preds.append(m.predict(X_te))
        imps.append(m.feature_importances_)
    ens = np.mean(preds, axis=0)
    r2 = r2_score(y_te, ens)
    corr, p = pearsonr(y_te, ens)
    log.info("TARGET %s | R2=%.4f  pearson=%.4f  p=%.2e", target, r2, corr, p)
    imp = pd.Series(np.mean(imps, axis=0), index=FEATURES).sort_values(ascending=False)
    return {"target": target, "r2": float(r2), "corr": float(corr), "pval": float(p),
            "ens_pred": ens, "y_te": y_te, "imp": imp}


def gate_r5_by_vol(test: pd.DataFrame, ens_pred: np.ndarray) -> dict:
    """Simulate R5 on test, with gating: only trade if predicted vol in
    upper/middle/lower tercile."""
    test = test.copy().reset_index(drop=True)
    test["vol_pred"] = ens_pred
    lo_q, hi_q = test["vol_pred"].quantile([0.33, 0.67])
    results = {}
    for bucket, mask_fn in [
        ("ALL", lambda r: True),
        ("LOW_VOL", lambda r: r["vol_pred"] < lo_q),
        ("MID_VOL", lambda r: (r["vol_pred"] >= lo_q) and (r["vol_pred"] < hi_q)),
        ("HI_VOL", lambda r: r["vol_pred"] >= hi_q),
    ]:
        n_trades = 0; pnl_sum = 0.0; wins = 0
        pnl_list = []
        cooldown = 0
        for i in range(len(test) - 1):
            if cooldown > 0: cooldown -= 1; continue
            r = test.iloc[i]
            if not (r["s_close"] > r["call_wall"] + 10): continue
            if not mask_fn(r): continue
            ei = i + 1
            if ei >= len(test) or test.iloc[ei]["date"] != r["date"]: continue
            ep = test.iloc[ei]["s_open"] - 1.0  # slippage
            best = ep; xp = ep; xr = "TIMEOUT"
            for k in range(1, 13):  # hold 60min
                j = ei + k
                if j >= len(test) or test.iloc[j]["date"] != r["date"]: break
                hi = test.iloc[j]["s_high"]; lo = test.iloc[j]["s_low"]
                if hi >= ep + 10: xp = ep + 10; xr = "SL"; break
                if lo < best: best = lo
                if (ep - best) >= 4 and hi >= best + 4:
                    xp = best + 4; xr = "TRAIL"; break
                if k == 12:
                    xp = test.iloc[j]["s_close"]; xr = "TIMEOUT"
            pnl = ep - xp - 1.0
            pnl_list.append(pnl)
            n_trades += 1; pnl_sum += pnl
            if pnl > 0: wins += 1
            cooldown = 12
        if n_trades == 0:
            results[bucket] = {"n": 0}
        else:
            pnl_arr = np.array(pnl_list)
            results[bucket] = {"n": n_trades, "total_pt": float(pnl_sum),
                               "mean_pt": float(pnl_sum / n_trades),
                               "win_rate": float(wins / n_trades),
                               "sharpe": float(pnl_arr.mean() / pnl_arr.std(ddof=0))
                                         if pnl_arr.std(ddof=0) > 0 else 0.0}
    return results


def main():
    t0 = time.time()
    df = features_and_target(load_data())
    before = len(df)
    df = df.dropna(subset=FEATURES + ["rv_fwd_24", "s_close", "s_open", "call_wall"]).reset_index(drop=True)
    log.info("After dropna: %d rows (dropped %d)", len(df), before - len(df))

    dates = sorted(df["date"].unique())
    split = int(len(dates) * TRAIN_FRAC)
    train = df[df["date"].isin(set(dates[:split]))].reset_index(drop=True)
    test = df[df["date"].isin(set(dates[split:]))].reset_index(drop=True)
    log.info("Train %d rows / Test %d rows", len(train), len(test))

    all_results = {}
    for H in VOL_HORIZONS:
        target = f"rv_fwd_{H}"
        res = train_eval(train, test, target)
        all_results[target] = {"r2": res["r2"], "corr": res["corr"], "pval": res["pval"]}
        res["imp"].to_csv(OUT_DIR / f"feature_importance_{target}.csv", header=["importance"])
        # Plot predicted vs actual
        fig, ax = plt.subplots(figsize=(6, 5))
        idx = np.random.choice(len(res["y_te"]), size=min(3000, len(res["y_te"])), replace=False)
        ax.scatter(res["y_te"][idx], res["ens_pred"][idx], s=5, alpha=0.4)
        lo = min(res["y_te"].min(), res["ens_pred"].min())
        hi = max(res["y_te"].max(), res["ens_pred"].max())
        ax.plot([lo, hi], [lo, hi], "r--")
        ax.set_xlabel(f"actual {target}"); ax.set_ylabel(f"pred {target}")
        ax.set_title(f"{target}: R²={res['r2']:.4f}, ρ={res['corr']:.3f}")
        plt.tight_layout(); plt.savefig(OUT_DIR / f"scatter_{target}.png", dpi=100); plt.close()

        # Gate R5 on this predicted vol
        if H == 24:
            gate = gate_r5_by_vol(test, res["ens_pred"])
            all_results["r5_gated_by_rv24"] = gate
            log.info("R5 gated by predicted rv_fwd_24: %s", json.dumps(gate, indent=2))

    with open(OUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str)
    log.info("\n=== DONE in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
