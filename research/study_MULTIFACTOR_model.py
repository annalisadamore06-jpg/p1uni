"""
STUDY MULTIFACTOR - XGBoost GPU Ensemble
=========================================

GOAL
----
Predict sign(fwd_return_30m) for ES using a panel of engineered
features derived from gex_summary + orderflow + VIX cross-asset.
Train XGBoost GPU with 3 seeds (42/2026/7777) and ensemble (mean prob).

FEATURES
--------
1. z_net_gex (20d rolling z-score of net_gex_oi)
2. regime_label (PIN/TRN/AMP derived from z_net_gex)
3. z_delta_rr (20d rolling z-score of delta_rr)
4. delta_z_drr (30min Δ of z_delta_rr, D1b spike signal)
5. zona_triangle (ABOVE_CW / CW_ZG / ZG_PW / BELOW_PW)
6. dist_cw_pts, dist_pw_pts, dist_zg_pts (distance in points)
7. z_net_gex_VIX (cross-asset)
8. charm_magnitude (|zero_charm| from orderflow)
9. dex_flow, gex_flow, cvr_flow
10. hour_of_day, minute_of_day
11. n_aligned (ES, VIX, SPY all in same gamma regime)
12. majority_regime

TARGET
------
y = sign(ret_fwd_30m) → binary {1=UP, 0=DOWN}

SPLIT
-----
First 100 days → train, last 46 days → test.

BACKTEST
--------
Trade when P(UP)>0.6 LONG or P(UP)<0.4 SHORT.
TP=12 / SL=8 / max 24 bars / slippage 1pt.
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
from sklearn.metrics import roc_auc_score, accuracy_score

warnings.filterwarnings("ignore")

DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\MULTIFACTOR")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78
ROLLING_BARS = BARS_PER_RTH * 20
DELTA_LAG_BARS = 6

TRAIN_DAYS = 100
TEST_DAYS = 46

TP_PTS = 12.0
SL_PTS = 8.0
SLIPPAGE = 1.0
MAX_HOLD_BARS = 24
P_THR_LONG = 0.60
P_THR_SHORT = 0.40

SEEDS = [42, 2026, 7777]

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
              logging.StreamHandler()],
)
log = logging.getLogger("MULTI")


def load_gex(ticker: str) -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high, MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close,
        AVG(zero_gamma)   AS zero_gamma,
        AVG(call_wall_oi) AS call_wall,
        AVG(put_wall_oi)  AS put_wall,
        AVG(net_gex_oi)   AS net_gex_oi,
        AVG(delta_rr)     AS delta_rr
    FROM gex_summary
    WHERE ticker='{ticker}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    con = duckdb.connect(DB_PATH, read_only=True,
                         config={"threads": "32", "memory_limit": "110GB"})
    df = con.execute(q).df()
    con.close()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    return df


def load_orderflow(ticker: str = "ES") -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        AVG(zero_charm)   AS zero_charm,
        AVG(zero_vanna)   AS zero_vanna,
        AVG(dex_flow)     AS dex_flow,
        AVG(gex_flow)     AS gex_flow,
        AVG(cvr_flow)     AS cvr_flow
    FROM orderflow
    WHERE ticker='{ticker}'
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
    df["date"] = df["ts_bar"].dt.date
    return df


def compute_features(es: pd.DataFrame, vix: pd.DataFrame, spy: pd.DataFrame,
                     of: pd.DataFrame) -> pd.DataFrame:
    df = es.copy()

    # Rolling z-score for net_gex and delta_rr
    for col in ["net_gex_oi", "delta_rr"]:
        m = df[col].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
        s = df[col].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
        df[f"z_{col}"] = (df[col] - m) / s.replace(0, np.nan)

    # Regime label
    def _reg(z):
        if pd.isna(z): return np.nan
        return 1 if z >= 0.5 else (-1 if z <= -0.5 else 0)
    df["regime_ES"] = df["z_net_gex_oi"].apply(_reg)

    # Δz delta_rr over 30min
    df["z_drr_lag6"] = df["z_delta_rr"].shift(DELTA_LAG_BARS)
    same_day = df["date"] == df["date"].shift(DELTA_LAG_BARS)
    df["delta_z_drr"] = (df["z_delta_rr"] - df["z_drr_lag6"]).where(same_day, np.nan)

    # Zone + distances
    s = df["s_close"]; cw = df["call_wall"]; zg = df["zero_gamma"]; pw = df["put_wall"]
    conds = [s > cw, (s <= cw) & (s > zg), (s <= zg) & (s > pw), s <= pw]
    df["zone_code"] = np.select(conds, [0, 1, 2, 3], default=-1)
    df["dist_cw"] = cw - s
    df["dist_pw"] = s - pw
    df["dist_zg"] = zg - s

    # Cross-asset: VIX z_net_gex, SPY regime
    def _cross(other: pd.DataFrame, name: str) -> pd.DataFrame:
        o = other.copy()
        m = o["net_gex_oi"].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).mean()
        s2 = o["net_gex_oi"].rolling(ROLLING_BARS, min_periods=BARS_PER_RTH).std(ddof=0)
        o[f"z_net_gex_{name}"] = (o["net_gex_oi"] - m) / s2.replace(0, np.nan)
        o[f"regime_{name}"] = o[f"z_net_gex_{name}"].apply(_reg)
        return o[["ts_bar", f"z_net_gex_{name}", f"regime_{name}"]]

    vix_f = filter_rth(vix); spy_f = filter_rth(spy)
    df = df.merge(_cross(vix_f, "VIX"), on="ts_bar", how="left")
    df = df.merge(_cross(spy_f, "SPY"), on="ts_bar", how="left")

    # Alignment: ES, VIX (inverted), SPY all bullish/bearish
    def _align(row):
        regs = [row["regime_ES"], row["regime_SPY"], -row["regime_VIX"] if not pd.isna(row["regime_VIX"]) else np.nan]
        regs = [r for r in regs if not pd.isna(r)]
        if len(regs) == 0: return np.nan
        same = sum(1 for r in regs if r == regs[0])
        return same
    df["n_aligned"] = df.apply(_align, axis=1)

    def _majority(row):
        regs = [row["regime_ES"], row["regime_SPY"]]
        regs = [r for r in regs if not pd.isna(r)]
        if not regs: return np.nan
        return max(set(regs), key=regs.count)
    df["majority_regime"] = df.apply(_majority, axis=1)

    # Orderflow merge
    df = df.merge(of, on="ts_bar", how="left")
    df["charm_magnitude"] = df["zero_charm"].abs()

    # Time-of-day
    df["hour_of_day"] = df["ts_bar"].dt.hour
    df["minute_of_day"] = df["ts_bar"].dt.hour * 60 + df["ts_bar"].dt.minute

    # Target: fwd 30m return sign
    fc = df["s_close"].shift(-6)
    sd = df["date"].shift(-6) == df["date"]
    df["ret_fwd_30m"] = (fc - df["s_close"]).where(sd, np.nan)
    df["y"] = (df["ret_fwd_30m"] > 0).astype(int).where(df["ret_fwd_30m"].notna(), np.nan)

    return df


FEATURES = [
    "z_net_gex_oi", "regime_ES", "z_delta_rr", "delta_z_drr",
    "zone_code", "dist_cw", "dist_pw", "dist_zg",
    "z_net_gex_VIX", "regime_VIX",
    "z_net_gex_SPY", "regime_SPY",
    "n_aligned", "majority_regime",
    "charm_magnitude", "zero_vanna",
    "dex_flow", "gex_flow", "cvr_flow",
    "hour_of_day", "minute_of_day",
]


def train_ensemble(train: pd.DataFrame, test: pd.DataFrame) -> tuple[np.ndarray, list[dict], pd.DataFrame]:
    X_tr = train[FEATURES].values.astype("f4")
    y_tr = train["y"].values.astype(int)
    X_te = test[FEATURES].values.astype("f4")
    y_te = test["y"].values.astype(int)

    probs = []
    metrics = []
    importances = []
    for seed in SEEDS:
        m = xgb.XGBClassifier(
            device="cuda", tree_method="hist",
            n_estimators=400, max_depth=6, learning_rate=0.05,
            subsample=0.85, colsample_bytree=0.85,
            reg_lambda=1.0, reg_alpha=0.1,
            random_state=seed, verbosity=0,
        )
        m.fit(X_tr, y_tr)
        p = m.predict_proba(X_te)[:, 1]
        probs.append(p)
        auc = roc_auc_score(y_te, p) if len(np.unique(y_te)) > 1 else 0.5
        acc = accuracy_score(y_te, (p > 0.5).astype(int))
        metrics.append({"seed": seed, "auc": auc, "acc": acc})
        log.info("Seed %d: AUC=%.4f ACC=%.4f", seed, auc, acc)
        importances.append(pd.Series(m.feature_importances_, index=FEATURES))

    ens_p = np.mean(np.array(probs), axis=0)
    ens_auc = roc_auc_score(y_te, ens_p) if len(np.unique(y_te)) > 1 else 0.5
    ens_acc = accuracy_score(y_te, (ens_p > 0.5).astype(int))
    log.info("ENSEMBLE: AUC=%.4f ACC=%.4f", ens_auc, ens_acc)
    metrics.append({"seed": "ENSEMBLE", "auc": ens_auc, "acc": ens_acc})

    imp = pd.concat(importances, axis=1).mean(axis=1).sort_values(ascending=False)
    return ens_p, metrics, imp.reset_index().rename(columns={"index": "feature", 0: "gain_mean"})


def backtest_ensemble(test: pd.DataFrame, p: np.ndarray) -> dict:
    t = test.copy().reset_index(drop=True)
    t["p_up"] = p
    t["side"] = np.where(t["p_up"] > P_THR_LONG, "LONG",
                 np.where(t["p_up"] < P_THR_SHORT, "SHORT", None))
    trades = []
    cooldown = 0
    n = len(t); i = 0
    while i < n - 1:
        if cooldown > 0: cooldown -= 1; i += 1; continue
        side = t.loc[i, "side"]
        if side is None: i += 1; continue
        ei = i + 1
        if ei >= n or t.loc[ei, "date"] != t.loc[i, "date"]:
            i += 1; continue
        ep = t.loc[ei, "s_open"] + (SLIPPAGE if side == "LONG" else -SLIPPAGE)
        ed = t.loc[ei, "date"]
        tp = ep + (TP_PTS if side == "LONG" else -TP_PTS)
        sl = ep - (SL_PTS if side == "LONG" else -SL_PTS)
        xr = "TIMEOUT"; xp = ep; xi = ei
        for k in range(1, MAX_HOLD_BARS + 1):
            j = ei + k
            if j >= n: break
            if t.loc[j, "date"] != ed:
                xp = t.loc[j-1, "s_close"]; xi = j-1; xr = "EOD"; break
            hi, lo = t.loc[j, "s_high"], t.loc[j, "s_low"]
            if side == "LONG":
                if lo <= sl: xp = sl; xr = "SL"; xi = j; break
                if hi >= tp: xp = tp; xr = "TP"; xi = j; break
            else:
                if hi >= sl: xp = sl; xr = "SL"; xi = j; break
                if lo <= tp: xp = tp; xr = "TP"; xi = j; break
            if k == MAX_HOLD_BARS:
                xp = t.loc[j, "s_close"]; xi = j; xr = "TIMEOUT"
        pnl = (xp - ep) if side == "LONG" else (ep - xp)
        pnl -= SLIPPAGE
        trades.append({
            "entry_ts": t.loc[ei, "ts_bar"], "date": ed,
            "p_up": t.loc[i, "p_up"], "side": side,
            "entry_px": ep, "exit_px": xp, "exit_reason": xr, "pnl_pt": pnl,
        })
        cooldown = 6
        i = xi + 1
    tdf = pd.DataFrame(trades)
    tdf.to_csv(OUT_DIR / "trades_ensemble.csv", index=False)
    if len(tdf) == 0:
        return {"n": 0}
    pnl = tdf["pnl_pt"]; eq = pnl.cumsum()
    dd = (eq.cummax() - eq).max()
    s = {
        "n": int(len(tdf)),
        "total_pt": float(pnl.sum()), "mean_pt": float(pnl.mean()),
        "win_rate": float((pnl > 0).mean()),
        "sharpe_per_trade": float(pnl.mean()/pnl.std(ddof=0)) if pnl.std(ddof=0) > 0 else 0.0,
        "tp_rate": float((tdf["exit_reason"] == "TP").mean()),
        "sl_rate": float((tdf["exit_reason"] == "SL").mean()),
        "max_dd_pt": float(dd),
        "n_long": int((tdf["side"] == "LONG").sum()),
        "n_short": int((tdf["side"] == "SHORT").sum()),
    }
    log.info("Backtest ENSEMBLE: %s", json.dumps(s, default=str))
    return s


def make_plots(test: pd.DataFrame, p: np.ndarray, imp: pd.DataFrame):
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    axes[0].hist(p, bins=50, color="tab:blue"); axes[0].axvline(0.5, color="k", ls="--")
    axes[0].axvline(P_THR_LONG, color="g"); axes[0].axvline(P_THR_SHORT, color="r")
    axes[0].set_title("Ensemble P(UP) distribution (TEST)")
    axes[0].set_xlabel("P(UP)")

    imp.head(15).iloc[::-1].plot.barh(x="feature", y="gain_mean", ax=axes[1], legend=False, color="tab:purple")
    axes[1].set_title("Top 15 feature importances (mean over 3 seeds)")

    tr = pd.read_csv(OUT_DIR / "trades_ensemble.csv")
    if len(tr):
        tr["cum"] = tr["pnl_pt"].cumsum()
        axes[2].plot(range(len(tr)), tr["cum"], color="tab:green")
        axes[2].set_title(f"MULTIFACTOR equity ({len(tr)} trades, {tr['pnl_pt'].sum():.1f}pt)")
        axes[2].axhline(0, color="k", ls="--", alpha=0.5)
    plt.tight_layout(); plt.savefig(OUT_DIR / "multifactor_overview.png", dpi=120); plt.close()


def main():
    t0 = time.time()
    log.info("Loading ES/VIX/SPY gex_summary + orderflow ...")
    es = filter_rth(load_gex("ES"))
    vix = load_gex("VIX")
    spy = load_gex("SPY")
    of = load_orderflow("ES")
    log.info("ES:%d  VIX:%d  SPY:%d  orderflow:%d", len(es), len(vix), len(spy), len(of))

    log.info("Computing features ...")
    df = compute_features(es, vix, spy, of)

    # drop rows missing features or target
    before = len(df)
    need = FEATURES + ["y", "date", "s_open", "s_high", "s_low", "s_close", "ts_bar", "ret_fwd_30m"]
    df = df[need].dropna().reset_index(drop=True)
    log.info("After dropna: %d rows (dropped %d)", len(df), before - len(df))

    df.head(200).to_csv(OUT_DIR / "features_sample.csv", index=False)

    # Split first 100 days / last 46 days
    dates = sorted(df["date"].unique())
    if len(dates) < TRAIN_DAYS + 20:
        log.error("Not enough days: %d", len(dates))
        return
    train_dates = set(dates[:TRAIN_DAYS])
    test_dates = set(dates[TRAIN_DAYS:TRAIN_DAYS + TEST_DAYS])
    train = df[df["date"].isin(train_dates)].reset_index(drop=True)
    test = df[df["date"].isin(test_dates)].reset_index(drop=True)
    log.info("Train: %d rows (%d days) | Test: %d rows (%d days)",
             len(train), len(train_dates), len(test), len(test_dates))

    log.info("Training XGBoost GPU ensemble (3 seeds) ...")
    p, metrics, imp = train_ensemble(train, test)
    imp.to_csv(OUT_DIR / "feature_importance.csv", index=False)
    pd.DataFrame(metrics).to_csv(OUT_DIR / "seed_metrics.csv", index=False)

    log.info("Top 10 features:\n%s", imp.head(10).to_string(index=False))

    log.info("Running backtest with P>%.2f LONG, P<%.2f SHORT ...", P_THR_LONG, P_THR_SHORT)
    bt = backtest_ensemble(test, p)

    out = {"metrics": metrics, "backtest": bt,
           "n_train": int(len(train)), "n_test": int(len(test)),
           "n_features": len(FEATURES)}
    with open(OUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)

    make_plots(test, p, imp)
    log.info("\n=== DONE in %.1fs ===", time.time() - t0)
    log.info("Output: %s", OUT_DIR)


if __name__ == "__main__":
    main()
