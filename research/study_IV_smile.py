"""
STUDY IV SMILE DYNAMICS
========================

Build 5-min IV smile features from greeks_contracts (5.7B rows).
All aggregation done server-side with DuckDB.

For each 5-min bar (ES, greek_type='delta'):
  - ATM_IV    = IV at strike nearest spot (avg of put_iv+call_iv in [spot-30, spot+30])
  - SKEW_25   = IV where |delta|≈0.25 put - IV where |delta|≈0.25 call
  - BFLY_ATM  = avg(OTM put IV + OTM call IV) - ATM_IV  (convexity)
  - TERM      = 1DTE ATM_IV - 0DTE ATM_IV

Compare across 0DTE / 1DTE via dte_type.

Then:
  - compute 30-min deltas
  - correlations with fwd 15/30/60/120m returns
  - walk-forward backtest of top signals
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
from scipy.stats import pearsonr

warnings.filterwarnings("ignore")

DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
TICKER = "ES"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\IV_SMILE")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
DELTA_LAG_BARS = 6     # 30min
TRAIN_FRAC = 0.70
TP = 12.0; SL = 8.0; MAX_HOLD = 24; SLIPPAGE = 1.0; COOLDOWN = 12

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
                              logging.StreamHandler()])
log = logging.getLogger("IV")


def _connect():
    return duckdb.connect(DB_PATH, read_only=True,
                          config={"threads": "32", "memory_limit": "110GB"})


def iv_smile_features(dte: str = "zero") -> pd.DataFrame:
    """Server-side aggregation of IV smile features per 5-min bar."""
    q = f"""
    WITH base AS (
        SELECT
            time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
            spot, strike, call_iv, put_iv, greek_value
        FROM greeks_contracts
        WHERE ticker='{TICKER}' AND greek_type='delta' AND dte_type='{dte}'
    ), bar_spot AS (
        SELECT ts_bar, AVG(spot) AS spot_avg FROM base GROUP BY ts_bar
    ), atm AS (
        SELECT
            b.ts_bar,
            AVG(CASE WHEN call_iv > 0 THEN call_iv END) AS atm_call_iv,
            AVG(CASE WHEN put_iv > 0 THEN put_iv END) AS atm_put_iv,
            COUNT(*) AS atm_n
        FROM base b
        JOIN bar_spot bs USING (ts_bar)
        WHERE ABS(b.strike - bs.spot_avg) <= 15
        GROUP BY b.ts_bar
    ), p25 AS (
        SELECT b.ts_bar,
               AVG(CASE WHEN put_iv > 0 THEN put_iv END) AS p25_iv
        FROM base b
        WHERE ABS(b.greek_value + 0.25) <= 0.05
        GROUP BY b.ts_bar
    ), c25 AS (
        SELECT b.ts_bar,
               AVG(CASE WHEN call_iv > 0 THEN call_iv END) AS c25_iv
        FROM base b
        WHERE ABS(b.greek_value - 0.25) <= 0.05
        GROUP BY b.ts_bar
    ), otm_p AS (
        SELECT b.ts_bar,
               AVG(CASE WHEN put_iv > 0 THEN put_iv END) AS otm_put_iv
        FROM base b
        JOIN bar_spot bs USING (ts_bar)
        WHERE b.strike BETWEEN bs.spot_avg - 50 AND bs.spot_avg - 20
        GROUP BY b.ts_bar
    ), otm_c AS (
        SELECT b.ts_bar,
               AVG(CASE WHEN call_iv > 0 THEN call_iv END) AS otm_call_iv
        FROM base b
        JOIN bar_spot bs USING (ts_bar)
        WHERE b.strike BETWEEN bs.spot_avg + 20 AND bs.spot_avg + 50
        GROUP BY b.ts_bar
    )
    SELECT a.ts_bar, bs.spot_avg AS spot,
           a.atm_call_iv, a.atm_put_iv, a.atm_n,
           p25.p25_iv, c25.c25_iv,
           op.otm_put_iv, oc.otm_call_iv
    FROM atm a
    JOIN bar_spot bs USING (ts_bar)
    LEFT JOIN p25 USING (ts_bar)
    LEFT JOIN c25 USING (ts_bar)
    LEFT JOIN otm_p op USING (ts_bar)
    LEFT JOIN otm_c oc USING (ts_bar)
    ORDER BY a.ts_bar
    """
    con = _connect()
    t0 = time.time()
    log.info("Running IV aggregation for dte=%s (this may take ~60-120s for 5.7B rows)", dte)
    df = con.execute(q).df()
    con.close()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    log.info("  -> %d bars for %s in %.1fs", len(df), dte, time.time() - t0)
    return df


def build_features(df0: pd.DataFrame, df1: pd.DataFrame, es_bars: pd.DataFrame) -> pd.DataFrame:
    """Combine 0DTE and 1DTE features; merge ES price bars; compute deltas."""
    d0 = df0.rename(columns={c: f"{c}_0" for c in df0.columns if c != "ts_bar"})
    d1 = df1.rename(columns={c: f"{c}_1" for c in df1.columns if c != "ts_bar"})
    df = d0.merge(d1, on="ts_bar", how="outer")

    # ATM IV
    df["atm_iv_0"] = df[["atm_call_iv_0", "atm_put_iv_0"]].mean(axis=1)
    df["atm_iv_1"] = df[["atm_call_iv_1", "atm_put_iv_1"]].mean(axis=1)
    # 25-delta skew
    df["skew25_0"] = df["p25_iv_0"] - df["c25_iv_0"]
    df["skew25_1"] = df["p25_iv_1"] - df["c25_iv_1"]
    # Butterfly (convexity)
    df["bfly_0"] = (df["otm_put_iv_0"] + df["otm_call_iv_0"]) / 2 - df["atm_iv_0"]
    df["bfly_1"] = (df["otm_put_iv_1"] + df["otm_call_iv_1"]) / 2 - df["atm_iv_1"]
    # Term structure
    df["term"] = df["atm_iv_1"] - df["atm_iv_0"]

    # 30-min deltas
    for c in ["atm_iv_0", "atm_iv_1", "skew25_0", "skew25_1", "bfly_0", "bfly_1", "term"]:
        df[f"d_{c}"] = df[c] - df[c].shift(DELTA_LAG_BARS)

    # merge ES OHLC
    df = df.merge(es_bars, on="ts_bar", how="left")

    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date

    for k, name in [(3, "15m"), (6, "30m"), (12, "60m"), (24, "120m")]:
        fc = df["s_close"].shift(-k)
        same_day = df["date"].shift(-k) == df["date"]
        df[f"ret_fwd_{name}"] = (fc - df["s_close"]).where(same_day, np.nan)
    return df


def load_es_bars() -> pd.DataFrame:
    con = _connect()
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high, MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    df = con.execute(q).df()
    con.close()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    return df


def correlate_all(df: pd.DataFrame) -> pd.DataFrame:
    feat_cols = [c for c in df.columns if c.startswith("atm_iv_") or
                 c.startswith("skew25_") or c.startswith("bfly_") or c == "term" or
                 c.startswith("d_")]
    rows = []
    for c in feat_cols:
        x = df[c].values
        for hz in ["15m", "30m", "60m", "120m"]:
            y = df[f"ret_fwd_{hz}"].values
            ok = np.isfinite(x) & np.isfinite(y)
            if ok.sum() < 500: continue
            r, p = pearsonr(x[ok], y[ok])
            rows.append({"col": c, "horizon": hz, "n": int(ok.sum()),
                         "pearson": r, "p_val": p})
    out = pd.DataFrame(rows)
    out["abs_pearson"] = out["pearson"].abs()
    out = out.sort_values("abs_pearson", ascending=False).reset_index(drop=True)
    return out


def quintile_fwd(df: pd.DataFrame, col: str, horizon: str) -> pd.DataFrame:
    x = df[col].values
    y = df[f"ret_fwd_{horizon}"].values
    ok = np.isfinite(x) & np.isfinite(y)
    if ok.sum() < 500: return pd.DataFrame()
    d = pd.DataFrame({"x": x[ok], "y": y[ok]})
    d["q"] = pd.qcut(d["x"], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
    g = d.groupby("q").agg(n=("y", "count"), mean=("y", "mean"),
                            wr=("y", lambda s: (s > 0).mean()),
                            std=("y", "std")).reset_index()
    g["sharpe"] = g["mean"] / g["std"].replace(0, np.nan)
    g["col"] = col; g["horizon"] = horizon
    return g


def backtest(df: pd.DataFrame, signal_bool: np.ndarray, side_arr: np.ndarray) -> pd.DataFrame:
    trades = []; cooldown = 0
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


def walk_forward(df: pd.DataFrame, col: str, horizon: str, sign: int) -> dict:
    dates = sorted(df["date"].unique())
    split = int(len(dates) * TRAIN_FRAC)
    train = df[df["date"].isin(set(dates[:split]))].reset_index(drop=True)
    test = df[df["date"].isin(set(dates[split:]))].reset_index(drop=True)
    results = {}
    for label, sub in [("TRAIN", train), ("TEST", test)]:
        x = sub[col].values
        valid = x[np.isfinite(x)]
        if len(valid) < 50:
            results[label] = {"n": 0}; continue
        lo_q, hi_q = np.nanquantile(x, 0.15), np.nanquantile(x, 0.85)
        sig = np.zeros(len(sub), dtype=bool)
        side = np.array([None] * len(sub), dtype=object)
        top = x >= hi_q; bot = x <= lo_q
        if sign > 0: side[top] = "LONG"; side[bot] = "SHORT"
        else: side[top] = "SHORT"; side[bot] = "LONG"
        sig[top | bot] = True
        tr = backtest(sub, sig, side)
        if len(tr) == 0:
            results[label] = {"n": 0}; continue
        p = tr["pnl_pt"]
        results[label] = {"n": int(len(tr)), "total_pt": float(p.sum()),
                          "mean_pt": float(p.mean()),
                          "win_rate": float((p > 0).mean()),
                          "sharpe_per_trade": float(p.mean()/p.std(ddof=0)) if p.std(ddof=0) > 0 else 0.0}
    return results


def main():
    t0 = time.time()
    # Load IV smile features per DTE
    df0 = iv_smile_features("zero")
    df1 = iv_smile_features("one")
    es = load_es_bars()
    df = build_features(df0, df1, es)
    df.head(200).to_csv(OUT_DIR / "features_sample.csv", index=False)
    log.info("Merged features: %d rows", len(df))

    # correlations
    corr = correlate_all(df)
    corr.to_csv(OUT_DIR / "all_correlations.csv", index=False)
    log.info("\n=== TOP-15 IV smile correlations ===\n%s",
             corr.head(15).round(5).to_string(index=False))

    # quintiles for top-5
    top5 = corr.head(5)
    q_rows = []
    for _, r in top5.iterrows():
        q = quintile_fwd(df, r["col"], r["horizon"])
        q_rows.append(q)
    Q = pd.concat(q_rows, ignore_index=True)
    Q.to_csv(OUT_DIR / "top5_quintiles.csv", index=False)
    log.info("\n=== TOP-5 QUINTILES ===\n%s", Q.round(4).to_string(index=False))

    # walk-forward
    BT = []
    for _, r in top5.iterrows():
        q = quintile_fwd(df, r["col"], r["horizon"])
        if len(q) == 0 or "Q5" not in q["q"].astype(str).values:
            continue
        q5 = q[q["q"].astype(str) == "Q5"]["mean"].iloc[0]
        sign = 1 if q5 > 0 else -1
        wf = walk_forward(df, r["col"], r["horizon"], sign)
        BT.append({"col": r["col"], "horizon": r["horizon"], "pearson": r["pearson"], "sign": sign,
                   **{f"{k}_{sk}": sv for k, v in wf.items() for sk, sv in v.items()}})
    BT = pd.DataFrame(BT)
    BT.to_csv(OUT_DIR / "top5_walkforward.csv", index=False)
    log.info("\n=== TOP-5 WALK-FORWARD ===\n%s", BT.round(4).to_string(index=False))

    # plot
    fig, ax = plt.subplots(figsize=(10, 6))
    t15 = corr.head(15)
    ax.barh(range(len(t15)), t15["abs_pearson"], color="tab:orange")
    ax.set_yticks(range(len(t15)))
    ax.set_yticklabels([f"{r.col}/{r.horizon}" for _, r in t15.iterrows()])
    ax.invert_yaxis(); ax.set_xlabel("|pearson|"); ax.set_title("IV smile features — top 15 correlations")
    plt.tight_layout(); plt.savefig(OUT_DIR / "iv_smile_corrs.png", dpi=120); plt.close()

    log.info("\n=== DONE in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
