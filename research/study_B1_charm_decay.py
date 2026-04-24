"""
STUDY B1 - Charm Decay Intraday Drift
=====================================

HYPOTHESIS
----------
Charm (dDelta/dt) creates a directional hedging pressure that accelerates
toward RTH close. For 0DTE options the pressure is enormous in the last
~90 minutes. The *sign* of net_charm_0dte predicts the sign of the
closing drift; its magnitude (scaled by 1/time_to_close) predicts the
size.

Intuition: as expiration approaches, call delta -> 0 or 1 very fast.
Dealers short gamma on calls above spot must buy as charm drags delta up.
Dealers short gamma on puts below spot must sell as charm drags delta down.
Net effect: a directional tailwind that accelerates into the bell.

DATA
----
- orderflow.zero_charm  (0DTE aggregate charm, ES_SPX)
- orderflow.one_charm   (1DTE aggregate charm, control)
- gex_summary.spot for price path (hub='classic', aggregation='gex_zero')

METHOD
------
1. Aggregate charm to 5-min bars (mean, filtered/winsorized)
2. Intraday profile: mean charm vs UTC time of day
3. Features: charm_per_minute_to_close = charm * (1/ttc_min)
4. Forward returns at 30/60/90 min
5. Dedicated last-90-min analysis: bias = sign(zero_charm @ 18:25 UTC),
   decision made ONCE per day, evaluated vs actual close drift
6. Backtest: entry at 18:25 UTC, size=sign(zero_charm), TP=12pt/SL=8pt, exit EOD 19:55 UTC
7. Control: random entry side (same days, seeded), same TP/SL/EOD
8. HMM on daily |mean_charm_last90| to separate strong vs weak charm days
9. Plots: intraday profile, scatter charm vs close drift, P&L curves, hourly heatmap
"""
from __future__ import annotations

import json
import logging
import math
import random
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from hmmlearn.hmm import GaussianHMM
from scipy import stats

warnings.filterwarnings("ignore")
sns.set_theme(style="whitegrid", context="talk")

# ============================================================
# CONFIG
# ============================================================
DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\backups\phase1\ml_gold_20260407_211327.duckdb"
TICKER = "ES_SPX"
OUT_DIR = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\B1")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:25"  # 09:25 ET
RTH_END_UTC = "19:55"    # 15:55 ET (15:30 ET would be 19:30, close is 20:00 UTC / 16:00 ET)
# Note: using 19:55 to match study A1. Last-90min = from 18:25 UTC to 19:55 UTC.

LAST_N_MIN = 90
ENTRY_TIME_UTC = "18:25"  # 90 min before close
EXIT_TIME_UTC = "19:55"
TP_PTS = 12.0
SL_PTS = 8.0

# Winsorize charm to clip extreme outliers (EOD spikes)
CHARM_WINSOR_Q = 0.995  # clip |charm| > 99.5% quantile

ROLLING_DAYS = 20
RANDOM_SEED = 42

# ============================================================
# LOGGING
# ============================================================
LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
              logging.StreamHandler()],
)
log = logging.getLogger("B1")


# ============================================================
# DATA LOAD + AGGREGATE
# ============================================================
def load_and_aggregate(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Join charm (orderflow) + spot OHLC (gex_summary) at 5-min bars."""
    q = f"""
    WITH of_agg AS (
        SELECT
            time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
            AVG(zero_charm)  AS zero_charm,
            AVG(one_charm)   AS one_charm,
            AVG(zero_vanna)  AS zero_vanna,
            AVG(one_vanna)   AS one_vanna,
            AVG(zero_agg_dex) AS zero_agg_dex,
            AVG(one_agg_dex)  AS one_agg_dex,
            COUNT(*)         AS n_ticks
        FROM orderflow
        WHERE ticker = '{TICKER}'
        GROUP BY 1
    ),
    gex_agg AS (
        SELECT
            time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
            FIRST(spot ORDER BY ts_utc) AS s_open,
            MAX(spot)                   AS s_high,
            MIN(spot)                   AS s_low,
            LAST(spot ORDER BY ts_utc)  AS s_close,
            AVG(net_gex_oi)             AS net_gex_oi,
            AVG(zero_gamma)             AS zero_gamma,
            AVG(call_wall_oi)           AS call_wall,
            AVG(put_wall_oi)            AS put_wall
        FROM gex_summary
        WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
        GROUP BY 1
    )
    SELECT
        o.ts_bar,
        g.s_open, g.s_high, g.s_low, g.s_close,
        g.net_gex_oi, g.zero_gamma, g.call_wall, g.put_wall,
        o.zero_charm, o.one_charm,
        o.zero_vanna, o.one_vanna,
        o.zero_agg_dex, o.one_agg_dex, o.n_ticks
    FROM of_agg o
    INNER JOIN gex_agg g USING(ts_bar)
    ORDER BY o.ts_bar
    """
    t0 = time.time()
    df = con.execute(q).df()
    log.info("Loaded %s bars in %.1fs", f"{len(df):,}", time.time() - t0)
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    return df


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    out = df[mask].copy().reset_index(drop=True)
    out["date"] = out["ts_bar"].dt.date
    out["time_str"] = t[mask].reset_index(drop=True)
    out["minute_of_day"] = out["ts_bar"].dt.hour * 60 + out["ts_bar"].dt.minute
    close_minute = int(EXIT_TIME_UTC[:2]) * 60 + int(EXIT_TIME_UTC[3:])
    out["ttc_min"] = (close_minute - out["minute_of_day"]).clip(lower=1)
    log.info("After RTH/weekday: %s bars  (%s -> %s)",
             f"{len(out):,}", out["ts_bar"].min(), out["ts_bar"].max())
    return out


def winsorize(s: pd.Series, q: float) -> pd.Series:
    lo, hi = s.quantile(1 - q), s.quantile(q)
    return s.clip(lo, hi)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Winsorize charm (heavy tails)
    df["zero_charm_w"] = winsorize(df["zero_charm"], CHARM_WINSOR_Q)
    df["one_charm_w"] = winsorize(df["one_charm"], CHARM_WINSOR_Q)

    # Log-returns on spot close
    df["log_ret"] = np.log(df["s_close"]).diff()

    # Charm per minute to close (hedging pressure intensity)
    df["charm_per_ttc"] = df["zero_charm_w"] / df["ttc_min"]

    # Directional bias indicator
    df["charm_sign"] = np.sign(df["zero_charm_w"]).astype(int)

    # Rolling daily std for normalization
    df["charm_abs"] = df["zero_charm_w"].abs()

    # Flag last 90 minutes
    close_minute = int(EXIT_TIME_UTC[:2]) * 60 + int(EXIT_TIME_UTC[3:])
    df["is_last90"] = (df["minute_of_day"] >= close_minute - LAST_N_MIN)

    return df


# ============================================================
# INTRADAY PROFILE
# ============================================================
def intraday_profile(df: pd.DataFrame) -> pd.DataFrame:
    prof = (df.groupby("minute_of_day")
              .agg(n=("zero_charm_w", "size"),
                   mean_charm_0=("zero_charm_w", "mean"),
                   median_charm_0=("zero_charm_w", "median"),
                   std_charm_0=("zero_charm_w", "std"),
                   mean_charm_1=("one_charm_w", "mean"),
                   mean_abs_charm_0=("charm_abs", "mean"),
                   mean_log_ret=("log_ret", "mean"))
              .reset_index())
    prof["utc_time"] = prof["minute_of_day"].apply(
        lambda m: f"{m // 60:02d}:{m % 60:02d}")
    return prof


# ============================================================
# FORWARD RETURNS
# ============================================================
def label_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    horizons = {"30m": 6, "60m": 12, "90m": 18}
    # Forward returns in points (same-day only; set to NaN if crosses day)
    for name, k in horizons.items():
        fwd = df["s_close"].shift(-k) - df["s_close"]
        same_day = df["date"].shift(-k) == df["date"]
        fwd = fwd.where(same_day, np.nan)
        df[f"ret_fwd_{name}"] = fwd
    # Close of day drift from each bar
    day_close = df.groupby("date")["s_close"].transform("last")
    df["ret_to_close_pts"] = day_close - df["s_close"]
    return df


# ============================================================
# REGRESSION: charm*1/ttc vs forward return
# ============================================================
def charm_regression(df: pd.DataFrame, horizons=("30m","60m","90m")) -> pd.DataFrame:
    rows = []
    for h in horizons:
        y = df[f"ret_fwd_{h}"].values
        x = df["charm_per_ttc"].values
        mask = np.isfinite(x) & np.isfinite(y)
        x, y = x[mask], y[mask]
        if len(x) < 100:
            continue
        slope, intercept, r, p, se = stats.linregress(x, y)
        # sign agreement
        sign_agree = np.mean(np.sign(x) == np.sign(y))
        rows.append({
            "horizon": h,
            "n": len(x),
            "slope": slope,
            "intercept": intercept,
            "r": r,
            "r_squared": r**2,
            "p_value": p,
            "std_err": se,
            "sign_agreement_rate": sign_agree,
        })
    out = pd.DataFrame(rows)
    log.info("\n=== CHARM REGRESSION (charm/ttc -> fwd return) ===\n%s",
             out.to_string(index=False))
    return out


# ============================================================
# LAST-90MIN DAILY ANALYSIS
# ============================================================
def last90_daily(df: pd.DataFrame) -> pd.DataFrame:
    """One row per day. Signal = zero_charm at 18:25 UTC. Outcome = close drift."""
    # Snapshot at entry time
    snap = df[df["time_str"] == ENTRY_TIME_UTC].copy()
    # Day-level aggregates for last90
    last90 = df[df["is_last90"]].groupby("date").agg(
        mean_charm_0_last90=("zero_charm_w", "mean"),
        sum_charm_0_last90=("zero_charm_w", "sum"),
        std_charm_0_last90=("zero_charm_w", "std"),
        n_bars_last90=("zero_charm_w", "size"),
    ).reset_index()
    # Day-level exit price
    daily_close = df.groupby("date").agg(
        day_close=("s_close", "last"),
        day_first_spot=("s_close", "first"),
    ).reset_index()
    out = (snap[["date", "ts_bar", "s_close", "zero_charm_w", "one_charm_w",
                 "net_gex_oi", "charm_per_ttc"]]
           .rename(columns={"s_close": "spot_entry",
                            "zero_charm_w": "charm_entry_0",
                            "one_charm_w": "charm_entry_1"}))
    out = out.merge(last90, on="date", how="left")
    out = out.merge(daily_close, on="date", how="left")
    out["drift_to_close"] = out["day_close"] - out["spot_entry"]
    out["entry_sign"] = np.sign(out["charm_entry_0"]).astype(int)
    out["drift_sign"] = np.sign(out["drift_to_close"]).astype(int)
    out["sign_agree"] = (out["entry_sign"] == out["drift_sign"]).astype(int)
    return out


# ============================================================
# HMM: strong vs weak charm days
# ============================================================
def fit_hmm_on_days(daily: pd.DataFrame):
    feat = daily[["mean_charm_0_last90", "std_charm_0_last90"]].dropna().copy()
    feat["log_abs_mean"] = np.log1p(feat["mean_charm_0_last90"].abs())
    feat["log_std"] = np.log1p(feat["std_charm_0_last90"].abs())
    X = feat[["log_abs_mean", "log_std"]].values
    if len(X) < 30:
        log.warning("Too few days for HMM")
        return None, None
    log.info("HMM on %d days, features=%s", len(X), list(feat.columns[-2:]))
    model = GaussianHMM(n_components=2, covariance_type="full",
                        n_iter=300, random_state=RANDOM_SEED)
    model.fit(X)
    states = model.predict(X)
    # Identify STRONG state = higher mean of log_abs_mean
    state0_mean = model.means_[0, 0]
    state1_mean = model.means_[1, 0]
    strong_state = 0 if state0_mean > state1_mean else 1
    is_strong = (states == strong_state).astype(int)
    feat["hmm_state"] = states
    feat["is_strong_charm"] = is_strong
    log.info("HMM states means log_abs_mean: s0=%.3f  s1=%.3f -> strong=%d",
             state0_mean, state1_mean, strong_state)
    # Merge back to daily
    idx = daily.dropna(subset=["mean_charm_0_last90", "std_charm_0_last90"]).index
    daily = daily.copy()
    daily.loc[idx, "hmm_state"] = states
    daily.loc[idx, "is_strong_charm"] = is_strong
    return daily, {"model": model, "strong_state": int(strong_state),
                   "n_strong": int(is_strong.sum()),
                   "n_weak": int(len(is_strong) - is_strong.sum())}


# ============================================================
# BACKTEST
# ============================================================
def _simulate_trade(path: pd.DataFrame, side: int, entry_px: float,
                    tp_pts: float, sl_pts: float, entry_ts, exit_ts):
    """Walk forward bars after entry, exit on TP/SL/EOD."""
    for i, row in path.iterrows():
        hi, lo, cl = row["s_high"], row["s_low"], row["s_close"]
        if side == 1:
            if hi - entry_px >= tp_pts:
                return tp_pts, "TP", row["ts_bar"], i
            if entry_px - lo >= sl_pts:
                return -sl_pts, "SL", row["ts_bar"], i
        else:
            if entry_px - lo >= tp_pts:
                return tp_pts, "TP", row["ts_bar"], i
            if hi - entry_px >= sl_pts:
                return -sl_pts, "SL", row["ts_bar"], i
    # EOD exit using last close
    last = path.iloc[-1]
    pnl = (last["s_close"] - entry_px) if side == 1 else (entry_px - last["s_close"])
    return pnl, "EOD", last["ts_bar"], len(path) - 1


def run_backtest(df: pd.DataFrame, daily_signals: pd.DataFrame,
                 label: str) -> pd.DataFrame:
    trades = []
    bars_by_day = {d: g.sort_values("ts_bar").reset_index(drop=True)
                   for d, g in df.groupby("date")}

    for _, row in daily_signals.iterrows():
        d = row["date"]
        side = int(row["side"])
        if side == 0 or d not in bars_by_day:
            continue
        day_bars = bars_by_day[d]
        entry_mask = day_bars["time_str"] == ENTRY_TIME_UTC
        if not entry_mask.any():
            continue
        entry_idx = day_bars.index[entry_mask][0]
        entry_bar = day_bars.iloc[entry_idx]
        entry_px = entry_bar["s_close"]
        exit_mask = day_bars["time_str"] <= EXIT_TIME_UTC
        fwd = day_bars.iloc[entry_idx + 1:][exit_mask.iloc[entry_idx + 1:].values].copy()
        if fwd.empty:
            continue
        pnl, reason, exit_ts, exit_i = _simulate_trade(fwd, side, entry_px,
                                                       TP_PTS, SL_PTS,
                                                       entry_bar["ts_bar"], None)
        trades.append({
            "label": label,
            "date": d,
            "entry_ts": entry_bar["ts_bar"],
            "entry_px": entry_px,
            "side": "LONG" if side == 1 else "SHORT",
            "pnl_pts": pnl,
            "reason": reason,
            "exit_ts": exit_ts,
            "bars_held": exit_i + 1,
            "charm_entry": row.get("charm_entry_0", np.nan),
            "hmm_strong": row.get("is_strong_charm", np.nan),
        })
    out = pd.DataFrame(trades)
    log.info("%s: %d trades", label, len(out))
    return out


def backtest_stats(trades: pd.DataFrame, group_col=None) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    def _stats(g):
        n = len(g); pnl = g["pnl_pts"]
        wr = (pnl > 0).mean()
        ev = pnl.mean()
        sd = pnl.std(ddof=0) if n > 1 else 0.0
        sharpe = ev / sd if sd > 0 else 0.0
        cumsum = pnl.cumsum().values
        running_max = np.maximum.accumulate(cumsum) if n > 0 else [0]
        dd = (cumsum - running_max).min() if n > 0 else 0.0
        return pd.Series({
            "n": n, "wr": wr, "ev_pts": ev, "std_pts": sd,
            "sharpe_trade": sharpe, "total_pnl": pnl.sum(),
            "max_dd_pts": dd,
            "tp_rate": (g["reason"] == "TP").mean(),
            "sl_rate": (g["reason"] == "SL").mean(),
            "eod_rate": (g["reason"] == "EOD").mean(),
            "avg_bars_held": g["bars_held"].mean(),
        })
    if group_col is None:
        return pd.DataFrame([_stats(trades)])
    return trades.groupby(group_col).apply(_stats).reset_index()


# ============================================================
# PLOTS
# ============================================================
def plot_intraday_profile(prof: pd.DataFrame, path: Path):
    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.plot(prof["minute_of_day"], prof["mean_charm_0"],
             label="0DTE mean charm", color="crimson", lw=2.2)
    ax1.plot(prof["minute_of_day"], prof["mean_charm_1"],
             label="1DTE mean charm", color="steelblue", lw=1.5, alpha=0.8)
    ax1.axhline(0, color="k", lw=0.8, alpha=0.5)
    ax1.axvline(int(ENTRY_TIME_UTC[:2]) * 60 + int(ENTRY_TIME_UTC[3:]),
                color="purple", ls="--", lw=1.5, label="Entry 18:25 UTC (-90min)")
    tick_minutes = list(range(13 * 60 + 30, 20 * 60 + 1, 30))
    ax1.set_xticks(tick_minutes)
    ax1.set_xticklabels([f"{m//60:02d}:{m%60:02d}" for m in tick_minutes],
                        rotation=45, fontsize=9)
    ax1.set_xlabel("UTC time of day")
    ax1.set_ylabel("Net charm (winsorized)")
    ax1.set_title("Intraday charm profile — 0DTE vs 1DTE (ES_SPX)")
    ax1.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(path, dpi=120); plt.close(fig)


def plot_charm_vs_drift(daily: pd.DataFrame, path: Path):
    fig, ax = plt.subplots(figsize=(10, 8))
    d = daily.dropna(subset=["charm_entry_0", "drift_to_close"])
    # clip x for visibility
    qlo, qhi = d["charm_entry_0"].quantile([0.02, 0.98])
    x = d["charm_entry_0"].clip(qlo, qhi)
    y = d["drift_to_close"]
    ax.scatter(x, y, alpha=0.55, s=36, c=np.where(d["sign_agree"] == 1, "seagreen", "crimson"))
    ax.axhline(0, color="k", lw=0.8); ax.axvline(0, color="k", lw=0.8)
    if len(d) > 10:
        slope, intercept, r, p, _ = stats.linregress(x, y)
        xs = np.linspace(x.min(), x.max(), 50)
        ax.plot(xs, intercept + slope * xs, "b-", lw=2,
                label=f"r={r:.3f}  p={p:.2e}  slope={slope:.2e}")
        ax.legend()
    ax.set_xlabel("charm_0DTE at 18:25 UTC")
    ax.set_ylabel("Drift entry->close (pts)")
    ax.set_title(f"Charm @ entry vs close drift — N={len(d)} days\n"
                 f"sign agreement = {d['sign_agree'].mean():.1%}")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_backtest_pnl(trades_dict: dict, path: Path):
    fig, ax = plt.subplots(figsize=(13, 6))
    for label, trades in trades_dict.items():
        if trades.empty:
            continue
        t = trades.sort_values("entry_ts").reset_index(drop=True)
        cum = t["pnl_pts"].cumsum()
        ax.plot(t["entry_ts"], cum, lw=2, label=f"{label}  N={len(t)}  total={cum.iloc[-1]:+.1f}pt")
    ax.axhline(0, color="k", lw=0.7)
    ax.set_ylabel("Cumulative P&L (pts, per-contract)")
    ax.set_title("Backtest — charm-guided vs random entry vs 1DTE-control")
    ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_hour_heatmap(df: pd.DataFrame, path: Path):
    d = df[df["is_last90"]].copy()
    d["hour"] = d["ts_bar"].dt.hour
    d["minute_bucket"] = (d["minute_of_day"] // 10) * 10
    piv = d.pivot_table(index="minute_bucket", columns="date",
                        values="zero_charm_w", aggfunc="mean")
    # reduce dates for plot
    step = max(1, piv.shape[1] // 50)
    piv_p = piv.iloc[:, ::step]
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.heatmap(piv_p, cmap="RdBu_r", center=0, ax=ax,
                cbar_kws={"label": "mean zero_charm"})
    ax.set_title(f"Last-90min charm heatmap ({piv_p.shape[1]} dates sampled)")
    ax.set_xlabel("Date"); ax.set_ylabel("Minute-of-day bucket (UTC)")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_hmm_split(trades_strong: pd.DataFrame, trades_weak: pd.DataFrame, path: Path):
    fig, ax = plt.subplots(figsize=(11, 6))
    for label, t in [("STRONG charm days", trades_strong), ("WEAK charm days", trades_weak)]:
        if t is None or t.empty:
            continue
        t = t.sort_values("entry_ts")
        cum = t["pnl_pts"].cumsum()
        ax.plot(t["entry_ts"], cum, lw=2, label=f"{label}  N={len(t)}  total={cum.iloc[-1]:+.1f}pt")
    ax.axhline(0, color="k", lw=0.7)
    ax.set_title("Charm-guided backtest split by HMM STRONG vs WEAK days")
    ax.legend(); ax.set_ylabel("Cumulative P&L (pts)")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_regression(reg: pd.DataFrame, path: Path):
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(reg))
    ax.bar(x - 0.2, reg["r"], width=0.4, label="Pearson r", color="steelblue")
    ax.bar(x + 0.2, reg["sign_agreement_rate"] - 0.5,
           width=0.4, label="Sign agreement - 0.5", color="salmon")
    ax.set_xticks(x); ax.set_xticklabels(reg["horizon"])
    ax.axhline(0, color="k", lw=0.7)
    ax.legend(); ax.set_title("Charm/ttc predictive power — Pearson r & sign agreement")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


# ============================================================
# MAIN
# ============================================================
def main():
    t_start = time.time()
    log.info("=" * 70)
    log.info("STUDY B1 — Charm Decay Intraday Drift")
    log.info("=" * 70)

    params = {
        "db_path": DB_PATH, "ticker": TICKER,
        "bar_min": BAR_MIN, "rth": [RTH_START_UTC, RTH_END_UTC],
        "entry_utc": ENTRY_TIME_UTC, "exit_utc": EXIT_TIME_UTC,
        "last_n_min": LAST_N_MIN, "tp_pts": TP_PTS, "sl_pts": SL_PTS,
        "winsor_q": CHARM_WINSOR_Q, "seed": RANDOM_SEED,
    }
    (OUT_DIR / "params.json").write_text(json.dumps(params, indent=2))

    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")

    df = load_and_aggregate(con)
    con.close()

    df = filter_rth(df)
    df = compute_features(df)
    df = label_forward_returns(df)

    # INTRADAY PROFILE
    prof = intraday_profile(df)
    prof.to_csv(OUT_DIR / "intraday_profile.csv", index=False)
    log.info("\n=== INTRADAY CHARM PROFILE (selected) ===\n%s",
             prof[["utc_time", "n", "mean_charm_0", "median_charm_0",
                   "mean_charm_1", "mean_log_ret"]].iloc[::6].to_string(index=False))

    # CHARM REGRESSION
    reg = charm_regression(df)
    reg.to_csv(OUT_DIR / "charm_regression.csv", index=False)

    # LAST-90MIN DAILY
    daily = last90_daily(df)
    daily.to_csv(OUT_DIR / "last90_daily.csv", index=False)
    log.info("\n=== LAST-90MIN DAILY AGGREGATE ===\n"
             "N=%d days | mean sign agreement = %.3f | mean drift = %.2fpt | std = %.2fpt",
             len(daily), daily["sign_agree"].mean(),
             daily["drift_to_close"].mean(), daily["drift_to_close"].std())
    # Chi-square test: is sign agreement > 50%?
    n_agree = int(daily["sign_agree"].sum()); n_total = len(daily)
    binom_p = stats.binomtest(n_agree, n_total, p=0.5, alternative="greater").pvalue
    log.info("Binomial test sign-agreement > 50%%: agree=%d/%d  p=%.4g",
             n_agree, n_total, binom_p)

    # HMM
    daily_hmm, hmm_info = fit_hmm_on_days(daily)
    if daily_hmm is not None:
        daily_hmm.to_csv(OUT_DIR / "last90_daily_hmm.csv", index=False)
    else:
        daily_hmm = daily

    # BACKTESTS
    # 1. CHARM-GUIDED: side = sign(charm @ entry)
    sig_charm = daily_hmm.copy()
    sig_charm["side"] = sig_charm["entry_sign"]
    trades_charm = run_backtest(df, sig_charm, "CHARM")

    # 2. RANDOM: same days, random side
    rng = np.random.default_rng(RANDOM_SEED)
    sig_rand = daily_hmm.copy()
    sig_rand["side"] = rng.choice([-1, 1], size=len(sig_rand))
    trades_rand = run_backtest(df, sig_rand, "RANDOM")

    # 3. 1DTE control: side = sign(charm_1DTE)
    sig_1dte = daily_hmm.copy()
    sig_1dte["side"] = np.sign(sig_1dte["charm_entry_1"]).astype(int)
    trades_1dte = run_backtest(df, sig_1dte, "CHARM_1DTE")

    # 4. CHARM x STRONG HMM: only enter on strong-charm days
    if "is_strong_charm" in daily_hmm.columns:
        sig_strong = daily_hmm[daily_hmm["is_strong_charm"] == 1].copy()
        sig_strong["side"] = sig_strong["entry_sign"]
        trades_strong = run_backtest(df, sig_strong, "CHARM_STRONG")

        sig_weak = daily_hmm[daily_hmm["is_strong_charm"] == 0].copy()
        sig_weak["side"] = sig_weak["entry_sign"]
        trades_weak = run_backtest(df, sig_weak, "CHARM_WEAK")
    else:
        trades_strong = pd.DataFrame(); trades_weak = pd.DataFrame()

    all_trades = pd.concat([trades_charm, trades_rand, trades_1dte,
                            trades_strong, trades_weak], ignore_index=True)
    all_trades.to_csv(OUT_DIR / "trades.csv", index=False)

    stats_by_label = backtest_stats(all_trades, "label")
    stats_by_label.to_csv(OUT_DIR / "backtest_stats.csv", index=False)
    log.info("\n=== BACKTEST STATS ===\n%s", stats_by_label.to_string(index=False))

    # Split CHARM by side
    if not trades_charm.empty:
        by_side = backtest_stats(trades_charm, "side")
        by_side.to_csv(OUT_DIR / "backtest_charm_by_side.csv", index=False)
        log.info("\n=== CHARM by side ===\n%s", by_side.to_string(index=False))

    # Global summary
    for label, t in [("CHARM", trades_charm), ("RANDOM", trades_rand),
                     ("CHARM_1DTE", trades_1dte)]:
        if not t.empty:
            log.info("\n%-10s  N=%d  WR=%.3f  EV=%.3fpt  Total=%+.1fpt  Sharpe/tr=%.3f",
                     label, len(t), (t["pnl_pts"] > 0).mean(),
                     t["pnl_pts"].mean(), t["pnl_pts"].sum(),
                     t["pnl_pts"].mean() / t["pnl_pts"].std(ddof=0)
                     if t["pnl_pts"].std(ddof=0) > 0 else 0)

    # PLOTS
    plot_intraday_profile(prof, OUT_DIR / "01_intraday_charm_profile.png")
    plot_charm_vs_drift(daily_hmm, OUT_DIR / "02_charm_vs_close_drift.png")
    plot_backtest_pnl({
        "CHARM-guided": trades_charm,
        "RANDOM": trades_rand,
        "CHARM 1DTE ctrl": trades_1dte,
    }, OUT_DIR / "03_backtest_pnl.png")
    plot_hour_heatmap(df, OUT_DIR / "04_last90_heatmap.png")
    plot_regression(reg, OUT_DIR / "05_regression_r.png")
    plot_hmm_split(trades_strong, trades_weak, OUT_DIR / "06_hmm_split.png")

    # Save a small features sample
    df.sample(min(5000, len(df)), random_state=RANDOM_SEED) \
      .to_csv(OUT_DIR / "features_sample.csv", index=False)

    log.info("\nStudy B1 completed in %.1fs. Output: %s",
             time.time() - t_start, OUT_DIR)


if __name__ == "__main__":
    main()
