"""
STUDY A3 - GEX Profile Asymmetry (Gamma Center-of-Mass)
=======================================================

HYPOTHESIS
----------
The strike-distribution of GEX defines a gravitational field. Price is
attracted to the gamma barycenter (weighted mean strike, weights=|GEX|).
Skew and kurtosis of the distribution carry directional information:

    distance_norm = (spot - barycenter) / weighted_std_GEX
    |distance_norm| > 1  -> price displaced, magnet pull toward barycenter
    skewness > 0         -> mass concentrated on call side
    skewness < 0         -> mass concentrated on put side

Intuition: dealers are short gamma around the barycenter. Hedging
demand rises as spot moves away from the weighted mean, pulling it back.

DATA
----
gex_strikes table, ticker='ES_SPX', hub='classic', aggregation='gex_full'.
~193M rows. Columns: ts_utc, strike, gex_oi, spot.

METHOD
------
Stage 1 (SQL):  Aggregate ticks to 5-min bar × strike  (mean gex_oi)
Stage 2 (SQL):  Per 5-min bar, compute weighted moments of the
                GEX distribution:
                  S0 = sum(|w|)
                  S1 = sum(|w|*strike)
                  S2 = sum(|w|*strike^2)  etc. through S4
                  barycenter = S1/S0
                  weighted_std = sqrt(S2/S0 - barycenter^2)
                  skewness = central_M3 / std^3
                  kurtosis_ex = central_M4 / std^4 - 3
Stage 3 (Python): compute distance_norm, label fwd returns, run
                regression / threshold tests / backtest.

METRICS
-------
  dist_norm  = (spot - barycenter) / weighted_std
  signed_dist_pts = spot - barycenter
  skew_gex   = (M3 / M2^1.5)  (sample-weighted)
  kurt_gex   = (M4 / M2^2) - 3

BACKTEST
--------
  Entry: spot < barycenter - 1 sigma  -> LONG
         spot > barycenter + 1 sigma  -> SHORT
  TP=12, SL=8, max hold 24 bars (120 min), cooldown 6 bars.
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
import seaborn as sns
from scipy import stats

warnings.filterwarnings("ignore")
sns.set_theme(style="whitegrid", context="talk")

# ============================================================
# CONFIG
# ============================================================
DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\backups\phase1\ml_gold_20260407_211327.duckdb"
TICKER = "ES_SPX"
HUB = "classic"
AGGREGATION = "gex_full"
OUT_DIR = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\A3")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:25"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78

TP_PTS = 12.0
SL_PTS = 8.0
MAX_HOLD_BARS = 24
COOLDOWN_BARS = 6
ENTRY_SIGMA = 0.35  # Distribution is tight (q01=-0.55, q99=+0.56), scale accordingly
SKEW_HI = 0.30      # skewness threshold for directional buckets
SKEW_LO = -0.30
SEED = 42

# A1 regime file
A1_FEATURES = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\A1\features_sample.csv")

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
log = logging.getLogger("A3")


# ============================================================
# DATA LOAD — DuckDB computes weighted moments
# ============================================================
def load_profile_features(con) -> pd.DataFrame:
    """One row per 5-min bar with barycenter, std, skew, kurtosis of GEX dist."""
    q = f"""
    WITH per_bar_strike AS (
        -- Step 1: average each strike's |gex_oi| within a 5-min bar
        SELECT
            time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
            strike,
            AVG(ABS(gex_oi)) AS w
        FROM gex_strikes
        WHERE ticker = '{TICKER}'
          AND hub = '{HUB}'
          AND aggregation = '{AGGREGATION}'
          AND gex_oi IS NOT NULL
        GROUP BY 1, 2
    ),
    per_bar_spot AS (
        -- Step 2: representative spot for the bar (median of strike-level rows)
        SELECT
            time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
            AVG(spot) AS spot_avg
        FROM gex_strikes
        WHERE ticker = '{TICKER}'
          AND hub = '{HUB}'
          AND aggregation = '{AGGREGATION}'
        GROUP BY 1
    ),
    moments_raw AS (
        -- Step 3: weighted moments over strike distribution per bar
        SELECT
            ts_bar,
            SUM(w)                            AS S0,
            SUM(w * strike)                   AS S1,
            SUM(w * strike * strike)          AS S2,
            SUM(w * strike * strike * strike) AS S3,
            SUM(w * POW(strike, 4))           AS S4,
            COUNT(*)                          AS n_strikes,
            MIN(strike)                       AS strike_min,
            MAX(strike)                       AS strike_max
        FROM per_bar_strike
        GROUP BY 1
    ),
    gex_sum AS (
        -- Signed GEX metrics per bar (without abs) for skew-of-gamma
        SELECT
            time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
            SUM(gex_oi)         AS total_gex,
            SUM(CASE WHEN gex_oi > 0 THEN gex_oi ELSE 0 END)  AS pos_gex,
            SUM(CASE WHEN gex_oi < 0 THEN gex_oi ELSE 0 END)  AS neg_gex
        FROM gex_strikes
        WHERE ticker='{TICKER}' AND hub='{HUB}' AND aggregation='{AGGREGATION}'
        GROUP BY 1
    )
    SELECT
        m.ts_bar,
        s.spot_avg AS spot,
        m.S0, m.S1, m.S2, m.S3, m.S4,
        m.n_strikes, m.strike_min, m.strike_max,
        g.total_gex, g.pos_gex, g.neg_gex
    FROM moments_raw m
    INNER JOIN per_bar_spot s USING(ts_bar)
    INNER JOIN gex_sum g USING(ts_bar)
    ORDER BY ts_bar
    """
    t0 = time.time()
    df = con.execute(q).df()
    log.info("Loaded %s profile bars from %s rows in %.1fs",
             f"{len(df):,}", "gex_strikes", time.time() - t0)
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    return df


def compute_moments(df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw moments S0..S4 into centered moments and distance metrics."""
    df = df.copy()
    S0, S1, S2, S3, S4 = df["S0"], df["S1"], df["S2"], df["S3"], df["S4"]
    mu = S1 / S0
    # Centered moments:
    # M2 = E[(X-mu)^2] = E[X^2] - mu^2
    m2 = S2 / S0 - mu ** 2
    # M3 = E[X^3] - 3*mu*E[X^2] + 2*mu^3
    m3 = S3 / S0 - 3 * mu * (S2 / S0) + 2 * mu ** 3
    # M4 = E[X^4] - 4*mu*E[X^3] + 6*mu^2*E[X^2] - 3*mu^4
    m4 = S4 / S0 - 4 * mu * (S3 / S0) + 6 * mu ** 2 * (S2 / S0) - 3 * mu ** 4

    df["barycenter"] = mu
    df["gex_std"] = np.sqrt(m2.clip(lower=0))
    df["gex_skew"] = np.where(m2 > 0, m3 / np.power(m2.clip(lower=1e-9), 1.5), np.nan)
    df["gex_kurt"] = np.where(m2 > 0, m4 / np.power(m2.clip(lower=1e-9), 2) - 3, np.nan)

    df["signed_dist"] = df["spot"] - df["barycenter"]
    df["dist_norm"] = df["signed_dist"] / df["gex_std"].replace(0, np.nan)
    df["gex_range"] = df["strike_max"] - df["strike_min"]
    return df


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    df["time_str"] = df["ts_bar"].dt.strftime("%H:%M")
    df["minute_of_day"] = df["ts_bar"].dt.hour * 60 + df["ts_bar"].dt.minute
    log.info("After RTH/weekday: %s bars (%s -> %s)",
             f"{len(df):,}", df["ts_bar"].min(), df["ts_bar"].max())
    return df


def load_ohlc(con) -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high,
        MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    df = con.execute(q).df()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    return df


def label_fwd(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    horizons = {"5m": 1, "15m": 3, "30m": 6, "60m": 12}
    for name, k in horizons.items():
        fwd = df["s_close"].shift(-k) - df["s_close"]
        same_day = df["date"].shift(-k) == df["date"]
        df[f"ret_fwd_{name}"] = fwd.where(same_day, np.nan)
    return df


# ============================================================
# ANALYSIS
# ============================================================
def regression_dist_vs_fwd(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h in ["5m", "15m", "30m", "60m"]:
        for xcol in ["signed_dist", "dist_norm", "gex_skew"]:
            d = df[[xcol, f"ret_fwd_{h}"]].dropna()
            if len(d) < 100:
                continue
            slope, intercept, r, p, se = stats.linregress(d[xcol], d[f"ret_fwd_{h}"])
            # Sign agreement: for "magnet" hypothesis, we expect sign(fwd) == -sign(dist)
            # (price reverts toward barycenter)
            if xcol in ("signed_dist", "dist_norm"):
                sign_agree_magnet = (np.sign(d[f"ret_fwd_{h}"]) == -np.sign(d[xcol])).mean()
            else:
                sign_agree_magnet = np.nan
            rows.append({"x": xcol, "horizon": h, "n": len(d),
                         "slope": slope, "r": r, "r_squared": r**2, "p": p,
                         "sign_agree_magnet": sign_agree_magnet})
    out = pd.DataFrame(rows)
    log.info("\n=== REGRESSIONS (features -> fwd return) ===\n%s",
             out.round(6).to_string(index=False))
    return out


def quintile_stats(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["dist_norm"]).copy()
    d["q"] = pd.qcut(d["dist_norm"], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
    rows = []
    for q, g in d.groupby("q", observed=True):
        for h in ["5m", "15m", "30m", "60m"]:
            r = g[f"ret_fwd_{h}"].dropna()
            if len(r) == 0: continue
            rows.append({
                "quintile": q, "horizon": h, "n": len(r),
                "dist_norm_mean": g["dist_norm"].mean(),
                "signed_dist_mean": g["signed_dist"].mean(),
                "mean_pts": r.mean(), "std_pts": r.std(ddof=0),
                "median_pts": r.median(),
                "pct_positive": (r > 0).mean(),
                "sharpe_ann": r.mean() / r.std(ddof=0) * np.sqrt(BARS_PER_RTH * 252)
                              if r.std(ddof=0) > 0 else 0.0,
            })
    out = pd.DataFrame(rows)
    log.info("\n=== QUINTILES OF dist_norm ===\n%s", out.round(4).to_string(index=False))
    return out


def threshold_tests(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["dist_norm"]).copy()
    thresholds = [
        ("dist_norm <= -2", d["dist_norm"] <= -2),
        ("dist_norm <= -1", (d["dist_norm"] <= -1) & (d["dist_norm"] > -2)),
        ("|dist_norm| <= 0.5", d["dist_norm"].abs() <= 0.5),
        ("dist_norm >= +1", (d["dist_norm"] >= 1) & (d["dist_norm"] < 2)),
        ("dist_norm >= +2", d["dist_norm"] >= 2),
    ]
    rows = []
    for lab, m in thresholds:
        g = d[m]
        for h in ["5m", "15m", "30m", "60m"]:
            r = g[f"ret_fwd_{h}"].dropna()
            if len(r) == 0: continue
            k = (r > 0).sum()
            p = stats.binomtest(int(k), len(r), p=0.5, alternative="two-sided").pvalue
            rows.append({"threshold": lab, "horizon": h, "n": len(r),
                         "mean_pts": r.mean(), "pct_positive": k / len(r),
                         "binom_p_vs_50": p})
    out = pd.DataFrame(rows)
    log.info("\n=== THRESHOLD TESTS ===\n%s", out.round(5).to_string(index=False))
    return out


def skew_buckets(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["gex_skew"]).copy()
    d["skew_b"] = pd.cut(d["gex_skew"], bins=[-np.inf, -0.5, -0.1, 0.1, 0.5, np.inf],
                          labels=["strong_neg","mild_neg","flat","mild_pos","strong_pos"])
    rows = []
    for b, g in d.groupby("skew_b", observed=True):
        for h in ["15m", "30m", "60m"]:
            r = g[f"ret_fwd_{h}"].dropna()
            if len(r) == 0: continue
            rows.append({"skew_bucket": b, "horizon": h, "n": len(r),
                         "mean_pts": r.mean(), "pct_positive": (r > 0).mean(),
                         "mean_skew": g["gex_skew"].mean()})
    out = pd.DataFrame(rows)
    log.info("\n=== GEX SKEWNESS BUCKETS ===\n%s", out.round(4).to_string(index=False))
    return out


# ============================================================
# BACKTEST — magnet pull
# ============================================================
def _sim(day_bars: pd.DataFrame, start_idx: int, side: int,
         entry_px: float) -> dict:
    end_idx = min(start_idx + MAX_HOLD_BARS, len(day_bars) - 1)
    for j in range(start_idx + 1, end_idx + 1):
        row = day_bars.iloc[j]
        hi, lo = row["s_high"], row["s_low"]
        if side == 1:
            if hi - entry_px >= TP_PTS:
                return {"pnl": TP_PTS, "reason": "TP", "bars": j - start_idx,
                        "exit_ts": row["ts_bar"]}
            if entry_px - lo >= SL_PTS:
                return {"pnl": -SL_PTS, "reason": "SL", "bars": j - start_idx,
                        "exit_ts": row["ts_bar"]}
        else:
            if entry_px - lo >= TP_PTS:
                return {"pnl": TP_PTS, "reason": "TP", "bars": j - start_idx,
                        "exit_ts": row["ts_bar"]}
            if hi - entry_px >= SL_PTS:
                return {"pnl": -SL_PTS, "reason": "SL", "bars": j - start_idx,
                        "exit_ts": row["ts_bar"]}
    last = day_bars.iloc[end_idx]
    pnl = (last["s_close"] - entry_px) if side == 1 else (entry_px - last["s_close"])
    return {"pnl": float(pnl), "reason": "TIME",
            "bars": end_idx - start_idx, "exit_ts": last["ts_bar"]}


def backtest(df: pd.DataFrame, sigma: float = ENTRY_SIGMA) -> pd.DataFrame:
    trades = []
    for d, day_bars in df.groupby("date"):
        day_bars = day_bars.sort_values("ts_bar").reset_index(drop=True)
        last_exit = -10**9
        for i in range(len(day_bars) - 1):
            if i <= last_exit + COOLDOWN_BARS:
                continue
            row = day_bars.iloc[i]
            dn = row.get("dist_norm", np.nan)
            if not np.isfinite(dn):
                continue
            side = 0; trig = None
            if dn <= -sigma:
                side = 1; trig = "below_barycenter"
            elif dn >= sigma:
                side = -1; trig = "above_barycenter"
            if side == 0:
                continue
            entry_px = row["s_close"]
            res = _sim(day_bars, i, side, entry_px)
            trades.append({
                "date": d, "entry_ts": row["ts_bar"], "entry_px": entry_px,
                "side": "LONG" if side == 1 else "SHORT",
                "trigger": trig,
                "dist_norm": dn,
                "signed_dist": row["signed_dist"],
                "barycenter": row["barycenter"],
                "gex_skew": row.get("gex_skew", np.nan),
                "gex_std": row.get("gex_std", np.nan),
                "regime_a1": row.get("regime_a1", np.nan),
                "pnl_pts": res["pnl"], "reason": res["reason"],
                "bars_held": res["bars"], "exit_ts": res["exit_ts"],
            })
            last_exit = i + res["bars"]
    out = pd.DataFrame(trades)
    log.info("Backtest magnet-pull: %d trades across %d days",
             len(out), out["date"].nunique() if not out.empty else 0)
    return out


def backtest_continuation(df: pd.DataFrame, sigma: float = ENTRY_SIGMA) -> pd.DataFrame:
    """Opposite of magnet: LONG if spot > barycenter + sigma (continuation)."""
    trades = []
    for d, day_bars in df.groupby("date"):
        day_bars = day_bars.sort_values("ts_bar").reset_index(drop=True)
        last_exit = -10**9
        for i in range(len(day_bars) - 1):
            if i <= last_exit + COOLDOWN_BARS: continue
            row = day_bars.iloc[i]
            dn = row.get("dist_norm", np.nan)
            if not np.isfinite(dn): continue
            side = 0; trig = None
            if dn >= sigma:
                side = 1; trig = "above_barycenter_CONT"
            elif dn <= -sigma:
                side = -1; trig = "below_barycenter_CONT"
            if side == 0: continue
            entry_px = row["s_close"]
            res = _sim(day_bars, i, side, entry_px)
            trades.append({
                "date": d, "entry_ts": row["ts_bar"], "entry_px": entry_px,
                "side": "LONG" if side == 1 else "SHORT", "trigger": trig,
                "dist_norm": dn, "signed_dist": row["signed_dist"],
                "barycenter": row["barycenter"], "gex_skew": row.get("gex_skew", np.nan),
                "gex_std": row.get("gex_std", np.nan),
                "regime_a1": row.get("regime_a1", np.nan),
                "pnl_pts": res["pnl"], "reason": res["reason"],
                "bars_held": res["bars"], "exit_ts": res["exit_ts"],
            })
            last_exit = i + res["bars"]
    out = pd.DataFrame(trades)
    log.info("Backtest CONTINUATION: %d trades", len(out))
    return out


def backtest_skew(df: pd.DataFrame) -> pd.DataFrame:
    """LONG when gex_skew in (0, +0.5) (mild_pos); SHORT when skew > +0.5 or < -0.5."""
    trades = []
    for d, day_bars in df.groupby("date"):
        day_bars = day_bars.sort_values("ts_bar").reset_index(drop=True)
        last_exit = -10**9
        for i in range(len(day_bars) - 1):
            if i <= last_exit + COOLDOWN_BARS: continue
            row = day_bars.iloc[i]
            sk = row.get("gex_skew", np.nan)
            if not np.isfinite(sk): continue
            side = 0; trig = None
            if 0.05 < sk <= 0.5:
                side = 1; trig = "mild_pos_skew"
            elif sk > 0.5:
                side = -1; trig = "strong_pos_skew"
            elif sk < -0.5:
                side = -1; trig = "strong_neg_skew"
            if side == 0: continue
            entry_px = row["s_close"]
            res = _sim(day_bars, i, side, entry_px)
            trades.append({
                "date": d, "entry_ts": row["ts_bar"], "entry_px": entry_px,
                "side": "LONG" if side == 1 else "SHORT", "trigger": trig,
                "dist_norm": row.get("dist_norm", np.nan),
                "signed_dist": row.get("signed_dist", np.nan),
                "barycenter": row["barycenter"], "gex_skew": sk,
                "gex_std": row.get("gex_std", np.nan),
                "regime_a1": row.get("regime_a1", np.nan),
                "pnl_pts": res["pnl"], "reason": res["reason"],
                "bars_held": res["bars"], "exit_ts": res["exit_ts"],
            })
            last_exit = i + res["bars"]
    out = pd.DataFrame(trades)
    log.info("Backtest SKEW: %d trades", len(out))
    return out


def bt_stats(t: pd.DataFrame, group_col=None) -> pd.DataFrame:
    if t.empty: return pd.DataFrame()
    def _s(g):
        n = len(g); pnl = g["pnl_pts"]
        wr = (pnl > 0).mean(); ev = pnl.mean()
        sd = pnl.std(ddof=0) if n > 1 else 0.0
        sharpe = ev / sd if sd > 0 else 0.0
        cum = pnl.cumsum().values
        rm = np.maximum.accumulate(cum) if n > 0 else [0]
        dd = (cum - rm).min() if n > 0 else 0.0
        has_r = "reason" in g.columns
        return pd.Series({
            "n": n, "wr": wr, "ev_pts": ev, "std_pts": sd,
            "sharpe_trade": sharpe, "total_pnl": pnl.sum(), "max_dd": dd,
            "tp_rate": (g["reason"] == "TP").mean() if has_r else np.nan,
            "sl_rate": (g["reason"] == "SL").mean() if has_r else np.nan,
            "time_rate": (g["reason"] == "TIME").mean() if has_r else np.nan,
            "avg_bars": g["bars_held"].mean(),
        })
    if group_col is None:
        return pd.DataFrame([_s(t)])
    return t.groupby(group_col, observed=True).apply(_s).reset_index()


# ============================================================
# A1 REGIME (on the fly using gex_summary.net_gex_oi)
# ============================================================
def compute_a1_regime(con) -> pd.DataFrame:
    q = f"""
    SELECT time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
           AVG(net_gex_oi) AS net_gex_oi
    FROM gex_summary WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    d = con.execute(q).df()
    d["ts_bar"] = pd.to_datetime(d["ts_bar"], utc=True)
    w = BARS_PER_RTH * 20
    mu = d["net_gex_oi"].rolling(w, min_periods=w // 4).mean()
    sd = d["net_gex_oi"].rolling(w, min_periods=w // 4).std(ddof=0)
    z = (d["net_gex_oi"] - mu) / sd.replace(0, np.nan)
    d["ngex_z"] = z
    d["regime_a1"] = np.where(z >= 0.5, "PINNING",
                      np.where(z <= -0.5, "AMPLIFICATION", "TRANSITION"))
    d.loc[z.isna(), "regime_a1"] = np.nan
    return d[["ts_bar", "regime_a1", "ngex_z"]]


# ============================================================
# PLOTS
# ============================================================
def plot_barycenter_vs_spot(df: pd.DataFrame, path: Path):
    d = df.dropna(subset=["barycenter", "spot"]).sample(
        min(15000, len(df)), random_state=SEED)
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.scatter(d["barycenter"], d["spot"], alpha=0.12, s=5, c="navy")
    lo, hi = min(d["spot"].min(), d["barycenter"].min()), max(d["spot"].max(), d["barycenter"].max())
    ax.plot([lo, hi], [lo, hi], "r-", lw=1.5, label="spot = barycenter")
    ax.set_xlabel("GEX barycenter (weighted mean strike)")
    ax.set_ylabel("Spot")
    ax.set_title(f"Barycenter vs Spot  (N={len(d):,})")
    ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_dist_distribution(df: pd.DataFrame, path: Path):
    z = df["dist_norm"].dropna()
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].hist(z, bins=80, color="steelblue", alpha=0.85)
    for x in [-2, -1, 1, 2]:
        axes[0].axvline(x, color="crimson", ls="--", lw=1, alpha=0.7)
    axes[0].set_title(f"dist_norm distribution  (N={len(z):,})")
    axes[0].set_xlabel("(spot - barycenter) / gex_std")
    signed = df["signed_dist"].dropna()
    axes[1].hist(signed, bins=80, color="orange", alpha=0.85)
    axes[1].set_title(f"signed_dist (pts)  (N={len(signed):,})")
    axes[1].set_xlabel("spot - barycenter (pts)")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_quintile_matrix(q: pd.DataFrame, path: Path):
    piv = q.pivot(index="quintile", columns="horizon", values="mean_pts") \
            .reindex(index=["Q1","Q2","Q3","Q4","Q5"], columns=["5m","15m","30m","60m"])
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.heatmap(piv, annot=True, fmt=".2f", cmap="RdBu_r", center=0, ax=ax)
    ax.set_title("Mean fwd return (pts) by dist_norm quintile × horizon")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_scatter_dist_fwd(df: pd.DataFrame, path: Path):
    d = df.dropna(subset=["dist_norm", "ret_fwd_60m"]).copy()
    if len(d) > 40000:
        d = d.sample(40000, random_state=SEED)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(d["dist_norm"], d["ret_fwd_60m"], alpha=0.1, s=6, c="navy")
    d["bin"] = pd.cut(d["dist_norm"], bins=30)
    agg = d.groupby("bin", observed=True).agg(x=("dist_norm", "mean"),
                                                y=("ret_fwd_60m", "mean"))
    ax.plot(agg["x"], agg["y"], "r-", lw=2.5, label="bin mean")
    for v in [-1, 1, -2, 2]:
        ax.axvline(v, color="purple", ls="--", lw=1.0, alpha=0.6)
    ax.axhline(0, color="k", lw=0.7)
    ax.set_xlabel("dist_norm = (spot - barycenter)/gex_std")
    ax.set_ylabel("Fwd return 60m (pts)")
    ax.set_title("dist_norm vs 60-min forward return")
    ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_pnl(splits: dict, path: Path):
    fig, ax = plt.subplots(figsize=(13, 6))
    for label, t in splits.items():
        if t is None or t.empty: continue
        t = t.sort_values("entry_ts")
        cum = t["pnl_pts"].cumsum()
        ax.plot(t["entry_ts"], cum, lw=2,
                label=f"{label}  N={len(t)}  total={cum.iloc[-1]:+.1f}pt")
    ax.axhline(0, color="k", lw=0.7)
    ax.legend()
    ax.set_title("Backtest — gamma barycenter magnet-pull")
    ax.set_ylabel("Cumulative P&L (pts)")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_skew_effect(sk: pd.DataFrame, path: Path):
    piv = sk.pivot(index="skew_bucket", columns="horizon", values="mean_pts") \
            .reindex(index=["strong_neg","mild_neg","flat","mild_pos","strong_pos"],
                     columns=["15m","30m","60m"])
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.heatmap(piv, annot=True, fmt=".2f", cmap="RdBu_r", center=0, ax=ax)
    ax.set_title("Mean fwd return by GEX skewness bucket")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


# ============================================================
# MAIN
# ============================================================
def main():
    t_start = time.time()
    log.info("=" * 70)
    log.info("STUDY A3 — GEX Profile Asymmetry")
    log.info("=" * 70)

    (OUT_DIR / "params.json").write_text(json.dumps({
        "db_path": DB_PATH, "ticker": TICKER,
        "hub": HUB, "aggregation": AGGREGATION,
        "bar_min": BAR_MIN, "rth": [RTH_START_UTC, RTH_END_UTC],
        "entry_sigma": ENTRY_SIGMA, "tp_pts": TP_PTS, "sl_pts": SL_PTS,
        "max_hold_bars": MAX_HOLD_BARS, "cooldown_bars": COOLDOWN_BARS,
    }, indent=2))

    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")

    prof = load_profile_features(con)
    prof = compute_moments(prof)

    ohlc = load_ohlc(con)
    regime = compute_a1_regime(con)
    con.close()

    df = prof.merge(ohlc, on="ts_bar", how="inner")
    df = df.merge(regime, on="ts_bar", how="left")
    df = filter_rth(df)
    df = label_fwd(df)

    log.info("Coverage: %d bars  |  dist_norm valid=%d  |  skew valid=%d",
             len(df), df["dist_norm"].notna().sum(), df["gex_skew"].notna().sum())
    log.info("dist_norm: mean=%.3f std=%.3f  q01=%.2f q99=%.2f",
             df["dist_norm"].mean(), df["dist_norm"].std(),
             df["dist_norm"].quantile(0.01), df["dist_norm"].quantile(0.99))
    log.info("gex_skew:  mean=%.3f std=%.3f  q01=%.2f q99=%.2f",
             df["gex_skew"].mean(), df["gex_skew"].std(),
             df["gex_skew"].quantile(0.01), df["gex_skew"].quantile(0.99))
    log.info("signed_dist(pts): mean=%.2f std=%.2f",
             df["signed_dist"].mean(), df["signed_dist"].std())

    reg = regression_dist_vs_fwd(df); reg.to_csv(OUT_DIR / "regressions.csv", index=False)
    q = quintile_stats(df); q.to_csv(OUT_DIR / "quintiles.csv", index=False)
    thr = threshold_tests(df); thr.to_csv(OUT_DIR / "thresholds.csv", index=False)
    sk = skew_buckets(df); sk.to_csv(OUT_DIR / "skew_buckets.csv", index=False)

    trades = backtest(df, sigma=ENTRY_SIGMA)
    trades["strat"] = "MAGNET"

    # Second strategy: CONTINUATION (opposite of magnet) based on quintile evidence
    trades_cont = backtest_continuation(df, sigma=ENTRY_SIGMA)
    if not trades_cont.empty:
        trades_cont["strat"] = "CONTINUATION"

    # Third: SKEW-based (mild_pos=LONG, strong_pos or neg=SHORT)
    trades_skew = backtest_skew(df)
    if not trades_skew.empty:
        trades_skew["strat"] = "SKEW"

    trades = pd.concat([trades, trades_cont, trades_skew], ignore_index=True)
    trades.to_csv(OUT_DIR / "trades.csv", index=False)
    if not trades.empty:
        st_all = bt_stats(trades)
        log.info("\n=== BACKTEST ALL ===\n%s", st_all.to_string(index=False))
        st_all.to_csv(OUT_DIR / "bt_stats_all.csv", index=False)

        st_strat = bt_stats(trades, "strat"); st_strat.to_csv(OUT_DIR / "bt_stats_by_strat.csv", index=False)
        log.info("\n=== BY STRATEGY ===\n%s", st_strat.to_string(index=False))

        st_trig = bt_stats(trades, "trigger"); st_trig.to_csv(OUT_DIR / "bt_stats_by_trigger.csv", index=False)
        st_rsn  = bt_stats(trades, "reason");  st_rsn.to_csv(OUT_DIR / "bt_stats_by_reason.csv", index=False)
        log.info("\n=== BY TRIGGER ===\n%s", st_trig.to_string(index=False))
        log.info("\n=== BY EXIT REASON ===\n%s", st_rsn.to_string(index=False))

        if trades["regime_a1"].notna().any():
            st_reg = bt_stats(trades.dropna(subset=["regime_a1"]), "regime_a1")
            log.info("\n=== BY A1 REGIME ===\n%s", st_reg.to_string(index=False))
            st_reg.to_csv(OUT_DIR / "bt_stats_by_regime.csv", index=False)

    # Plots
    plot_barycenter_vs_spot(df, OUT_DIR / "01_barycenter_vs_spot.png")
    plot_dist_distribution(df, OUT_DIR / "02_distance_distribution.png")
    plot_scatter_dist_fwd(df, OUT_DIR / "03_scatter_dist_vs_fwd60m.png")
    plot_quintile_matrix(q, OUT_DIR / "04_quintile_matrix.png")
    plot_skew_effect(sk, OUT_DIR / "05_skew_effect.png")
    if not trades.empty:
        plot_pnl({
            "MAGNET (ALL)": trades[trades["strat"] == "MAGNET"],
            "CONTINUATION (ALL)": trades[trades["strat"] == "CONTINUATION"],
            "SKEW (ALL)": trades[trades["strat"] == "SKEW"],
            "SKEW mild_pos LONG": trades[trades["trigger"] == "mild_pos_skew"],
            "CONT LONG above-bary": trades[trades["trigger"] == "above_barycenter_CONT"],
        }, OUT_DIR / "06_pnl_curves.png")

    cols_small = ["ts_bar","spot","barycenter","signed_dist","dist_norm",
                  "gex_skew","gex_kurt","gex_std","regime_a1"]
    df[cols_small].sample(min(5000, len(df)), random_state=SEED) \
        .to_csv(OUT_DIR / "features_sample.csv", index=False)

    log.info("\nStudy A3 completed in %.1fs. Output: %s",
             time.time() - t_start, OUT_DIR)


if __name__ == "__main__":
    main()
