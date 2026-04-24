"""
STUDY D1 - Risk-Reversal Dynamics (Skew)
=========================================

HYPOTHESIS
----------
delta_rr (0DTE) measures the call-vs-put skew. When z-score of delta_rr
crashes to z <= -2, it signals put capitulation (crowded downside bid)
and should mark a tactical bottom. When z-score spikes to z >= +2, it
signals call euphoria and marks a tactical top.

DATA
----
gex_summary.delta_rr, hub='classic', aggregation='gex_zero' (0DTE skew).
ticker='ES_SPX'. Unconditional distribution is right-skewed (mean ~+3.5,
99% quantile ~+20.6) so we z-score in a rolling window to get a
stationary signal.

METHOD
------
1. Aggregate to 5-min RTH bars (mean delta_rr per bar)
2. Rolling 20-day (1560 bars) z-score of delta_rr
3. Forward returns at 5/15/30/60/120 min (same-day only)
4. Quintile analysis: forward returns per z-score quintile
5. Extreme threshold tests: z <= -2, -1.5, +1.5, +2
6. Time-to-reversion: bars until |z| <= 0.5 after an extreme trigger
7. Backtest: LONG at z <= -2, SHORT at z >= +2, TP=12 / SL=8, max 120min
8. Intraday clustering: do extreme triggers concentrate morning vs afternoon?
9. Plots: z distribution, scatter, quintile fwd returns, reversion histogram,
   cumulative P&L.
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
OUT_DIR = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\D1")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:25"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78
ROLLING_BARS = BARS_PER_RTH * 20  # 20-day rolling = 1560 bars

Z_EXTREME = 2.0
Z_SOFT = 1.5
Z_REVERT_TOL = 0.5

TP_PTS = 12.0
SL_PTS = 8.0
MAX_HOLD_BARS = 24  # 120 min = 24 * 5-min bars
COOLDOWN_BARS = 6   # 30 min cooldown after exit
SEED = 42

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
log = logging.getLogger("D1")


# ============================================================
# DATA
# ============================================================
def load_bars(con) -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high,
        MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close,
        AVG(delta_rr)     AS delta_rr,
        AVG(net_gex_oi)   AS net_gex_oi,
        AVG(zero_gamma)   AS zero_gamma,
        AVG(call_wall_oi) AS call_wall,
        AVG(put_wall_oi)  AS put_wall,
        COUNT(*)          AS n_ticks
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
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
    df = df[mask].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    df["time_str"] = df["ts_bar"].dt.strftime("%H:%M")
    df["minute_of_day"] = df["ts_bar"].dt.hour * 60 + df["ts_bar"].dt.minute
    log.info("After RTH/weekday: %s bars (%s -> %s)",
             f"{len(df):,}", df["ts_bar"].min(), df["ts_bar"].max())
    return df


def compute_zscore(df: pd.DataFrame, window: int = ROLLING_BARS) -> pd.DataFrame:
    df = df.copy()
    mu = df["delta_rr"].rolling(window, min_periods=window // 4).mean()
    sd = df["delta_rr"].rolling(window, min_periods=window // 4).std(ddof=0)
    df["drr_z"] = (df["delta_rr"] - mu) / sd.replace(0, np.nan)
    df["drr_mu"] = mu
    df["drr_sd"] = sd
    return df


def label_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    horizons = {"5m": 1, "15m": 3, "30m": 6, "60m": 12, "120m": 24}
    for name, k in horizons.items():
        fwd = df["s_close"].shift(-k) - df["s_close"]
        same_day = df["date"].shift(-k) == df["date"]
        df[f"ret_fwd_{name}"] = fwd.where(same_day, np.nan)
    return df


# ============================================================
# QUINTILE & THRESHOLD ANALYSIS
# ============================================================
def quintile_stats(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["drr_z"]).copy()
    d["z_quintile"] = pd.qcut(d["drr_z"], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
    rows = []
    for q, g in d.groupby("z_quintile", observed=True):
        for h in ["5m", "15m", "30m", "60m", "120m"]:
            r = g[f"ret_fwd_{h}"].dropna()
            if len(r) == 0:
                continue
            rows.append({
                "quintile": q, "horizon": h, "n": len(r),
                "z_mean": g["drr_z"].mean(),
                "mean_pts": r.mean(), "std_pts": r.std(ddof=0),
                "median_pts": r.median(),
                "pct_positive": (r > 0).mean(),
                "sharpe_ann": r.mean() / r.std(ddof=0) * np.sqrt(BARS_PER_RTH * 252)
                              if r.std(ddof=0) > 0 else 0.0,
            })
    out = pd.DataFrame(rows)
    log.info("\n=== QUINTILE FORWARD RETURNS ===\n%s",
             out.round(4).to_string(index=False))
    return out


def threshold_stats(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["drr_z"]).copy()
    thresholds = [
        ("z <= -2", d["drr_z"] <= -Z_EXTREME),
        ("z <= -1.5", (d["drr_z"] <= -Z_SOFT) & (d["drr_z"] > -Z_EXTREME)),
        ("|z| <= 0.5", d["drr_z"].abs() <= Z_REVERT_TOL),
        ("z >= 1.5", (d["drr_z"] >= Z_SOFT) & (d["drr_z"] < Z_EXTREME)),
        ("z >= 2", d["drr_z"] >= Z_EXTREME),
    ]
    rows = []
    for label, mask in thresholds:
        g = d[mask]
        for h in ["5m", "15m", "30m", "60m", "120m"]:
            r = g[f"ret_fwd_{h}"].dropna()
            if len(r) == 0:
                continue
            # Binomial test
            n_pos = (r > 0).sum()
            p = stats.binomtest(int(n_pos), len(r), p=0.5,
                                alternative="two-sided").pvalue
            rows.append({
                "threshold": label, "horizon": h, "n": len(r),
                "mean_pts": r.mean(), "std_pts": r.std(ddof=0),
                "pct_positive": n_pos / len(r),
                "binom_p_vs_50": p,
                "mean_pts_se": r.std(ddof=0) / np.sqrt(len(r)),
            })
    out = pd.DataFrame(rows)
    log.info("\n=== EXTREME THRESHOLD STATS ===\n%s",
             out.round(5).to_string(index=False))
    return out


def time_to_reversion(df: pd.DataFrame, trigger_z: float = -Z_EXTREME,
                      direction: str = "below") -> pd.DataFrame:
    """After a trigger (z crosses into extreme), count bars until |z| <= 0.5."""
    z = df["drr_z"].values
    d = df["date"].values
    # trigger: first bar in a run where z is beyond threshold
    if direction == "below":
        in_extreme = z <= trigger_z
    else:
        in_extreme = z >= trigger_z
    triggers = in_extreme & np.roll(~in_extreme, 1)
    triggers[0] = in_extreme[0]
    idx_triggers = np.where(triggers)[0]
    reversions = []
    for i in idx_triggers:
        day = d[i]
        found = None
        for j in range(i + 1, min(i + 240, len(z))):
            if d[j] != day:
                break
            if np.isfinite(z[j]) and abs(z[j]) <= Z_REVERT_TOL:
                found = j - i
                break
        reversions.append({
            "trigger_idx": int(i), "date": day,
            "z_at_trigger": float(z[i]) if np.isfinite(z[i]) else np.nan,
            "bars_to_revert": int(found) if found is not None else np.nan,
        })
    out = pd.DataFrame(reversions)
    if out.empty:
        return out
    n_rev = out["bars_to_revert"].notna().sum()
    log.info("Reversion analysis (trigger %s %.2f): %d triggers, %d reverted same-day",
             direction, trigger_z, len(out), n_rev)
    if n_rev > 0:
        rev = out["bars_to_revert"].dropna()
        log.info("  Bars-to-reversion: median=%.1f mean=%.1f p75=%.1f max=%.1f",
                 rev.median(), rev.mean(), rev.quantile(0.75), rev.max())
    return out


# ============================================================
# INTRADAY CLUSTERING
# ============================================================
def intraday_cluster(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["drr_z"]).copy()
    d["period"] = pd.cut(
        d["minute_of_day"],
        bins=[0, 14 * 60, 16 * 60, 18 * 60, 24 * 60],
        labels=["open", "morning", "midday", "afternoon"], right=False,
    )
    rows = []
    for period, g in d.groupby("period", observed=True):
        rows.append({
            "period": period, "n_bars": len(g),
            "mean_z": g["drr_z"].mean(),
            "std_z": g["drr_z"].std(ddof=0),
            "n_z_le_m2": (g["drr_z"] <= -Z_EXTREME).sum(),
            "n_z_ge_p2": (g["drr_z"] >= Z_EXTREME).sum(),
            "pct_extreme": ((g["drr_z"].abs() >= Z_EXTREME).mean()),
        })
    out = pd.DataFrame(rows)
    log.info("\n=== INTRADAY Z-EXTREME CLUSTERING ===\n%s",
             out.round(4).to_string(index=False))
    return out


# ============================================================
# BACKTEST
# ============================================================
def _simulate(day_bars: pd.DataFrame, start_idx: int, side: int,
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


def backtest(df: pd.DataFrame, z_low: float = -Z_EXTREME,
             z_high: float = Z_EXTREME) -> pd.DataFrame:
    trades = []
    for d, day_bars in df.groupby("date"):
        day_bars = day_bars.sort_values("ts_bar").reset_index(drop=True)
        last_exit = -10**9
        for i in range(len(day_bars) - 1):
            if i <= last_exit + COOLDOWN_BARS:
                continue
            row = day_bars.iloc[i]
            z = row["drr_z"]
            if not np.isfinite(z):
                continue
            side = 0
            trigger = None
            if z <= z_low:
                side = 1; trigger = "z_low"
            elif z >= z_high:
                side = -1; trigger = "z_high"
            if side == 0:
                continue
            entry_px = row["s_close"]
            res = _simulate(day_bars, i, side, entry_px)
            trades.append({
                "date": d, "entry_ts": row["ts_bar"], "entry_px": entry_px,
                "side": "LONG" if side == 1 else "SHORT",
                "side_int": side, "trigger": trigger,
                "z_entry": z, "net_gex_oi": row["net_gex_oi"],
                "pnl_pts": res["pnl"], "reason": res["reason"],
                "bars_held": res["bars"], "exit_ts": res["exit_ts"],
            })
            last_exit = i + res["bars"]
    out = pd.DataFrame(trades)
    log.info("Backtest EXTREME-RR: %d trades across %d days",
             len(out), out["date"].nunique() if not out.empty else 0)
    return out


def bt_stats(t: pd.DataFrame, group_col=None) -> pd.DataFrame:
    if t.empty:
        return pd.DataFrame()
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
# REGIME COMBO (A1-style on the fly, since features_sample lacks ts_bar)
# ============================================================
def compute_a1_regime(df: pd.DataFrame, window: int = ROLLING_BARS,
                     z_pin: float = 0.5, z_amp: float = -0.5) -> pd.DataFrame:
    df = df.copy()
    mu = df["net_gex_oi"].rolling(window, min_periods=window // 4).mean()
    sd = df["net_gex_oi"].rolling(window, min_periods=window // 4).std(ddof=0)
    z = (df["net_gex_oi"] - mu) / sd.replace(0, np.nan)
    df["ngex_z"] = z
    regime = np.where(z >= z_pin, "PINNING",
              np.where(z <= z_amp, "AMPLIFICATION", "TRANSITION"))
    regime = pd.Series(regime).where(z.notna(), np.nan)
    df["regime_a1"] = regime.values
    return df


# ============================================================
# PLOTS
# ============================================================
def plot_z_distribution(df: pd.DataFrame, path: Path):
    z = df["drr_z"].dropna()
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].hist(z, bins=80, color="steelblue", alpha=0.85)
    for x in [-Z_EXTREME, -Z_SOFT, Z_SOFT, Z_EXTREME]:
        axes[0].axvline(x, color="crimson", ls="--", lw=1, alpha=0.7)
    axes[0].set_title(f"delta_rr z-score distribution  (N={len(z):,})")
    axes[0].set_xlabel("z-score")
    # Intraday profile
    ts = df.groupby("minute_of_day")["drr_z"].mean()
    axes[1].plot(ts.index, ts.values, color="darkorange", lw=1.8)
    axes[1].axhline(0, color="k", lw=0.8)
    tick_minutes = list(range(13 * 60 + 30, 20 * 60 + 1, 30))
    axes[1].set_xticks(tick_minutes)
    axes[1].set_xticklabels([f"{m//60:02d}:{m%60:02d}" for m in tick_minutes],
                            rotation=45, fontsize=9)
    axes[1].set_title("Mean z by UTC minute-of-day")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_scatter_z_vs_fwd(df: pd.DataFrame, path: Path):
    d = df.dropna(subset=["drr_z", "ret_fwd_60m"]).copy()
    if len(d) > 40000:
        d = d.sample(40000, random_state=SEED)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(d["drr_z"], d["ret_fwd_60m"], alpha=0.1, s=6, c="navy")
    d["bin"] = pd.cut(d["drr_z"], bins=30)
    agg = d.groupby("bin", observed=True).agg(x=("drr_z", "mean"),
                                               y=("ret_fwd_60m", "mean"))
    ax.plot(agg["x"], agg["y"], "r-", lw=2.5, label="bin mean")
    ax.axhline(0, color="k", lw=0.7)
    for v in [-Z_EXTREME, Z_EXTREME]:
        ax.axvline(v, color="purple", ls="--", lw=1.2)
    ax.set_xlabel("delta_rr z-score")
    ax.set_ylabel("Forward return 60m (pts)")
    ax.set_title("delta_rr z-score vs 60-min forward return")
    ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_quintile_returns(q: pd.DataFrame, path: Path):
    piv = q.pivot(index="quintile", columns="horizon", values="mean_pts") \
            .reindex(index=["Q1","Q2","Q3","Q4","Q5"],
                     columns=["5m","15m","30m","60m","120m"])
    fig, ax = plt.subplots(figsize=(11, 5))
    sns.heatmap(piv, annot=True, fmt=".2f", cmap="RdBu_r", center=0, ax=ax)
    ax.set_title("Mean forward return (pts) by z-score quintile × horizon")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_threshold_returns(t: pd.DataFrame, path: Path):
    piv = t.pivot(index="threshold", columns="horizon", values="mean_pts") \
           .reindex(index=["z <= -2","z <= -1.5","|z| <= 0.5","z >= 1.5","z >= 2"],
                    columns=["5m","15m","30m","60m","120m"])
    fig, ax = plt.subplots(figsize=(11, 5))
    sns.heatmap(piv, annot=True, fmt=".2f", cmap="RdBu_r", center=0, ax=ax)
    ax.set_title("Mean forward return (pts) by z-threshold × horizon")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_reversion_hist(rev_low: pd.DataFrame, rev_high: pd.DataFrame, path: Path):
    fig, ax = plt.subplots(figsize=(11, 5))
    if rev_low is not None and not rev_low.empty:
        bars = rev_low["bars_to_revert"].dropna()
        ax.hist(bars, bins=30, alpha=0.6, label=f"From z <= -2 (n={len(bars)})",
                color="seagreen")
    if rev_high is not None and not rev_high.empty:
        bars = rev_high["bars_to_revert"].dropna()
        ax.hist(bars, bins=30, alpha=0.6, label=f"From z >= +2 (n={len(bars)})",
                color="crimson")
    ax.set_xlabel("Bars (5-min) until |z| <= 0.5")
    ax.set_ylabel("Trigger count")
    ax.set_title("Time to z-score reversion after extreme trigger")
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
    ax.set_title("Backtest — extreme delta_rr z-score signal")
    ax.set_ylabel("Cumulative P&L (pts)")
    ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


# ============================================================
# MAIN
# ============================================================
def main():
    t_start = time.time()
    log.info("=" * 70)
    log.info("STUDY D1 — delta_rr Risk-Reversal Z-Score")
    log.info("=" * 70)

    (OUT_DIR / "params.json").write_text(json.dumps({
        "db_path": DB_PATH, "ticker": TICKER, "bar_min": BAR_MIN,
        "rth": [RTH_START_UTC, RTH_END_UTC],
        "rolling_bars": ROLLING_BARS,
        "z_extreme": Z_EXTREME, "z_soft": Z_SOFT, "z_revert_tol": Z_REVERT_TOL,
        "tp_pts": TP_PTS, "sl_pts": SL_PTS,
        "max_hold_bars": MAX_HOLD_BARS, "cooldown_bars": COOLDOWN_BARS,
    }, indent=2))

    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df_all = load_bars(con)
    con.close()

    df = filter_rth(df_all)
    df = compute_zscore(df)
    df = compute_a1_regime(df)
    df = label_forward_returns(df)

    log.info("Coverage: %d bars, %d with z", len(df), df["drr_z"].notna().sum())
    log.info("drr_z: mean=%.3f std=%.3f  q01=%.2f q99=%.2f",
             df["drr_z"].mean(), df["drr_z"].std(),
             df["drr_z"].quantile(0.01), df["drr_z"].quantile(0.99))
    log.info("Extremes: z<=-2 = %d bars, z>=+2 = %d bars",
             int((df["drr_z"] <= -Z_EXTREME).sum()),
             int((df["drr_z"] >= Z_EXTREME).sum()))

    # Quintile
    q = quintile_stats(df); q.to_csv(OUT_DIR / "quintile_stats.csv", index=False)
    # Thresholds
    thr = threshold_stats(df); thr.to_csv(OUT_DIR / "threshold_stats.csv", index=False)
    # Time to reversion
    rev_low = time_to_reversion(df, -Z_EXTREME, "below")
    rev_high = time_to_reversion(df, Z_EXTREME, "above")
    rev_low.to_csv(OUT_DIR / "reversion_from_z_low.csv", index=False)
    rev_high.to_csv(OUT_DIR / "reversion_from_z_high.csv", index=False)
    # Intraday
    intra = intraday_cluster(df); intra.to_csv(OUT_DIR / "intraday_cluster.csv", index=False)

    # Backtest
    trades = backtest(df)
    trades.to_csv(OUT_DIR / "trades.csv", index=False)

    if not trades.empty:
        st_all = bt_stats(trades); log.info("\n=== BACKTEST ALL ===\n%s", st_all.to_string(index=False))
        st_all.to_csv(OUT_DIR / "bt_stats_all.csv", index=False)

        st_side = bt_stats(trades, "side");       st_side.to_csv(OUT_DIR / "bt_stats_by_side.csv", index=False)
        st_trig = bt_stats(trades, "trigger");    st_trig.to_csv(OUT_DIR / "bt_stats_by_trigger.csv", index=False)
        st_rsn  = bt_stats(trades, "reason");     st_rsn.to_csv(OUT_DIR / "bt_stats_by_reason.csv", index=False)
        log.info("\n=== BY SIDE ===\n%s", st_side.to_string(index=False))
        log.info("\n=== BY TRIGGER ===\n%s", st_trig.to_string(index=False))
        log.info("\n=== BY EXIT REASON ===\n%s", st_rsn.to_string(index=False))

        # Regime combo
        regime_map = df[["ts_bar", "regime_a1"]].dropna()
        trades_r = trades.merge(regime_map, left_on="entry_ts", right_on="ts_bar", how="left")
        if trades_r["regime_a1"].notna().any():
            st_reg = bt_stats(trades_r.dropna(subset=["regime_a1"]), "regime_a1")
            log.info("\n=== BY A1 REGIME ===\n%s", st_reg.to_string(index=False))
            st_reg.to_csv(OUT_DIR / "bt_stats_by_regime.csv", index=False)

    # Plots
    plot_z_distribution(df, OUT_DIR / "01_z_distribution.png")
    plot_scatter_z_vs_fwd(df, OUT_DIR / "02_scatter_z_vs_fwd60m.png")
    plot_quintile_returns(q, OUT_DIR / "03_quintile_returns.png")
    plot_threshold_returns(thr, OUT_DIR / "04_threshold_returns.png")
    plot_reversion_hist(rev_low, rev_high, OUT_DIR / "05_reversion_hist.png")
    if not trades.empty:
        plot_pnl({
            "ALL": trades,
            "LONG z<=-2": trades[trades["trigger"] == "z_low"],
            "SHORT z>=+2": trades[trades["trigger"] == "z_high"],
        }, OUT_DIR / "06_pnl_curves.png")

    df[["ts_bar","drr_z","delta_rr","s_close","regime_a1"]] \
        .sample(min(5000, len(df)), random_state=SEED) \
        .to_csv(OUT_DIR / "features_sample.csv", index=False)

    log.info("\nStudy D1 completed in %.1fs. Output: %s",
             time.time() - t_start, OUT_DIR)


if __name__ == "__main__":
    main()
