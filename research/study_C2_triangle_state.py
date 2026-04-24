"""
STUDY C2 - ZG + Walls Triangle State Machine
=============================================

HYPOTHESIS
----------
The triplet (call_wall, zero_gamma, put_wall) defines a dealer-hedging
force field. Price is magnetically attracted to zero_gamma (pinning
point). The 4 zones above/between/below the walls have distinct
statistical behaviour:

    Z1 = ABOVE_CW    spot > call_wall            (unstable / squeeze)
    Z2 = CW_ZG       zero_gamma < spot <= call_wall   (mean-revert DOWN to ZG)
    Z3 = ZG_PW       put_wall   < spot <= zero_gamma  (mean-revert UP to ZG)
    Z4 = BELOW_PW    spot <= put_wall            (unstable / squeeze)

METHOD
------
1. 5-min OHLC bars (hub='classic', aggregation='gex_zero'), RTH, ES_SPX
2. Classify each bar into Z1..Z4
3. Transition matrix Z_t -> Z_{t+1}
4. Forward returns at 5/15/30/60 min per zone
5. Magnitude regression:  |spot - ZG|  -> |fwd_return_30m|
6. Direction test:         sign(ZG - spot)  vs  sign(fwd_return_30m)
7. Magnetic pull: P( |spot_s - ZG_s| <= 5pt within 60min | zone_t )
8. Backtest FADE-TOWARD-ZG in Z2,Z3: entry side=sign(ZG-spot), TP=12/SL=8,
   hold up to 60min, exit at ZG touch, or EOD
9. Optional: join with A1 regime classifier to split by regime
10. Plots: zone histogram, transition heatmap, fwd-return by zone,
    magnetic-pull bar, P&L curves, scatter distance vs magnitude.
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
OUT_DIR = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\C2")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:25"
RTH_END_UTC = "19:55"
BARS_PER_RTH = 78

TP_PTS = 12.0
SL_PTS = 8.0
MAGNET_PROX_PTS = 5.0      # "magnetic hit" = spot within ±5pt of ZG
MAGNET_HORIZON_BARS = 12   # 60 min
MAX_HOLD_BARS = 12         # backtest max hold 60 min
SEED = 42

# Zone labels
Z1 = "ABOVE_CW"; Z2 = "CW_ZG"; Z3 = "ZG_PW"; Z4 = "BELOW_PW"
ZONE_ORDER = [Z1, Z2, Z3, Z4]

# A1 results path (optional regime join)
A1_RESULTS = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\A1\features_sample.csv")

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
log = logging.getLogger("C2")


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
        AVG(zero_gamma)   AS zero_gamma,
        AVG(call_wall_oi) AS call_wall,
        AVG(put_wall_oi)  AS put_wall,
        AVG(net_gex_oi)   AS net_gex_oi,
        COUNT(*)          AS n_ticks
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1
    ORDER BY 1
    """
    t0 = time.time()
    df = con.execute(q).df()
    log.info("Loaded %s bars in %.1fs", f"{len(df):,}", time.time() - t0)
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    return df


def filter_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)

    # Require valid triangle
    valid = (df["zero_gamma"].notna() & df["call_wall"].notna() & df["put_wall"].notna()
             & (df["call_wall"] > df["put_wall"])
             & (df["zero_gamma"] > 0))
    before = len(df)
    df = df[valid].copy().reset_index(drop=True)
    log.info("After RTH/weekday/valid-triangle: %s bars (dropped %d)  (%s -> %s)",
             f"{len(df):,}", before - len(df), df["ts_bar"].min(), df["ts_bar"].max())

    df["date"] = df["ts_bar"].dt.date
    df["time_str"] = df["ts_bar"].dt.strftime("%H:%M")
    df["minute_of_day"] = df["ts_bar"].dt.hour * 60 + df["ts_bar"].dt.minute
    return df


def classify_zones(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    s = df["s_close"]; cw = df["call_wall"]; zg = df["zero_gamma"]; pw = df["put_wall"]
    conds = [s > cw, (s <= cw) & (s > zg), (s <= zg) & (s > pw), s <= pw]
    df["zone"] = np.select(conds, ZONE_ORDER, default=None)
    df["dist_to_zg"] = zg - s  # positive means ZG above spot (magnet pulls UP)
    df["abs_dist_to_zg"] = df["dist_to_zg"].abs()
    df["dir_to_zg"] = np.sign(df["dist_to_zg"]).astype(int)
    # normalized width of triangle
    df["triangle_width"] = cw - pw
    df["pos_in_triangle"] = (s - pw) / df["triangle_width"].replace(0, np.nan)
    return df


def label_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    horizons = {"5m": 1, "15m": 3, "30m": 6, "60m": 12}
    for name, k in horizons.items():
        fwd_close = df["s_close"].shift(-k)
        same_day = df["date"].shift(-k) == df["date"]
        fwd = (fwd_close - df["s_close"]).where(same_day, np.nan)
        df[f"ret_fwd_{name}"] = fwd
    # Forward high/low within 60m for magnetic-hit test
    hi_rolls, lo_rolls = [], []
    for k in range(1, MAGNET_HORIZON_BARS + 1):
        hi_rolls.append(df["s_high"].shift(-k))
        lo_rolls.append(df["s_low"].shift(-k))
    same_day_60 = df["date"].shift(-MAGNET_HORIZON_BARS) == df["date"]
    df["hi_60m"] = pd.concat(hi_rolls, axis=1).max(axis=1).where(same_day_60, np.nan)
    df["lo_60m"] = pd.concat(lo_rolls, axis=1).min(axis=1).where(same_day_60, np.nan)
    # magnetic hit: did ZG get bracketed within prox within 60min?
    within_prox = ((df["lo_60m"] <= df["zero_gamma"] + MAGNET_PROX_PTS) &
                   (df["hi_60m"] >= df["zero_gamma"] - MAGNET_PROX_PTS))
    df["magnet_hit_60m"] = within_prox.astype(float).where(same_day_60, np.nan)
    # actual touch (spot crosses ZG)
    touched = ((df["lo_60m"] <= df["zero_gamma"]) & (df["hi_60m"] >= df["zero_gamma"]))
    df["zg_touch_60m"] = touched.astype(float).where(same_day_60, np.nan)
    return df


# ============================================================
# ZONE DISTRIBUTION + TRANSITION
# ============================================================
def zone_distribution(df: pd.DataFrame) -> pd.DataFrame:
    dist = df["zone"].value_counts().reindex(ZONE_ORDER).reset_index()
    dist.columns = ["zone", "n"]
    dist["pct"] = dist["n"] / dist["n"].sum()
    log.info("\n=== ZONE DISTRIBUTION ===\n%s", dist.to_string(index=False))
    return dist


def transition_matrix(df: pd.DataFrame) -> pd.DataFrame:
    z0 = df["zone"]
    z1 = df["zone"].shift(-1)
    # Only within same day
    same_day = df["date"].shift(-1) == df["date"]
    mask = z0.notna() & z1.notna() & same_day
    ct = pd.crosstab(z0[mask], z1[mask], normalize="index")
    ct = ct.reindex(index=ZONE_ORDER, columns=ZONE_ORDER, fill_value=0.0)
    log.info("\n=== TRANSITION MATRIX (next 5-min bar) ===\n%s",
             ct.round(4).to_string())
    return ct


def zone_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for zone in ZONE_ORDER:
        d = df[df["zone"] == zone]
        for h in ["5m", "15m", "30m", "60m"]:
            r = d[f"ret_fwd_{h}"].dropna()
            if len(r) == 0:
                continue
            rows.append({
                "zone": zone, "horizon": h, "n": len(r),
                "mean_pts": r.mean(), "std_pts": r.std(ddof=0),
                "median_pts": r.median(),
                "pct_positive": (r > 0).mean(),
                "sharpe_ann_252": r.mean() / r.std(ddof=0) * np.sqrt(BARS_PER_RTH * 252)
                                  if r.std(ddof=0) > 0 else 0.0,
                "q25": r.quantile(0.25), "q75": r.quantile(0.75),
            })
    out = pd.DataFrame(rows)
    log.info("\n=== FORWARD RETURNS BY ZONE ===\n%s",
             out.round(4).to_string(index=False))
    return out


# ============================================================
# MAGNITUDE + DIRECTION TESTS
# ============================================================
def magnitude_regression(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h in ["5m", "15m", "30m", "60m"]:
        x = df["abs_dist_to_zg"].values
        y = df[f"ret_fwd_{h}"].abs().values
        m = np.isfinite(x) & np.isfinite(y)
        x, y = x[m], y[m]
        if len(x) < 100:
            continue
        slope, intercept, r, p, se = stats.linregress(x, y)
        rows.append({"horizon": h, "n": len(x), "r": r, "r_squared": r**2,
                     "slope": slope, "intercept": intercept, "p": p})
    out = pd.DataFrame(rows)
    log.info("\n=== MAGNITUDE REGRESSION: |spot-ZG| vs |fwd return| ===\n%s",
             out.round(6).to_string(index=False))
    return out


def direction_test(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h in ["5m", "15m", "30m", "60m"]:
        x = df["dir_to_zg"].values
        y = np.sign(df[f"ret_fwd_{h}"].values)
        m = np.isfinite(df[f"ret_fwd_{h}"].values) & (x != 0) & (y != 0)
        xs, ys = x[m], y[m]
        if len(xs) < 100:
            continue
        agree = (xs == ys).mean()
        # binomial vs 50%
        k = int((xs == ys).sum())
        p_binom = stats.binomtest(k, len(xs), p=0.5, alternative="greater").pvalue
        rows.append({"horizon": h, "n": len(xs), "sign_agreement": agree,
                     "binomial_p": p_binom})
    out = pd.DataFrame(rows)
    log.info("\n=== DIRECTION TEST: sign(ZG-spot) predicts sign(fwd) ===\n%s",
             out.round(6).to_string(index=False))
    return out


def magnetic_pull_stats(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for zone in ZONE_ORDER:
        d = df[df["zone"] == zone]
        hit = d["zg_touch_60m"].dropna()
        prox = d["magnet_hit_60m"].dropna()
        rows.append({
            "zone": zone, "n": len(hit),
            "p_zg_touch_60m": hit.mean() if len(hit) else np.nan,
            "p_within_5pt_60m": prox.mean() if len(prox) else np.nan,
            "mean_dist_to_zg": d["abs_dist_to_zg"].mean(),
            "median_dist_to_zg": d["abs_dist_to_zg"].median(),
        })
    # overall
    hit_all = df["zg_touch_60m"].dropna()
    prox_all = df["magnet_hit_60m"].dropna()
    rows.append({"zone": "ALL", "n": len(hit_all),
                 "p_zg_touch_60m": hit_all.mean() if len(hit_all) else np.nan,
                 "p_within_5pt_60m": prox_all.mean() if len(prox_all) else np.nan,
                 "mean_dist_to_zg": df["abs_dist_to_zg"].mean(),
                 "median_dist_to_zg": df["abs_dist_to_zg"].median()})
    out = pd.DataFrame(rows)
    log.info("\n=== MAGNETIC PULL (60min horizon) ===\n%s",
             out.round(4).to_string(index=False))
    return out


# ============================================================
# BACKTEST — FADE toward ZG in middle zones
# ============================================================
def _simulate_trade(day_bars: pd.DataFrame, start_idx: int, side: int,
                    entry_px: float, zg: float) -> dict:
    end_idx = min(start_idx + MAX_HOLD_BARS, len(day_bars) - 1)
    for j in range(start_idx + 1, end_idx + 1):
        row = day_bars.iloc[j]
        hi, lo = row["s_high"], row["s_low"]
        # TP / SL check
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
        # ZG touch = take profit at ZG
        if (side == 1 and hi >= zg) or (side == -1 and lo <= zg):
            pnl = (zg - entry_px) if side == 1 else (entry_px - zg)
            return {"pnl": float(pnl), "reason": "ZG_TOUCH",
                    "bars": j - start_idx, "exit_ts": row["ts_bar"]}
    # Time exit
    last = day_bars.iloc[end_idx]
    pnl = (last["s_close"] - entry_px) if side == 1 else (entry_px - last["s_close"])
    return {"pnl": float(pnl), "reason": "TIME",
            "bars": end_idx - start_idx, "exit_ts": last["ts_bar"]}


def backtest_fade_to_zg(df: pd.DataFrame, cooldown_bars: int = 3) -> pd.DataFrame:
    trades = []
    for d, day_bars in df.groupby("date"):
        day_bars = day_bars.sort_values("ts_bar").reset_index(drop=True)
        last_exit = -10**9
        for i in range(len(day_bars) - 1):
            if i <= last_exit + cooldown_bars:
                continue
            row = day_bars.iloc[i]
            if row["zone"] not in (Z2, Z3):
                continue
            if not np.isfinite(row["dist_to_zg"]) or row["dist_to_zg"] == 0:
                continue
            side = int(row["dir_to_zg"])  # +1 means ZG above spot -> LONG
            entry_px = row["s_close"]
            zg = row["zero_gamma"]
            # Require enough room
            if abs(entry_px - zg) < 1.0:
                continue
            res = _simulate_trade(day_bars, i, side, entry_px, zg)
            trades.append({
                "date": d,
                "entry_ts": row["ts_bar"],
                "entry_px": entry_px,
                "zg_at_entry": zg,
                "zone": row["zone"],
                "side": "LONG" if side == 1 else "SHORT",
                "side_int": side,
                "dist_to_zg": row["dist_to_zg"],
                "call_wall": row["call_wall"],
                "put_wall": row["put_wall"],
                "net_gex_oi": row["net_gex_oi"],
                "pnl_pts": res["pnl"],
                "reason": res["reason"],
                "bars_held": res["bars"],
                "exit_ts": res["exit_ts"],
            })
            last_exit = i + res["bars"]
    out = pd.DataFrame(trades)
    log.info("Backtest FADE-TO-ZG: %d trades across %d days",
             len(out), out["date"].nunique() if not out.empty else 0)
    return out


def backtest_stats(t: pd.DataFrame, group_col=None) -> pd.DataFrame:
    if t.empty:
        return pd.DataFrame()
    def _s(g):
        n = len(g); pnl = g["pnl_pts"]
        wr = (pnl > 0).mean()
        ev = pnl.mean()
        sd = pnl.std(ddof=0) if n > 1 else 0.0
        sharpe = ev / sd if sd > 0 else 0.0
        cum = pnl.cumsum().values
        run_max = np.maximum.accumulate(cum) if n > 0 else [0]
        dd = (cum - run_max).min() if n > 0 else 0.0
        has_reason = "reason" in g.columns
        return pd.Series({
            "n": n, "wr": wr, "ev_pts": ev, "std_pts": sd,
            "sharpe_trade": sharpe, "total_pnl": pnl.sum(), "max_dd": dd,
            "tp_rate": (g["reason"] == "TP").mean() if has_reason else np.nan,
            "sl_rate": (g["reason"] == "SL").mean() if has_reason else np.nan,
            "zg_rate": (g["reason"] == "ZG_TOUCH").mean() if has_reason else np.nan,
            "time_rate": (g["reason"] == "TIME").mean() if has_reason else np.nan,
            "avg_bars": g["bars_held"].mean(),
        })
    if group_col is None:
        return pd.DataFrame([_s(t)])
    return t.groupby(group_col, observed=True).apply(_s).reset_index()


# ============================================================
# PLOTS
# ============================================================
def plot_zone_dist(dist: pd.DataFrame, path: Path):
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.barplot(data=dist, x="zone", y="n", order=ZONE_ORDER, ax=ax,
                palette="viridis")
    for i, row in dist.reset_index(drop=True).iterrows():
        ax.text(i, row["n"], f"{row['pct']:.1%}", ha="center", va="bottom")
    ax.set_title("Bar distribution by triangle zone (ES_SPX 5-min RTH)")
    ax.set_ylabel("N bars")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_transition(mat: pd.DataFrame, path: Path):
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(mat, annot=True, fmt=".3f", cmap="RdBu_r", center=0.25,
                vmin=0, vmax=1, ax=ax)
    ax.set_title("Transition matrix — P(zone_{t+1} | zone_t)")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_fwd_return_by_zone(stats_df: pd.DataFrame, path: Path):
    piv_mean = stats_df.pivot(index="zone", columns="horizon", values="mean_pts") \
                       .reindex(index=ZONE_ORDER, columns=["5m","15m","30m","60m"])
    piv_pp = stats_df.pivot(index="zone", columns="horizon", values="pct_positive") \
                     .reindex(index=ZONE_ORDER, columns=["5m","15m","30m","60m"])
    fig, axes = plt.subplots(1, 2, figsize=(15, 5))
    sns.heatmap(piv_mean, annot=True, fmt=".2f", cmap="RdBu_r", center=0, ax=axes[0])
    axes[0].set_title("Mean fwd return (pts) by zone × horizon")
    sns.heatmap(piv_pp, annot=True, fmt=".3f", cmap="RdBu_r", center=0.5,
                vmin=0.4, vmax=0.6, ax=axes[1])
    axes[1].set_title("% positive fwd return")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_magnet_pull(mag: pd.DataFrame, path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    m = mag[mag["zone"] != "ALL"].copy()
    axes[0].bar(m["zone"], m["p_zg_touch_60m"], color="teal")
    for x, v in zip(m["zone"], m["p_zg_touch_60m"]):
        axes[0].text(x, v, f"{v:.2f}", ha="center", va="bottom")
    axes[0].set_title("P(spot touches ZG within 60min) by zone")
    axes[0].set_ylabel("Probability"); axes[0].set_ylim(0, 1)
    axes[1].bar(m["zone"], m["mean_dist_to_zg"], color="orange")
    for x, v in zip(m["zone"], m["mean_dist_to_zg"]):
        axes[1].text(x, v, f"{v:.1f}", ha="center", va="bottom")
    axes[1].set_title("Mean |spot - ZG| (pts) by zone")
    axes[1].set_ylabel("Points")
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_pnl_curves(splits: dict, path: Path):
    fig, ax = plt.subplots(figsize=(13, 6))
    for label, t in splits.items():
        if t is None or t.empty:
            continue
        t = t.sort_values("entry_ts")
        cum = t["pnl_pts"].cumsum()
        ax.plot(t["entry_ts"], cum, lw=2,
                label=f"{label} N={len(t)} total={cum.iloc[-1]:+.1f}pt")
    ax.axhline(0, color="k", lw=0.7)
    ax.set_title("Backtest — fade toward ZG (split by zone / side)")
    ax.set_ylabel("Cumulative P&L (pts)")
    ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_dist_vs_magnitude(df: pd.DataFrame, path: Path):
    d = df.dropna(subset=["abs_dist_to_zg", "ret_fwd_30m"]).copy()
    if len(d) > 50000:
        d = d.sample(50000, random_state=SEED)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(d["abs_dist_to_zg"], d["ret_fwd_30m"].abs(),
               alpha=0.12, s=6, c="navy")
    # binned means
    d["bin"] = pd.cut(d["abs_dist_to_zg"], bins=20)
    agg = d.groupby("bin", observed=True).agg(
        x=("abs_dist_to_zg","mean"), y=("ret_fwd_30m", lambda s: s.abs().mean()))
    ax.plot(agg["x"], agg["y"], "r-", lw=2.5, label="bin mean")
    ax.set_xlabel("|spot - zero_gamma| (pts)")
    ax.set_ylabel("|fwd return 30m| (pts)")
    ax.set_title("Distance to ZG vs forward-return magnitude (30m)")
    ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


# ============================================================
# MAIN
# ============================================================
def main():
    t_start = time.time()
    log.info("=" * 70)
    log.info("STUDY C2 — ZG + Walls Triangle State Machine")
    log.info("=" * 70)

    (OUT_DIR / "params.json").write_text(json.dumps({
        "db_path": DB_PATH, "ticker": TICKER, "bar_min": BAR_MIN,
        "rth": [RTH_START_UTC, RTH_END_UTC],
        "tp_pts": TP_PTS, "sl_pts": SL_PTS,
        "magnet_prox_pts": MAGNET_PROX_PTS,
        "magnet_horizon_bars": MAGNET_HORIZON_BARS,
        "max_hold_bars": MAX_HOLD_BARS, "seed": SEED,
    }, indent=2))

    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df = load_bars(con)
    con.close()

    df = filter_and_enrich(df)
    df = classify_zones(df)
    df = label_forward_returns(df)

    # --- Distributions & transitions ---
    dist = zone_distribution(df); dist.to_csv(OUT_DIR / "zone_distribution.csv", index=False)
    trans = transition_matrix(df); trans.to_csv(OUT_DIR / "transition_matrix.csv")

    # --- Forward returns per zone ---
    fwd_stats = zone_forward_returns(df); fwd_stats.to_csv(OUT_DIR / "fwd_stats_by_zone.csv", index=False)

    # --- Regression / direction / magnet ---
    mag_reg = magnitude_regression(df); mag_reg.to_csv(OUT_DIR / "magnitude_regression.csv", index=False)
    dir_test = direction_test(df); dir_test.to_csv(OUT_DIR / "direction_test.csv", index=False)
    magnet = magnetic_pull_stats(df); magnet.to_csv(OUT_DIR / "magnetic_pull.csv", index=False)

    # --- Backtest ---
    trades = backtest_fade_to_zg(df)
    trades.to_csv(OUT_DIR / "trades.csv", index=False)

    if not trades.empty:
        st_all = backtest_stats(trades)
        st_all["label"] = "ALL"
        log.info("\n=== BACKTEST ALL ===\n%s", st_all.to_string(index=False))

        st_zone = backtest_stats(trades, "zone"); st_zone["group"] = "zone"
        st_side = backtest_stats(trades, "side"); st_side["group"] = "side"
        st_reason = backtest_stats(trades, "reason"); st_reason["group"] = "reason"

        log.info("\n=== BACKTEST BY ZONE ===\n%s", st_zone.to_string(index=False))
        log.info("\n=== BACKTEST BY SIDE ===\n%s", st_side.to_string(index=False))
        log.info("\n=== BACKTEST BY EXIT REASON ===\n%s", st_reason.to_string(index=False))

        st_all.to_csv(OUT_DIR / "bt_stats_all.csv", index=False)
        st_zone.to_csv(OUT_DIR / "bt_stats_by_zone.csv", index=False)
        st_side.to_csv(OUT_DIR / "bt_stats_by_side.csv", index=False)
        st_reason.to_csv(OUT_DIR / "bt_stats_by_reason.csv", index=False)

        # Join A1 regime if available
        if A1_RESULTS.exists():
            try:
                a1 = pd.read_csv(A1_RESULTS, usecols=["ts_bar", "regime"])
                a1["ts_bar"] = pd.to_datetime(a1["ts_bar"], utc=True)
                trades_r = trades.merge(a1, left_on="entry_ts", right_on="ts_bar", how="left")
                log.info("A1 regime matched on %d/%d trades",
                         trades_r["regime"].notna().sum(), len(trades_r))
                if trades_r["regime"].notna().any():
                    st_regime = backtest_stats(trades_r.dropna(subset=["regime"]), "regime")
                    log.info("\n=== BACKTEST BY A1 REGIME ===\n%s", st_regime.to_string(index=False))
                    st_regime.to_csv(OUT_DIR / "bt_stats_by_a1_regime.csv", index=False)
            except Exception as e:
                log.warning("A1 join failed: %s", e)

    # --- Plots ---
    plot_zone_dist(dist, OUT_DIR / "01_zone_distribution.png")
    plot_transition(trans, OUT_DIR / "02_transition_matrix.png")
    plot_fwd_return_by_zone(fwd_stats, OUT_DIR / "03_fwd_return_by_zone.png")
    plot_magnet_pull(magnet, OUT_DIR / "04_magnetic_pull.png")
    plot_dist_vs_magnitude(df, OUT_DIR / "05_distance_vs_magnitude.png")
    if not trades.empty:
        plot_pnl_curves({
            "ALL": trades,
            f"Zone {Z2}": trades[trades["zone"] == Z2],
            f"Zone {Z3}": trades[trades["zone"] == Z3],
            "LONG": trades[trades["side"] == "LONG"],
            "SHORT": trades[trades["side"] == "SHORT"],
        }, OUT_DIR / "06_pnl_curves.png")

    # feature sample
    df.sample(min(5000, len(df)), random_state=SEED) \
      .to_csv(OUT_DIR / "features_sample.csv", index=False)

    log.info("\nStudy C2 completed in %.1fs. Output: %s",
             time.time() - t_start, OUT_DIR)


if __name__ == "__main__":
    main()
