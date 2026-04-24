"""
STUDIO F1 - 9-TICKER CROSS-ASSET REGIME ALIGNMENT
==================================================

Ipotesi:
    L'allineamento GEX cross-asset e' un filtro di conviction.
    Trade ES (long/short) solo quando >=5/9 asset confermano lo stesso
    regime. L'idea: se solo ES e' in amplification ma il resto del
    mercato e' in pinning, il segnale e' rumore locale.

Ticker disponibili nel backup DB:
    ES_SPX, SPY, QQQ, NQ_NDX, VIX, IWM, GLD, TLT, UVXY
    (coverage full da 2025-09-15, IWM da 2026-01-05, GLD/TLT/UVXY da 2026-03-18)

Metodo:
    1. Per ogni ticker, load 5-min bars di net_gex_oi.
    2. z-score rolling 20 giorni (stessa logica A1).
    3. Regime = PINNING (z>+0.5) / AMPLIFICATION (z<-0.5) / TRANSITION.
    4. Pivot per ts_bar: regime di ciascun ticker in colonna.
    5. Per ogni bar: n_pinning, n_amp, n_trans, n_avail.
       alignment = max(n_p, n_a) / n_avail
       majority_regime = argmax.
    6. Forward returns ES (5-min OHLC da historical_trades ES tick)
       @ 30-min horizon, per bucket di alignment e per majority_regime.
    7. Backtest comparativo: stesso setup A1 (breakout/fade base su ES
       net_gex z-score) MA filtrato da alignment >= 5/9.
    8. Lead/lag: per ogni ticker, correlazione cross-regime-ES a lag
       -6..+6 bars (cerca chi cambia PRIMA).
    9. Plot: heatmap regime tempo-ticker, alignment timeline, forward
       return per bucket, P&L filtered vs unfiltered.

Uscita: research/results/F1/
"""
from __future__ import annotations

import json
import os
import sys
import time
import warnings
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy import stats

warnings.filterwarnings("ignore")

# ---------------------------- CONFIG ----------------------------------------
GEX_DB = r"C:\Users\annal\Desktop\ML DATABASE\backups\phase1\ml_gold_20260407_211327.duckdb"
HIST_DB = r"C:\Users\annal\Desktop\P1UNI\data\p1uni_history.duckdb"
OUT_DIR = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\F1")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TICKERS = ["ES_SPX", "SPY", "QQQ", "NQ_NDX", "VIX", "IWM", "GLD", "TLT", "UVXY"]
HUB = "classic"
AGG = "gex_full"
BAR_MIN = 5

# RTH UTC 13:30-20:00 (9:30-16:00 ET approx)
RTH_START_UTC = 13 * 60 + 30
RTH_END_UTC = 20 * 60

# Regime thresholds (same convention as A1)
Z_PINNING = 0.5
Z_AMP = -0.5
ROLL_DAYS = 20

# Alignment threshold
ALIGN_MIN_N = 5        # minimum same-regime tickers for "aligned"

# Backtest parameters
TP_PT = 12.0
SL_PT = 8.0
BT_TIMEOUT_BARS = 6
BT_COOLDOWN_BARS = 3

# ES ticker for trades db
ES_TRADES_TICKER = "ES"

# --------------------- LOG --------------------------------------------------
LOG_LINES: list[str] = []
def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)

def save_log() -> None:
    (OUT_DIR / "run.log").write_text("\n".join(LOG_LINES), encoding="utf-8")


# ---------------------------- STEP 1: Load GEX ------------------------------
def load_all_tickers_gex() -> pd.DataFrame:
    log("Loading gex_summary for 9 tickers...")
    con = duckdb.connect(GEX_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    tickers_list = ", ".join(f"'{t}'" for t in TICKERS)
    df = con.execute(f"""
        SELECT ticker,
               time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
               AVG(net_gex_oi) AS net_gex_oi,
               AVG(spot) AS spot
        FROM gex_summary
        WHERE ticker IN ({tickers_list})
          AND hub='{HUB}'
          AND aggregation='{AGG}'
        GROUP BY ticker, ts_bar
        ORDER BY ticker, ts_bar
    """).df()
    con.close()
    log(f"  rows loaded: {len(df):,}")
    by_t = df.groupby("ticker").agg(n=("ts_bar", "size"), tmin=("ts_bar", "min"), tmax=("ts_bar", "max"))
    log(f"  coverage by ticker:\n{by_t}")
    return df


# ---------------------------- STEP 2: Regime --------------------------------
def classify_regime(df: pd.DataFrame) -> pd.DataFrame:
    """Per-ticker z-score over ROLL_DAYS daily-mean of net_gex_oi, then
    map to PINNING/AMPLIFICATION/TRANSITION.
    z broadcast to every 5-min bar of that day."""
    df = df.copy()
    df["date"] = df["ts_bar"].dt.date

    # Daily mean per ticker
    daily = df.groupby(["ticker", "date"])["net_gex_oi"].mean().reset_index(name="daily_gex")
    daily = daily.sort_values(["ticker", "date"])
    daily["roll_mu"] = daily.groupby("ticker")["daily_gex"].transform(
        lambda s: s.rolling(ROLL_DAYS, min_periods=5).mean()
    )
    daily["roll_sd"] = daily.groupby("ticker")["daily_gex"].transform(
        lambda s: s.rolling(ROLL_DAYS, min_periods=5).std()
    )
    daily["z"] = (daily["daily_gex"] - daily["roll_mu"]) / daily["roll_sd"]

    # Merge back
    df = df.merge(daily[["ticker", "date", "z"]], on=["ticker", "date"], how="left")
    df["regime"] = np.select(
        [df["z"] > Z_PINNING, df["z"] < Z_AMP],
        ["PINNING", "AMPLIFICATION"],
        default="TRANSITION",
    )
    # Mask where z is nan: keep as NA
    df.loc[df["z"].isna(), "regime"] = None
    return df


# ---------------------------- STEP 3: Pivot + alignment ---------------------
def build_alignment(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot regime per ticker per bar, compute alignment metrics."""
    log("Pivoting regimes per ticker per bar...")
    piv = df.pivot_table(index="ts_bar", columns="ticker", values="regime",
                          aggfunc="first")
    # Ensure all tickers present
    for t in TICKERS:
        if t not in piv.columns:
            piv[t] = None
    piv = piv[TICKERS]

    # Count regimes per bar
    def _counts(row):
        vals = row.values
        n_p = sum(v == "PINNING" for v in vals)
        n_a = sum(v == "AMPLIFICATION" for v in vals)
        n_t = sum(v == "TRANSITION" for v in vals)
        n_avail = sum(v is not None and v == v for v in vals)
        return n_p, n_a, n_t, n_avail
    counts = np.array([_counts(row) for _, row in piv.iterrows()])
    piv["n_pinning"] = counts[:, 0]
    piv["n_amp"] = counts[:, 1]
    piv["n_trans"] = counts[:, 2]
    piv["n_avail"] = counts[:, 3]
    piv["align_max"] = np.maximum(piv["n_pinning"], piv["n_amp"])
    piv["align_score"] = np.where(
        piv["n_avail"] > 0,
        piv["align_max"] / piv["n_avail"],
        np.nan,
    )
    piv["majority_regime"] = np.where(
        piv["n_pinning"] > piv["n_amp"], "PINNING",
        np.where(piv["n_amp"] > piv["n_pinning"], "AMPLIFICATION", "TRANSITION")
    )
    piv["aligned_5_9"] = piv["align_max"] >= ALIGN_MIN_N
    piv = piv.reset_index()
    log(f"  alignment table: {len(piv):,} bars, mean n_avail={piv['n_avail'].mean():.2f}")
    return piv


# ---------------------------- STEP 4: Load ES price bars --------------------
def load_es_price_bars(ts_min, ts_max) -> pd.DataFrame:
    log("Loading ES OHLC from historical_trades...")
    con = duckdb.connect(HIST_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df = con.execute(f"""
        SELECT time_bucket(INTERVAL '{BAR_MIN} minutes', ts_event) AS ts_bar,
               first(price ORDER BY ts_event) AS open,
               MAX(price) AS high,
               MIN(price) AS low,
               last(price ORDER BY ts_event)  AS close,
               SUM(size) AS vol
        FROM historical_trades
        WHERE ticker='{ES_TRADES_TICKER}'
          AND ts_event >= TIMESTAMP '{ts_min}'
          AND ts_event <  TIMESTAMP '{ts_max}'
        GROUP BY ts_bar
        ORDER BY ts_bar
    """).df()
    con.close()
    log(f"  ES price bars: {len(df):,}")
    return df


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    mins = df["ts_bar"].dt.hour * 60 + df["ts_bar"].dt.minute
    return df[(mins >= RTH_START_UTC) & (mins < RTH_END_UTC)].reset_index(drop=True)


# ---------------------------- STEP 5: Forward returns -----------------------
def add_forward_returns(align: pd.DataFrame, es_px: pd.DataFrame,
                         horizons=(6, 12, 24)) -> pd.DataFrame:
    """Merge ES close, compute forward returns at given bar horizons."""
    m = align.merge(es_px[["ts_bar", "open", "high", "low", "close"]], on="ts_bar", how="inner")
    m = m.sort_values("ts_bar").reset_index(drop=True)
    for h in horizons:
        m[f"fwd_ret_{h*BAR_MIN}m"] = m["close"].shift(-h) - m["close"]
    return m


def fwd_stats_by_bucket(m: pd.DataFrame, col: str, h_min: int = 30) -> pd.DataFrame:
    c = f"fwd_ret_{h_min}m"
    if c not in m.columns:
        return pd.DataFrame()
    g = m.dropna(subset=[c, col]).groupby(col).agg(
        n=(c, "size"),
        mean=(c, "mean"),
        median=(c, "median"),
        std=(c, "std"),
        wr_pos=(c, lambda s: (s > 0).mean()),
    )
    # t-test vs 0
    t_vals, p_vals = [], []
    for k, sub in m.dropna(subset=[c, col]).groupby(col):
        t, p = stats.ttest_1samp(sub[c].values, 0)
        t_vals.append(t); p_vals.append(p)
    g["t_stat"] = t_vals
    g["p_value"] = p_vals
    return g.round(4)


# ---------------------------- STEP 6: Backtest ------------------------------
def bt_a1_signals(df: pd.DataFrame, filter_aligned: bool) -> pd.DataFrame:
    """Replicate A1 base signals (breakout CW / breakdown PW / fade-to-ZG)
    using ES z-score of net_gex_oi. Optionally filter by alignment >= 5/9."""
    d = df.copy()
    d["es_z"] = pd.Series(stats.zscore(d["ES_SPX"].map(
        {"PINNING": 1.0, "TRANSITION": 0.0, "AMPLIFICATION": -1.0}).astype(float).values, nan_policy="omit"))
    # Compute ES regime from the ES_SPX column (already classified)
    # We'll synthesize: in AMPLIFICATION, if price breaks out -> LONG (breakout_call_wall from A1).
    # In PINNING, if price is far from ZG, fade back toward ZG.
    # We approximate price proximity by raw close; we don't have CW/ZG in this merged df.
    # Simplification: use ES_SPX regime directly as the signal:
    #   AMPLIFICATION -> trend-follow LONG on 5-bar momentum
    #   PINNING -> fade 5-bar momentum
    d = d.sort_values("ts_bar").reset_index(drop=True)
    d["mom5"] = d["close"] - d["close"].shift(5)
    trades = []
    i_last_exit = -10**9
    for i, r in d.iterrows():
        if i < 5:
            continue
        if i < i_last_exit + BT_COOLDOWN_BARS:
            continue
        if pd.isna(r.get("mom5")):
            continue
        if filter_aligned and (not r.get("aligned_5_9")):
            continue
        es_reg = r.get("ES_SPX")
        mom = r["mom5"]
        side = 0
        reason_entry = None
        if es_reg == "AMPLIFICATION" and abs(mom) >= 3.0:
            side = 1 if mom > 0 else -1
            reason_entry = "amp_momentum"
        elif es_reg == "PINNING" and abs(mom) >= 6.0:
            side = -1 if mom > 0 else 1
            reason_entry = "pin_fade"
        if side == 0:
            continue
        i_entry = i + 1
        if i_entry >= len(d):
            continue
        entry_px = d.loc[i_entry, "open"]
        if pd.isna(entry_px):
            continue
        tp = entry_px + side * TP_PT
        sl = entry_px - side * SL_PT
        exit_px = None; reason = None; exit_bar = None
        for j in range(i_entry, min(len(d), i_entry + BT_TIMEOUT_BARS)):
            hh, ll = d.loc[j, "high"], d.loc[j, "low"]
            if side == 1:
                if ll <= sl:
                    exit_px, reason, exit_bar = sl, "SL", j; break
                if hh >= tp:
                    exit_px, reason, exit_bar = tp, "TP", j; break
            else:
                if hh >= sl:
                    exit_px, reason, exit_bar = sl, "SL", j; break
                if ll <= tp:
                    exit_px, reason, exit_bar = tp, "TP", j; break
        if exit_px is None:
            exit_bar = min(len(d) - 1, i_entry + BT_TIMEOUT_BARS - 1)
            exit_px = d.loc[exit_bar, "close"]; reason = "TIME"
        pnl = side * (exit_px - entry_px)
        i_last_exit = exit_bar
        trades.append({
            "ts_bar": r["ts_bar"], "side": side,
            "entry_px": entry_px, "exit_px": exit_px,
            "reason": reason, "reason_entry": reason_entry,
            "pnl": float(pnl),
            "es_regime": es_reg, "align_score": r["align_score"],
            "majority_regime": r["majority_regime"],
        })
    return pd.DataFrame(trades)


def summarize_trades(tr: pd.DataFrame, label: str) -> dict:
    if tr.empty:
        return {"label": label, "n": 0}
    r = tr["pnl"]
    return {
        "label": label,
        "n": int(len(tr)),
        "total_pt": float(r.sum()),
        "mean_pt": float(r.mean()),
        "win_rate": float((r > 0).mean()),
        "sharpe_per_trade": float(r.mean() / (r.std() + 1e-9)),
        "tp_rate": float((tr["reason"] == "TP").mean()),
        "sl_rate": float((tr["reason"] == "SL").mean()),
        "time_rate": float((tr["reason"] == "TIME").mean()),
    }


# ---------------------------- STEP 7: Lead/Lag ------------------------------
def cross_regime_leadlag(df_regime: pd.DataFrame, max_lag: int = 6) -> pd.DataFrame:
    """For each non-ES ticker, compute Pearson corr of regime numeric code
    with ES_SPX regime code at lags -max_lag..+max_lag bars."""
    log("Computing lead/lag correlations vs ES...")
    piv = df_regime.pivot_table(index="ts_bar", columns="ticker", values="regime",
                                 aggfunc="first")
    # Numeric code: PIN=+1, TRANS=0, AMP=-1
    cmap = {"PINNING": 1, "TRANSITION": 0, "AMPLIFICATION": -1}
    num = piv.apply(lambda col: col.map(lambda v: cmap.get(v, np.nan)))
    es = num["ES_SPX"]
    out = []
    for t in TICKERS:
        if t == "ES_SPX":
            continue
        col = num.get(t)
        if col is None:
            continue
        best_lag, best_corr = 0, 0.0
        row = {"ticker": t}
        for lag in range(-max_lag, max_lag + 1):
            # shift(+L) means col(t+L) aligned with es(t), so if L>0, col leads es by L bars
            shifted = col.shift(lag)
            mask = shifted.notna() & es.notna()
            if mask.sum() < 500:
                row[f"lag_{lag:+d}"] = np.nan
                continue
            corr = shifted[mask].corr(es[mask])
            row[f"lag_{lag:+d}"] = corr
            if abs(corr) > abs(best_corr):
                best_corr = corr
                best_lag = lag
        row["best_lag"] = best_lag
        row["best_corr"] = best_corr
        out.append(row)
    return pd.DataFrame(out)


# ---------------------------- STEP 8: Plots ---------------------------------
def plot_regime_heatmap(df_regime: pd.DataFrame):
    log("Plotting regime heatmap...")
    piv = df_regime.pivot_table(index="ts_bar", columns="ticker", values="regime",
                                 aggfunc="first")
    for t in TICKERS:
        if t not in piv.columns:
            piv[t] = None
    piv = piv[TICKERS]
    # Downsample to 1/day
    piv_d = piv.resample("1D").first()
    cmap = {"PINNING": 1, "TRANSITION": 0, "AMPLIFICATION": -1}
    mat = piv_d.apply(lambda col: col.map(lambda v: cmap.get(v, np.nan)))
    fig, ax = plt.subplots(figsize=(14, 5))
    im = ax.imshow(mat.T.values, aspect="auto", cmap="RdYlGn",
                   vmin=-1, vmax=1, interpolation="nearest")
    ax.set_yticks(range(len(TICKERS))); ax.set_yticklabels(TICKERS)
    nx = mat.shape[0]
    step = max(1, nx // 10)
    ax.set_xticks(range(0, nx, step))
    ax.set_xticklabels([str(d.date()) for d in mat.index[::step]], rotation=45, ha="right")
    cbar = plt.colorbar(im, ax=ax, ticks=[-1, 0, 1])
    cbar.ax.set_yticklabels(["AMP", "TRANS", "PIN"])
    ax.set_title("Regime per ticker (daily snapshot)")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "regime_heatmap.png", dpi=120)
    plt.close(fig)


def plot_alignment_timeline(al: pd.DataFrame):
    fig, axes = plt.subplots(2, 1, figsize=(13, 6), sharex=True)
    ax = axes[0]
    ax.plot(al["ts_bar"], al["align_score"], lw=0.4, color="C0")
    ax.axhline(5/9, color="r", lw=0.6, ls="--", label="5/9 threshold")
    ax.set_ylabel("alignment score")
    ax.legend(loc="upper right")
    ax.set_title("Cross-asset GEX regime alignment over time")
    ax2 = axes[1]
    ax2.fill_between(al["ts_bar"], al["n_pinning"], color="green", alpha=0.5, label="PINNING")
    ax2.fill_between(al["ts_bar"], -al["n_amp"], color="red", alpha=0.5, label="AMPLIFICATION")
    ax2.set_ylabel("# tickers")
    ax2.axhline(0, color="k", lw=0.4)
    ax2.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "alignment_timeline.png", dpi=120)
    plt.close(fig)


def plot_fwd_return_by_bucket(m: pd.DataFrame, h_min: int = 30):
    c = f"fwd_ret_{h_min}m"
    if c not in m.columns:
        return
    mm = m.dropna(subset=[c]).copy()
    # Bucket by align_max (0..9)
    g = mm.groupby("align_max").agg(n=(c, "size"), mean=(c, "mean"), sd=(c, "std"))
    g["se"] = g["sd"] / np.sqrt(g["n"])
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(g.index, g["mean"], yerr=g["se"], color="C0", alpha=0.7)
    ax.axhline(0, color="k", lw=0.6)
    for i, (x, y, n) in enumerate(zip(g.index, g["mean"], g["n"])):
        ax.annotate(f"n={n}", (x, y), ha="center", fontsize=7,
                     xytext=(0, 4 if y >= 0 else -10), textcoords="offset points")
    ax.set_xlabel("align_max (# tickers in dominant regime)")
    ax.set_ylabel(f"mean fwd return {h_min}min (pt)")
    ax.set_title("ES forward return vs cross-asset alignment")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fwd_return_by_alignment.png", dpi=120)
    plt.close(fig)


def plot_fwd_return_by_majority(m: pd.DataFrame, h_min: int = 30):
    c = f"fwd_ret_{h_min}m"
    mm = m.dropna(subset=[c]).copy()
    piv = mm.pivot_table(index="aligned_5_9", columns="majority_regime",
                          values=c, aggfunc="mean")
    counts = mm.pivot_table(index="aligned_5_9", columns="majority_regime",
                             values=c, aggfunc="size")
    fig, ax = plt.subplots(figsize=(9, 5))
    piv.plot(kind="bar", ax=ax)
    ax.axhline(0, color="k", lw=0.6)
    ax.set_ylabel(f"mean fwd return {h_min}min (pt)")
    ax.set_title("ES forward return by majority regime x alignment")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fwd_return_by_majority.png", dpi=120)
    plt.close(fig)
    return piv, counts


def plot_pnl_cum(trades: dict[str, pd.DataFrame]):
    fig, ax = plt.subplots(figsize=(10, 5))
    for name, tr in trades.items():
        if tr.empty: continue
        c = tr.sort_values("ts_bar")["pnl"].cumsum().values
        ax.plot(c, label=f"{name} (n={len(tr)}, tot={tr['pnl'].sum():.1f})")
    ax.axhline(0, color="k", lw=0.5)
    ax.set_xlabel("trade #"); ax.set_ylabel("cum pnl (pt)")
    ax.set_title(f"ES momentum-regime backtest | TP={TP_PT}/SL={SL_PT}")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "pnl_cumulative.png", dpi=120)
    plt.close(fig)


# ---------------------------- MAIN ------------------------------------------
def main():
    t0 = time.time()
    log("=== STUDIO F1 - 9-TICKER REGIME ALIGNMENT ===")
    log(f"GEX_DB  = {GEX_DB}")
    log(f"HIST_DB = {HIST_DB}")

    df = load_all_tickers_gex()
    df = classify_regime(df)
    log(f"Regime counts per ticker:\n{df.groupby('ticker')['regime'].value_counts()}")

    # Lead/lag BEFORE pivot (needs regime column)
    ll = cross_regime_leadlag(df, max_lag=6)
    ll.to_csv(OUT_DIR / "lead_lag_corr.csv", index=False)
    log(f"Lead/lag (best):\n{ll[['ticker', 'best_lag', 'best_corr']]}")

    # Alignment
    al = build_alignment(df)
    al.to_csv(OUT_DIR / "alignment_timeline.csv", index=False)

    # Plot heatmap + alignment timeline
    plot_regime_heatmap(df)
    plot_alignment_timeline(al)

    # Distribution of alignment scores
    ag = al["align_max"].value_counts().sort_index()
    log(f"Distribution of align_max (# same-regime tickers):\n{ag}")
    (OUT_DIR / "align_max_distribution.csv").write_text(ag.to_csv(), encoding="utf-8")

    # ES price bars
    ts_min = al["ts_bar"].min()
    ts_max = al["ts_bar"].max() + pd.Timedelta(hours=1)
    es_px = load_es_price_bars(ts_min, ts_max)
    es_px = filter_rth(es_px)

    # Merge alignment + ES price + forward returns
    m = add_forward_returns(al, es_px, horizons=(3, 6, 12))
    m = filter_rth(m)
    log(f"Merged bars RTH: {len(m):,}")

    # Forward return stats by bucket
    stats_align = fwd_stats_by_bucket(m, "align_max", h_min=30)
    stats_align.to_csv(OUT_DIR / "fwd_stats_by_align_max.csv")
    log(f"Fwd return stats (30m) by align_max:\n{stats_align}")

    stats_majority = fwd_stats_by_bucket(m, "majority_regime", h_min=30)
    stats_majority.to_csv(OUT_DIR / "fwd_stats_by_majority.csv")
    log(f"Fwd return stats (30m) by majority_regime:\n{stats_majority}")

    m["regime_combo"] = m["majority_regime"].astype(str) + "_align" + m["aligned_5_9"].map(
        {True: "HI", False: "LO"}).astype(str)
    stats_combo = fwd_stats_by_bucket(m, "regime_combo", h_min=30)
    stats_combo.to_csv(OUT_DIR / "fwd_stats_by_combo.csv")
    log(f"Fwd return stats by combo:\n{stats_combo}")

    plot_fwd_return_by_bucket(m, h_min=30)
    plot_fwd_return_by_majority(m, h_min=30)

    # Backtest: filtered vs unfiltered
    log("Backtest unfiltered...")
    tr_all = bt_a1_signals(m, filter_aligned=False)
    s_all = summarize_trades(tr_all, "unfiltered")
    log(f"  unfiltered: {s_all}")

    log("Backtest aligned >= 5/9...")
    tr_align = bt_a1_signals(m, filter_aligned=True)
    s_align = summarize_trades(tr_align, "aligned_5_9")
    log(f"  aligned: {s_align}")

    # Additional: filter by majority regime
    m_pin_al = m[(m["majority_regime"] == "PINNING") & (m["aligned_5_9"])]
    m_amp_al = m[(m["majority_regime"] == "AMPLIFICATION") & (m["aligned_5_9"])]
    tr_pin_al = bt_a1_signals(m_pin_al, filter_aligned=False)
    tr_amp_al = bt_a1_signals(m_amp_al, filter_aligned=False)
    s_pin = summarize_trades(tr_pin_al, "pin_aligned")
    s_amp = summarize_trades(tr_amp_al, "amp_aligned")
    log(f"  pin_aligned: {s_pin}")
    log(f"  amp_aligned: {s_amp}")

    pd.DataFrame([s_all, s_align, s_pin, s_amp]).to_csv(OUT_DIR / "backtest_summary.csv", index=False)
    for name, tr in [("unfiltered", tr_all), ("aligned_5_9", tr_align),
                      ("pin_aligned", tr_pin_al), ("amp_aligned", tr_amp_al)]:
        if not tr.empty:
            tr.to_csv(OUT_DIR / f"trades_{name}.csv", index=False)

    plot_pnl_cum({"unfiltered": tr_all, "aligned_5_9": tr_align,
                   "pin_aligned": tr_pin_al, "amp_aligned": tr_amp_al})

    # Correlation matrix of regimes between tickers
    log("Computing regime correlation matrix...")
    piv = df.pivot_table(index="ts_bar", columns="ticker", values="regime",
                          aggfunc="first")
    cmap = {"PINNING": 1, "TRANSITION": 0, "AMPLIFICATION": -1}
    num = piv.apply(lambda col: col.map(lambda v: cmap.get(v, np.nan)))
    corr = num.corr()
    corr.to_csv(OUT_DIR / "regime_correlation.csv")
    log(f"Regime correlation vs ES:\n{corr['ES_SPX'].sort_values(ascending=False)}")

    fig, ax = plt.subplots(figsize=(8, 6))
    im = ax.imshow(corr.values, cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(len(corr.columns))); ax.set_xticklabels(corr.columns, rotation=45)
    ax.set_yticks(range(len(corr.index))); ax.set_yticklabels(corr.index)
    for i in range(len(corr)):
        for j in range(len(corr.columns)):
            v = corr.values[i, j]
            if pd.notna(v):
                ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=7,
                         color="white" if abs(v) > 0.5 else "black")
    plt.colorbar(im, ax=ax)
    ax.set_title("Cross-ticker GEX regime correlation")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "regime_correlation.png", dpi=120)
    plt.close(fig)

    # Summary JSON
    summary = {
        "tickers": TICKERS,
        "n_bars_total": int(len(al)),
        "n_bars_merged_rth": int(len(m)),
        "align_max_distribution": {int(k): int(v) for k, v in ag.items()},
        "pct_bars_aligned_5of9": float(al["aligned_5_9"].mean()),
        "mean_n_avail": float(al["n_avail"].mean()),
        "backtests": [s_all, s_align, s_pin, s_amp],
        "lead_lag": ll.to_dict(orient="records"),
        "regime_corr_vs_ES": corr["ES_SPX"].dropna().to_dict(),
        "elapsed_sec": round(time.time() - t0, 1),
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log(f"=== DONE in {summary['elapsed_sec']}s ===")
    save_log()


if __name__ == "__main__":
    main()
