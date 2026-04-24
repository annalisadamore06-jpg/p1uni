"""
study_A1_gex_regime.py  --  Net-GEX Regime Classifier (Pinning vs Amplification)
================================================================================
Obiettivo
---------
Dimostrare empiricamente che il regime di dealer-gamma (long vs short)
predice il comportamento del prezzo di ES:
  - Net GEX >> 0  -> dealer long gamma -> contrarian hedging -> MEAN REVERSION
  - Net GEX << 0  -> dealer short gamma -> pro-cyclical hedging -> MOMENTUM

Dataset
-------
ml_gold_20260407_211327.duckdb (backup, 2025-09-15 -> 2026-04-01, ~6.5 mesi)
  gex_summary : ticker='ES_SPX', hub='classic', aggregation='gex_zero'
                net_gex_oi, zero_gamma, spot, call_wall_oi, put_wall_oi (sub-second)

Metodologia
-----------
1. Aggrega a barre 5-min (OHLC spot, last net_gex_oi / walls / ZG).
2. Filtra RTH: 13:25 - 19:55 UTC (15:25 - 21:55 CEST in estate).
3. Rolling Z-score 20 giorni RTH di net_gex_oi (adattivo).
4. Classifica regime:
      z > +0.5   -> PINNING           (dealer long gamma, stabilizza)
     -0.5 <= z <= +0.5 -> TRANSITION  (vicino alla neutralita')
      z < -0.5   -> AMPLIFICATION     (dealer short gamma, amplifica)
5. Forward returns +5 / +15 / +30 / +60 min.
6. Score matematici:
      mean_reversion_score = -sign(r_5m) * sign(r_30m)   (inverte => +1)
      trend_score          =  sign(r_5m) * sign(r_30m)   (continua => +1)
7. HMM 2-stati Gaussiano su (ret_5min, z_net_gex) -> confronto con
   classificazione Z-score via concordanza Cohen kappa.
8. Backtest:
      PINNING       -> fade tocchi: LONG @ touch(put_wall), SHORT @ touch(call_wall)
      AMPLIFICATION -> breakout:    LONG > call_wall, SHORT < put_wall
      TRANSITION    -> no trade
      TP=12 pt, SL=8 pt, exit fine sessione
9. Stats per regime: N trades, WR, EV, Sharpe, MaxDD.
10. Transition matrix regimi.
11. Plot: time series regime colorati, distribuzione forward return per regime,
    P&L cumulato, heatmap transition matrix.

Output: research/results/A1/
"""
from __future__ import annotations

import json
import logging
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats
from sklearn.metrics import cohen_kappa_score, confusion_matrix

warnings.filterwarnings("ignore")

# -- HMM (optional) -----------------------------------------------------------
try:
    from hmmlearn.hmm import GaussianHMM
    HAS_HMM = True
except ImportError:
    HAS_HMM = False

# =============================================================================
# CONFIG
# =============================================================================
DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\backups\phase1\ml_gold_20260407_211327.duckdb"
TICKER  = "ES_SPX"
OUT_DIR = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\A1")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN           = 5            # barre da 5 minuti
RTH_START_UTC     = "13:25"      # apertura RTH estesa (15:25 CEST)
RTH_END_UTC       = "19:55"      # chiusura RTH (21:55 CEST, Bulenox hard stop)
BARS_PER_RTH      = int(((19*60+55) - (13*60+25)) / BAR_MIN)  # = 78
ROLLING_DAYS      = 20
Z_PIN             = 0.5
Z_AMP             = -0.5
MOMENTUM_LAG      = 5            # barre
TP_PTS            = 12.0
SL_PTS            = 8.0
WALL_TOUCH_TOL    = 3.0          # pt entro cui consideriamo "tocco"
BREAKOUT_BUFFER   = 1.5          # pt oltre il wall = breakout
COOLDOWN_BARS     = 12           # 1 ora di pausa fra trade
MIN_TRADES_PER_REGIME = 5         # statistiche solo se almeno N trades

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5s] %(message)s",
    handlers=[
        logging.FileHandler(OUT_DIR / "run.log", encoding="utf-8", mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("A1")


# =============================================================================
# 1. LOAD & AGGREGATE TO 5-MIN BARS
# =============================================================================

def load_and_aggregate() -> pd.DataFrame:
    """Carica GEX sub-second + aggrega a barre 5-min con OHLC spot.

    Ritorna DataFrame index=ts (bar start UTC), colonne:
        open, high, low, close   (spot)
        net_gex_oi, zero_gamma, call_wall_oi, put_wall_oi, delta_rr, n_strikes
    """
    log.info(f"Connecting read_only to {DB_PATH}")
    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("SET threads=16")
    con.execute("SET memory_limit='8GB'")

    # Aggregazione a 5-min fatta direttamente in DuckDB (molto piu' veloce di pandas.resample)
    sql = f"""
    WITH src AS (
        SELECT
            ts_utc,
            -- Arrotonda al bar 5-min (floor)
            time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS bar_start,
            spot, net_gex_oi, net_gex_vol, zero_gamma,
            call_wall_oi, put_wall_oi, delta_rr, n_strikes
        FROM gex_summary
        WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
          AND spot IS NOT NULL AND net_gex_oi IS NOT NULL
    )
    SELECT
        bar_start AS ts,
        FIRST(spot ORDER BY ts_utc)             AS open,
        MAX(spot)                               AS high,
        MIN(spot)                               AS low,
        LAST(spot ORDER BY ts_utc)              AS close,
        LAST(net_gex_oi ORDER BY ts_utc)        AS net_gex_oi,
        LAST(net_gex_vol ORDER BY ts_utc)       AS net_gex_vol,
        LAST(zero_gamma ORDER BY ts_utc)        AS zero_gamma,
        LAST(call_wall_oi ORDER BY ts_utc)      AS call_wall_oi,
        LAST(put_wall_oi ORDER BY ts_utc)       AS put_wall_oi,
        LAST(delta_rr ORDER BY ts_utc)          AS delta_rr,
        AVG(n_strikes)                          AS n_strikes,
        COUNT(*)                                AS tick_count
    FROM src
    GROUP BY bar_start
    ORDER BY bar_start
    """
    t0 = time.time()
    df = con.execute(sql).fetchdf()
    con.close()
    log.info(f"Loaded {len(df):,} bars in {time.time()-t0:.1f}s")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)

    # Filtra RTH only
    df["time_of_day"] = df.index.time
    rth_start = pd.to_datetime(RTH_START_UTC).time()
    rth_end   = pd.to_datetime(RTH_END_UTC).time()
    mask = (df["time_of_day"] >= rth_start) & (df["time_of_day"] <= rth_end)
    df = df[mask].copy()
    df.drop(columns=["time_of_day"], inplace=True)
    # Remove weekends
    df = df[df.index.dayofweek < 5].copy()

    log.info(f"After RTH/weekday filter: {len(df):,} bars "
             f"({df.index[0]} -> {df.index[-1]})")
    return df


# =============================================================================
# 2. FEATURES
# =============================================================================

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Rolling Z-score adattivo + momentum/acceleration.

    Rolling window = ROLLING_DAYS * BARS_PER_RTH (20 gg * 78 barre = 1560)
    """
    w = ROLLING_DAYS * BARS_PER_RTH
    df["rolling_mean"] = df["net_gex_oi"].rolling(w, min_periods=w // 2).mean()
    df["rolling_std"]  = df["net_gex_oi"].rolling(w, min_periods=w // 2).std()
    df["z_net_gex"]    = (df["net_gex_oi"] - df["rolling_mean"]) / df["rolling_std"]

    # Momentum / acceleration normalizzati
    df["gex_delta"]         = df["net_gex_oi"].diff(MOMENTUM_LAG)
    df["gex_momentum"]      = df["gex_delta"] / df["rolling_std"]
    df["gex_acceleration"]  = df["gex_momentum"].diff()

    # Log return del close spot
    df["log_ret"] = np.log(df["close"] / df["close"].shift(1))
    # Realized vol locale (ATR-like in punti, 12 barre = 1h)
    df["atr_12"] = (df["high"] - df["low"]).rolling(12).mean()

    # Regime discreto da Z-score
    conds = [
        df["z_net_gex"] > Z_PIN,
        df["z_net_gex"] < Z_AMP,
    ]
    df["regime"] = np.select(conds, ["PINNING", "AMPLIFICATION"], default="TRANSITION")
    df["regime"] = df["regime"].astype("category")

    # Distanza spot <-> walls e ZG (in punti)
    df["dist_call_wall"] = df["close"] - df["call_wall_oi"]   # positivo = sopra
    df["dist_put_wall"]  = df["close"] - df["put_wall_oi"]
    df["dist_zg"]        = df["close"] - df["zero_gamma"]

    df = df.dropna(subset=["z_net_gex"]).copy()
    log.info(f"Features computed. Regime distribution:\n{df['regime'].value_counts()}")
    return df


# =============================================================================
# 3. FORWARD RETURNS + SCORES
# =============================================================================

def label_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Forward return a 5/15/30/60 min (= 1/3/6/12 barre)."""
    horizons_bars = {"5m": 1, "15m": 3, "30m": 6, "60m": 12}
    for name, h in horizons_bars.items():
        df[f"ret_fwd_{name}"] = df["close"].shift(-h) / df["close"] - 1.0
        df[f"ret_fwd_{name}_pts"] = df["close"].shift(-h) - df["close"]

    # mean-reversion score: se r5 e r30 hanno segno opposto -> mean revert (+1)
    df["mean_revert_score"] = -np.sign(df["ret_fwd_5m"]) * np.sign(df["ret_fwd_30m"])
    df["trend_score"]       =  np.sign(df["ret_fwd_5m"]) * np.sign(df["ret_fwd_30m"])
    return df


# =============================================================================
# 4. STATS PER REGIME
# =============================================================================

def regime_stats(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for reg, sub in df.groupby("regime", observed=True):
        for h in ["5m", "15m", "30m", "60m"]:
            r = sub[f"ret_fwd_{h}_pts"].dropna()
            if len(r) < 10:
                continue
            rows.append({
                "regime": reg,
                "horizon": h,
                "n": len(r),
                "mean_pts": r.mean(),
                "std_pts": r.std(),
                "sharpe_ann": r.mean() / r.std() * np.sqrt(252 * BARS_PER_RTH)
                              if r.std() > 0 else 0.0,
                "pct_positive": (r > 0).mean(),
                "pct_negative": (r < 0).mean(),
                "p25": r.quantile(0.25),
                "median": r.median(),
                "p75": r.quantile(0.75),
                "mean_revert_rate": sub["mean_revert_score"].mean(),
                "trend_rate":       sub["trend_score"].mean(),
            })
    return pd.DataFrame(rows)


def transition_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Matrice di transizione one-step tra regimi."""
    cur = df["regime"]
    nxt = df["regime"].shift(-1)
    mat = pd.crosstab(cur, nxt, normalize="index")
    mat = mat.reindex(index=["AMPLIFICATION", "TRANSITION", "PINNING"],
                      columns=["AMPLIFICATION", "TRANSITION", "PINNING"], fill_value=0)
    return mat


# =============================================================================
# 5. HMM 2-STATI
# =============================================================================

def fit_hmm(df: pd.DataFrame) -> tuple[pd.Series | None, dict]:
    """Fit Gaussian HMM 2-stati su (log_ret, z_net_gex).

    Associa stato a 'HIGH_VOL' (AMPLIFICATION) vs 'LOW_VOL' (PINNING)
    in base alla varianza dei log-return.
    """
    if not HAS_HMM:
        log.warning("hmmlearn non installato - skip HMM")
        return None, {}

    feat = df[["log_ret", "z_net_gex"]].dropna().copy()
    X = feat.values

    log.info(f"Fitting GaussianHMM(n_components=2) on {len(X):,} obs...")
    t0 = time.time()
    hmm = GaussianHMM(n_components=2, covariance_type="full",
                      n_iter=200, random_state=42, tol=1e-4)
    hmm.fit(X)
    states = hmm.predict(X)
    log.info(f"HMM fitted in {time.time()-t0:.1f}s, converged={hmm.monitor_.converged}")

    # Identifica stati: stato con varianza log_ret maggiore = AMPLIFICATION
    vars_ = [hmm.covars_[s][0, 0] for s in range(2)]
    amp_state = int(np.argmax(vars_))
    pin_state = 1 - amp_state

    hmm_regime = pd.Series(
        np.where(states == amp_state, "AMPLIFICATION", "PINNING"),
        index=feat.index, name="hmm_regime",
    )

    info = {
        "converged": bool(hmm.monitor_.converged),
        "iterations": int(hmm.monitor_.iter),
        "log_likelihood": float(hmm.score(X)),
        "state_variances_logret": [float(v) for v in vars_],
        "amp_state_idx": amp_state,
        "transition_matrix": hmm.transmat_.tolist(),
        "means": hmm.means_.tolist(),
    }
    return hmm_regime, info


# =============================================================================
# 6. BACKTEST
# =============================================================================

def _simulate_trade(side: str, entry_px: float, entry_ts, bars_day: pd.DataFrame,
                    tp: float, sl: float) -> dict:
    """Simula entry intra-bar: scorre le barre successive, checka TP/SL
    con high/low. Exit al close dell'ultima barra se nessuno e' colpito.
    """
    fwd = bars_day[bars_day.index > entry_ts]
    if fwd.empty:
        return {"pnl": 0.0, "exit_reason": "NO_FWD", "bars_held": 0,
                "exit_ts": entry_ts, "exit_px": entry_px}

    for i, (ts, row) in enumerate(fwd.iterrows(), 1):
        h, l, c = row["high"], row["low"], row["close"]
        if side == "LONG":
            if l <= entry_px - sl:
                return {"pnl": -sl, "exit_reason": "SL", "bars_held": i,
                        "exit_ts": ts, "exit_px": entry_px - sl}
            if h >= entry_px + tp:
                return {"pnl": +tp, "exit_reason": "TP", "bars_held": i,
                        "exit_ts": ts, "exit_px": entry_px + tp}
        else:  # SHORT
            if h >= entry_px + sl:
                return {"pnl": -sl, "exit_reason": "SL", "bars_held": i,
                        "exit_ts": ts, "exit_px": entry_px + sl}
            if l <= entry_px - tp:
                return {"pnl": +tp, "exit_reason": "TP", "bars_held": i,
                        "exit_ts": ts, "exit_px": entry_px - tp}
    # EOD exit
    last = fwd.iloc[-1]
    pnl = (last["close"] - entry_px) if side == "LONG" else (entry_px - last["close"])
    return {"pnl": float(pnl), "exit_reason": "EOD", "bars_held": len(fwd),
            "exit_ts": fwd.index[-1], "exit_px": float(last["close"])}


def backtest(df: pd.DataFrame) -> pd.DataFrame:
    """Per ogni giorno RTH, scorri le barre e genera segnali per regime.

    Regole:
      PINNING:
        - touch(call_wall) entro WALL_TOUCH_TOL -> SHORT (fade)
        - touch(put_wall)  entro WALL_TOUCH_TOL -> LONG  (fade)
      AMPLIFICATION:
        - close > call_wall + BREAKOUT_BUFFER  -> LONG  (breakout)
        - close < put_wall - BREAKOUT_BUFFER   -> SHORT (breakdown)
      TRANSITION:
        - nessun trade

    Debounce: COOLDOWN_BARS barre fra trade consecutivi.
    """
    trades = []
    grouped = df.groupby(df.index.date)
    n_days = len(grouped)
    log.info(f"Backtesting {n_days} days...")

    for day, bars_day in grouped:
        if len(bars_day) < 20:
            continue
        last_entry_idx = -COOLDOWN_BARS - 1
        for i, (ts, row) in enumerate(bars_day.iterrows()):
            if (i - last_entry_idx) < COOLDOWN_BARS:
                continue
            reg = row["regime"]
            if reg == "TRANSITION":
                continue
            if pd.isna(row["call_wall_oi"]) or pd.isna(row["put_wall_oi"]):
                continue

            side = None
            trigger = None
            entry_px = float(row["close"])

            if reg == "PINNING":
                # tocca il call_wall dall'interno -> SHORT
                if abs(entry_px - row["call_wall_oi"]) <= WALL_TOUCH_TOL and \
                        entry_px < row["call_wall_oi"] + WALL_TOUCH_TOL:
                    side, trigger = "SHORT", "fade_call_wall"
                elif abs(entry_px - row["put_wall_oi"]) <= WALL_TOUCH_TOL and \
                        entry_px > row["put_wall_oi"] - WALL_TOUCH_TOL:
                    side, trigger = "LONG", "fade_put_wall"

            elif reg == "AMPLIFICATION":
                if entry_px > row["call_wall_oi"] + BREAKOUT_BUFFER:
                    side, trigger = "LONG", "breakout_call_wall"
                elif entry_px < row["put_wall_oi"] - BREAKOUT_BUFFER:
                    side, trigger = "SHORT", "breakdown_put_wall"

            if side is None:
                continue

            result = _simulate_trade(side, entry_px, ts, bars_day, TP_PTS, SL_PTS)
            trades.append({
                "date": day,
                "entry_ts": ts,
                "regime": reg,
                "side": side,
                "trigger": trigger,
                "entry_px": entry_px,
                "z_net_gex": float(row["z_net_gex"]),
                "net_gex_oi": float(row["net_gex_oi"]),
                "zero_gamma": float(row["zero_gamma"]),
                "call_wall_oi": float(row["call_wall_oi"]),
                "put_wall_oi": float(row["put_wall_oi"]),
                **result,
            })
            last_entry_idx = i

    tdf = pd.DataFrame(trades)
    log.info(f"Generated {len(tdf)} trades across {n_days} days")
    if not tdf.empty:
        tdf["pnl_cum"] = tdf["pnl"].cumsum()
    return tdf


def backtest_stats(trades: pd.DataFrame) -> pd.DataFrame:
    """Win rate, EV, Sharpe, MaxDD per regime e per trigger."""
    if trades.empty:
        return pd.DataFrame()

    rows = []
    for group_col in ["regime", "trigger", "side"]:
        for key, sub in trades.groupby(group_col):
            if len(sub) < MIN_TRADES_PER_REGIME:
                continue
            cumpnl = sub["pnl"].cumsum()
            dd = (cumpnl - cumpnl.cummax()).min()
            rows.append({
                "dim": group_col,
                "key": key,
                "n_trades": len(sub),
                "win_rate": (sub["pnl"] > 0).mean(),
                "ev_pts": sub["pnl"].mean(),
                "std_pts": sub["pnl"].std(),
                "sharpe_per_trade": sub["pnl"].mean() / sub["pnl"].std()
                                      if sub["pnl"].std() > 0 else 0,
                "total_pnl": sub["pnl"].sum(),
                "max_dd_pts": float(dd) if pd.notna(dd) else 0,
                "tp_rate": (sub["exit_reason"] == "TP").mean(),
                "sl_rate": (sub["exit_reason"] == "SL").mean(),
                "eod_rate": (sub["exit_reason"] == "EOD").mean(),
                "avg_bars_held": sub["bars_held"].mean(),
            })
    return pd.DataFrame(rows)


# =============================================================================
# 7. PLOTTING
# =============================================================================

def plot_all(df: pd.DataFrame, trades: pd.DataFrame, hmm_regime: pd.Series | None,
             reg_stats: pd.DataFrame, tr_mat: pd.DataFrame) -> None:
    sns.set_style("whitegrid")
    palette = {"PINNING": "#2ca02c", "TRANSITION": "#7f7f7f", "AMPLIFICATION": "#d62728"}

    # --- 1. Z-score time series con regime ----------------------------------
    fig, ax = plt.subplots(figsize=(16, 5))
    ax.plot(df.index, df["z_net_gex"], color="black", lw=0.4, alpha=0.6)
    ax.axhline(Z_PIN, color="green", ls="--", lw=0.7, label=f"z = +{Z_PIN} (PIN)")
    ax.axhline(Z_AMP, color="red",   ls="--", lw=0.7, label=f"z = {Z_AMP} (AMP)")
    for reg, c in palette.items():
        mask = df["regime"] == reg
        ax.scatter(df.index[mask], df["z_net_gex"][mask], s=2, color=c, label=reg, alpha=0.4)
    ax.set_title("Z-score Net-GEX (Rolling 20 gg RTH) + Regime")
    ax.set_ylabel("Z(net_gex_oi)")
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "01_z_net_gex_regime.png", dpi=120)
    plt.close(fig)

    # --- 2. Forward-return distribution per regime --------------------------
    fig, axes = plt.subplots(1, 4, figsize=(18, 4), sharey=True)
    for ax, h in zip(axes, ["5m", "15m", "30m", "60m"]):
        for reg, c in palette.items():
            r = df[df["regime"] == reg][f"ret_fwd_{h}_pts"].dropna()
            if len(r) > 20:
                sns.kdeplot(r, ax=ax, color=c, label=reg, bw_adjust=0.8, clip=(-20, 20))
        ax.set_title(f"Forward Return (pts) | +{h}")
        ax.axvline(0, color="k", lw=0.5)
        ax.set_xlabel("pts")
    axes[0].legend(loc="upper left", fontsize=9)
    fig.suptitle("Distribuzione Forward Return per Regime", fontsize=14)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "02_fwd_return_by_regime.png", dpi=120)
    plt.close(fig)

    # --- 3. Mean-reversion rate bar per regime ------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    reg_order = ["AMPLIFICATION", "TRANSITION", "PINNING"]
    mr = df.groupby("regime", observed=True)["mean_revert_score"].mean().reindex(reg_order)
    tr = df.groupby("regime", observed=True)["trend_score"].mean().reindex(reg_order)
    axes[0].bar(mr.index, mr.values, color=[palette[r] for r in mr.index])
    axes[0].axhline(0, color="k", lw=0.5)
    axes[0].set_title("Mean-Reversion Score per Regime\n(+1 = sempre inverte r5m vs r30m)")
    axes[1].bar(tr.index, tr.values, color=[palette[r] for r in tr.index])
    axes[1].axhline(0, color="k", lw=0.5)
    axes[1].set_title("Trend Score per Regime\n(+1 = sempre continua)")
    for ax in axes:
        ax.set_ylim(-0.3, 0.3)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "03_mr_trend_scores.png", dpi=120)
    plt.close(fig)

    # --- 4. Transition matrix heatmap ---------------------------------------
    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(tr_mat, annot=True, fmt=".3f", cmap="viridis",
                cbar_kws={"label": "P(next | current)"}, ax=ax)
    ax.set_title("Matrice di Transizione Regime (one-step)")
    ax.set_xlabel("Regime successivo")
    ax.set_ylabel("Regime corrente")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "04_transition_matrix.png", dpi=120)
    plt.close(fig)

    # --- 5. HMM vs Z-score regime confronto ---------------------------------
    if hmm_regime is not None:
        joined = pd.DataFrame({
            "z_regime": df["regime"].astype(str),
            "hmm_regime": hmm_regime.astype(str),
        }).dropna()
        fig, ax = plt.subplots(figsize=(5, 4))
        cm = pd.crosstab(joined["z_regime"], joined["hmm_regime"], normalize="index")
        sns.heatmap(cm, annot=True, fmt=".3f", cmap="coolwarm",
                    cbar_kws={"label": "P(HMM | Zscore)"}, ax=ax)
        ax.set_title("HMM regime vs Z-score regime")
        fig.tight_layout()
        fig.savefig(OUT_DIR / "05_hmm_vs_zscore.png", dpi=120)
        plt.close(fig)

    # --- 6. Cumulative P&L backtest -----------------------------------------
    if not trades.empty:
        fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
        # Globale
        axes[0].plot(trades["entry_ts"], trades["pnl_cum"], lw=1, color="black")
        axes[0].axhline(0, color="k", lw=0.5)
        axes[0].set_title(f"P&L Cumulativo Backtest  |  trades={len(trades)}  "
                          f"final={trades['pnl_cum'].iloc[-1]:.1f} pt")
        axes[0].set_ylabel("pt")
        # Per regime
        for reg, c in palette.items():
            sub = trades[trades["regime"] == reg]
            if sub.empty:
                continue
            axes[1].plot(sub["entry_ts"], sub["pnl"].cumsum(),
                         lw=1, color=c, label=f"{reg} (n={len(sub)})")
        axes[1].axhline(0, color="k", lw=0.5)
        axes[1].legend()
        axes[1].set_title("P&L Cumulativo per Regime")
        axes[1].set_ylabel("pt")
        axes[1].set_xlabel("entry_ts")
        fig.tight_layout()
        fig.savefig(OUT_DIR / "06_backtest_pnl.png", dpi=120)
        plt.close(fig)

    # --- 7. Fwd-return mean pts per regime x horizon -----------------------
    if not reg_stats.empty:
        pvt = reg_stats.pivot(index="horizon", columns="regime", values="mean_pts")
        pvt = pvt.reindex(["5m", "15m", "30m", "60m"])
        fig, ax = plt.subplots(figsize=(8, 4))
        pvt.plot(kind="bar", ax=ax, color=[palette.get(c, "gray") for c in pvt.columns])
        ax.axhline(0, color="k", lw=0.5)
        ax.set_title("Mean Forward Return (pts) per Regime x Horizon")
        ax.set_ylabel("pt")
        fig.tight_layout()
        fig.savefig(OUT_DIR / "07_mean_fwd_ret_matrix.png", dpi=120)
        plt.close(fig)

    log.info(f"Plots saved to {OUT_DIR}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    t_start = time.time()
    log.info("=" * 70)
    log.info("STUDY A1 - Net-GEX Regime Classifier")
    log.info("=" * 70)

    df = load_and_aggregate()
    df = compute_features(df)
    df = label_forward_returns(df)

    # -- Statistiche descrittive --
    reg_stats = regime_stats(df)
    log.info("\n=== REGIME STATS (fwd returns) ===\n" + reg_stats.to_string(index=False))
    reg_stats.to_csv(OUT_DIR / "regime_stats.csv", index=False)

    # -- Transition matrix --
    tr_mat = transition_matrix(df)
    log.info("\n=== TRANSITION MATRIX ===\n" + tr_mat.to_string())
    tr_mat.to_csv(OUT_DIR / "transition_matrix.csv")

    # -- HMM --
    hmm_regime, hmm_info = fit_hmm(df)
    kappa = None
    if hmm_regime is not None:
        # Mappa Z-regime binario (PINNING vs AMPLIFICATION) vs HMM
        z_bin = df["regime"].map({"PINNING": "PINNING", "AMPLIFICATION": "AMPLIFICATION",
                                    "TRANSITION": None})
        joined = pd.DataFrame({"z": z_bin, "hmm": hmm_regime}).dropna()
        if len(joined) > 10:
            kappa = float(cohen_kappa_score(joined["z"], joined["hmm"]))
            agree = float((joined["z"] == joined["hmm"]).mean())
            hmm_info["cohen_kappa_vs_zscore"] = kappa
            hmm_info["agreement_rate"] = agree
            log.info(f"HMM vs Z-score: kappa={kappa:.3f}, agreement={agree:.3f}")

    # -- Backtest --
    trades = backtest(df)
    if not trades.empty:
        trades.to_csv(OUT_DIR / "trades.csv", index=False)
        bt_stats = backtest_stats(trades)
        log.info("\n=== BACKTEST STATS ===\n" + bt_stats.to_string(index=False))
        bt_stats.to_csv(OUT_DIR / "backtest_stats.csv", index=False)

        total_pnl = float(trades["pnl"].sum())
        wr        = float((trades["pnl"] > 0).mean())
        ev        = float(trades["pnl"].mean())
        sh_trade  = float(trades["pnl"].mean() / trades["pnl"].std()) if trades["pnl"].std() > 0 else 0.0
        log.info(f"\nGLOBAL BACKTEST: n={len(trades)}  WR={wr:.3f}  EV={ev:.3f}pt  "
                 f"Sharpe/trade={sh_trade:.3f}  Total={total_pnl:.1f}pt")
    else:
        bt_stats = pd.DataFrame()

    # -- Plots --
    plot_all(df, trades, hmm_regime, reg_stats, tr_mat)

    # -- Export bars + features sample (head 5000 + tail 5000) ---------------
    sample = pd.concat([df.head(5000), df.tail(5000)])
    sample.to_csv(OUT_DIR / "features_sample.csv")

    # -- Export parametri ----------------------------------------------------
    params = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": DB_PATH,
        "ticker": TICKER,
        "bar_minutes": BAR_MIN,
        "rth_window_utc": [RTH_START_UTC, RTH_END_UTC],
        "bars_per_rth": BARS_PER_RTH,
        "rolling_days": ROLLING_DAYS,
        "z_pin": Z_PIN,
        "z_amp": Z_AMP,
        "momentum_lag_bars": MOMENTUM_LAG,
        "tp_pts": TP_PTS,
        "sl_pts": SL_PTS,
        "wall_touch_tol_pts": WALL_TOUCH_TOL,
        "breakout_buffer_pts": BREAKOUT_BUFFER,
        "cooldown_bars": COOLDOWN_BARS,
        "dataset_bars": int(len(df)),
        "dataset_start": str(df.index[0]),
        "dataset_end": str(df.index[-1]),
        "regime_distribution": df["regime"].value_counts().to_dict(),
        "hmm_available": HAS_HMM,
        "hmm_info": hmm_info,
        "n_trades": int(len(trades)),
        "global_pnl_pts": float(trades["pnl"].sum()) if not trades.empty else 0.0,
        "runtime_sec": time.time() - t_start,
    }
    # Convert non-serializable
    params["regime_distribution"] = {str(k): int(v) for k, v in params["regime_distribution"].items()}
    (OUT_DIR / "params.json").write_text(json.dumps(params, indent=2, default=str), encoding="utf-8")

    log.info(f"\nStudy A1 completed in {time.time() - t_start:.1f}s. Output: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
