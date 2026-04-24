"""
STUDIO E1 - AGGRESSOR IMBALANCE AI LIVELLI
==========================================

Ipotesi:
    Ai tocchi di call_wall / put_wall / zero_gamma l'imbalance buy/sell
    (aggressor side) nei 60s immediatamente precedenti il tocco predice
    BREAKOUT vs BOUNCE.
    - |imbalance| grande, segno coerente con direzione di attacco  -> BREAKOUT
    - |imbalance| piccolo o contrario                               -> BOUNCE

Metodo:
    1. Touch events identificati sulle 5-min bars (ricavate da OHLC
       calcolato su historical_trades.ES) quando high/low entra in
       corridoio di TOUCH_BAND punti dal livello (CW/PW/ZG).
    2. Imbalance nei 60s antecedenti il tocco calcolato via
       per-secondo aggregazione di historical_trades (side 'A' = buy
       aggressor, side 'B' = sell aggressor).
    3. Outcome forward a 30min (6 bars): BREAKOUT se prezzo rompe il
       livello di piu' di BREAK_PT nella stessa direzione di attacco;
       BOUNCE se torna indietro di BOUNCE_PT in direzione opposta.
    4. Logistic regression P(breakout) ~ imbalance_norm + |net_gex_oi|
       + regime_z.
    5. Backtest: alla barra del tocco, se |imbalance_norm| >
       IMB_THRESHOLD entra in direzione breakout, altrimenti in
       direzione bounce (fade). TP=12pt, SL=8pt, timeout 30min.
    6. ROC curve su vari IMB_THRESHOLD.

Uscita: research/results/E1/
"""
from __future__ import annotations

import json
import math
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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_curve, auc
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

# ---------------------------- CONFIG ----------------------------------------
GEX_DB = r"C:\Users\annal\Desktop\ML DATABASE\backups\phase1\ml_gold_20260407_211327.duckdb"
HIST_DB = r"C:\Users\annal\Desktop\P1UNI\data\p1uni_history.duckdb"
OUT_DIR = Path(r"C:\Users\annal\Desktop\P1UNI\research\results\E1")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TICKER_GEX = "ES_SPX"
TICKER_TRADES = "ES"
AGG = "gex_full"
HUB = "classic"

# RTH filter UTC: 13:30-20:00 (copre 09:30-16:00 ET in EDT/EST con qualche margine)
RTH_START_UTC = 13 * 60 + 30
RTH_END_UTC = 20 * 60

BAR_MIN = 5
TOUCH_BAND = 3.0           # punti entro i quali consideriamo tocco
BREAK_PT = 5.0             # breakout se superamento di BREAK_PT oltre il livello
BOUNCE_PT = 5.0            # bounce se ritracciamento di BOUNCE_PT dal livello
FWD_HORIZON_BARS = 6       # 30 min a 5-min

IMB_WIN_SEC = 60           # finestra imbalance pre-touch

TP_PT = 12.0
SL_PT = 8.0
BT_TIMEOUT_BARS = 6        # 30 min
BT_COOLDOWN_BARS = 3

IMB_THRESHOLD = 0.25       # soglia per distinguere breakout vs bounce strategy

# ---------------------------- OUTPUT LOG ------------------------------------
LOG_LINES: list[str] = []
def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


def save_log() -> None:
    (OUT_DIR / "run.log").write_text("\n".join(LOG_LINES), encoding="utf-8")


# ---------------------------- STEP 1: GEX bars ------------------------------
def load_gex_bars() -> pd.DataFrame:
    log("Loading gex_summary...")
    con = duckdb.connect(GEX_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df = con.execute(f"""
        SELECT time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
               AVG(spot)        AS spot,
               AVG(zero_gamma)  AS zero_gamma,
               AVG(call_wall_oi) AS call_wall_oi,
               AVG(put_wall_oi)  AS put_wall_oi,
               AVG(net_gex_oi)   AS net_gex_oi,
               AVG(delta_rr)     AS delta_rr
        FROM gex_summary
        WHERE ticker='{TICKER_GEX}' AND hub='{HUB}' AND aggregation='{AGG}'
        GROUP BY ts_bar
        ORDER BY ts_bar
    """).df()
    con.close()
    log(f"  gex bars: {len(df):,}  range {df['ts_bar'].min()} -> {df['ts_bar'].max()}")
    return df


def load_price_bars(ts_min, ts_max) -> pd.DataFrame:
    log("Loading OHLC from historical_trades...")
    con = duckdb.connect(HIST_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df = con.execute(f"""
        SELECT time_bucket(INTERVAL '{BAR_MIN} minutes', ts_event) AS ts_bar,
               first(price ORDER BY ts_event) AS open,
               MAX(price) AS high,
               MIN(price) AS low,
               last(price ORDER BY ts_event)  AS close,
               SUM(size)  AS vol,
               SUM(CASE WHEN side='A' THEN size ELSE 0 END) AS buy_sz,
               SUM(CASE WHEN side='B' THEN size ELSE 0 END) AS sell_sz
        FROM historical_trades
        WHERE ticker='{TICKER_TRADES}'
          AND ts_event >= TIMESTAMP '{ts_min}'
          AND ts_event <  TIMESTAMP '{ts_max}'
        GROUP BY ts_bar
        ORDER BY ts_bar
    """).df()
    con.close()
    log(f"  price bars: {len(df):,}")
    return df


def load_trades_per_second(ts_min, ts_max) -> pd.DataFrame:
    """Per-secondo buy/sell volume to compute imbalance windows."""
    log("Loading per-second aggressor volumes...")
    con = duckdb.connect(HIST_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df = con.execute(f"""
        SELECT date_trunc('second', ts_event) AS ts_s,
               SUM(CASE WHEN side='A' THEN size ELSE 0 END) AS buy_sz,
               SUM(CASE WHEN side='B' THEN size ELSE 0 END) AS sell_sz
        FROM historical_trades
        WHERE ticker='{TICKER_TRADES}'
          AND ts_event >= TIMESTAMP '{ts_min}'
          AND ts_event <  TIMESTAMP '{ts_max}'
        GROUP BY ts_s
        ORDER BY ts_s
    """).df()
    con.close()
    log(f"  per-sec rows: {len(df):,}")
    return df


# ---------------------------- STEP 2: Merge + touches -----------------------
def assemble(gex: pd.DataFrame, px: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(gex, px, on="ts_bar", how="inner")
    df = df.sort_values("ts_bar").reset_index(drop=True)
    # RTH filter
    minutes = df["ts_bar"].dt.hour * 60 + df["ts_bar"].dt.minute
    df = df[(minutes >= RTH_START_UTC) & (minutes < RTH_END_UTC)].reset_index(drop=True)
    # Fill GEX levels from last valid (they update less frequently than bars)
    for c in ["zero_gamma", "call_wall_oi", "put_wall_oi", "net_gex_oi", "delta_rr"]:
        df[c] = df[c].ffill()
    df = df.dropna(subset=["zero_gamma", "call_wall_oi", "put_wall_oi", "close", "high", "low"]).reset_index(drop=True)
    log(f"Assembled bars (RTH, with GEX levels): {len(df):,}")
    return df


def classify_touch(row) -> tuple[str | None, float | None, str | None]:
    """Return (level_name, level_price, touch_direction).
    touch_direction:
        'from_below' means spot was approaching level from below (up-move touch);
        'from_above' means approaching from above (down-move touch).
    A touch is registered when high/low enters TOUCH_BAND around the level.
    Only the first/most-relevant level this bar is used (priority CW, PW, ZG).
    """
    h, l, o = row["high"], row["low"], row["open"]
    cw, pw, zg = row["call_wall_oi"], row["put_wall_oi"], row["zero_gamma"]

    # Call Wall from below (resistance)
    if pd.notna(cw) and (h >= cw - TOUCH_BAND) and (o < cw + TOUCH_BAND) and (l < cw):
        return "CW", float(cw), "from_below"
    # Put Wall from above (support)
    if pd.notna(pw) and (l <= pw + TOUCH_BAND) and (o > pw - TOUCH_BAND) and (h > pw):
        return "PW", float(pw), "from_above"
    # Zero Gamma (magnet, both sides)
    if pd.notna(zg):
        if (h >= zg - TOUCH_BAND) and (o < zg):
            return "ZG", float(zg), "from_below"
        if (l <= zg + TOUCH_BAND) and (o > zg):
            return "ZG", float(zg), "from_above"
    return None, None, None


def tag_touches(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        name, lev, direction = classify_touch(r)
        rows.append((name, lev, direction))
    df[["touch_level", "touch_price", "touch_dir"]] = pd.DataFrame(rows, index=df.index)
    n_touch = df["touch_level"].notna().sum()
    log(f"Touch events: {n_touch:,} ({100*n_touch/len(df):.1f}% of bars)")
    by = df["touch_level"].value_counts().to_dict()
    log(f"  by level: {by}")
    return df


# ---------------------------- STEP 3: Imbalance -----------------------------
def compute_imbalance(df: pd.DataFrame, trades_s: pd.DataFrame) -> pd.DataFrame:
    """Imbalance in the 60s preceding each touch event's bar start.

    Using bar start as reference: ts_bar is the open of the 5-min bar,
    so the "approach" 60s is [ts_bar, ts_bar+60s) -- the first 60s of
    the bar, i.e. the aggressor behaviour as price moved toward the
    level. Prior studies/GEXbot convention: ts_bar = START of bucket.
    """
    log("Computing 60s aggressor imbalance per touch...")
    trades_s = trades_s.copy()
    trades_s["ts_s"] = pd.to_datetime(trades_s["ts_s"])
    trades_s = trades_s.set_index("ts_s").sort_index()
    # Cumulative sums for fast window queries
    cum_buy = trades_s["buy_sz"].cumsum()
    cum_sell = trades_s["sell_sz"].cumsum()
    cum_tot = (trades_s["buy_sz"] + trades_s["sell_sz"]).cumsum()

    touches = df[df["touch_level"].notna()].copy()
    ts_start = touches["ts_bar"].values.astype("datetime64[ns]")
    ts_end = ts_start + np.timedelta64(IMB_WIN_SEC, "s")

    # For each touch, find cumulative sums at window edges via searchsorted
    idx_arr = trades_s.index.values.astype("datetime64[ns]")
    i_start = np.searchsorted(idx_arr, ts_start, side="left")
    i_end = np.searchsorted(idx_arr, ts_end, side="left")

    cum_buy_arr = cum_buy.values
    cum_sell_arr = cum_sell.values
    cum_tot_arr = cum_tot.values

    def _sum_between(cum_arr, a, b):
        left = np.where(a == 0, 0, cum_arr[np.clip(a - 1, 0, len(cum_arr) - 1)])
        left = np.where(a == 0, 0, left)
        right = np.where(b == 0, 0, cum_arr[np.clip(b - 1, 0, len(cum_arr) - 1)])
        right = np.where(b == 0, 0, right)
        return right - left

    buy_w = _sum_between(cum_buy_arr, i_start, i_end)
    sell_w = _sum_between(cum_sell_arr, i_start, i_end)
    tot_w = _sum_between(cum_tot_arr, i_start, i_end)

    touches["buy_60s"] = buy_w.astype(float)
    touches["sell_60s"] = sell_w.astype(float)
    touches["tot_60s"] = tot_w.astype(float)
    touches["imb_norm"] = np.where(
        touches["tot_60s"] > 0,
        (touches["buy_60s"] - touches["sell_60s"]) / touches["tot_60s"],
        np.nan,
    )
    log(f"  imbalance computed: {touches['imb_norm'].notna().sum():,} valid / {len(touches):,}")
    # Merge back
    df = df.merge(
        touches[["ts_bar", "buy_60s", "sell_60s", "tot_60s", "imb_norm"]],
        on="ts_bar", how="left"
    )
    return df


# ---------------------------- STEP 4: Outcomes ------------------------------
def tag_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    """For each touch bar, forward over FWD_HORIZON_BARS bars:
       - direction = 'up' if approach from_below (tests resistance CW or ZG),
                     'down' if approach from_above
       - breakout: price goes BREAK_PT beyond level in direction of attack
       - bounce: price retraces BOUNCE_PT away from level in opposite direction
       - neither: inconclusive.
    We compute fwd_high and fwd_low over the next 6 bars (exclusive of current).
    """
    df = df.copy()
    n = len(df)
    fwd_high = np.full(n, np.nan)
    fwd_low = np.full(n, np.nan)
    for i in range(n):
        j_end = min(n, i + 1 + FWD_HORIZON_BARS)
        if j_end <= i + 1:
            continue
        fwd_high[i] = df["high"].iloc[i + 1:j_end].max()
        fwd_low[i] = df["low"].iloc[i + 1:j_end].min()
    df["fwd_high"] = fwd_high
    df["fwd_low"] = fwd_low

    outc = []
    for _, r in df.iterrows():
        if pd.isna(r["touch_level"]):
            outc.append(None)
            continue
        lev = r["touch_price"]
        direction = r["touch_dir"]
        if direction == "from_below":
            # attack UP: breakout = fwd_high > lev + BREAK_PT; bounce = fwd_low < lev - BOUNCE_PT
            bo = (r["fwd_high"] > lev + BREAK_PT) if pd.notna(r["fwd_high"]) else False
            bc = (r["fwd_low"] < lev - BOUNCE_PT) if pd.notna(r["fwd_low"]) else False
        else:
            # attack DOWN
            bo = (r["fwd_low"] < lev - BREAK_PT) if pd.notna(r["fwd_low"]) else False
            bc = (r["fwd_high"] > lev + BOUNCE_PT) if pd.notna(r["fwd_high"]) else False
        # If both occur, use whichever happened first: we don't have path order beyond 5min agg,
        # so mark BREAKOUT (more informative) only when breakout occurs AND bounce does not
        # exceed. For simplicity: BREAKOUT if bo and not bc; BOUNCE if bc and not bo; MIXED otherwise.
        if bo and not bc:
            outc.append("BREAKOUT")
        elif bc and not bo:
            outc.append("BOUNCE")
        elif bo and bc:
            outc.append("MIXED")
        else:
            outc.append("FLAT")
    df["outcome"] = outc
    return df


# ---------------------------- STEP 5: Analysis ------------------------------
def regime_from_net_gex(df: pd.DataFrame) -> pd.DataFrame:
    """Rolling 20-day z-score of net_gex_oi (same convention as A1)."""
    df = df.copy()
    df["date"] = df["ts_bar"].dt.date
    daily_gex = df.groupby("date")["net_gex_oi"].mean()
    roll_mu = daily_gex.rolling(20, min_periods=5).mean()
    roll_sd = daily_gex.rolling(20, min_periods=5).std()
    z = (daily_gex - roll_mu) / roll_sd
    z = z.reindex(df["date"]).values
    df["regime_z"] = z
    df["regime"] = np.select(
        [df["regime_z"] > 0.5, df["regime_z"] < -0.5],
        ["PINNING", "AMPLIFICATION"],
        default="TRANSITION",
    )
    return df


def outcome_stats(touches: pd.DataFrame) -> pd.DataFrame:
    """Outcome distribution by imbalance quintile."""
    t = touches.dropna(subset=["imb_norm", "outcome"]).copy()
    try:
        t["imb_q"] = pd.qcut(t["imb_norm"], 5, labels=["Q1_sell", "Q2", "Q3", "Q4", "Q5_buy"], duplicates="drop")
    except ValueError:
        t["imb_q"] = pd.cut(t["imb_norm"], 5, labels=["Q1_sell", "Q2", "Q3", "Q4", "Q5_buy"])
    ct = pd.crosstab(t["imb_q"], t["outcome"], normalize="index") * 100
    return ct.round(2)


def outcome_by_level_and_imb_sign(touches: pd.DataFrame) -> pd.DataFrame:
    t = touches.dropna(subset=["imb_norm", "outcome"]).copy()
    t["imb_sign"] = np.where(t["imb_norm"] > 0.1, "buy", np.where(t["imb_norm"] < -0.1, "sell", "flat"))
    g = (t.groupby(["touch_level", "touch_dir", "imb_sign", "outcome"]).size()
         .unstack("outcome", fill_value=0))
    g["TOTAL"] = g.sum(axis=1)
    for c in ["BREAKOUT", "BOUNCE", "MIXED", "FLAT"]:
        if c in g.columns:
            g[f"{c}_pct"] = (g[c] / g["TOTAL"] * 100).round(1)
    return g


def logistic_regression(touches: pd.DataFrame) -> dict:
    t = touches.dropna(subset=["imb_norm", "outcome", "net_gex_oi", "regime_z"]).copy()
    t = t[t["outcome"].isin(["BREAKOUT", "BOUNCE"])].copy()
    if len(t) < 50:
        log("  logistic regression: insufficient samples")
        return {}
    # Binary: y=1 if BREAKOUT
    y = (t["outcome"] == "BREAKOUT").astype(int).values
    # Feature: align imbalance sign with attack direction
    # If attack from_below, positive imbalance supports breakout. If from_above, negative imbalance supports breakout.
    imb_aligned = np.where(t["touch_dir"] == "from_below", t["imb_norm"], -t["imb_norm"])
    X = np.column_stack([
        imb_aligned,
        np.abs(t["net_gex_oi"]).values,
        t["regime_z"].fillna(0).values,
    ])
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    clf = LogisticRegression(max_iter=1000)
    clf.fit(Xs, y)
    coefs = dict(zip(["imb_aligned", "abs_net_gex", "regime_z"], clf.coef_[0].tolist()))
    intercept = float(clf.intercept_[0])
    proba = clf.predict_proba(Xs)[:, 1]
    fpr, tpr, _ = roc_curve(y, proba)
    auc_val = auc(fpr, tpr)
    return {
        "n": int(len(t)),
        "coefs": coefs,
        "intercept": intercept,
        "auc": float(auc_val),
        "fpr": fpr.tolist(),
        "tpr": tpr.tolist(),
        "mean_breakout": float(y.mean()),
    }


# ---------------------------- STEP 6: Backtest ------------------------------
def backtest(df: pd.DataFrame, imb_thresh: float, strat: str = "hybrid") -> pd.DataFrame:
    """Trade logic at each touch:
       - hybrid:
            attack from_below, imb_aligned > +thresh  -> LONG (breakout)
            attack from_below, imb_aligned < -thresh  -> SHORT (bounce)
            attack from_above, imb_aligned > +thresh  -> SHORT (breakout down)
            attack from_above, imb_aligned < -thresh  -> LONG (bounce up)
            else skip
       - breakout_only: only trade with flow direction.
       - bounce_only: only trade against flow direction.
    """
    df = df.copy()
    touches = df[df["touch_level"].notna() & df["imb_norm"].notna()].copy()
    touches = touches.sort_values("ts_bar").reset_index(drop=True)
    trades = []
    i_last_exit = -10**9
    for _, r in touches.iterrows():
        # Cooldown
        if df.index[df["ts_bar"] == r["ts_bar"]][0] < i_last_exit + BT_COOLDOWN_BARS:
            continue
        aligned = r["imb_norm"] if r["touch_dir"] == "from_below" else -r["imb_norm"]

        side = None
        reason_entry = None
        if strat == "hybrid":
            if aligned > imb_thresh:
                # breakout bet
                side = 1 if r["touch_dir"] == "from_below" else -1
                reason_entry = "breakout_flow"
            elif aligned < -imb_thresh:
                # bounce bet
                side = -1 if r["touch_dir"] == "from_below" else 1
                reason_entry = "bounce_counter"
        elif strat == "breakout_only":
            if aligned > imb_thresh:
                side = 1 if r["touch_dir"] == "from_below" else -1
                reason_entry = "breakout_flow"
        elif strat == "bounce_only":
            if aligned < -imb_thresh:
                side = -1 if r["touch_dir"] == "from_below" else 1
                reason_entry = "bounce_counter"
        if side is None:
            continue

        # Simulate entry at open of NEXT bar (avoid lookahead within touch bar)
        i_here = df.index[df["ts_bar"] == r["ts_bar"]][0]
        i_entry = i_here + 1
        if i_entry >= len(df):
            continue
        entry_px = df.loc[i_entry, "open"]
        if pd.isna(entry_px):
            continue
        tp = entry_px + side * TP_PT
        sl = entry_px - side * SL_PT
        exit_px = None
        exit_bar = None
        reason = None
        for j in range(i_entry, min(len(df), i_entry + BT_TIMEOUT_BARS)):
            hh = df.loc[j, "high"]
            ll = df.loc[j, "low"]
            if side == 1:
                if ll <= sl:
                    exit_px = sl; reason = "SL"; exit_bar = j; break
                if hh >= tp:
                    exit_px = tp; reason = "TP"; exit_bar = j; break
            else:
                if hh >= sl:
                    exit_px = sl; reason = "SL"; exit_bar = j; break
                if ll <= tp:
                    exit_px = tp; reason = "TP"; exit_bar = j; break
        if exit_px is None:
            exit_bar = min(len(df) - 1, i_entry + BT_TIMEOUT_BARS - 1)
            exit_px = df.loc[exit_bar, "close"]
            reason = "TIME"
        pnl = side * (exit_px - entry_px)
        i_last_exit = exit_bar
        trades.append({
            "ts_bar": r["ts_bar"],
            "touch_level": r["touch_level"],
            "touch_dir": r["touch_dir"],
            "imb_norm": r["imb_norm"],
            "imb_aligned": aligned,
            "side": side,
            "entry_px": entry_px,
            "exit_px": exit_px,
            "reason": reason,
            "reason_entry": reason_entry,
            "pnl": float(pnl),
            "regime": r.get("regime"),
        })
    return pd.DataFrame(trades)


def summarize_trades(tr: pd.DataFrame, label: str) -> dict:
    if tr.empty:
        return {"label": label, "n": 0}
    ret = tr["pnl"]
    return {
        "label": label,
        "n": int(len(tr)),
        "total_pt": float(ret.sum()),
        "mean_pt": float(ret.mean()),
        "win_rate": float((ret > 0).mean()),
        "sharpe_per_trade": float(ret.mean() / (ret.std() + 1e-9)),
        "tp_rate": float((tr["reason"] == "TP").mean()),
        "sl_rate": float((tr["reason"] == "SL").mean()),
        "time_rate": float((tr["reason"] == "TIME").mean()),
    }


# ---------------------------- STEP 7: Plots ---------------------------------
def plot_imb_distribution(touches: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(touches["imb_norm"].dropna(), bins=60, color="#447", alpha=0.85)
    ax.axvline(0, color="k", lw=0.8)
    ax.set_title("Imbalance norm (buy-sell)/total in 60s pre-touch")
    ax.set_xlabel("imbalance_norm")
    ax.set_ylabel("count")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "imbalance_distribution.png", dpi=120)
    plt.close(fig)


def plot_p_breakout_vs_imb(touches: pd.DataFrame):
    t = touches.dropna(subset=["imb_norm", "outcome"]).copy()
    t = t[t["outcome"].isin(["BREAKOUT", "BOUNCE"])].copy()
    if t.empty: return
    t["imb_aligned"] = np.where(t["touch_dir"] == "from_below", t["imb_norm"], -t["imb_norm"])
    try:
        t["bin"] = pd.qcut(t["imb_aligned"], 10, duplicates="drop")
    except ValueError:
        return
    g = t.groupby("bin").agg(p_breakout=("outcome", lambda s: (s == "BREAKOUT").mean()),
                              n=("outcome", "size"),
                              x=("imb_aligned", "mean"))
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(g["x"], g["p_breakout"], marker="o")
    ax.axhline(0.5, color="k", lw=0.5, ls="--")
    ax.set_xlabel("imbalance aligned with attack direction")
    ax.set_ylabel("P(BREAKOUT | touch)")
    ax.set_title("P(breakout) vs aggressor imbalance")
    for xi, yi, ni in zip(g["x"], g["p_breakout"], g["n"]):
        ax.annotate(f"n={ni}", (xi, yi), fontsize=7, xytext=(0, 4), textcoords="offset points")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "p_breakout_vs_imbalance.png", dpi=120)
    plt.close(fig)


def plot_roc(lr: dict):
    if not lr or "fpr" not in lr: return
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot(lr["fpr"], lr["tpr"], label=f"AUC={lr['auc']:.3f}")
    ax.plot([0, 1], [0, 1], "k--", lw=0.8)
    ax.set_xlabel("FPR"); ax.set_ylabel("TPR")
    ax.set_title("ROC  logistic P(BREAKOUT)")
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "roc_breakout.png", dpi=120)
    plt.close(fig)


def plot_pnl_cum(trades_by_strat: dict[str, pd.DataFrame]):
    fig, ax = plt.subplots(figsize=(10, 5))
    for name, tr in trades_by_strat.items():
        if tr.empty: continue
        c = tr.sort_values("ts_bar")["pnl"].cumsum().values
        ax.plot(c, label=f"{name} (n={len(tr)}, tot={tr['pnl'].sum():.1f})")
    ax.axhline(0, color="k", lw=0.5)
    ax.set_xlabel("trade #"); ax.set_ylabel("cum pnl (pt)")
    ax.set_title(f"Backtest cumulative P&L  TP={TP_PT}/SL={SL_PT}")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "pnl_cumulative.png", dpi=120)
    plt.close(fig)


def plot_threshold_scan(df: pd.DataFrame):
    """Scan imb_thresh from 0 to 0.6, record total P&L and Sharpe for hybrid."""
    rows = []
    for th in np.arange(0.0, 0.55, 0.05):
        tr = backtest(df, imb_thresh=float(th), strat="hybrid")
        if tr.empty:
            rows.append({"thresh": th, "n": 0, "total": 0, "sharpe": 0})
            continue
        ret = tr["pnl"]
        rows.append({
            "thresh": float(th),
            "n": int(len(tr)),
            "total": float(ret.sum()),
            "sharpe": float(ret.mean() / (ret.std() + 1e-9)),
            "wr": float((ret > 0).mean()),
        })
    tbl = pd.DataFrame(rows)
    tbl.to_csv(OUT_DIR / "threshold_scan.csv", index=False)
    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.plot(tbl["thresh"], tbl["total"], "o-", color="C0", label="total pt")
    ax1.set_xlabel("imb threshold")
    ax1.set_ylabel("total pt", color="C0")
    ax2 = ax1.twinx()
    ax2.plot(tbl["thresh"], tbl["sharpe"], "s-", color="C3", label="sharpe/trade")
    ax2.set_ylabel("sharpe/trade", color="C3")
    ax1.axhline(0, color="k", lw=0.5)
    ax1.set_title("Hybrid backtest threshold scan")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "threshold_scan.png", dpi=120)
    plt.close(fig)
    return tbl


# ---------------------------- MAIN ------------------------------------------
def main():
    t0 = time.time()
    log(f"=== STUDIO E1 - AGGRESSOR IMBALANCE ===")
    log(f"GEX_DB  = {GEX_DB}")
    log(f"HIST_DB = {HIST_DB}")
    log(f"OUT_DIR = {OUT_DIR}")

    gex = load_gex_bars()
    ts_min = gex["ts_bar"].min()
    ts_max = gex["ts_bar"].max() + pd.Timedelta(hours=1)
    log(f"Study window: {ts_min} -> {ts_max}")

    px = load_price_bars(ts_min, ts_max)
    trades_s = load_trades_per_second(ts_min, ts_max)

    df = assemble(gex, px)
    df = regime_from_net_gex(df)
    df = tag_touches(df)
    df = compute_imbalance(df, trades_s)
    df = tag_outcomes(df)

    touches = df[df["touch_level"].notna()].copy()
    log(f"Usable touches (with imb): {touches['imb_norm'].notna().sum():,}")

    # Outcome tables
    ct = outcome_stats(touches)
    ct.to_csv(OUT_DIR / "outcome_by_imb_quintile.csv")
    log(f"Outcome by imbalance quintile:\n{ct}")

    g = outcome_by_level_and_imb_sign(touches)
    g.to_csv(OUT_DIR / "outcome_by_level_and_imbsign.csv")
    log(f"Outcome by level x imb_sign:\n{g}")

    # Logistic regression
    lr = logistic_regression(touches)
    if lr:
        log(f"Logistic regression n={lr['n']} AUC={lr['auc']:.3f} coefs={lr['coefs']}")
    (OUT_DIR / "logistic.json").write_text(json.dumps({k: v for k, v in lr.items() if k not in ('fpr','tpr')}, indent=2), encoding="utf-8")

    # Plots - exploratory
    plot_imb_distribution(touches)
    plot_p_breakout_vs_imb(touches)
    plot_roc(lr)

    # Backtest: scan threshold
    log("Backtest: scanning thresholds...")
    scan = plot_threshold_scan(df)
    log(f"Threshold scan:\n{scan}")

    # Fixed-threshold backtests
    strat_trades = {}
    summaries = []
    for strat in ["hybrid", "breakout_only", "bounce_only"]:
        tr = backtest(df, imb_thresh=IMB_THRESHOLD, strat=strat)
        strat_trades[strat] = tr
        s = summarize_trades(tr, label=f"{strat}@th={IMB_THRESHOLD}")
        summaries.append(s)
        log(f"  {strat}: {s}")

    pd.DataFrame(summaries).to_csv(OUT_DIR / "backtest_summary.csv", index=False)
    for k, v in strat_trades.items():
        if not v.empty:
            v.to_csv(OUT_DIR / f"trades_{k}.csv", index=False)

    plot_pnl_cum(strat_trades)

    # Save touches with full annotations for downstream use
    touches.to_csv(OUT_DIR / "touches.csv", index=False)

    # Final summary JSON
    summary = {
        "bars_total": int(len(df)),
        "touches_total": int(len(touches)),
        "touches_with_imb": int(touches['imb_norm'].notna().sum()),
        "outcome_counts": touches["outcome"].value_counts().to_dict() if "outcome" in touches else {},
        "logistic": {k: v for k, v in lr.items() if k not in ('fpr', 'tpr')} if lr else None,
        "backtests": summaries,
        "elapsed_sec": round(time.time() - t0, 1),
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log(f"=== DONE in {summary['elapsed_sec']}s ===")
    save_log()


if __name__ == "__main__":
    main()
