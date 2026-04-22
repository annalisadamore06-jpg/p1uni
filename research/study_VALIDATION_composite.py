"""
VALIDATION COMPOSITA - 5 REGOLE
================================

Ogni regola e' validata su 5 dimensioni:
    a) Walk-forward OOS: split date 70/30, calibra thresholds su train,
       testa su test.
    b) Sensitivity thresholds.
    c) Slippage: -1 pt/trade (0.5 entry + 0.5 exit).
    d) Correlazione trade fra regole.
    e) Max drawdown del portafoglio composito.

Regole:
    R1  drr_z >= +2 AND regime=TRANSITION  -> SHORT
    R2  gex_skew < -0.5 AND regime != TRANSITION -> SHORT
    R3  Fade call_wall in PINNING (spot touch CW from_below) -> SHORT
    R4  Breakout call_wall in AMPLIFICATION + aligned>=5/9 -> LONG
    R5  Spot > call_wall_oi (above CW, mean reversion) -> SHORT

Input:
    - ml_gold_20260407_211327.duckdb : gex_summary, gex_strikes
    - p1uni_history.duckdb            : historical_trades (OHLC ES)
    - research/results/F1/alignment_timeline.csv
    - research/results/E1/touches.csv (usato per benchmark cross-check)

Output in research/results/VALIDATION/
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

# --------------------------- CONFIG -----------------------------------------
GEX_DB = r"C:\Users\annal\Desktop\ML DATABASE\backups\phase1\ml_gold_20260407_211327.duckdb"
HIST_DB = r"C:\Users\annal\Desktop\P1UNI\data\p1uni_history.duckdb"
RESEARCH_ROOT = Path(r"C:\Users\annal\Desktop\P1UNI\research")
OUT_DIR = RESEARCH_ROOT / "results" / "VALIDATION"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TICKER = "ES_SPX"
TICKER_TRADES = "ES"
HUB = "classic"
AGG = "gex_full"
BAR_MIN = 5

RTH_START_UTC = 13 * 60 + 30
RTH_END_UTC = 20 * 60

ROLL_DAYS = 20
Z_PIN, Z_AMP = 0.5, -0.5

TP_PT = 12.0
SL_PT = 8.0
BT_TIMEOUT_BARS = 6
BT_COOLDOWN_BARS = 3
SLIPPAGE_PT = 1.0
TRAIN_FRAC = 0.70

TOUCH_BAND = 3.0

# --------------------------- LOG --------------------------------------------
LOG_LINES: list[str] = []
def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)

def save_log():
    (OUT_DIR / "run.log").write_text("\n".join(LOG_LINES), encoding="utf-8")


# --------------------------- DATA LOAD --------------------------------------
def load_gex_summary() -> pd.DataFrame:
    log("Loading gex_summary...")
    con = duckdb.connect(GEX_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df = con.execute(f"""
        SELECT time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
               AVG(spot)         AS spot,
               AVG(zero_gamma)   AS zero_gamma,
               AVG(call_wall_oi) AS call_wall_oi,
               AVG(put_wall_oi)  AS put_wall_oi,
               AVG(net_gex_oi)   AS net_gex_oi,
               AVG(delta_rr)     AS delta_rr
        FROM gex_summary
        WHERE ticker='{TICKER}' AND hub='{HUB}' AND aggregation='{AGG}'
        GROUP BY ts_bar
        ORDER BY ts_bar
    """).df()
    con.close()
    log(f"  gex bars: {len(df):,}")
    return df


def load_gex_skew() -> pd.DataFrame:
    """Compute weighted GEX skew per 5-min bar from gex_strikes.
       skew = M3 / M2**1.5 where M3/M2 are centered moments of strike
       distribution weighted by |gex_oi|.
       Positive skew = right-tail gamma (calls above); negative = puts dominant.
    """
    log("Computing GEX skew from gex_strikes...")
    con = duckdb.connect(GEX_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    con.execute("PRAGMA memory_limit='110GB'")
    df = con.execute(f"""
        WITH per_bar_strike AS (
            SELECT time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
                   strike, AVG(ABS(gex_oi)) AS w
            FROM gex_strikes
            WHERE ticker='{TICKER}' AND hub='{HUB}' AND aggregation='{AGG}'
            GROUP BY 1, 2
        ),
        moments AS (
            SELECT ts_bar,
                   SUM(w) AS S0,
                   SUM(w*strike) AS S1,
                   SUM(w*strike*strike) AS S2,
                   SUM(w*POW(strike,3)) AS S3
            FROM per_bar_strike
            GROUP BY ts_bar
        )
        SELECT ts_bar,
               CASE WHEN S0 > 0 THEN S1/S0 ELSE NULL END AS mean_s,
               CASE WHEN S0 > 0 THEN S2/S0 - POW(S1/S0,2) ELSE NULL END AS var_s,
               CASE WHEN S0 > 0 THEN
                    (S3/S0 - 3*(S2/S0)*(S1/S0) + 2*POW(S1/S0,3))
               ELSE NULL END AS m3
        FROM moments
        ORDER BY ts_bar
    """).df()
    con.close()
    df["std_s"] = np.sqrt(df["var_s"].clip(lower=0))
    df["gex_skew"] = df["m3"] / (df["std_s"] ** 3 + 1e-9)
    df = df[["ts_bar", "gex_skew"]]
    log(f"  skew rows: {len(df):,}")
    return df


def load_es_ohlc(ts_min, ts_max) -> pd.DataFrame:
    log("Loading ES OHLC...")
    con = duckdb.connect(HIST_DB, read_only=True)
    con.execute("PRAGMA threads=32")
    df = con.execute(f"""
        SELECT time_bucket(INTERVAL '{BAR_MIN} minutes', ts_event) AS ts_bar,
               first(price ORDER BY ts_event) AS open,
               MAX(price) AS high,
               MIN(price) AS low,
               last(price ORDER BY ts_event) AS close
        FROM historical_trades
        WHERE ticker='{TICKER_TRADES}'
          AND ts_event >= TIMESTAMP '{ts_min}'
          AND ts_event <  TIMESTAMP '{ts_max}'
        GROUP BY ts_bar
        ORDER BY ts_bar
    """).df()
    con.close()
    log(f"  ES ohlc bars: {len(df):,}")
    return df


def load_alignment() -> pd.DataFrame:
    p = RESEARCH_ROOT / "results" / "F1" / "alignment_timeline.csv"
    df = pd.read_csv(p, parse_dates=["ts_bar"])
    cols = ["ts_bar", "align_max", "align_score", "majority_regime", "aligned_5_9", "n_avail"]
    return df[cols]


# --------------------------- FEATURE BUILD ----------------------------------
def build_features() -> pd.DataFrame:
    gex = load_gex_summary()
    skew = load_gex_skew()
    df = gex.merge(skew, on="ts_bar", how="left")
    df["date"] = df["ts_bar"].dt.date

    # Regime A1: z-score rolling 20d of daily mean net_gex_oi
    daily = df.groupby("date")["net_gex_oi"].mean()
    rmu = daily.rolling(ROLL_DAYS, min_periods=5).mean()
    rsd = daily.rolling(ROLL_DAYS, min_periods=5).std()
    df["regime_z"] = df["date"].map((daily - rmu) / rsd)
    df["regime"] = np.select(
        [df["regime_z"] > Z_PIN, df["regime_z"] < Z_AMP],
        ["PINNING", "AMPLIFICATION"],
        default="TRANSITION",
    )

    # drr_z rolling 20d on bar-level delta_rr (intraday)
    # Use rolling window on bar count -> ~20*78bars = 1560 bars
    rroll = df["delta_rr"].rolling(1560, min_periods=500)
    df["drr_mu"] = rroll.mean()
    df["drr_sd"] = rroll.std()
    df["drr_z"] = (df["delta_rr"] - df["drr_mu"]) / (df["drr_sd"] + 1e-9)

    # Load ES price
    ts_min = df["ts_bar"].min()
    ts_max = df["ts_bar"].max() + pd.Timedelta(hours=2)
    ohlc = load_es_ohlc(ts_min, ts_max)
    df = df.merge(ohlc, on="ts_bar", how="inner")

    # Alignment
    al = load_alignment()
    df = df.merge(al, on="ts_bar", how="left")

    # RTH filter
    mins = df["ts_bar"].dt.hour * 60 + df["ts_bar"].dt.minute
    df = df[(mins >= RTH_START_UTC) & (mins < RTH_END_UTC)].reset_index(drop=True)

    # Forward fill GEX levels (change slowly)
    for c in ["spot", "zero_gamma", "call_wall_oi", "put_wall_oi",
              "net_gex_oi", "delta_rr", "gex_skew", "regime_z", "regime",
              "align_max", "align_score", "majority_regime", "aligned_5_9", "n_avail"]:
        if c in df.columns:
            df[c] = df[c].ffill()

    df = df.dropna(subset=["close", "high", "low", "open", "call_wall_oi",
                             "put_wall_oi", "zero_gamma"]).reset_index(drop=True)
    log(f"Feature frame: {len(df):,} bars, {df['date'].nunique()} days")
    return df


# --------------------------- TRADE ENGINE -----------------------------------
def simulate_exit(d: pd.DataFrame, i_entry: int, side: int, tp: float, sl: float):
    """Scan forward BT_TIMEOUT_BARS bars. Return (exit_px, reason, exit_bar_idx)."""
    for j in range(i_entry, min(len(d), i_entry + BT_TIMEOUT_BARS)):
        hh, ll = d.iloc[j]["high"], d.iloc[j]["low"]
        if side == 1:
            if ll <= sl: return sl, "SL", j
            if hh >= tp: return tp, "TP", j
        else:
            if hh >= sl: return sl, "SL", j
            if ll <= tp: return tp, "TP", j
    j = min(len(d) - 1, i_entry + BT_TIMEOUT_BARS - 1)
    return d.iloc[j]["close"], "TIME", j


def run_signals(d: pd.DataFrame, signals: np.ndarray, sides: np.ndarray,
                 rule_name: str, slippage: float = 0.0) -> pd.DataFrame:
    """Generic executor: signals[i] True -> enter at bar i+1 open with sides[i]."""
    d = d.reset_index(drop=True)
    trades = []
    i_last_exit = -10**9
    n = len(d)
    for i in range(n):
        if not signals[i]:
            continue
        if i < i_last_exit + BT_COOLDOWN_BARS:
            continue
        side = int(sides[i])
        if side == 0:
            continue
        i_entry = i + 1
        if i_entry >= n: break
        entry_px = d.iloc[i_entry]["open"]
        if pd.isna(entry_px): continue
        tp = entry_px + side * TP_PT
        sl = entry_px - side * SL_PT
        exit_px, reason, exit_bar = simulate_exit(d, i_entry, side, tp, sl)
        pnl = side * (exit_px - entry_px) - slippage
        i_last_exit = exit_bar
        trades.append({
            "rule": rule_name,
            "ts_entry": d.iloc[i_entry]["ts_bar"],
            "ts_signal": d.iloc[i]["ts_bar"],
            "side": side,
            "entry_px": float(entry_px),
            "exit_px": float(exit_px),
            "reason": reason,
            "pnl": float(pnl),
            "regime": d.iloc[i]["regime"],
            "align_max": d.iloc[i].get("align_max", np.nan),
        })
    return pd.DataFrame(trades)


# --------------------------- RULE SIGNAL GENERATORS -------------------------
def rule1_signals(d: pd.DataFrame, z_thresh: float) -> tuple[np.ndarray, np.ndarray]:
    """drr_z >= +z_thresh AND regime=TRANSITION -> SHORT."""
    sig = (d["drr_z"] >= z_thresh) & (d["regime"] == "TRANSITION")
    sides = np.where(sig, -1, 0)
    return sig.fillna(False).values, sides


def rule2_signals(d: pd.DataFrame, skew_thresh: float) -> tuple[np.ndarray, np.ndarray]:
    """gex_skew < skew_thresh (negative) AND regime != TRANSITION -> SHORT."""
    sig = (d["gex_skew"] < skew_thresh) & (d["regime"] != "TRANSITION")
    sides = np.where(sig, -1, 0)
    return sig.fillna(False).values, sides


def rule3_signals(d: pd.DataFrame, touch_band: float) -> tuple[np.ndarray, np.ndarray]:
    """Fade CW in PINNING: high enters [CW-band, CW], regime=PINNING -> SHORT."""
    cw = d["call_wall_oi"]
    sig = ((d["high"] >= cw - touch_band) & (d["high"] <= cw + touch_band)
           & (d["open"] < cw + touch_band) & (d["regime"] == "PINNING"))
    sides = np.where(sig, -1, 0)
    return sig.fillna(False).values, sides


def rule4_signals(d: pd.DataFrame, align_min: int) -> tuple[np.ndarray, np.ndarray]:
    """CW breakout in AMPLIFICATION + aligned>=align_min -> LONG.
       Breakout: close > CW (prior bar close was below), regime=AMP, align_max>=min."""
    cw = d["call_wall_oi"]
    broke = (d["close"] > cw) & (d["close"].shift(1) <= cw.shift(1))
    sig = broke & (d["regime"] == "AMPLIFICATION") & (d["align_max"] >= align_min)
    sides = np.where(sig, 1, 0)
    return sig.fillna(False).values, sides


def rule5_signals(d: pd.DataFrame, margin: float) -> tuple[np.ndarray, np.ndarray]:
    """Spot > CW + margin -> SHORT (mean reversion)."""
    sig = (d["close"] > d["call_wall_oi"] + margin)
    # Add edge filter: don't re-enter every bar, only on cross
    crossed = sig & (~sig.shift(1).fillna(False))
    sides = np.where(crossed, -1, 0)
    return crossed.fillna(False).values, sides


RULES = {
    "R1_drr_shortInTrans":  ("drr_z_thresh",   [1.5, 1.75, 2.0, 2.25, 2.5],          rule1_signals),
    "R2_skew_shortNonTrans": ("skew_thresh",   [-0.3, -0.4, -0.5, -0.6, -0.7],       rule2_signals),
    "R3_CW_fadePinning":     ("touch_band",    [2.0, 3.0, 4.0, 5.0, 6.0],             rule3_signals),
    "R4_CW_breakoutAmp":     ("align_min",     [3, 4, 5, 6, 7],                        rule4_signals),
    "R5_aboveCW_short":      ("margin",        [0.0, 2.0, 4.0, 6.0, 8.0],              rule5_signals),
}

RULE_DEFAULT_PARAM = {
    "R1_drr_shortInTrans": 2.0,
    "R2_skew_shortNonTrans": -0.5,
    "R3_CW_fadePinning": 3.0,
    "R4_CW_breakoutAmp": 5,
    "R5_aboveCW_short": 0.0,
}


def split_train_test(d: pd.DataFrame, frac: float = TRAIN_FRAC):
    dates = sorted(d["date"].unique())
    split_idx = int(len(dates) * frac)
    split_date = dates[split_idx]
    train = d[d["date"] < split_date].reset_index(drop=True)
    test = d[d["date"] >= split_date].reset_index(drop=True)
    log(f"Train: {len(train):,} bars ({train['date'].nunique()} days) | Test: {len(test):,} bars ({test['date'].nunique()} days) | split={split_date}")
    return train, test


def evaluate_backtest(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {"n": 0, "total_pt": 0, "mean_pt": 0, "win_rate": 0,
                 "sharpe": 0, "tp_rate": 0, "sl_rate": 0, "max_dd": 0}
    p = trades["pnl"]
    cum = p.cumsum().values
    running_max = np.maximum.accumulate(cum)
    dd = running_max - cum
    return {
        "n": int(len(p)),
        "total_pt": float(p.sum()),
        "mean_pt": float(p.mean()),
        "win_rate": float((p > 0).mean()),
        "sharpe": float(p.mean() / (p.std() + 1e-9)),
        "tp_rate": float((trades["reason"] == "TP").mean()),
        "sl_rate": float((trades["reason"] == "SL").mean()),
        "max_dd": float(dd.max()),
    }


# --------------------------- MAIN VALIDATION --------------------------------
def validation_one_rule(d_full, rule_name, param_name, param_grid, sig_fn):
    train, test = split_train_test(d_full)

    # (a) Sensitivity on full data
    sens_rows = []
    for val in param_grid:
        sig, sides = sig_fn(d_full, val)
        tr = run_signals(d_full, sig, sides, rule_name, slippage=SLIPPAGE_PT)
        stats_ = evaluate_backtest(tr)
        stats_.update({"split": "FULL", "param": val})
        sens_rows.append(stats_)

    # (b) Calibrate best param on train
    train_rows = []
    for val in param_grid:
        sig, sides = sig_fn(train, val)
        tr = run_signals(train, sig, sides, rule_name, slippage=SLIPPAGE_PT)
        stats_ = evaluate_backtest(tr)
        stats_.update({"split": "TRAIN", "param": val})
        train_rows.append(stats_)
    # best by total_pt then sharpe
    best_train = max(train_rows, key=lambda r: (r["total_pt"], r["sharpe"]))
    best_val = best_train["param"]

    # (c) OOS test with best param
    sig, sides = sig_fn(test, best_val)
    tr_test = run_signals(test, sig, sides, rule_name, slippage=SLIPPAGE_PT)
    oos = evaluate_backtest(tr_test)
    oos.update({"split": "TEST", "param": best_val})

    # Also default param OOS
    default_val = RULE_DEFAULT_PARAM[rule_name]
    sig_d, sides_d = sig_fn(test, default_val)
    tr_default = run_signals(test, sig_d, sides_d, rule_name, slippage=SLIPPAGE_PT)
    oos_default = evaluate_backtest(tr_default)
    oos_default.update({"split": "TEST_DEFAULT", "param": default_val})

    # Full trades with default for overlap analysis
    sig_full, sides_full = sig_fn(d_full, default_val)
    tr_full_default = run_signals(d_full, sig_full, sides_full, rule_name, slippage=SLIPPAGE_PT)

    return {
        "rule": rule_name,
        "param_name": param_name,
        "best_train_param": best_val,
        "train_best": best_train,
        "test_best": oos,
        "test_default": oos_default,
        "sensitivity_full": sens_rows,
        "sensitivity_train": train_rows,
        "trades_default_full": tr_full_default,
    }


def verdict_for(res: dict) -> str:
    tb = res["test_best"]
    td = res["test_default"]
    train_b = res["train_best"]
    MIN_N = 10  # require at least 10 OOS trades for a reliable verdict
    # Both samples too small
    if tb["n"] < MIN_N and td["n"] < MIN_N:
        return "FAIL (too few OOS trades n<10)"
    # Prefer the sample with more trades if either crosses MIN_N
    if td["n"] >= MIN_N and tb["n"] < MIN_N:
        test = td
    elif tb["n"] >= MIN_N and td["n"] < MIN_N:
        test = tb
    else:
        test = tb if tb["total_pt"] >= td["total_pt"] else td
    mean = test["mean_pt"]
    train_mean = train_b["mean_pt"] if train_b["n"] >= MIN_N else None
    if mean < 0.0:
        return f"FAIL (OOS negative, mean={mean:.2f}pt)"
    if train_mean is not None and train_mean > 0 and mean < train_mean * 0.3 and mean < 0.5:
        return f"MARGINAL (overfit: train={train_mean:.2f} test={mean:.2f})"
    if mean >= 1.0:
        return f"PASS (mean={mean:.2f}pt net)"
    if mean >= 0.3:
        return f"MARGINAL (EV={mean:.2f}pt net)"
    return f"MARGINAL (low EV={mean:.2f}pt net)"


# --------------------------- CORRELATION + COMPOSITE ------------------------
def overlap_matrix(trade_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """For each pair of rules, count how many trade entries are within 5 min."""
    names = list(trade_dfs.keys())
    mat = pd.DataFrame(index=names, columns=names, dtype=float)
    for a in names:
        for b in names:
            if a == b:
                mat.loc[a, b] = len(trade_dfs[a])
                continue
            A = trade_dfs[a]["ts_entry"].values.astype("datetime64[ns]")
            B = trade_dfs[b]["ts_entry"].values.astype("datetime64[ns]")
            overlap = 0
            for t in A:
                diffs = np.abs((B - t).astype("timedelta64[m]").astype(int))
                if len(diffs) and diffs.min() <= 5:
                    overlap += 1
            mat.loc[a, b] = overlap
    return mat


def composite_backtest(trade_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Union of rule trades. Deduplicate by ts_entry (keep first rule)."""
    all_rows = []
    for rname, tr in trade_dfs.items():
        if tr.empty: continue
        tmp = tr.copy()
        tmp["source_rule"] = rname
        all_rows.append(tmp)
    if not all_rows:
        return pd.DataFrame()
    comp = pd.concat(all_rows, ignore_index=True)
    comp = comp.sort_values("ts_entry").reset_index(drop=True)
    # Dedup: if two rules fire within 5min, keep first occurrence (order unspecified)
    comp["ts_round"] = comp["ts_entry"].dt.floor("5min")
    comp = comp.drop_duplicates(subset="ts_round", keep="first").drop(columns="ts_round")
    return comp


def plot_sensitivity(results: list[dict]):
    fig, axes = plt.subplots(2, 3, figsize=(15, 9))
    axes = axes.flatten()
    for i, res in enumerate(results):
        ax = axes[i]
        df_full = pd.DataFrame(res["sensitivity_full"])
        df_train = pd.DataFrame(res["sensitivity_train"])
        ax.plot(df_full["param"], df_full["total_pt"], "o-", label="FULL")
        ax.plot(df_train["param"], df_train["total_pt"], "s--", label="TRAIN")
        ax.axhline(0, color="k", lw=0.5)
        ax.set_title(f"{res['rule']} sensitivity")
        ax.set_xlabel(res["param_name"])
        ax.set_ylabel("total pt (net of slippage)")
        ax.legend(fontsize=7)
    for j in range(len(results), len(axes)):
        axes[j].axis("off")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "sensitivity.png", dpi=120)
    plt.close(fig)


def plot_walkforward(results: list[dict]):
    fig, ax = plt.subplots(figsize=(11, 6))
    labels, trains, tests_best, tests_default = [], [], [], []
    for res in results:
        labels.append(res["rule"])
        trains.append(res["train_best"]["total_pt"])
        tests_best.append(res["test_best"]["total_pt"])
        tests_default.append(res["test_default"]["total_pt"])
    x = np.arange(len(labels))
    w = 0.28
    ax.bar(x - w, trains, w, label="TRAIN (best)", alpha=0.7)
    ax.bar(x, tests_best, w, label="TEST (best-param)", alpha=0.7)
    ax.bar(x + w, tests_default, w, label="TEST (default)", alpha=0.7)
    ax.axhline(0, color="k", lw=0.6)
    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=9)
    ax.set_ylabel("total pt (net of slippage)")
    ax.set_title("Walk-forward: TRAIN best vs TEST (best-param and default-param)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "walkforward_bars.png", dpi=120)
    plt.close(fig)


def plot_composite_equity(comp: pd.DataFrame):
    if comp.empty: return
    comp = comp.sort_values("ts_entry").reset_index(drop=True)
    cum = comp["pnl"].cumsum().values
    running_max = np.maximum.accumulate(cum)
    dd = running_max - cum
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    axes[0].plot(cum, lw=1.5)
    axes[0].set_ylabel("cum pnl (pt)")
    axes[0].axhline(0, color="k", lw=0.5)
    axes[0].set_title(f"Composite equity curve | n={len(comp)} trades | total={cum[-1]:.1f}pt | maxDD={dd.max():.1f}pt")
    axes[1].fill_between(np.arange(len(dd)), -dd, 0, color="red", alpha=0.4)
    axes[1].set_ylabel("drawdown (pt)")
    axes[1].set_xlabel("trade #")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "composite_equity.png", dpi=120)
    plt.close(fig)


# --------------------------- MAIN -------------------------------------------
def main():
    t0 = time.time()
    log("=== VALIDATION STUDY - 5 RULES ===")
    log(f"Slippage per trade: {SLIPPAGE_PT} pt")
    log(f"Train fraction: {TRAIN_FRAC}")
    log(f"TP={TP_PT} / SL={SL_PT} / timeout={BT_TIMEOUT_BARS} bars")

    d = build_features()
    d.to_csv(OUT_DIR / "features_sample.csv", index=False)
    log(f"Feature frame saved: {len(d):,} bars, {d['date'].nunique()} days")

    results = []
    for rule_name, (param_name, grid, fn) in RULES.items():
        log(f"--- VALIDATING {rule_name} ---")
        res = validation_one_rule(d, rule_name, param_name, grid, fn)
        v = verdict_for(res)
        res["verdict"] = v
        log(f"  TRAIN_BEST param={res['best_train_param']} -> {res['train_best']}")
        log(f"  TEST_BEST   param={res['best_train_param']} -> {res['test_best']}")
        log(f"  TEST_DEFAULT param={RULE_DEFAULT_PARAM[rule_name]} -> {res['test_default']}")
        log(f"  VERDICT: {v}")
        results.append(res)

    # Sensitivity tables
    sens_all = []
    for r in results:
        for s in r["sensitivity_full"]:
            row = {"rule": r["rule"], "param_name": r["param_name"], **s}
            sens_all.append(row)
    pd.DataFrame(sens_all).to_csv(OUT_DIR / "sensitivity_full.csv", index=False)
    plot_sensitivity(results)
    plot_walkforward(results)

    # Summary table
    summary_rows = []
    for r in results:
        row = {
            "rule": r["rule"],
            "param_name": r["param_name"],
            "best_train_param": r["best_train_param"],
            "default_param": RULE_DEFAULT_PARAM[r["rule"]],
            "train_n": r["train_best"]["n"],
            "train_total": r["train_best"]["total_pt"],
            "train_mean": r["train_best"]["mean_pt"],
            "train_sharpe": r["train_best"]["sharpe"],
            "test_best_n": r["test_best"]["n"],
            "test_best_total": r["test_best"]["total_pt"],
            "test_best_mean": r["test_best"]["mean_pt"],
            "test_best_sharpe": r["test_best"]["sharpe"],
            "test_default_n": r["test_default"]["n"],
            "test_default_total": r["test_default"]["total_pt"],
            "test_default_mean": r["test_default"]["mean_pt"],
            "verdict": r["verdict"],
        }
        summary_rows.append(row)
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(OUT_DIR / "validation_summary.csv", index=False)
    log("SUMMARY TABLE:")
    log(summary_df.to_string())

    # Overlap analysis
    log("--- OVERLAP MATRIX ---")
    td = {r["rule"]: r["trades_default_full"] for r in results}
    for k, v in td.items():
        if not v.empty:
            v.to_csv(OUT_DIR / f"trades_{k}.csv", index=False)
    overlap = overlap_matrix(td)
    overlap.to_csv(OUT_DIR / "overlap_matrix.csv")
    log(f"\n{overlap}")

    # Composite backtest
    log("--- COMPOSITE BACKTEST (union, dedup 5min) ---")
    comp = composite_backtest(td)
    comp.to_csv(OUT_DIR / "composite_trades.csv", index=False)
    comp_stats = evaluate_backtest(comp)
    log(f"  COMPOSITE: {comp_stats}")
    plot_composite_equity(comp)

    # Final JSON
    final = {
        "config": {
            "TP": TP_PT, "SL": SL_PT, "timeout_bars": BT_TIMEOUT_BARS,
            "slippage_pt": SLIPPAGE_PT, "train_frac": TRAIN_FRAC,
            "roll_days": ROLL_DAYS,
        },
        "data": {
            "bars_total": int(len(d)),
            "days_total": int(d["date"].nunique()),
            "date_min": str(d["date"].min()),
            "date_max": str(d["date"].max()),
        },
        "rules": [
            {k: v for k, v in r.items() if k != "trades_default_full" and k != "sensitivity_full" and k != "sensitivity_train"}
            for r in results
        ],
        "composite": comp_stats,
        "overlap_matrix": overlap.to_dict(),
        "elapsed_sec": round(time.time() - t0, 1),
    }
    (OUT_DIR / "validation_summary.json").write_text(
        json.dumps(final, indent=2, default=str), encoding="utf-8")

    log(f"=== DONE in {final['elapsed_sec']}s ===")
    save_log()


if __name__ == "__main__":
    main()
