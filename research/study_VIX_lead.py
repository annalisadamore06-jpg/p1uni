"""
study_VIX_lead.py  --  VIX Regime Lead Effect on ES
==================================================================
Da F1: VIX ha correlazione regime -0.54 con ES a lag -1 bar (VIX anticipa ES
di 5 min). Questo studio formalizza quell'intuizione come segnale direzionale.

Metodologia
-----------
1. Aggrega gex_summary a barre 5-min RTH per VIX e ES (entrambi hub='classic',
   aggregation='gex_zero').
2. Rolling z-score 20-giorni RTH di net_gex_oi VIX -> classifica regime VIX:
     z_VIX >= +0.5  -> VIX_PIN (complacency, dealer long gamma su VIX)
    -0.5 <= z <= +0.5 -> VIX_TRN
     z <= -0.5     -> VIX_AMP (stress, dealer short gamma su VIX)
3. Change-detection: transizione di regime VIX da t-1 a t.
4. Test stat: forward return ES (5/15/30/60 min) condizionale al:
     - regime VIX corrente
     - cambio di regime VIX
5. Backtest con lead 1-bar (entry barra t su segnale barra t-1 VIX):
     - VIX_PIN_to_AMP (stress che emerge)    -> SHORT ES
     - VIX_AMP_to_PIN (ritorno a calma)      -> LONG  ES
     - Altre transizioni                      -> no trade
   TP=12pt, SL=8pt, timeout=6 bars (30min), slippage 1pt.
6. Confronto con R5 (above_CW short) standalone.

Output: research/results_v2/VIX_LEAD/
"""
from __future__ import annotations
import json
import logging
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
from scipy import stats

warnings.filterwarnings("ignore")

# ============================================================================
# CONFIG
# ============================================================================
DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
TICKER_ES = "ES"
TICKER_VX = "VIX"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\VIX_LEAD")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_UTC = ("13:30", "19:55")
ROLL_DAYS = 20
Z_PIN = 0.5
Z_AMP = -0.5

TP = 12.0
SL = 8.0
TIMEOUT_BARS = 6
SLIPPAGE = 1.0
COOLDOWN_BARS = 12

# ============================================================================
# LOGGING
# ============================================================================
log_path = OUT_DIR / "run.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_path, mode="w", encoding="utf-8"),
              logging.StreamHandler()],
)
log = logging.getLogger("VIX_LEAD")


# ============================================================================
# DATA
# ============================================================================
def load_bars(ticker: str) -> pd.DataFrame:
    log.info(f"Loading {ticker} 5-min bars from ml_gold...")
    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("SET threads=32; SET memory_limit='110GB'")
    sql = f"""
    WITH src AS (
        SELECT ts_utc,
               time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS bar_start,
               spot, net_gex_oi, net_gex_vol, zero_gamma,
               call_wall_oi, put_wall_oi, delta_rr
        FROM gex_summary
        WHERE ticker='{ticker}' AND hub='classic' AND aggregation='gex_zero'
          AND spot IS NOT NULL AND net_gex_oi IS NOT NULL
    )
    SELECT bar_start AS ts,
           FIRST(spot ORDER BY ts_utc) AS open,
           MAX(spot)                    AS high,
           MIN(spot)                    AS low,
           LAST(spot ORDER BY ts_utc)   AS close,
           LAST(net_gex_oi ORDER BY ts_utc)  AS net_gex_oi,
           LAST(net_gex_vol ORDER BY ts_utc) AS net_gex_vol,
           LAST(zero_gamma ORDER BY ts_utc)  AS zero_gamma,
           LAST(call_wall_oi ORDER BY ts_utc) AS call_wall_oi,
           LAST(put_wall_oi ORDER BY ts_utc) AS put_wall_oi,
           LAST(delta_rr ORDER BY ts_utc) AS delta_rr,
           COUNT(*) AS tick_count
    FROM src
    GROUP BY bar_start
    ORDER BY bar_start
    """
    df = con.execute(sql).fetchdf()
    con.close()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    log.info(f"  {ticker}: {len(df):,} bars")
    return df


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    t = df["ts"].dt.strftime("%H:%M")
    return df[(t >= RTH_UTC[0]) & (t <= RTH_UTC[1]) & (df["ts"].dt.dayofweek < 5)].reset_index(drop=True)


def compute_regime(df: pd.DataFrame, label_prefix: str = "") -> pd.DataFrame:
    """Rolling 20d z-score of net_gex_oi. Regime: PIN / TRN / AMP."""
    d = df.copy()
    d["date"] = d["ts"].dt.date
    d = d.sort_values("ts").reset_index(drop=True)
    # Rolling window in BARS (78 per RTH day * 20 days ~ 1560 bars, but use actual session bars)
    window = ROLL_DAYS * 78
    d["net_gex_mean"] = d["net_gex_oi"].rolling(window, min_periods=window // 2).mean()
    d["net_gex_std"]  = d["net_gex_oi"].rolling(window, min_periods=window // 2).std()
    d["z"] = (d["net_gex_oi"] - d["net_gex_mean"]) / d["net_gex_std"]
    def _reg(z):
        if pd.isna(z): return np.nan
        if z >= Z_PIN: return "PIN"
        if z <= Z_AMP: return "AMP"
        return "TRN"
    d["regime"] = d["z"].apply(_reg)
    return d


# ============================================================================
# MERGE VIX + ES
# ============================================================================
def merge_vix_es(vix: pd.DataFrame, es: pd.DataFrame) -> pd.DataFrame:
    """Align VIX regime (lagged 1 bar) with ES forward returns."""
    vx = vix[["ts", "regime", "z"]].rename(columns={"regime": "regime_vix", "z": "z_vix"}).copy()
    es_cols = ["ts", "open", "high", "low", "close", "call_wall_oi", "put_wall_oi", "zero_gamma", "net_gex_oi"]
    e = es[es_cols].rename(columns={c: f"es_{c}" for c in es_cols[1:]}).copy()
    df = e.merge(vx, on="ts", how="inner").sort_values("ts").reset_index(drop=True)

    # VIX regime CHANGE (t-1 -> t)
    df["regime_vix_prev"] = df["regime_vix"].shift(1)
    df["vix_transition"] = df["regime_vix_prev"].astype(str) + "_to_" + df["regime_vix"].astype(str)

    # Forward ES returns (5/15/30/60 min)
    for h in (1, 3, 6, 12):
        df[f"es_fwd_{h*5}m"] = df["es_close"].shift(-h) - df["es_close"]
    return df


# ============================================================================
# STATS
# ============================================================================
def fwd_ret_by_regime(df: pd.DataFrame) -> pd.DataFrame:
    """Forward ES returns grouped by VIX regime (current, not change)."""
    rows = []
    for h in (5, 15, 30, 60):
        col = f"es_fwd_{h}m"
        for reg in ("PIN", "TRN", "AMP"):
            d = df[df["regime_vix"] == reg][col].dropna()
            if len(d) < 5: continue
            t_stat, p_val = stats.ttest_1samp(d, 0.0)
            rows.append({
                "regime_vix": reg, "horizon_min": h, "n": len(d),
                "mean_pts": d.mean(), "median_pts": d.median(), "std_pts": d.std(),
                "wr_pos": (d > 0).mean(), "t_stat": t_stat, "p_value": p_val,
            })
    return pd.DataFrame(rows)


def fwd_ret_by_transition(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h in (5, 15, 30, 60):
        col = f"es_fwd_{h}m"
        grp = df.groupby("vix_transition")
        for key, g in grp:
            d = g[col].dropna()
            if len(d) < 5: continue
            t_stat, p_val = stats.ttest_1samp(d, 0.0)
            rows.append({
                "transition": key, "horizon_min": h, "n": len(d),
                "mean_pts": d.mean(), "median_pts": d.median(), "std_pts": d.std(),
                "wr_pos": (d > 0).mean(), "t_stat": t_stat, "p_value": p_val,
            })
    return pd.DataFrame(rows)


# ============================================================================
# BACKTEST (1-bar lead from VIX regime change)
# ============================================================================
def backtest_vix_lead(df: pd.DataFrame, label: str = "all",
                      long_triggers=("AMP_to_PIN",),
                      short_triggers=("PIN_to_AMP",)) -> tuple[dict, pd.DataFrame]:
    d = df.copy().sort_values("ts").reset_index(drop=True)
    trades = []
    last_exit = -10**9
    for i in range(len(d) - TIMEOUT_BARS - 1):
        if i < last_exit + COOLDOWN_BARS:
            continue
        trig = d.loc[i, "vix_transition"]
        if trig in long_triggers:
            side = 1
        elif trig in short_triggers:
            side = -1
        else:
            continue
        entry = d.loc[i + 1, "es_open"]
        if pd.isna(entry): continue
        tp_px = entry + side * TP
        sl_px = entry - side * SL
        exit_px, exit_reason, exit_i = None, None, None
        for j in range(i + 1, min(i + 1 + TIMEOUT_BARS, len(d))):
            hi = d.loc[j, "es_high"]; lo = d.loc[j, "es_low"]
            if side == 1:
                if lo <= sl_px: exit_px, exit_reason, exit_i = sl_px, "SL", j; break
                if hi >= tp_px: exit_px, exit_reason, exit_i = tp_px, "TP", j; break
            else:
                if hi >= sl_px: exit_px, exit_reason, exit_i = sl_px, "SL", j; break
                if lo <= tp_px: exit_px, exit_reason, exit_i = tp_px, "TP", j; break
        if exit_px is None:
            last_j = min(i + TIMEOUT_BARS, len(d) - 1)
            exit_px = d.loc[last_j, "es_close"]
            exit_reason = "TIME"
            exit_i = last_j
        pnl = side * (exit_px - entry) - SLIPPAGE
        trades.append({
            "ts_entry": d.loc[i + 1, "ts"], "ts_exit": d.loc[exit_i, "ts"],
            "trigger": trig, "side": side, "entry": entry, "exit": exit_px,
            "exit_reason": exit_reason, "pnl": pnl,
        })
        last_exit = exit_i
    tdf = pd.DataFrame(trades)
    if tdf.empty:
        return {"label": label, "n": 0}, tdf
    stats_row = {
        "label": label,
        "n": len(tdf),
        "total_pt": float(tdf["pnl"].sum()),
        "mean_pt": float(tdf["pnl"].mean()),
        "win_rate": float((tdf["pnl"] > 0).mean()),
        "sharpe_per_trade": float(tdf["pnl"].mean() / tdf["pnl"].std()) if tdf["pnl"].std() > 0 else 0,
        "tp_rate": float((tdf["exit_reason"] == "TP").mean()),
        "sl_rate": float((tdf["exit_reason"] == "SL").mean()),
        "time_rate": float((tdf["exit_reason"] == "TIME").mean()),
        "max_dd": float((tdf["pnl"].cumsum().cummax() - tdf["pnl"].cumsum()).max()),
    }
    return stats_row, tdf


# ============================================================================
# MAIN
# ============================================================================
def main():
    t0 = time.time()
    log.info("=" * 70)
    log.info("STUDY VIX_LEAD - VIX Regime Lead Effect on ES")
    log.info("=" * 70)

    vix_raw = filter_rth(load_bars(TICKER_VX))
    es_raw  = filter_rth(load_bars(TICKER_ES))
    log.info(f"After RTH filter: VIX {len(vix_raw):,} bars, ES {len(es_raw):,} bars")

    vix = compute_regime(vix_raw, "VIX")
    es  = compute_regime(es_raw, "ES")
    log.info("Regime classification done.")
    log.info(f"VIX regime distribution:\n{vix['regime'].value_counts()}")

    df = merge_vix_es(vix, es)
    df = df.dropna(subset=["regime_vix", "regime_vix_prev"]).reset_index(drop=True)
    log.info(f"Merged VIX+ES: {len(df):,} bars with both regimes")
    log.info(f"VIX transition distribution:\n{df['vix_transition'].value_counts()}")

    # Save feature sample
    df.head(200).to_csv(OUT_DIR / "features_sample.csv", index=False)

    # === Stats: forward returns by current VIX regime ===
    stats_reg = fwd_ret_by_regime(df)
    stats_reg.to_csv(OUT_DIR / "fwd_by_vix_regime.csv", index=False)
    log.info(f"\n=== ES fwd returns by CURRENT VIX regime ===\n{stats_reg.to_string(index=False)}")

    # === Stats: forward returns by VIX transition ===
    stats_trans = fwd_ret_by_transition(df)
    stats_trans.to_csv(OUT_DIR / "fwd_by_vix_transition.csv", index=False)
    log.info(f"\n=== ES fwd returns by VIX transition ===\n{stats_trans.to_string(index=False)}")

    # === Backtest: 1-bar lead from VIX regime change ===
    log.info("\nBacktest: VIX_PIN_to_AMP -> SHORT ES, VIX_AMP_to_PIN -> LONG ES")
    stats_main, trades_main = backtest_vix_lead(df, label="vix_transition")
    log.info(f"  RESULT: {stats_main}")

    # Also try TRN transitions
    log.info("\nBacktest alt: TRN_to_AMP -> SHORT, TRN_to_PIN -> LONG")
    stats_alt, trades_alt = backtest_vix_lead(
        df, label="trn_transitions",
        long_triggers=("TRN_to_PIN",), short_triggers=("TRN_to_AMP",),
    )
    log.info(f"  RESULT: {stats_alt}")

    log.info("\nBacktest wide: all transitions that involve AMP")
    stats_wide, trades_wide = backtest_vix_lead(
        df, label="all_amp_transitions",
        long_triggers=("AMP_to_PIN", "AMP_to_TRN"),
        short_triggers=("PIN_to_AMP", "TRN_to_AMP"),
    )
    log.info(f"  RESULT: {stats_wide}")

    all_bt = pd.DataFrame([stats_main, stats_alt, stats_wide])
    all_bt.to_csv(OUT_DIR / "backtests.csv", index=False)
    trades_main.to_csv(OUT_DIR / "trades_main.csv", index=False)
    trades_wide.to_csv(OUT_DIR / "trades_wide.csv", index=False)

    # === Plot ===
    fig, ax = plt.subplots(2, 1, figsize=(11, 8))
    piv = stats_reg.pivot(index="regime_vix", columns="horizon_min", values="mean_pts")
    piv.plot(kind="bar", ax=ax[0], title="ES fwd return (pt) by VIX regime")
    ax[0].axhline(0, color="k", lw=0.5)
    ax[0].set_ylabel("Mean ES fwd ret (pt)")

    if not trades_wide.empty:
        trades_wide["cum_pnl"] = trades_wide["pnl"].cumsum()
        ax[1].plot(pd.to_datetime(trades_wide["ts_entry"]), trades_wide["cum_pnl"],
                   label=f"all_amp_transitions (n={len(trades_wide)}, total={stats_wide.get('total_pt',0):.1f}pt)")
    if not trades_main.empty:
        trades_main["cum_pnl"] = trades_main["pnl"].cumsum()
        ax[1].plot(pd.to_datetime(trades_main["ts_entry"]), trades_main["cum_pnl"],
                   label=f"PIN<->AMP only (n={len(trades_main)}, total={stats_main.get('total_pt',0):.1f}pt)")
    ax[1].set_title("Cumulative P&L (pt)")
    ax[1].legend()
    ax[1].axhline(0, color="k", lw=0.5)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "vix_lead_overview.png", dpi=120)
    plt.close()

    # === Compare with R5 (simplified: short when ES spot > call_wall_oi + 2pt) ===
    log.info("\nR5 comparison baseline: short ES when es_close > call_wall_oi + 2")
    df_r5 = df.copy()
    df_r5["r5_signal"] = (df_r5["es_close"] > df_r5["es_call_wall_oi"] + 2).astype(int)
    df_r5_trans = df_r5[df_r5["r5_signal"] == 1].copy()
    if len(df_r5_trans) > 0 and "es_fwd_30m" in df_r5_trans:
        mean_r5 = df_r5_trans["es_fwd_30m"].dropna().mean()
        n_r5 = len(df_r5_trans["es_fwd_30m"].dropna())
        log.info(f"  R5 raw signal: n={n_r5}, mean_fwd_30m={mean_r5:.3f}pt (sign-flipped for short: {-mean_r5:.3f}pt)")

    summary = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": DB_PATH,
        "bars_vix": len(vix),
        "bars_es": len(es),
        "bars_merged": len(df),
        "vix_regime_dist": vix["regime"].value_counts().to_dict(),
        "vix_transition_dist": df["vix_transition"].value_counts().to_dict(),
        "backtests": [stats_main, stats_alt, stats_wide],
        "elapsed_sec": round(time.time() - t0, 1),
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log.info(f"\n=== DONE in {summary['elapsed_sec']}s ===")
    log.info(f"Output: {OUT_DIR}")


if __name__ == "__main__":
    main()
