"""
STUDY C2b - WALL DRIFT (Inverted C2)
=====================================

HYPOTHESIS
----------
C2 tested mean-reversion TOWARD zero_gamma. C2b tests the opposite:
price DRIFTS toward the nearest wall once a zone is "established" (3
consecutive bars). Above call_wall -> mean reversion DOWN (R5 logic).

Zones:
    Z1 = ABOVE_CW   -> SHORT (squeeze fade, same as R5)
    Z2 = CW_ZG      -> LONG  (drift up to CW)
    Z3 = ZG_PW      -> SHORT (drift down to PW)
    Z4 = BELOW_PW   -> LONG  (squeeze fade)

Entry: require 3 consecutive bars in the SAME zone (confirmation).
Exit: TP=12 / SL=8 / max 120min (24 bars).
Walk-forward: 70/30 split (train stats only, OOS backtest on test).
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

warnings.filterwarnings("ignore")

# ============================================================
# CONFIG
# ============================================================
DB_PATH = r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb"
TICKER = "ES"
OUT_DIR = Path(r"C:\Users\annal\Desktop\p1-clean\research\results_v2\C2b")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BAR_MIN = 5
RTH_START_UTC = "13:30"
RTH_END_UTC = "19:55"
ZONE_CONFIRM_BARS = 3       # need 3 consecutive bars in zone to enter
TP_PTS = 12.0
SL_PTS = 8.0
SLIPPAGE = 1.0
MAX_HOLD_BARS = 24          # 120 min
COOLDOWN_BARS = 12
TRAIN_FRAC = 0.70

Z1 = "ABOVE_CW"; Z2 = "CW_ZG"; Z3 = "ZG_PW"; Z4 = "BELOW_PW"
ZONE_ORDER = [Z1, Z2, Z3, Z4]
ZONE_SIDE = {Z1: "SHORT", Z2: "LONG", Z3: "SHORT", Z4: "LONG"}

LOG_PATH = OUT_DIR / "run.log"
for h in list(logging.getLogger().handlers):
    logging.getLogger().removeHandler(h)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w"),
              logging.StreamHandler()],
)
log = logging.getLogger("C2b")


def load_bars() -> pd.DataFrame:
    q = f"""
    SELECT
        time_bucket(INTERVAL '{BAR_MIN} minutes', ts_utc) AS ts_bar,
        FIRST(spot ORDER BY ts_utc) AS s_open,
        MAX(spot) AS s_high,
        MIN(spot) AS s_low,
        LAST(spot ORDER BY ts_utc) AS s_close,
        AVG(zero_gamma)   AS zero_gamma,
        AVG(call_wall_oi) AS call_wall,
        AVG(put_wall_oi)  AS put_wall
    FROM gex_summary
    WHERE ticker='{TICKER}' AND hub='classic' AND aggregation='gex_zero'
    GROUP BY 1 ORDER BY 1
    """
    con = duckdb.connect(DB_PATH, read_only=True,
                         config={"threads": "32", "memory_limit": "110GB"})
    t0 = time.time()
    df = con.execute(q).df()
    con.close()
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)
    log.info("Loaded %s bars in %.1fs", f"{len(df):,}", time.time() - t0)
    return df


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["ts_bar"].dt
    t = ts.strftime("%H:%M")
    mask = (t >= RTH_START_UTC) & (t <= RTH_END_UTC) & ts.dayofweek.isin(range(5))
    df = df[mask].copy().reset_index(drop=True)
    valid = (df["zero_gamma"].notna() & df["call_wall"].notna() & df["put_wall"].notna()
             & (df["call_wall"] > df["put_wall"]) & (df["zero_gamma"] > 0))
    df = df[valid].copy().reset_index(drop=True)
    df["date"] = df["ts_bar"].dt.date
    log.info("RTH valid bars: %s (%s -> %s)",
             f"{len(df):,}", df["ts_bar"].min(), df["ts_bar"].max())
    return df


def classify_zones(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    s = df["s_close"]; cw = df["call_wall"]; zg = df["zero_gamma"]; pw = df["put_wall"]
    conds = [s > cw, (s <= cw) & (s > zg), (s <= zg) & (s > pw), s <= pw]
    df["zone"] = np.select(conds, ZONE_ORDER, default=None)
    df["dist_cw"] = cw - s
    df["dist_pw"] = s - pw
    df["dist_zg"] = zg - s
    return df


def detect_zone_runs(df: pd.DataFrame, min_bars: int) -> pd.Series:
    """Return a Series: count of consecutive bars in same zone (per day, reset on change)."""
    same_zone = df["zone"] == df["zone"].shift(1)
    same_day = df["date"] == df["date"].shift(1)
    run_continues = same_zone & same_day
    # Run-length encode
    run_id = (~run_continues).cumsum()
    run_len = df.groupby(run_id).cumcount() + 1
    return run_len


def backtest_walk_forward(df: pd.DataFrame) -> dict:
    """Walk-forward 70/30: compute entry signals, execute on OOS test period."""
    df = df.copy().reset_index(drop=True)
    df["run_len"] = detect_zone_runs(df, ZONE_CONFIRM_BARS)
    df["entry_ok"] = df["run_len"] >= ZONE_CONFIRM_BARS
    df["side"] = df["zone"].map(ZONE_SIDE)
    df["ts_bar"] = pd.to_datetime(df["ts_bar"], utc=True)

    unique_dates = sorted(df["date"].unique())
    split_idx = int(len(unique_dates) * TRAIN_FRAC)
    train_dates = set(unique_dates[:split_idx])
    test_dates = set(unique_dates[split_idx:])
    log.info("Walk-forward: %d train days, %d test days (split at %s)",
             len(train_dates), len(test_dates), unique_dates[split_idx])

    results = {}
    all_trades = []
    for split_name, date_set in [("TRAIN", train_dates), ("TEST", test_dates)]:
        sub = df[df["date"].isin(date_set)].reset_index(drop=True)
        trades = _run_trades(sub)
        trades["split"] = split_name
        all_trades.append(trades)
        stats = _summarize(trades, split_name)
        results[split_name] = stats
        log.info("%s: %s", split_name, json.dumps(stats, default=str))

    # By-zone stats on TEST
    test_trades = all_trades[1]
    by_zone = test_trades.groupby("zone").apply(
        lambda g: pd.Series({
            "n": len(g), "mean_pt": g["pnl_pt"].mean(),
            "wr": (g["pnl_pt"] > 0).mean(),
            "total_pt": g["pnl_pt"].sum(),
        })).reset_index()
    by_zone.to_csv(OUT_DIR / "by_zone_test.csv", index=False)
    log.info("\n=== TEST by zone ===\n%s", by_zone.to_string(index=False))

    trades_df = pd.concat(all_trades, ignore_index=True)
    trades_df.to_csv(OUT_DIR / "trades_all.csv", index=False)
    return results


def _run_trades(df: pd.DataFrame) -> pd.DataFrame:
    trades = []
    cooldown = 0
    i = 0
    n = len(df)
    while i < n - 1:
        if cooldown > 0:
            cooldown -= 1; i += 1; continue
        if not df.loc[i, "entry_ok"] or pd.isna(df.loc[i, "side"]):
            i += 1; continue
        side = df.loc[i, "side"]
        # entry at next bar open
        entry_idx = i + 1
        if entry_idx >= n: break
        if df.loc[entry_idx, "date"] != df.loc[i, "date"]:
            i += 1; continue
        entry_price = df.loc[entry_idx, "s_open"]
        entry_price += (SLIPPAGE if side == "LONG" else -SLIPPAGE)
        entry_date = df.loc[entry_idx, "date"]
        entry_zone = df.loc[i, "zone"]

        tp_px = entry_price + (TP_PTS if side == "LONG" else -TP_PTS)
        sl_px = entry_price - (SL_PTS if side == "LONG" else -SL_PTS)

        exit_reason = "TIMEOUT"; exit_price = entry_price; exit_idx = entry_idx
        for k in range(1, MAX_HOLD_BARS + 1):
            j = entry_idx + k
            if j >= n: break
            if df.loc[j, "date"] != entry_date:
                exit_price = df.loc[j - 1, "s_close"]; exit_idx = j - 1; exit_reason = "EOD"; break
            hi, lo = df.loc[j, "s_high"], df.loc[j, "s_low"]
            if side == "LONG":
                if lo <= sl_px: exit_price = sl_px; exit_reason = "SL"; exit_idx = j; break
                if hi >= tp_px: exit_price = tp_px; exit_reason = "TP"; exit_idx = j; break
            else:
                if hi >= sl_px: exit_price = sl_px; exit_reason = "SL"; exit_idx = j; break
                if lo <= tp_px: exit_price = tp_px; exit_reason = "TP"; exit_idx = j; break
            if k == MAX_HOLD_BARS:
                exit_price = df.loc[j, "s_close"]; exit_idx = j; exit_reason = "TIMEOUT"

        pnl = (exit_price - entry_price) if side == "LONG" else (entry_price - exit_price)
        pnl -= SLIPPAGE  # exit slippage
        trades.append({
            "entry_ts": df.loc[entry_idx, "ts_bar"], "date": entry_date,
            "zone": entry_zone, "side": side,
            "entry_px": entry_price, "exit_px": exit_price,
            "exit_reason": exit_reason, "pnl_pt": pnl,
        })
        cooldown = COOLDOWN_BARS
        i = exit_idx + 1
    return pd.DataFrame(trades)


def _summarize(trades: pd.DataFrame, label: str) -> dict:
    if len(trades) == 0:
        return {"split": label, "n": 0}
    pnl = trades["pnl_pt"]
    eq = pnl.cumsum()
    dd = (eq.cummax() - eq).max() if len(eq) else 0.0
    return {
        "split": label, "n": int(len(trades)),
        "total_pt": float(pnl.sum()), "mean_pt": float(pnl.mean()),
        "win_rate": float((pnl > 0).mean()),
        "sharpe_per_trade": float(pnl.mean() / pnl.std(ddof=0)) if pnl.std(ddof=0) > 0 else 0.0,
        "tp_rate": float((trades["exit_reason"] == "TP").mean()),
        "sl_rate": float((trades["exit_reason"] == "SL").mean()),
        "max_dd_pt": float(dd),
    }


def make_plots(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    df["zone"].value_counts().reindex(ZONE_ORDER).plot(
        kind="bar", ax=axes[0], color=["tab:red", "tab:orange", "tab:green", "tab:blue"])
    axes[0].set_title("Zone distribution (all bars)"); axes[0].set_ylabel("bars")
    tr = pd.read_csv(OUT_DIR / "trades_all.csv")
    test = tr[tr["split"] == "TEST"].copy()
    if len(test):
        test["cum_pnl"] = test["pnl_pt"].cumsum()
        axes[1].plot(range(len(test)), test["cum_pnl"], color="tab:green")
        axes[1].set_title(f"C2b TEST equity ({len(test)} trades, total {test['pnl_pt'].sum():.1f}pt)")
        axes[1].axhline(0, color="k", ls="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "c2b_overview.png", dpi=120); plt.close()


def main():
    t0 = time.time()
    df = load_bars()
    df = filter_rth(df)
    df = classify_zones(df)
    df.to_csv(OUT_DIR / "bars_sample.csv", index=False)
    results = backtest_walk_forward(df)
    with open(OUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    make_plots(df)
    log.info("\n=== DONE in %.1fs ===", time.time() - t0)
    log.info("Output: %s", OUT_DIR)


if __name__ == "__main__":
    main()
