#!/usr/bin/env python3
"""
ml_train_v42_gexbot_combined.py — V42 GEXBot Combined ML Trainer

LightGBM DART + Optuna hyperparameter search on features_v4_firsttouch.parquet.
Trains models for multiple targets: session_close_dir, touch_r1_dir, touch_r2_dir, r1_bounce.

Split strategy (by session_date):
  - Train:      sessions [0, N-20)
  - Validation:  sessions [N-20, N-10)
  - Test holdout: sessions [N-10, N)

Usage:
  python scripts/ml_train_v42_gexbot_combined.py                  # full run
  python scripts/ml_train_v42_gexbot_combined.py --dry-run        # show data shape only
  python scripts/ml_train_v42_gexbot_combined.py --trials 50      # fewer trials
  python scripts/ml_train_v42_gexbot_combined.py --budget 3600    # 1h budget
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    log_loss,
    roc_auc_score,
)

warnings.filterwarnings("ignore", category=UserWarning)
optuna.logging.set_verbosity(optuna.logging.WARNING)

# ── Paths ────────────────────────────────────────────────────────────────
FEATURES_PATH = Path("C:/Users/annal/Desktop/ML DATABASE/training/features_v4_firsttouch.parquet")
OUTPUT_DIR = Path("C:/Users/annal/Desktop/ML DATABASE/training")
MODELS_DIR = OUTPUT_DIR / "models_v42"

# ── Targets ──────────────────────────────────────────────────────────────
TARGETS = [
    {
        "name": "session_close_dir",
        "label_col": "label_session_close_dir",
        "task": "binary",           # 1=up, -1=down → remap to 0/1
        "filter_col": None,
        "remap": {-1.0: 0, 0.0: None, 1.0: 1},  # drop 0 (flat) days
    },
    {
        "name": "touch_r1_dir",
        "label_col": "label_touch_r1_dir",
        "task": "multiclass",       # -1, 0, 1
        "filter_col": None,
        "remap": {-1: 0, 0: 1, 1: 2},
    },
    {
        "name": "touch_r2_dir",
        "label_col": "label_touch_r2_dir",
        "task": "multiclass",
        "filter_col": None,
        "remap": {-1: 0, 0: 1, 1: 2},
    },
    {
        "name": "r1_bounce",
        "label_col": "label_r1_bounce",
        "task": "binary",           # 0/1
        "filter_col": None,
        "remap": None,
    },
]

# ── Feature columns (exclude labels, identifiers, timestamps) ───────────
EXCLUDE_PREFIXES = ("label_", "ts_utc", "session_date", "es_price_now")
# String columns to exclude
STRING_COLS = {"c_regime", "h_spine_type", "h_session_phase"}


def load_features() -> pd.DataFrame:
    """Load parquet and prepare feature matrix."""
    print(f"[LOAD] Reading {FEATURES_PATH} ...")
    df = pd.read_parquet(FEATURES_PATH)
    print(f"[LOAD] Shape: {df.shape}, sessions: {df['session_date'].nunique()}")
    return df


def get_feature_cols(df: pd.DataFrame) -> list[str]:
    """Get numeric feature columns, excluding labels and identifiers."""
    cols = []
    for c in df.columns:
        if any(c.startswith(p) for p in EXCLUDE_PREFIXES):
            continue
        if c in STRING_COLS:
            continue
        if df[c].dtype in ("object", "str"):
            continue
        if df[c].dtype == "bool":
            continue
        cols.append(c)
    return cols


def split_by_session(df: pd.DataFrame, n_val: int = 10, n_test: int = 10):
    """Split data by session date into train/val/test."""
    sessions = sorted(df["session_date"].unique())
    n = len(sessions)

    train_sessions = sessions[: n - n_val - n_test]
    val_sessions = sessions[n - n_val - n_test : n - n_test]
    test_sessions = sessions[n - n_test :]

    print(f"[SPLIT] Total sessions: {n}")
    print(f"[SPLIT] Train: {len(train_sessions)} sessions ({train_sessions[0]} to {train_sessions[-1]})")
    print(f"[SPLIT] Val:   {len(val_sessions)} sessions ({val_sessions[0]} to {val_sessions[-1]})")
    print(f"[SPLIT] Test:  {len(test_sessions)} sessions ({test_sessions[0]} to {test_sessions[-1]})")

    train_mask = df["session_date"].isin(train_sessions)
    val_mask = df["session_date"].isin(val_sessions)
    test_mask = df["session_date"].isin(test_sessions)

    return df[train_mask], df[val_mask], df[test_mask]


def prepare_xy(df: pd.DataFrame, target: dict, feature_cols: list[str]):
    """Extract X, y from dataframe for a given target."""
    label_col = target["label_col"]
    remap = target["remap"]

    y = df[label_col].copy()

    # Apply remap if needed
    if remap is not None:
        y = y.map(remap)

    # Drop rows where label is NaN (unmapped or missing)
    valid = y.notna()
    X = df.loc[valid, feature_cols].values.astype(np.float64)
    y = y[valid].values.astype(np.float64)

    # Replace any remaining NaN in features with NaN (LightGBM handles them)
    return X, y


def objective_factory(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    task: str,
    feature_names: list[str],
):
    """Create Optuna objective for LightGBM DART tuning."""

    def objective(trial: optuna.Trial) -> float:
        params = {
            "boosting_type": "dart",
            "objective": "binary" if task == "binary" else "multiclass",
            "metric": "auc" if task == "binary" else "multi_logloss",
            "verbosity": -1,
            "n_jobs": 4,  # limita CPU: usa solo 4 core invece di tutti
            "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.15, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 15, 127),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 100),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
            "drop_rate": trial.suggest_float("drop_rate", 0.05, 0.3),
            "skip_drop": trial.suggest_float("skip_drop", 0.3, 0.7),
            "max_drop": trial.suggest_int("max_drop", 20, 80),
        }

        if task == "binary":
            params["is_unbalance"] = True
        else:
            params["num_class"] = 3

        n_rounds = trial.suggest_int("n_rounds", 200, 1500)

        dtrain = lgb.Dataset(X_train, label=y_train, feature_name=feature_names, free_raw_data=False)
        dval = lgb.Dataset(X_val, label=y_val, feature_name=feature_names, free_raw_data=False)

        callbacks = [
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=0),
        ]

        model = lgb.train(
            params,
            dtrain,
            num_boost_round=n_rounds,
            valid_sets=[dval],
            callbacks=callbacks,
        )

        y_pred = model.predict(X_val)

        if task == "binary":
            try:
                score = roc_auc_score(y_val, y_pred)
            except ValueError:
                score = 0.5
            return score  # maximize
        else:
            # For multiclass, minimize logloss
            try:
                score = log_loss(y_val, y_pred)
            except ValueError:
                score = 10.0
            return score  # minimize

    return objective


def train_final_model(
    params: dict,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    task: str,
    feature_names: list[str],
) -> lgb.Booster:
    """Train final model with best params on train+val data."""
    # Combine train+val for final model
    X_all = np.vstack([X_train, X_val])
    y_all = np.concatenate([y_train, y_val])

    n_rounds = params.pop("n_rounds", 800)

    base = {
        "boosting_type": "dart",
        "objective": "binary" if task == "binary" else "multiclass",
        "metric": "auc" if task == "binary" else "multi_logloss",
        "verbosity": -1,
        "n_jobs": 4,  # limita CPU: usa solo 4 core invece di tutti
    }
    if task == "binary":
        base["is_unbalance"] = True
    else:
        base["num_class"] = 3

    base.update(params)

    dtrain = lgb.Dataset(X_all, label=y_all, feature_name=feature_names, free_raw_data=False)

    model = lgb.train(base, dtrain, num_boost_round=n_rounds)
    return model


def evaluate_on_holdout(
    model: lgb.Booster,
    X_test: np.ndarray,
    y_test: np.ndarray,
    task: str,
    target_name: str,
) -> dict:
    """Evaluate model on holdout test set."""
    y_pred_raw = model.predict(X_test)
    metrics = {"n_test": len(X_test)}

    if task == "binary":
        y_pred_class = (y_pred_raw >= 0.5).astype(int)
        try:
            metrics["auc"] = float(roc_auc_score(y_test, y_pred_raw))
        except ValueError:
            metrics["auc"] = None
        metrics["accuracy"] = float(accuracy_score(y_test, y_pred_class))
        metrics["f1"] = float(f1_score(y_test, y_pred_class, zero_division=0))
        try:
            metrics["logloss"] = float(log_loss(y_test, y_pred_raw))
        except ValueError:
            metrics["logloss"] = None
    else:
        y_pred_class = np.argmax(y_pred_raw, axis=1)
        metrics["accuracy"] = float(accuracy_score(y_test, y_pred_class))
        metrics["f1_macro"] = float(f1_score(y_test, y_pred_class, average="macro", zero_division=0))
        try:
            metrics["logloss"] = float(log_loss(y_test, y_pred_raw))
        except ValueError:
            metrics["logloss"] = None
        try:
            metrics["auc_ovr"] = float(roc_auc_score(y_test, y_pred_raw, multi_class="ovr"))
        except ValueError:
            metrics["auc_ovr"] = None

    print(f"\n  [{target_name}] HOLDOUT RESULTS:")
    for k, v in metrics.items():
        if k == "n_test":
            continue
        v_str = f"{v:.4f}" if isinstance(v, float) else str(v)
        print(f"    {k:15s}: {v_str}")

    return metrics


def main():
    parser = argparse.ArgumentParser(description="V42 GEXBot Combined ML Trainer")
    parser.add_argument("--dry-run", action="store_true", help="Show data shape only")
    parser.add_argument("--trials", type=int, default=200, help="Optuna trials (default: 200)")
    parser.add_argument("--budget", type=int, default=7200, help="Total time budget in seconds (default: 7200 = 2h)")
    parser.add_argument("--targets", nargs="*", help="Specific targets to train (default: all)")
    args = parser.parse_args()

    print("=" * 70)
    print("V42 GEXBot Combined ML Trainer")
    print("=" * 70)
    print(f"  Features:  {FEATURES_PATH}")
    print(f"  Output:    {OUTPUT_DIR}")
    print(f"  Trials:    {args.trials}")
    print(f"  Budget:    {args.budget}s ({args.budget/3600:.1f}h)")
    print(f"  Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Load data
    df = load_features()
    feature_cols = get_feature_cols(df)
    print(f"[FEATURES] {len(feature_cols)} numeric features selected")

    # Split
    df_train, df_val, df_test = split_by_session(df)

    if args.dry_run:
        print(f"\n[DRY RUN] Train: {len(df_train):,} rows")
        print(f"[DRY RUN] Val:   {len(df_val):,} rows")
        print(f"[DRY RUN] Test:  {len(df_test):,} rows")
        print(f"[DRY RUN] Features: {feature_cols[:20]}...")
        return

    # Create output dirs
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = OUTPUT_DIR / f"v42_run_{run_ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # Filter targets if specified
    targets = TARGETS
    if args.targets:
        targets = [t for t in TARGETS if t["name"] in args.targets]

    budget_per_target = args.budget / len(targets)
    all_results = {}
    global_start = time.time()

    for target in targets:
        tname = target["name"]
        task = target["task"]
        print()
        print("=" * 70)
        print(f"TARGET: {tname} ({task})")
        print("=" * 70)

        # Prepare data
        X_train, y_train = prepare_xy(df_train, target, feature_cols)
        X_val, y_val = prepare_xy(df_val, target, feature_cols)
        X_test, y_test = prepare_xy(df_test, target, feature_cols)

        print(f"  Train: {X_train.shape[0]:,} samples, {X_train.shape[1]} features")
        print(f"  Val:   {X_val.shape[0]:,} samples")
        print(f"  Test:  {X_test.shape[0]:,} samples")
        print(f"  Label balance (train): {np.unique(y_train, return_counts=True)}")

        if len(X_train) < 100 or len(X_val) < 50:
            print(f"  SKIP: insufficient data")
            continue

        # Optuna search
        direction = "maximize" if task == "binary" else "minimize"
        study = optuna.create_study(
            direction=direction,
            study_name=f"v42_{tname}",
            sampler=optuna.samplers.TPESampler(seed=42),
        )

        objective = objective_factory(X_train, y_train, X_val, y_val, task, feature_cols)

        target_start = time.time()

        def time_callback(study, trial):
            elapsed = time.time() - target_start
            if elapsed > budget_per_target:
                study.stop()

        print(f"\n  Optuna search: {args.trials} trials, budget {budget_per_target:.0f}s ...")
        study.optimize(
            objective,
            n_trials=args.trials,
            callbacks=[time_callback],
            show_progress_bar=False,
        )

        best = study.best_trial
        print(f"  Best trial #{best.number}: value={best.value:.4f}")
        print(f"  Best params: {json.dumps(best.params, indent=4)}")
        print(f"  Trials completed: {len(study.trials)}")

        # Train final model with best params
        print(f"\n  Training final model on train+val ...")
        best_params = best.params.copy()
        final_model = train_final_model(
            best_params, X_train, y_train, X_val, y_val, task, feature_cols
        )

        # Evaluate on holdout
        holdout_metrics = evaluate_on_holdout(final_model, X_test, y_test, task, tname)

        # Feature importance
        importance = final_model.feature_importance(importance_type="gain")
        feat_imp = sorted(zip(feature_cols, importance), key=lambda x: x[1], reverse=True)
        print(f"\n  Top 10 features:")
        for feat, imp in feat_imp[:10]:
            print(f"    {feat:35s}: {imp:.1f}")

        # Save model
        model_path = MODELS_DIR / f"v42_{tname}.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(final_model, f)
        print(f"  Model saved: {model_path}")

        # Collect results
        all_results[tname] = {
            "task": task,
            "best_trial": best.number,
            "best_value": best.value,
            "best_params": best.params,
            "n_trials": len(study.trials),
            "holdout_metrics": holdout_metrics,
            "top_features": feat_imp[:20],
            "train_samples": len(X_train),
            "val_samples": len(X_val),
            "test_samples": len(X_test),
        }

    # Save summary
    elapsed_total = time.time() - global_start
    summary = {
        "version": "v42",
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": elapsed_total,
        "n_features": len(feature_cols),
        "feature_cols": feature_cols,
        "targets": {},
    }

    for tname, result in all_results.items():
        summary["targets"][tname] = {
            "task": result["task"],
            "best_trial": result["best_trial"],
            "best_value": result["best_value"],
            "best_params": result["best_params"],
            "n_trials": result["n_trials"],
            "holdout_metrics": result["holdout_metrics"],
            "top_features": [(f, float(i)) for f, i in result["top_features"]],
            "train_samples": result["train_samples"],
            "val_samples": result["val_samples"],
            "test_samples": result["test_samples"],
        }

    summary_path = run_dir / "summary_v42.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\n[SAVED] Summary: {summary_path}")

    # Final report
    print()
    print("=" * 70)
    print("V42 FINAL REPORT")
    print("=" * 70)
    print(f"  Total time: {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")
    print(f"  Features:   {len(feature_cols)}")
    print()
    for tname, result in all_results.items():
        hm = result["holdout_metrics"]
        task = result["task"]
        print(f"  {tname}:")
        if task == "binary":
            auc = hm.get("auc", "N/A")
            auc_str = f"{auc:.4f}" if isinstance(auc, float) else auc
            print(f"    AUC={auc_str}  ACC={hm['accuracy']:.4f}  F1={hm['f1']:.4f}")
        else:
            print(f"    ACC={hm['accuracy']:.4f}  F1_macro={hm['f1_macro']:.4f}")
        print(f"    Optuna best: {result['best_value']:.4f} ({result['n_trials']} trials)")
    print()
    print("DONE.")


if __name__ == "__main__":
    main()
