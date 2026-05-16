"""
06_train_readmission.py — Ray Pipeline Step 6
Train an XGBoost readmission risk model and evaluate it with C-index.

Based on logic from:
    - src/predictcare-new_readmission.ipynb

Pipeline:
    1. Load Gold dataset (train/val/test splits)
    2. Preprocess (impute missing, encode gender)
    3. Train XGBoost risk model for readmission
    4. Evaluate with concordance index (C-index)
    5. Log metrics + save model
"""

import ray
import ray.data as rd
import pandas as pd
import numpy as np
import time
import json
import os
from pathlib import Path
from sklearn.impute import SimpleImputer
from lifelines.utils import concordance_index
import xgboost as xgb
import sys

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT  = Path(__file__).resolve().parents[2]
OUT      = PROJECT / "outputs"
LOGS_DIR = PROJECT / "logs"
LOGS_DIR.mkdir(exist_ok=True)
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── Ray Init ──────────────────────────────────────────────────────────────
ray.init(ignore_reinit_error=True)
print(f"[INFO] Ray cluster resources: {ray.cluster_resources()}")

t_total = start_timer()

# ─── 1. Load Gold Dataset ───────────────────────────────────────────────────
print("\n[STEP 6.1] Loading Gold dataset ...")
t = start_timer()

# Try to load per split (written with partition)
gold_path = f"local://{OUT}/gold_dataset"
gold_df = rd.read_parquet(gold_path).to_pandas()

# Separate splits
df_train = gold_df[gold_df["split"] == "train"].copy()
df_val   = gold_df[gold_df["split"] == "val"].copy()
df_test  = gold_df[gold_df["split"] == "test"].copy()

print(f"[METRIC] Train: {df_train.shape}")
print(f"[METRIC] Val:   {df_val.shape}")
print(f"[METRIC] Test:  {df_test.shape}")
log_stage("load_gold", t)

# ─── 2. Preprocessing ───────────────────────────────────────────────────────
print("\n[STEP 6.2] Preprocessing ...")
t = start_timer()

EXCLUDE_COLS = [
    "hadm_id", "subject_id", "duration_days",
    "event_flag_mortality", "event_flag_readmission",
    "split", "admityear",
    "time_to_event_hours",
]

def preprocess(df, is_train=False, imputer=None):
    df = df.copy()

    # Encode gender
    if "gender" in df.columns:
        df["gender"] = df["gender"].map({"F": 0, "M": 1}).fillna(0)
        df["gender"] = pd.to_numeric(df["gender"], errors="coerce")

    T = df["duration_days"]
    E = df["event_flag_readmission"]

    feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS]
    X = df[feature_cols]

    if is_train:
        imp = SimpleImputer(strategy="median")
        X_imp = pd.DataFrame(imp.fit_transform(X), columns=X.columns)
        return X_imp, T, E, imp
    else:
        X_imp = pd.DataFrame(imputer.transform(X), columns=X.columns)
        return X_imp, T, E

X_train, T_train, E_train, fitted_imputer = preprocess(df_train, is_train=True)
X_val,   T_val,   E_val                   = preprocess(df_val,   imputer=fitted_imputer)
X_test,  T_test,  E_test                  = preprocess(df_test,  imputer=fitted_imputer)

print(f"[METRIC] Feature count: {X_train.shape[1]}")
print(f"[METRIC] Train readmission rate: {E_train.mean()*100:.2f}%")
print(f"[METRIC] Val   readmission rate: {E_val.mean()*100:.2f}%")
print(f"[METRIC] Test  readmission rate: {E_test.mean()*100:.2f}%")
log_stage("preprocess", t)

# ─── 3. Train XGBoost ───────────────────────────────────────────────────────
print("\n[STEP 6.3] Training XGBoost for 30-day readmission risk ...")
t = start_timer()

def make_survival_labels(times: pd.Series, events: pd.Series) -> np.ndarray:
    times_array = times.astype(float).to_numpy()
    events_array = events.astype(int).to_numpy()
    # Positive time = observed event, negative time = censored observation.
    return np.where(events_array == 1, times_array, -times_array)

train_labels = make_survival_labels(T_train, E_train)
val_labels   = make_survival_labels(T_val,   E_val)
test_labels  = make_survival_labels(T_test,  E_test)

dtrain = xgb.DMatrix(X_train, label=train_labels)
dval   = xgb.DMatrix(X_val,   label=val_labels)
dtest  = xgb.DMatrix(X_test,  label=test_labels)

params = {
    "objective":  "survival:cox",
    "eval_metric": "cox-nloglik",
    "tree_method": "hist",
    "max_depth":   6,
    "eta":         0.05,
    "subsample":   0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "gamma":       1,
    "seed":        42,
}

# Try GPU if available
try:
    params["device"] = "cuda"
    test_model = xgb.train(params, dtrain, num_boost_round=1)
    print("[INFO] Using GPU for XGBoost training.")
except Exception:
    params.pop("device", None)
    print("[INFO] GPU not available. Using CPU for XGBoost training.")

evals = [(dtrain, "train"), (dval, "val")]
evals_result = {}

model = xgb.train(
    params,
    dtrain,
    num_boost_round=500,
    evals=evals,
    evals_result=evals_result,
    early_stopping_rounds=30,
    verbose_eval=50,
)
train_time = log_stage("train_xgboost_readmission", t)

# ─── 4. Evaluate ────────────────────────────────────────────────────────────
print("\n[STEP 6.4] Evaluating model ...")
t = start_timer()

y_train_pred = model.predict(dtrain)
y_val_pred   = model.predict(dval)
y_test_pred  = model.predict(dtest)

train_c_index = concordance_index(T_train.values, -y_train_pred, E_train.values)
val_c_index   = concordance_index(T_val.values,   -y_val_pred,   E_val.values)
test_c_index  = concordance_index(T_test.values,  -y_test_pred,  E_test.values)

print(f"\n[RESULT] === 30-day Readmission Prediction (C-index) ===")
print(f"[RESULT] Best iteration: {model.best_iteration}")
print(f"[RESULT] Train C-index: {train_c_index:.4f}")
print(f"[RESULT] Val   C-index: {val_c_index:.4f}")
print(f"[RESULT] Test  C-index: {test_c_index:.4f}")
log_stage("evaluate", t)

# ─── 5. Save model + metrics ────────────────────────────────────────────────
print("\n[STEP 6.5] Saving model and metrics ...")
model_path = str(OUT / "xgb_readmission_model.json")
model.save_model(model_path)
print(f"[INFO] Model saved to: {model_path}")

metrics = {
    "task": "30-day readmission",
    "train_c_index":   round(train_c_index, 4),
    "val_c_index":     round(val_c_index,   4),
    "test_c_index":    round(test_c_index,  4),
    "best_iteration":  model.best_iteration,
    "train_rows":      len(df_train),
    "val_rows":        len(df_val),
    "test_rows":       len(df_test),
    "feature_count":   X_train.shape[1],
    "train_readmission_rate_pct": round(float(E_train.mean()*100), 2),
    "val_readmission_rate_pct":   round(float(E_val.mean()*100),   2),
    "test_readmission_rate_pct":  round(float(E_test.mean()*100),  2),
    "train_time_seconds": round(train_time, 1),
}

metrics_path = str(LOGS_DIR / "readmission_metrics.json")
with open(metrics_path, "w") as f:
    json.dump(metrics, f, indent=2)
print(f"[INFO] Metrics saved to: {metrics_path}")

print(f"\n{'='*60}")
for k, v in metrics.items():
    print(f"  {k}: {v}")

log_stage("TOTAL step 06 (readmission)", t_total)
