"""
05_build_gold.py — Ray Pipeline Step 5
Join label_table + vitals_agg + labs_agg + diagnoses_onehot → Gold dataset.

Translates logic from:
  - src/etl/build_gold_dataset.py  (Spark version)

Adds:
  - Temporal train/val/test split based on admityear quantiles (70/85/100%)
  - All feature tables left-joined on hadm_id

Outputs:
  → outputs/gold_dataset/  (Parquet, partitioned by split)
"""

import ray
import ray.data as rd
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT = Path(__file__).resolve().parents[2]
OUT     = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── Ray Init ──────────────────────────────────────────────────────────────
ray.init(address="auto", ignore_reinit_error=True)
print(f"[INFO] Ray cluster resources: {ray.cluster_resources()}")

t_total = start_timer()

# ─── 1. Load all silver tables ──────────────────────────────────────────────
print("\n[STEP 5.1] Loading label_table ...")
t = start_timer()
df_label = rd.read_parquet(f"local://{OUT}/label_table").to_pandas()
df_label["hadm_id"] = df_label["hadm_id"].astype("Int64")
print(f"[METRIC] Label table rows: {len(df_label)}")
log_stage("load_label", t)

print("\n[STEP 5.2] Loading vitals_agg ...")
t = start_timer()
try:
    df_vitals = rd.read_parquet(f"local://{OUT}/vitals_agg").to_pandas()
    df_vitals["hadm_id"] = df_vitals["hadm_id"].astype("Int64")
    print(f"[METRIC] Vitals rows: {len(df_vitals)} | Cols: {len(df_vitals.columns)}")
except Exception as e:
    print(f"[WARN] vitals_agg not found, skipping: {e}")
    df_vitals = None
log_stage("load_vitals", t)

print("\n[STEP 5.3] Loading labs_agg ...")
t = start_timer()
try:
    df_labs = rd.read_parquet(f"local://{OUT}/labs_agg").to_pandas()
    df_labs["hadm_id"] = df_labs["hadm_id"].astype("Int64")
    print(f"[METRIC] Labs rows: {len(df_labs)} | Cols: {len(df_labs.columns)}")
except Exception as e:
    print(f"[WARN] labs_agg not found, skipping: {e}")
    df_labs = None
log_stage("load_labs", t)

print("\n[STEP 5.4] Loading diagnoses_onehot ...")
t = start_timer()
try:
    df_diag = rd.read_parquet(f"local://{OUT}/diagnoses_onehot").to_pandas()
    df_diag["hadm_id"] = df_diag["hadm_id"].astype("Int64")
    print(f"[METRIC] Diagnoses rows: {len(df_diag)} | Cols: {len(df_diag.columns)}")
except Exception as e:
    print(f"[WARN] diagnoses_onehot not found, skipping: {e}")
    df_diag = None
log_stage("load_diagnoses", t)

# ─── 2. Join all tables ──────────────────────────────────────────────────────
print("\n[STEP 5.5] Joining all feature tables ...")
t = start_timer()

df = df_label.copy()
base_count = len(df)

if df_vitals is not None:
    df = df.merge(df_vitals, on="hadm_id", how="left")
    print(f"[METRIC] After vitals join: {len(df)} rows")

if df_labs is not None:
    df = df.merge(df_labs, on="hadm_id", how="left")
    print(f"[METRIC] After labs join: {len(df)} rows")

if df_diag is not None:
    df = df.merge(df_diag, on="hadm_id", how="left")
    print(f"[METRIC] After diagnoses join: {len(df)} rows")

print(f"[METRIC] Total columns after joins: {len(df.columns)}")
log_stage("join_tables", t)

# ─── 3. Temporal split ───────────────────────────────────────────────────────
print("\n[STEP 5.6] Computing temporal split ...")
t = start_timer()

df["admityear"] = pd.to_numeric(df["admityear"], errors="coerce")
valid_years = df[df["admityear"].notna()]["admityear"]
train_max = int(valid_years.quantile(0.70))
val_max   = int(valid_years.quantile(0.85))

print(f"[METRIC] Temporal split: train ≤ {train_max}, val ≤ {val_max}, test > {val_max}")

def assign_split(year):
    if pd.isna(year):
        return "test"
    if year <= train_max:
        return "train"
    elif year <= val_max:
        return "val"
    else:
        return "test"

df["split"] = df["admityear"].apply(assign_split)

split_dist = df.groupby("split").size()
print(f"[METRIC] Split distribution:\n{split_dist}")
log_stage("temporal_split", t)

# ─── 4. Summary metrics ─────────────────────────────────────────────────────
print("\n[STEP 5.7] Computing event rates by split ...")
for split_name, grp in df.groupby("split"):
    n = len(grp)
    mort = grp["event_flag_mortality"].mean() * 100
    read = grp["event_flag_readmission"].mean() * 100
    print(f"  [{split_name}] n={n}, mortality={mort:.2f}%, readmission={read:.2f}%")

total_mort = df["event_flag_mortality"].mean() * 100
total_read = df["event_flag_readmission"].mean() * 100
print(f"[METRIC] Overall mortality rate: {total_mort:.2f}%")
print(f"[METRIC] Overall 30-day readmission rate: {total_read:.2f}%")

# Feature completeness
feature_cols = [c for c in df.columns if c not in [
    "subject_id", "hadm_id", "admittime", "dischtime",
    "event_flag_mortality", "event_flag_readmission",
    "time_to_event_hours", "split", "admityear", "gender",
    "age", "duration_days",
]]
print(f"\n[METRIC] Feature columns: {len(feature_cols)}")
missing_pct = df[feature_cols].isna().mean() * 100
top_missing = missing_pct.nlargest(10)
print(f"[METRIC] Top-10 columns with most missing:\n{top_missing}")

# ─── 5. Write output ────────────────────────────────────────────────────────
print("\n[STEP 5.8] Writing gold_dataset ...")
t = start_timer()

# Drop raw datetime cols before writing
df_out = df.drop(columns=["admittime", "dischtime"], errors="ignore")

gold_ds = rd.from_pandas(df_out)
gold_ds.write_parquet(f"local://{OUT}/gold_dataset")
log_stage("write_gold", t)

print(f"\n{'=' * 60}")
print(f"[RESULT] gold_dataset written to: {OUT}/gold_dataset")
print(f"[RESULT] Total rows: {len(df_out)}")
print(f"[RESULT] Total columns: {len(df_out.columns)}")
print(f"[RESULT] Columns: {list(df_out.columns)}")
log_stage("TOTAL step 05", t_total)
