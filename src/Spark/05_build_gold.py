"""
05_build_gold.py — PySpark Pipeline Step 5
Join label_table + vitals_agg + labs_agg + diagnoses_onehot → Gold dataset.

PySpark equivalent of Ray step 5.
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
import pandas as pd
import numpy as np

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT = Path(__file__).resolve().parents[2]
OUT = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── PySpark Init ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MIMIC-IV-Gold-Dataset") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.network.timeout", "300s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
t_total = start_timer()

# ─── 1. Load all feature tables ────────────────────────────────────────────
print("\n[STEP 5.1] Loading label_table ...")
t = start_timer()
df_label = spark.read.parquet(f"{OUT}/label_table_spark").toPandas()
df_label["hadm_id"] = df_label["hadm_id"].astype("Int64")
print(f"[METRIC] Label table rows: {len(df_label)}")
log_stage("load_label", t)

print("\n[STEP 5.2] Loading vitals_agg ...")
t = start_timer()
try:
    df_vitals = spark.read.parquet(f"{OUT}/vitals_agg_spark").toPandas()
    df_vitals["hadm_id"] = df_vitals["hadm_id"].astype("Int64")
    print(f"[METRIC] Vitals rows: {len(df_vitals)} | Cols: {len(df_vitals.columns)}")
except Exception as e:
    print(f"[WARN] vitals_agg not found, skipping: {e}")
    df_vitals = None
log_stage("load_vitals", t)

print("\n[STEP 5.3] Loading labs_agg ...")
t = start_timer()
try:
    df_labs = spark.read.parquet(f"{OUT}/labs_agg_spark").toPandas()
    df_labs["hadm_id"] = df_labs["hadm_id"].astype("Int64")
    print(f"[METRIC] Labs rows: {len(df_labs)} | Cols: {len(df_labs.columns)}")
except Exception as e:
    print(f"[WARN] labs_agg not found, skipping: {e}")
    df_labs = None
log_stage("load_labs", t)

print("\n[STEP 5.4] Loading diagnoses_onehot ...")
t = start_timer()
try:
    df_diag = spark.read.parquet(f"{OUT}/diagnoses_onehot_spark").toPandas()
    df_diag["hadm_id"] = df_diag["hadm_id"].astype("Int64")
    print(f"[METRIC] Diagnoses rows: {len(df_diag)} | Cols: {len(df_diag.columns)}")
except Exception as e:
    print(f"[WARN] diagnoses_onehot not found, skipping: {e}")
    df_diag = None
log_stage("load_diagnoses", t)

# ─── 2. Join all tables ────────────────────────────────────────────────────
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

# ─── 3. Temporal split ─────────────────────────────────────────────────────
print("\n[STEP 5.6] Computing temporal split ...")
t = start_timer()

df["admityear"] = pd.to_numeric(df["admityear"], errors="coerce")
valid_years = df[df["admityear"].notna()]["admityear"]
train_max = int(valid_years.quantile(0.70))
val_max = int(valid_years.quantile(0.85))

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

# ─── 4. Summary metrics ────────────────────────────────────────────────────
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

# ─── 5. Write output ──────────────────────────────────────────────────────
print("\n[STEP 5.8] Writing gold_dataset ...")
t = start_timer()

# Drop raw datetime cols before writing
df_out = df.drop(columns=["admittime", "dischtime"], errors="ignore")

# Prefer writing with pandas/pyarrow to avoid serializing a large DataFrame through Spark
output_path = f"{OUT}/gold_dataset_spark.parquet"
try:
    df_out.to_parquet(output_path, index=False)
    log_stage("write_gold", t)
    print(f"\n{'=' * 60}")
    print(f"[RESULT] gold_dataset written to: {output_path}")
    print(f"[RESULT] Total rows: {len(df_out)}")
    print(f"[RESULT] Total columns: {len(df_out.columns)}")
    log_stage("TOTAL step 05", t_total)
except Exception as e:
    # Fallback: try writing via Spark (may require more driver/executor memory)
    print(f"[WARN] pandas.to_parquet failed, falling back to Spark write: {e}")
    gold_spark = spark.createDataFrame(df_out)
    output_dir = f"{OUT}/gold_dataset_spark"
    gold_spark.write.mode("overwrite").parquet(output_dir)
    log_stage("write_gold", t)
    print(f"\n{'=' * 60}")
    print(f"[RESULT] gold_dataset written to: {output_dir}")
    print(f"[RESULT] Total rows: {len(df_out)}")
    print(f"[RESULT] Total columns: {len(df_out.columns)}")
    log_stage("TOTAL step 05", t_total)

spark.stop()
