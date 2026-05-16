"""
02_make_vitals.py — Ray Pipeline Step 2
Filter chartevents to extract vital signs for each hadm_id (first 24h window).

Translates logic from:
  - src/etl/silver_vitals_mimic.py   (Spark version)

Vital signs extracted:
  - sbp (systolic blood pressure): itemids 220050, 220179
  - spo2: 220277
  - hr (heart rate): 220045
  - temperature: 223761 (°F → °C), 223762 (°C)

Filters to first 24h of admission window.
Aggregates: mean, min, max, count per (hadm_id, vital_name).

Outputs:
  → outputs/vitals_agg/  (Parquet, wide format with columns like sbp_mean, hr_max ...)
"""

import ray
import ray.data as rd
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT = Path(__file__).resolve().parents[2]
DATA_ICU = PROJECT / "data" / "raw" / "icu"
OUT      = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── Config ────────────────────────────────────────────────────────────────
VITAL_ITEMIDS = {
    220050: "sbp",
    220179: "sbp",
    220277: "spo2",
    220045: "hr",
    223761: "temperature",   # °F → °C conversion applied below
    223762: "temperature",
}

VITAL_RANGES = {
    "sbp":         (40, 300),
    "spo2":        (50, 100),
    "hr":          (20, 250),
    "temperature": (25, 45),
}

ALL_ITEMIDS = list(VITAL_ITEMIDS.keys())

# ─── Ray Init ──────────────────────────────────────────────────────────────
ray.init(ignore_reinit_error=True)
print(f"[INFO] Ray cluster resources: {ray.cluster_resources()}")

t_total = start_timer()

# ─── 1. Load label table (for admittime filter) ─────────────────────────────
print("\n[STEP 2.1] Loading label_table for admission times ...")
t = start_timer()
label_df = rd.read_parquet(f"local://{OUT}/label_table").to_pandas()
label_df["hadm_id"]   = label_df["hadm_id"].astype("Int64")
label_df["admittime"] = pd.to_datetime(label_df["admittime"], errors="coerce")
label_df["dischtime"] = pd.to_datetime(label_df["dischtime"], errors="coerce")
# Only keep what we need
adm_times = label_df[["hadm_id", "admittime"]].drop_duplicates("hadm_id").set_index("hadm_id")
valid_hadm_ids = set(label_df["hadm_id"].dropna().astype(int).tolist())
print(f"[METRIC] Valid hadm_ids from label table: {len(valid_hadm_ids)}")
log_stage("load_label_table", t)

# ─── 2. Read chartevents with Ray Data ─────────────────────────────────────
print("\n[STEP 2.2] Reading chartevents.csv (full dataset) ...")
t = start_timer()
chartevents = rd.read_csv(
    f"local://{DATA_ICU}/chartevents.csv",
    include_paths=False,
)
# Select only needed columns
chartevents = chartevents.select_columns([
    "subject_id", "hadm_id", "charttime", "itemid", "valuenum",
])
raw_count = chartevents.count()
print(f"[METRIC] Raw chartevents rows: {raw_count}")
log_stage("read_chartevents", t)

# ─── 3. Filter by itemid using map_batches ──────────────────────────────────
print("\n[STEP 2.3] Filtering by vital itemids + cleaning ...")
t = start_timer()

broadcast_adm_times = ray.put(adm_times)
broadcast_valid_hadm = ray.put(valid_hadm_ids)

def filter_and_tag_vitals(batch: pd.DataFrame) -> pd.DataFrame:
    adm_times_local   = ray.get(broadcast_adm_times)
    valid_hadm_local  = ray.get(broadcast_valid_hadm)

    # Cast types
    batch["itemid"]    = pd.to_numeric(batch["itemid"],    errors="coerce")
    batch["valuenum"]  = pd.to_numeric(batch["valuenum"],  errors="coerce")
    batch["hadm_id"]   = pd.to_numeric(batch["hadm_id"],   errors="coerce")
    batch["charttime"] = pd.to_datetime(batch["charttime"], errors="coerce")

    # Filter: only desired itemids, non-null value & hadm_id
    batch = batch[
        batch["itemid"].isin(VITAL_ITEMIDS.keys())
        & batch["valuenum"].notna()
        & batch["hadm_id"].notna()
        & batch["charttime"].notna()
    ].copy()

    if batch.empty:
        return pd.DataFrame(columns=["hadm_id", "vital_name", "valuenum", "charttime"])

    # Filter: only hadm_ids in our cohort
    batch["hadm_id"] = batch["hadm_id"].astype("Int64")
    batch = batch[batch["hadm_id"].isin(valid_hadm_local)].copy()

    if batch.empty:
        return pd.DataFrame(columns=["hadm_id", "vital_name", "valuenum", "charttime"])

    # Map itemid → vital_name
    batch["vital_name"] = batch["itemid"].map(VITAL_ITEMIDS)

    # Convert °F → °C for itemid 223761
    mask_f = batch["itemid"] == 223761
    batch.loc[mask_f, "valuenum"] = (batch.loc[mask_f, "valuenum"] - 32) * 5 / 9

    # Apply physiological range filter
    def in_range(row):
        lo, hi = VITAL_RANGES[row["vital_name"]]
        return lo <= row["valuenum"] <= hi

    batch = batch[batch.apply(in_range, axis=1)].copy()

    if batch.empty:
        return pd.DataFrame(columns=["hadm_id", "vital_name", "valuenum", "charttime"])

    # Filter to first 24h of admission
    batch = batch.merge(
        adm_times_local.reset_index(),
        on="hadm_id", how="inner"
    )
    batch = batch[
        (batch["charttime"] >= batch["admittime"])
        & (batch["charttime"] < batch["admittime"] + pd.Timedelta(hours=24))
    ].copy()

    return batch[["hadm_id", "vital_name", "valuenum", "charttime"]]

vitals_filtered = chartevents.map_batches(
    filter_and_tag_vitals,
    batch_format="pandas",
    batch_size=200_000,
)
filtered_count = vitals_filtered.count()
print(f"[METRIC] Rows after vital filter + 24h window: {filtered_count}")
log_stage("filter_vitals", t)

# ─── 4. Aggregate per (hadm_id, vital_name) → wide format ──────────────────
print("\n[STEP 2.4] Aggregating vitals to wide format ...")
t = start_timer()

df_vitals = vitals_filtered.to_pandas()
df_vitals["hadm_id"] = df_vitals["hadm_id"].astype("Int64")

agg = (
    df_vitals.groupby(["hadm_id", "vital_name"])["valuenum"]
    .agg(["mean", "min", "max", "count"])
    .reset_index()
)

wide = agg.pivot_table(
    index="hadm_id",
    columns="vital_name",
    values=["mean", "min", "max", "count"],
).copy()

wide.columns = [f"{vital}_{stat}" for stat, vital in wide.columns]
wide = wide.reset_index()

# Fill counts with 0 where missing
count_cols = [c for c in wide.columns if c.endswith("_count")]
wide[count_cols] = wide[count_cols].fillna(0)

print(f"[METRIC] Admissions with vitals: {len(wide)}")
print(f"[METRIC] Vitals columns: {[c for c in wide.columns if c != 'hadm_id']}")

# Coverage report
for vital in ["sbp", "spo2", "hr", "temperature"]:
    col_name = f"{vital}_mean"
    if col_name in wide.columns:
        non_null = wide[col_name].notna().sum()
        print(f"[METRIC] Coverage {vital}: {non_null}/{len(wide)} ({100*non_null/len(wide):.1f}%)")
log_stage("aggregate_vitals", t)

# ─── 5. Write output ────────────────────────────────────────────────────────
print("\n[STEP 2.5] Writing vitals_agg ...")
t = start_timer()
vitals_ds = rd.from_pandas(wide)
vitals_ds.write_parquet(f"local://{OUT}/vitals_agg")
log_stage("write_vitals", t)

print(f"\n[RESULT] vitals_agg written to: {OUT}/vitals_agg")
print(f"[RESULT] Rows: {len(wide)} | Columns: {len(wide.columns)}")
log_stage("TOTAL step 02", t_total)
