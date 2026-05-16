"""
01_make_label.py — Ray Pipeline Step 1
Build label table from MIMIC-IV patients + admissions.

Translates logic from:
  - src/etl/silver_admissions.py   (Spark version)

Produces:
  - subject_id, hadm_id, gender, age, duration_days
  - event_flag_mortality   (1 = died in-hospital)
  - event_flag_readmission (1 = readmitted within 30 days)
  - time_to_event_hours    (survival label)
  - admittime, dischtime, admityear
  → outputs/label_table/   (Parquet)
"""

import ray
import ray.data as rd
import pandas as pd
from pathlib import Path
import sys, os

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT = Path(__file__).resolve().parents[2]
DATA    = PROJECT / "data" / "raw" / "hosp"
OUT     = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── Ray Init ──────────────────────────────────────────────────────────────
ray.init(ignore_reinit_error=True)
print(f"[INFO] Ray cluster resources: {ray.cluster_resources()}")

t_total = start_timer()

# ─── 1. Read patients ───────────────────────────────────────────────────────
print("\n[STEP 1.1] Reading patients.csv ...")
t = start_timer()
patients = rd.read_csv(
    f"local://{DATA}/patients.csv",
    include_paths=False,
)
patients = patients.select_columns(["subject_id", "gender", "anchor_age", "anchor_year", "dod"])
pat_count = patients.count()
print(f"[METRIC] Patients raw count: {pat_count}")
log_stage("read_patients", t)

# ─── 2. Read admissions ─────────────────────────────────────────────────────
print("\n[STEP 1.2] Reading admissions.csv ...")
t = start_timer()
admissions = rd.read_csv(f"local://{DATA}/admissions.csv")
admissions = admissions.select_columns([
    "subject_id", "hadm_id",
    "admittime", "dischtime", "deathtime",
    "hospital_expire_flag",
])
adm_count = admissions.count()
print(f"[METRIC] Admissions raw count: {adm_count}")
log_stage("read_admissions", t)

# ─── 3. Join + transform in pandas batches ──────────────────────────────────
print("\n[STEP 1.3] Joining + computing labels ...")
t = start_timer()

# Convert to pandas for join (admissions+patients both small enough)
df_pat = patients.to_pandas()
df_adm = admissions.to_pandas()

df_pat["subject_id"]  = df_pat["subject_id"].astype("Int64")
df_adm["subject_id"]  = df_adm["subject_id"].astype("Int64")
df_adm["hadm_id"]     = df_adm["hadm_id"].astype("Int64")
df_adm["hospital_expire_flag"] = df_adm["hospital_expire_flag"].astype("Int64")

# ── 3a. Join
df = df_adm.merge(
    df_pat[["subject_id", "gender", "anchor_age", "anchor_year"]],
    on="subject_id",
    how="inner",
)

# ── 3b. Parse timestamps
for col in ["admittime", "dischtime", "deathtime"]:
    df[col] = pd.to_datetime(df[col], errors="coerce")

# ── 3c. Compute age at admission
df["admityear"] = df["admittime"].dt.year
df["age"] = df["anchor_age"] + (df["admityear"] - df["anchor_year"])

# ── 3d. Filter: age ≥ 18, duration ≥ 1 day
df["duration_days"] = (df["dischtime"] - df["admittime"]).dt.total_seconds() / 86400.0
df = df[(df["age"] >= 18) & (df["duration_days"] >= 1)].copy()

# ── 3e. Mortality flag
df["event_flag_mortality"] = df["hospital_expire_flag"].fillna(0).astype(int)

# ── 3f. Survival label
end_time = df["deathtime"].fillna(df["dischtime"])
df["time_to_event_hours"] = (end_time - df["admittime"]).dt.total_seconds() / 3600.0
df = df[df["time_to_event_hours"].notna() & (df["time_to_event_hours"] > 0)]

# ── 3g. 30-day readmission flag (within subject, sorted by admittime)
df = df.sort_values(["subject_id", "admittime"])
df["next_admittime"] = df.groupby("subject_id")["admittime"].shift(-1)
df["days_to_readmission"] = (
    df["next_admittime"] - df["dischtime"]
).dt.total_seconds() / 86400.0
df["event_flag_readmission"] = (
    df["days_to_readmission"].notna()
    & (df["days_to_readmission"] >= 0)
    & (df["days_to_readmission"] <= 30)
).astype(int)

# ── 3h. Select final columns
df_label = df[[
    "subject_id", "hadm_id",
    "gender", "age", "duration_days",
    "admittime", "dischtime", "admityear",
    "event_flag_mortality",
    "event_flag_readmission",
    "time_to_event_hours",
]].copy()

final_count = len(df_label)
mortality_rate   = df_label["event_flag_mortality"].mean() * 100
readmission_rate = df_label["event_flag_readmission"].mean() * 100

print(f"[METRIC] Label table rows (after filters): {final_count}")
print(f"[METRIC] Mortality rate: {mortality_rate:.2f}%")
print(f"[METRIC] 30-day readmission rate: {readmission_rate:.2f}%")
print(f"[METRIC] Age stats — min: {df_label['age'].min():.0f}, "
      f"max: {df_label['age'].max():.0f}, "
      f"mean: {df_label['age'].mean():.1f}")
print(f"[METRIC] Duration days — min: {df_label['duration_days'].min():.1f}, "
      f"max: {df_label['duration_days'].max():.1f}, "
      f"mean: {df_label['duration_days'].mean():.1f}")
log_stage("make_label", t)

# ─── 4. Write output ────────────────────────────────────────────────────────
print("\n[STEP 1.4] Writing label_table ...")
t = start_timer()
label_ds = rd.from_pandas(df_label)
label_ds.write_parquet(f"local://{OUT}/label_table")
log_stage("write_label_table", t)

print(f"\n[RESULT] label_table written to: {OUT}/label_table")
print(f"[RESULT] Total rows: {final_count}")
log_stage("TOTAL step 01", t_total)
