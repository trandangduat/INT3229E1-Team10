"""
04_make_diagnoses.py — Ray Pipeline Step 4
One-hot encode ICD chapters from diagnoses_icd.csv.

Translates logic from:
  - src/etl/silver_diagnoses.py + build_gold_dataset.py  (Spark version)

ICD10 and ICD9 codes are mapped to 21 chapter categories.
One-hot per admission (hadm_id).

Outputs:
  → outputs/diagnoses_onehot/  (Parquet)
"""

import ray
import ray.data as rd
import pandas as pd
from pathlib import Path
import sys

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT   = Path(__file__).resolve().parents[2]
DATA_HOSP = PROJECT / "data" / "raw" / "hosp"
OUT       = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── ICD chapter mappings (from build_gold_dataset.py) ─────────────────────
ICD10_CHAPTER_RANGES = [
    ("A00", "B99",  1), ("C00", "D49",  2), ("D50", "D89",  3),
    ("E00", "E89",  4), ("F01", "F99",  5), ("G00", "G99",  6),
    ("H00", "H59",  7), ("H60", "H95",  8), ("I00", "I99",  9),
    ("J00", "J99", 10), ("K00", "K95", 11), ("L00", "L99", 12),
    ("M00", "M99", 13), ("N00", "N99", 14), ("O00", "O9A", 15),
    ("P00", "P96", 16), ("Q00", "Q99", 17), ("R00", "R99", 18),
    ("S00", "T88", 19), ("V00", "Y99", 20), ("Z00", "Z99", 21),
]

ICD9_CHAPTER_RANGES = [
    ("001", "139",  1), ("140", "239",  2), ("240", "279",  4),
    ("280", "289",  3), ("290", "319",  5), ("320", "389",  6),
    ("390", "459",  9), ("460", "519", 10), ("520", "579", 11),
    ("580", "629", 14), ("630", "679", 15), ("680", "709", 12),
    ("710", "739", 13), ("740", "759", 17), ("760", "779", 16),
    ("780", "799", 18), ("800", "999", 19),
]

ICD10_CHAPTER_NAMES = {
    1: "infectious_parasitic",   2: "neoplasms",
    3: "blood_diseases",         4: "endocrine_metabolic",
    5: "mental_disorders",       6: "nervous_system",
    7: "eye_diseases",           8: "ear_diseases",
    9: "circulatory",           10: "respiratory",
    11: "digestive",            12: "skin_diseases",
    13: "musculoskeletal",      14: "genitourinary",
    15: "pregnancy_childbirth", 16: "perinatal",
    17: "congenital",           18: "symptoms_signs",
    19: "injury_poisoning",     20: "external_causes",
    21: "health_status_factors",
}


def map_icd10_to_chapter(code: str):
    if not code:
        return None
    code = code.upper().strip()
    for low, high, chapter in ICD10_CHAPTER_RANGES:
        if low <= code <= high:
            return chapter
    return None


def map_icd9_to_chapter(code: str):
    if not code:
        return None
    code = code.strip().zfill(3)
    for low, high, chapter in ICD9_CHAPTER_RANGES:
        if low <= code <= high:
            return chapter
    return None


def map_icd_to_chapter(icd_code, icd_version):
    if pd.isna(icd_code) or icd_code is None:
        return None
    if icd_version == 10:
        return map_icd10_to_chapter(str(icd_code))
    elif icd_version == 9:
        return map_icd9_to_chapter(str(icd_code))
    return None


# ─── Ray Init ──────────────────────────────────────────────────────────────
ray.init(address="auto", ignore_reinit_error=True)
print(f"[INFO] Ray cluster resources: {ray.cluster_resources()}")

t_total = start_timer()

# ─── 1. Load valid hadm_ids ─────────────────────────────────────────────────
print("\n[STEP 4.1] Loading label_table ...")
t = start_timer()
label_df = rd.read_parquet(f"local://{OUT}/label_table").to_pandas()
label_df["hadm_id"] = label_df["hadm_id"].astype("Int64")
valid_hadm_ids = set(label_df["hadm_id"].dropna().astype(int).tolist())
print(f"[METRIC] Valid hadm_ids: {len(valid_hadm_ids)}")
log_stage("load_label", t)

# ─── 2. Read diagnoses_icd.csv ──────────────────────────────────────────────
print("\n[STEP 4.2] Reading diagnoses_icd.csv ...")
t = start_timer()
diagnoses = rd.read_csv(
    f"local://{DATA_HOSP}/diagnoses_icd.csv",
    include_paths=False,
)
diagnoses = diagnoses.select_columns([
    "subject_id", "hadm_id", "seq_num", "icd_code", "icd_version",
])
raw_count = diagnoses.count()
print(f"[METRIC] Raw diagnoses rows: {raw_count}")
log_stage("read_diagnoses", t)

# ─── 3. Map ICD → chapters ──────────────────────────────────────────────────
print("\n[STEP 4.3] Mapping ICD codes → chapter numbers ...")
t = start_timer()

df_diag = diagnoses.to_pandas()
df_diag["hadm_id"]     = pd.to_numeric(df_diag["hadm_id"], errors="coerce").astype("Int64")
df_diag["icd_version"] = pd.to_numeric(df_diag["icd_version"], errors="coerce")
df_diag["icd_code"]    = df_diag["icd_code"].astype(str).str.upper().str.strip()

# Filter to cohort
df_diag = df_diag[
    df_diag["hadm_id"].notna()
    & df_diag["icd_code"].notna()
    & (df_diag["icd_code"] != "")
    & (df_diag["icd_code"] != "NAN")
    & df_diag["icd_version"].notna()
].copy()

df_diag = df_diag[df_diag["hadm_id"].isin(valid_hadm_ids)].copy()
cohort_count = len(df_diag)
print(f"[METRIC] Diagnoses rows in cohort: {cohort_count}")

# Map chapters
df_diag["icd_chapter"] = df_diag.apply(
    lambda row: map_icd_to_chapter(row["icd_code"], row["icd_version"]),
    axis=1
)

df_diag = df_diag[df_diag["icd_chapter"].notna()].copy()
df_diag["icd_chapter"] = df_diag["icd_chapter"].astype(int)

chapter_dist = df_diag["icd_chapter"].value_counts().sort_index()
print(f"[METRIC] Chapters present: {sorted(df_diag['icd_chapter'].unique().tolist())}")
log_stage("map_icd_chapters", t)

# ─── 4. One-hot encoding per hadm_id ────────────────────────────────────────
print("\n[STEP 4.4] One-hot encoding ICD chapters ...")
t = start_timer()

chapters_present = sorted(df_diag["icd_chapter"].unique().tolist())

# Deduplicate (hadm_id, icd_chapter)
df_onehot = df_diag[["hadm_id", "icd_chapter"]].drop_duplicates()

# Create wide format
df_wide = pd.DataFrame({"hadm_id": df_onehot["hadm_id"].unique()})
for ch in chapters_present:
    ch_name = ICD10_CHAPTER_NAMES.get(ch, f"chap_{ch:02d}")
    col_name = f"icd10_chap_{ch:02d}_{ch_name}"
    hadm_with_ch = set(df_onehot[df_onehot["icd_chapter"] == ch]["hadm_id"].tolist())
    df_wide[col_name] = df_wide["hadm_id"].isin(hadm_with_ch).astype(int)

print(f"[METRIC] Admissions with diagnoses: {len(df_wide)}")
print(f"[METRIC] ICD chapter columns: {len([c for c in df_wide.columns if c != 'hadm_id'])}")
log_stage("onehot_icd", t)

# ─── 5. Write output ────────────────────────────────────────────────────────
print("\n[STEP 4.5] Writing diagnoses_onehot ...")
t = start_timer()
diag_ds = rd.from_pandas(df_wide)
diag_ds.write_parquet(f"local://{OUT}/diagnoses_onehot")
log_stage("write_diagnoses", t)

print(f"\n[RESULT] diagnoses_onehot written to: {OUT}/diagnoses_onehot")
print(f"[RESULT] Rows: {len(df_wide)} | Columns: {len(df_wide.columns)}")
log_stage("TOTAL step 04", t_total)
