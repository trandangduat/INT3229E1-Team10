"""
03_make_labs.py — Ray Pipeline Step 3
Filter labevents to extract 23 lab features for each hadm_id (first 24h window).

Translates logic from:
  - src/etl/silver_labs.py   (Spark version)

Lab features (23 items):
  hematocrit, hemoglobin, platelet, wbc,
  creatinine, bun, sodium, potassium, chloride, bicarbonate,
  anion_gap, glucose, calcium, magnesium, phosphate,
  inr, pt, ptt, alt, ast, bilirubin_total, albumin, lactate

Outputs:
  → outputs/labs_agg/  (Parquet, wide format)
"""

import ray
import ray.data as rd
import pandas as pd
from pathlib import Path
import sys

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT  = Path(__file__).resolve().parents[2]
DATA_HOSP = PROJECT / "data" / "raw" / "hosp"
OUT       = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── Config (from silver_labs.py) ──────────────────────────────────────────
LAB_ITEMIDS = {
    "hematocrit":     [51221],
    "hemoglobin":     [51222],
    "platelet":       [51265],
    "wbc":            [51301],
    "creatinine":     [50912, 52546],
    "bun":            [51006, 52647],
    "sodium":         [50983, 52623],
    "potassium":      [50971, 52610],
    "chloride":       [50902, 52535],
    "bicarbonate":    [50882],
    "anion_gap":      [50868],
    "glucose":        [50931, 52569],
    "calcium":        [50893],
    "magnesium":      [50960],
    "phosphate":      [50970],
    "inr":            [51237, 51675],
    "pt":             [51274],
    "ptt":            [51275, 52923],
    "alt":            [50861],
    "ast":            [50878],
    "bilirubin_total":[50885, 53089],
    "albumin":        [50862, 53085],
    "lactate":        [50813, 52442, 53154],
}

ALL_LAB_ITEMIDS = [iid for ids in LAB_ITEMIDS.values() for iid in ids]
ITEMID_TO_LABNAME = {iid: name for name, ids in LAB_ITEMIDS.items() for iid in ids}

# ─── Ray Init ──────────────────────────────────────────────────────────────
ray.init(address="auto", ignore_reinit_error=True)
print(f"[INFO] Ray cluster resources: {ray.cluster_resources()}")

t_total = start_timer()

# ─── 1. Load label table for admission times ────────────────────────────────
print("\n[STEP 3.1] Loading label_table for admission window ...")
t = start_timer()
label_df = rd.read_parquet(f"local://{OUT}/label_table").to_pandas()
label_df["hadm_id"]   = label_df["hadm_id"].astype("Int64")
label_df["admittime"] = pd.to_datetime(label_df["admittime"], errors="coerce")
adm_times = label_df[["hadm_id", "admittime"]].drop_duplicates("hadm_id").set_index("hadm_id")
valid_hadm_ids = set(label_df["hadm_id"].dropna().astype(int).tolist())
print(f"[METRIC] Valid hadm_ids: {len(valid_hadm_ids)}")
log_stage("load_label", t)

# ─── 2. Read labevents with Ray Data ────────────────────────────────────────
print("\n[STEP 3.2] Reading labevents.csv (full dataset) ...")
t = start_timer()
labevents = rd.read_csv(
    f"local://{DATA_HOSP}/labevents.csv",
    include_paths=False,
)
labevents = labevents.select_columns([
    "subject_id", "hadm_id", "itemid", "charttime", "valuenum",
])
raw_count = labevents.count()
print(f"[METRIC] Raw labevents rows: {raw_count}")
log_stage("read_labevents", t)

# ─── 3. Filter + map batches ────────────────────────────────────────────────
print("\n[STEP 3.3] Filtering labevents ...")
t = start_timer()

broadcast_adm_times  = ray.put(adm_times)
broadcast_valid_hadm = ray.put(valid_hadm_ids)
broadcast_itemid_map = ray.put(ITEMID_TO_LABNAME)
broadcast_all_ids    = ray.put(set(ALL_LAB_ITEMIDS))

def filter_labs(batch: pd.DataFrame) -> pd.DataFrame:
    adm_times_local  = ray.get(broadcast_adm_times)
    valid_hadm_local = ray.get(broadcast_valid_hadm)
    itemid_map       = ray.get(broadcast_itemid_map)
    all_ids          = ray.get(broadcast_all_ids)

    batch["itemid"]    = pd.to_numeric(batch["itemid"],   errors="coerce")
    batch["valuenum"]  = pd.to_numeric(batch["valuenum"], errors="coerce")
    batch["hadm_id"]   = pd.to_numeric(batch["hadm_id"],  errors="coerce")
    batch["charttime"] = pd.to_datetime(batch["charttime"], errors="coerce")

    batch = batch[
        batch["itemid"].isin(all_ids)
        & batch["valuenum"].notna()
        & batch["hadm_id"].notna()
        & batch["charttime"].notna()
    ].copy()

    if batch.empty:
        return pd.DataFrame(columns=["hadm_id", "lab_name", "valuenum"])

    batch["hadm_id"] = batch["hadm_id"].astype("Int64")
    batch = batch[batch["hadm_id"].isin(valid_hadm_local)].copy()

    if batch.empty:
        return pd.DataFrame(columns=["hadm_id", "lab_name", "valuenum"])

    batch["lab_name"] = batch["itemid"].map(itemid_map)

    # 24h window filter
    batch = batch.merge(
        adm_times_local.reset_index(),
        on="hadm_id", how="inner"
    )
    batch = batch[
        (batch["charttime"] >= batch["admittime"])
        & (batch["charttime"] < batch["admittime"] + pd.Timedelta(hours=24))
    ].copy()

    return batch[["hadm_id", "lab_name", "valuenum"]]

labs_filtered = labevents.map_batches(
    filter_labs,
    batch_format="pandas",
    batch_size=200_000,
)
filtered_count = labs_filtered.count()
print(f"[METRIC] Lab rows after filter + 24h window: {filtered_count}")
log_stage("filter_labs", t)

# ─── 4. Aggregate → wide format ─────────────────────────────────────────────
print("\n[STEP 3.4] Aggregating labs to wide format ...")
t = start_timer()

df_labs = labs_filtered.to_pandas()
df_labs["hadm_id"] = df_labs["hadm_id"].astype("Int64")

agg = (
    df_labs.groupby(["hadm_id", "lab_name"])["valuenum"]
    .agg(["mean", "min", "max", "count"])
    .reset_index()
)

wide = agg.pivot_table(
    index="hadm_id",
    columns="lab_name",
    values=["mean", "min", "max"],
).copy()
wide.columns = [f"{lab}_{stat}" for stat, lab in wide.columns]
wide = wide.reset_index()

print(f"[METRIC] Admissions with labs: {len(wide)}")
print(f"[METRIC] Lab feature columns: {len([c for c in wide.columns if c != 'hadm_id'])}")

# Coverage per lab
for lab in list(LAB_ITEMIDS.keys())[:10]:
    col = f"{lab}_mean"
    if col in wide.columns:
        nn = wide[col].notna().sum()
        print(f"[METRIC] {col}: {nn}/{len(wide)} ({100*nn/len(wide):.1f}%)")
log_stage("aggregate_labs", t)

# ─── 5. Write output ────────────────────────────────────────────────────────
print("\n[STEP 3.5] Writing labs_agg ...")
t = start_timer()
labs_ds = rd.from_pandas(wide)
labs_ds.write_parquet(f"local://{OUT}/labs_agg")
log_stage("write_labs", t)

print(f"\n[RESULT] labs_agg written to: {OUT}/labs_agg")
print(f"[RESULT] Rows: {len(wide)} | Columns: {len(wide.columns)}")
log_stage("TOTAL step 03", t_total)
