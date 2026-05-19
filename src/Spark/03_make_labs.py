"""
03_make_labs.py — PySpark Pipeline Step 3
Filter labevents to extract 23 lab features for each hadm_id (first 24h window).

PySpark equivalent of Ray step 3.
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT = Path(__file__).resolve().parents[2]
DATA_HOSP = PROJECT / "data" / "raw" / "hosp"
OUT = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── Config (from silver_labs.py) ──────────────────────────────────────────
LAB_ITEMIDS = {
    "hematocrit": [51221],
    "hemoglobin": [51222],
    "platelet": [51265],
    "wbc": [51301],
    "creatinine": [50912, 52546],
    "bun": [51006, 52647],
    "sodium": [50983, 52623],
    "potassium": [50971, 52610],
    "chloride": [50902, 52535],
    "bicarbonate": [50882],
    "anion_gap": [50868],
    "glucose": [50931, 52569],
    "calcium": [50893],
    "magnesium": [50960],
    "phosphate": [50970],
    "inr": [51237, 51675],
    "pt": [51274],
    "ptt": [51275, 52923],
    "alt": [50861],
    "ast": [50878],
    "bilirubin_total": [50885, 53089],
    "albumin": [50862, 53085],
    "lactate": [50813, 52442, 53154],
}

ALL_LAB_ITEMIDS = [iid for ids in LAB_ITEMIDS.values() for iid in ids]
ITEMID_TO_LABNAME = {iid: name for name, ids in LAB_ITEMIDS.items() for iid in ids}
PIVOT_VALUES = list(LAB_ITEMIDS.keys())

# ─── PySpark Init ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MIMIC-IV-Labs-Extraction") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.network.timeout", "300s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
t_total = start_timer()

# ─── 1. Load label table for admission times ────────────────────────────────
print("\n[STEP 3.1] Loading label_table for admission window ...")
t = start_timer()
label_df = spark.read.parquet(f"{OUT}/label_table_spark").select(
    F.col("hadm_id").cast("long").alias("hadm_id"),
    F.col("admittime").cast("timestamp").alias("admittime"),
)
valid_hadm_ids = label_df.select("hadm_id").dropna().distinct()
valid_hadm_count = valid_hadm_ids.count()
print(f"[METRIC] Valid hadm_ids: {valid_hadm_count}")
log_stage("load_label", t)

# ─── 2. Read labevents ────────────────────────────────────────────────────
print("\n[STEP 3.2] Reading labevents.csv ...")
t = start_timer()
labevents = spark.read.csv(
    f"file://{DATA_HOSP}/labevents.csv",
    header=True,
    inferSchema=True,
).select([
    "subject_id", "hadm_id", "itemid", "charttime", "valuenum",
])
raw_count = labevents.count()
print(f"[METRIC] Raw labevents rows: {raw_count}")
log_stage("read_labevents", t)

# ─── 3. Filter + map labs ─────────────────────────────────────────────────
print("\n[STEP 3.3] Filtering labevents ...")
t = start_timer()

lab_map = F.create_map(*[
    x for pair in ITEMID_TO_LABNAME.items() for x in (F.lit(pair[0]), F.lit(pair[1]))
])

# Cast types and filter
labs = (
    labevents
    .withColumn("itemid", F.col("itemid").cast("int"))
    .withColumn("valuenum", F.col("valuenum").cast("double"))
    .withColumn("hadm_id", F.col("hadm_id").cast("long"))
    .withColumn("charttime", F.to_timestamp("charttime"))
    .filter(F.col("itemid").isin(ALL_LAB_ITEMIDS))
    .filter(F.col("valuenum").isNotNull())
    .filter(F.col("hadm_id").isNotNull())
    .filter(F.col("charttime").isNotNull())
)

lab_lookup = spark.createDataFrame(
    [(itemid, lab_name) for itemid, lab_name in ITEMID_TO_LABNAME.items()],
    ["itemid", "lab_name"],
)
labs = labs.join(F.broadcast(lab_lookup), on="itemid", how="inner")

# Filter to first 24h and valid hadm_ids
labs = labs.join(F.broadcast(label_df), on="hadm_id", how="inner")
labs = labs.filter(
    (F.col("charttime") >= F.col("admittime")) &
    (F.col("charttime") < F.col("admittime") + F.expr("INTERVAL 1 DAY"))
).select("hadm_id", "lab_name", "valuenum", "charttime")

filtered_count = labs.count()
print(f"[METRIC] Lab rows after filter + 24h window: {filtered_count}")
log_stage("filter_labs", t)

# ─── 4. Aggregate to wide format ───────────────────────────────────────────
print("\n[STEP 3.4] Aggregating labs to wide format ...")
t = start_timer()

# Simple single pivot: mean values only (to get through quickly)
# Min/max can be added later if needed
wide = (
    labs
    .select("hadm_id", "lab_name", "valuenum")
    .groupBy("hadm_id")
    .pivot("lab_name", PIVOT_VALUES)
    .agg(F.avg("valuenum"))
)

# Rename columns to include _mean suffix
for col in wide.columns:
    if col != "hadm_id":
        wide = wide.withColumnRenamed(col, f"{col}_mean")

wide_rows = wide.count()

print(f"[METRIC] Admissions with labs: {wide_rows}")
print(f"[METRIC] Lab feature columns: {len([c for c in wide.columns if c != 'hadm_id'])}")

# Coverage per lab
for lab in list(LAB_ITEMIDS.keys())[:10]:
    col = f"{lab}_mean"
    if col in wide.columns:
        nn = wide.filter(F.col(col).isNotNull()).count()
        print(f"[METRIC] {col}: {nn}/{wide_rows} ({100*nn/wide_rows:.1f}%)")
log_stage("aggregate_labs", t)

# ─── 5. Write output ──────────────────────────────────────────────────────
print("\n[STEP 3.5] Writing labs_agg ...")
t = start_timer()
output_path = f"{OUT}/labs_agg_spark"
wide.write.mode("overwrite").parquet(output_path)
log_stage("write_labs", t)

print(f"\n[RESULT] labs_agg written to: {output_path}")
print(f"[RESULT] Rows: {wide_rows} | Columns: {len(wide.columns)}")
log_stage("TOTAL step 03", t_total)

spark.stop()
