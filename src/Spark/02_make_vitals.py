"""
02_make_vitals.py — PySpark Pipeline Step 2
Filter chartevents to extract vital signs for each hadm_id (first 24h window).

PySpark equivalent of Ray step 2.
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

# ─── Paths ─────────────────────────────────────────────────────────────────
PROJECT = Path(__file__).resolve().parents[2]
DATA_ICU = PROJECT / "data" / "raw" / "icu"
OUT = PROJECT / "outputs"
OUT.mkdir(exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from utils import start_timer, log_stage

# ─── Config ────────────────────────────────────────────────────────────────
VITAL_ITEMIDS = {
    220050: "sbp",
    220179: "sbp",
    220277: "spo2",
    220045: "hr",
    223761: "temperature",
    223762: "temperature",
}

VITAL_RANGES = {
    "sbp": (40, 300),
    "spo2": (50, 100),
    "hr": (20, 250),
    "temperature": (25, 45),
}

ALL_ITEMIDS = list(VITAL_ITEMIDS.keys())
PIVOT_VALUES = ["sbp", "spo2", "hr", "temperature"]

# ─── PySpark Init ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MIMIC-IV-Vitals-Extraction") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
t_total = start_timer()

# ─── 1. Load label table for admission times ────────────────────────────────
print("\n[STEP 2.1] Loading label_table for admission times ...")
t = start_timer()
label_df = spark.read.parquet(f"{OUT}/label_table_spark").select(
    F.col("hadm_id").cast("long").alias("hadm_id"),
    F.col("admittime").cast("timestamp").alias("admittime"),
)
valid_hadm_ids = label_df.select("hadm_id").dropna().distinct()
valid_hadm_count = valid_hadm_ids.count()
print(f"[METRIC] Valid hadm_ids: {valid_hadm_count}")
log_stage("load_label_table", t)

# ─── 2. Read chartevents ──────────────────────────────────────────────────
print("\n[STEP 2.2] Reading chartevents.csv ...")
t = start_timer()
chartevents = spark.read.csv(
    f"file://{DATA_ICU}/chartevents.csv",
    header=True,
    inferSchema=True,
).select([
    "subject_id", "hadm_id", "charttime", "itemid", "valuenum",
])
raw_count = chartevents.count()
print(f"[METRIC] Raw chartevents rows: {raw_count}")
log_stage("read_chartevents", t)

# ─── 3. Filter + transform vitals ──────────────────────────────────────────
print("\n[STEP 2.3] Filtering by vital itemids + cleaning ...")
t = start_timer()

vitals = (
    chartevents
    .withColumn("itemid", F.col("itemid").cast("int"))
    .withColumn("valuenum", F.col("valuenum").cast("double"))
    .withColumn("hadm_id", F.col("hadm_id").cast("long"))
    .withColumn("charttime", F.to_timestamp("charttime"))
    .filter(F.col("itemid").isin(ALL_ITEMIDS))
    .filter(F.col("valuenum").isNotNull())
    .filter(F.col("hadm_id").isNotNull())
    .filter(F.col("charttime").isNotNull())
)

vital_lookup = spark.createDataFrame(
    [(itemid, vital_name) for itemid, vital_name in VITAL_ITEMIDS.items()],
    ["itemid", "vital_name"],
)
vitals = vitals.join(F.broadcast(vital_lookup), on="itemid", how="inner")

# Convert F to C for itemid 223761
vitals = vitals.withColumn(
    "valuenum",
    F.when(
        F.col("itemid") == 223761,
        (F.col("valuenum") - 32) * 5 / 9
    ).otherwise(F.col("valuenum"))
)

# Apply physiological range filter
vitals = vitals.filter(
    (F.col("vital_name") == "sbp") & (F.col("valuenum").between(40, 300)) |
    (F.col("vital_name") == "spo2") & (F.col("valuenum").between(50, 100)) |
    (F.col("vital_name") == "hr") & (F.col("valuenum").between(20, 250)) |
    (F.col("vital_name") == "temperature") & (F.col("valuenum").between(25, 45))
)

# Filter to first 24h and valid hadm_ids
vitals = vitals.join(F.broadcast(label_df), on="hadm_id", how="inner")
vitals = vitals.filter(
    (F.col("charttime") >= F.col("admittime")) &
    (F.col("charttime") < F.col("admittime") + F.expr("INTERVAL 1 DAY"))
).select("hadm_id", "vital_name", "valuenum", "charttime")

filtered_count = vitals.count()
print(f"[METRIC] Rows after vital filter + 24h window: {filtered_count}")
log_stage("filter_vitals", t)

# ─── 4. Aggregate to wide format ───────────────────────────────────────────
print("\n[STEP 2.4] Aggregating vitals to wide format ...")
t = start_timer()

def pivot_stat(stat_name, agg_expr):
    df = vitals.groupBy("hadm_id").pivot("vital_name", PIVOT_VALUES).agg(agg_expr)
    for col in [c for c in df.columns if c != "hadm_id"]:
        df = df.withColumnRenamed(col, f"{col}_{stat_name}")
    return df

wide = pivot_stat("mean", F.avg("valuenum"))
for stat_name, agg_expr in [("min", F.min("valuenum")), ("max", F.max("valuenum")), ("count", F.count("valuenum"))]:
    wide = wide.join(pivot_stat(stat_name, agg_expr), on="hadm_id", how="outer")

count_cols = [c for c in wide.columns if c.endswith("_count")]
wide = wide.fillna(0, subset=count_cols)
wide = wide.cache()
wide_rows = wide.count()

print(f"[METRIC] Admissions with vitals: {wide_rows}")
print(f"[METRIC] Vitals columns: {[c for c in wide.columns if c != 'hadm_id']}")

# Coverage report
for vital in ["sbp", "spo2", "hr", "temperature"]:
    col_name = f"{vital}_mean"
    if col_name in wide.columns:
        non_null = wide.filter(F.col(col_name).isNotNull()).count()
        print(f"[METRIC] Coverage {vital}: {non_null}/{wide_rows} ({100*non_null/wide_rows:.1f}%)")
log_stage("aggregate_vitals", t)

# ─── 5. Write output ──────────────────────────────────────────────────────
print("\n[STEP 2.5] Writing vitals_agg ...")
t = start_timer()
output_path = f"{OUT}/vitals_agg_spark"
wide.write.mode("overwrite").parquet(output_path)
log_stage("write_vitals", t)

print(f"\n[RESULT] vitals_agg written to: {output_path}")
print(f"[RESULT] Rows: {wide_rows} | Columns: {len(wide.columns)}")
log_stage("TOTAL step 02", t_total)

spark.stop()
