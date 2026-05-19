"""
04_make_diagnoses.py — PySpark Pipeline Step 4
One-hot encode ICD chapters from diagnoses_icd.csv.

PySpark equivalent of Ray step 4.
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

# ─── ICD chapter mappings ──────────────────────────────────────────────────
ICD10_CHAPTER_RANGES = [
    ("A00", "B99", 1), ("C00", "D49", 2), ("D50", "D89", 3),
    ("E00", "E89", 4), ("F01", "F99", 5), ("G00", "G99", 6),
    ("H00", "H59", 7), ("H60", "H95", 8), ("I00", "I99", 9),
    ("J00", "J99", 10), ("K00", "K95", 11), ("L00", "L99", 12),
    ("M00", "M99", 13), ("N00", "N99", 14), ("O00", "O9A", 15),
    ("P00", "P96", 16), ("Q00", "Q99", 17), ("R00", "R99", 18),
    ("S00", "T88", 19), ("V00", "Y99", 20), ("Z00", "Z99", 21),
]

ICD9_CHAPTER_RANGES = [
    ("001", "139", 1), ("140", "239", 2), ("240", "279", 4),
    ("280", "289", 3), ("290", "319", 5), ("320", "389", 6),
    ("390", "459", 9), ("460", "519", 10), ("520", "579", 11),
    ("580", "629", 14), ("630", "679", 15), ("680", "709", 12),
    ("710", "739", 13), ("740", "759", 17), ("760", "779", 16),
    ("780", "799", 18), ("800", "999", 19),
]

ICD10_CHAPTER_NAMES = {
    1: "infectious_parasitic", 2: "neoplasms",
    3: "blood_diseases", 4: "endocrine_metabolic",
    5: "mental_disorders", 6: "nervous_system",
    7: "eye_diseases", 8: "ear_diseases",
    9: "circulatory", 10: "respiratory",
    11: "digestive", 12: "skin_diseases",
    13: "musculoskeletal", 14: "genitourinary",
    15: "pregnancy_childbirth", 16: "perinatal",
    17: "congenital", 18: "symptoms_signs",
    19: "injury_poisoning", 20: "external_causes",
    21: "health_status_factors",
}

ALL_CHAPTERS = list(range(1, 22))


def chapter_expr():
    code_upper = F.upper(F.trim(F.col("icd_code")))
    code_digits = F.lpad(F.regexp_extract(F.col("icd_code"), r"^(\d+)", 1), 3, "0")
    expr = F.lit(None).cast("int")

    for low, high, chapter in ICD10_CHAPTER_RANGES:
        expr = F.when(
            (F.col("icd_version") == 10)
            & (code_upper >= F.lit(low))
            & (code_upper <= F.lit(high)),
            F.lit(chapter),
        ).otherwise(expr)

    for low, high, chapter in ICD9_CHAPTER_RANGES:
        expr = F.when(
            (F.col("icd_version") == 9)
            & (code_digits >= F.lit(low))
            & (code_digits <= F.lit(high)),
            F.lit(chapter),
        ).otherwise(expr)

    return expr


# ─── PySpark Init ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MIMIC-IV-Diagnoses-Encoding") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.network.timeout", "300s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
t_total = start_timer()

# ─── 1. Load valid hadm_ids ───────────────────────────────────────────────
print("\n[STEP 4.1] Loading label_table ...")
t = start_timer()
label_df = spark.read.parquet(f"{OUT}/label_table_spark").select(
    F.col("hadm_id").cast("long").alias("hadm_id")
)
valid_hadm_count = label_df.count()
print(f"[METRIC] Valid hadm_ids: {valid_hadm_count}")
log_stage("load_label", t)

# ─── 2. Read diagnoses_icd.csv ────────────────────────────────────────────
print("\n[STEP 4.2] Reading diagnoses_icd.csv ...")
t = start_timer()
diagnoses = spark.read.csv(
    f"file://{DATA_HOSP}/diagnoses_icd.csv",
    header=True,
    inferSchema=True,
).select([
    "subject_id", "hadm_id", "seq_num", "icd_code", "icd_version",
])
raw_count = diagnoses.count()
print(f"[METRIC] Raw diagnoses rows: {raw_count}")
log_stage("read_diagnoses", t)

# ─── 3. Map ICD → chapters ────────────────────────────────────────────────
print("\n[STEP 4.3] Mapping ICD codes → chapter numbers ...")
t = start_timer()

df_diag = (
    diagnoses
    .withColumn("hadm_id", F.col("hadm_id").cast("long"))
    .withColumn("icd_version", F.col("icd_version").cast("int"))
    .withColumn("icd_code", F.upper(F.trim(F.col("icd_code"))))
    .filter(F.col("hadm_id").isNotNull())
    .filter(F.col("icd_code").isNotNull())
    .filter(F.col("icd_code") != "")
    .filter(F.col("icd_code") != "NAN")
    .filter(F.col("icd_version").isNotNull())
    .join(F.broadcast(label_df), on="hadm_id", how="inner")
)
cohort_count = df_diag.count()
print(f"[METRIC] Diagnoses rows in cohort: {cohort_count}")

df_diag = df_diag.withColumn("icd_chapter", chapter_expr())
df_diag = df_diag.filter(F.col("icd_chapter").isNotNull())

chapters_present = [row[0] for row in df_diag.groupBy("icd_chapter").count().orderBy("icd_chapter").collect()]
print(f"[METRIC] Chapters present: {chapters_present}")
log_stage("map_icd_chapters", t)

# ─── 4. One-hot encoding per hadm_id ───────────────────────────────────────
print("\n[STEP 4.4] One-hot encoding ICD chapters ...")
t = start_timer()

df_onehot = df_diag.select("hadm_id", "icd_chapter").dropDuplicates()
df_wide = df_onehot.groupBy("hadm_id").pivot("icd_chapter", ALL_CHAPTERS).agg(F.max(F.lit(1)))
for ch in ALL_CHAPTERS:
    ch_name = ICD10_CHAPTER_NAMES.get(ch, f"chap_{ch:02d}")
    df_wide = df_wide.withColumnRenamed(str(ch), f"icd10_chap_{ch:02d}_{ch_name}")
df_wide = df_wide.fillna(0)
df_wide = df_wide.cache()
wide_rows = df_wide.count()

print(f"[METRIC] Admissions with diagnoses: {wide_rows}")
print(f"[METRIC] ICD chapter columns: {len([c for c in df_wide.columns if c != 'hadm_id'])}")
log_stage("onehot_icd", t)

# ─── 5. Write output ──────────────────────────────────────────────────────
print("\n[STEP 4.5] Writing diagnoses_onehot ...")
t = start_timer()
output_path = f"{OUT}/diagnoses_onehot_spark"
df_wide.write.mode("overwrite").parquet(output_path)
log_stage("write_diagnoses", t)

print(f"\n[RESULT] diagnoses_onehot written to: {output_path}")
print(f"[RESULT] Rows: {wide_rows} | Columns: {len(df_wide.columns)}")
log_stage("TOTAL step 04", t_total)

spark.stop()
