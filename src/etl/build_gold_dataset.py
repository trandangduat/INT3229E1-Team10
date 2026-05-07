import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    first,
    lit,
    sum,
    udf,
    when,
)
from pyspark.sql.types import IntegerType


ICD10_CHAPTER_RANGES = [
    ("A00", "B99", 1),
    ("C00", "D49", 2),
    ("D50", "D89", 3),
    ("E00", "E89", 4),
    ("F01", "F99", 5),
    ("G00", "G99", 6),
    ("H00", "H59", 7),
    ("H60", "H95", 8),
    ("I00", "I99", 9),
    ("J00", "J99", 10),
    ("K00", "K95", 11),
    ("L00", "L99", 12),
    ("M00", "M99", 13),
    ("N00", "N99", 14),
    ("O00", "O9A", 15),
    ("P00", "P96", 16),
    ("Q00", "Q99", 17),
    ("R00", "R99", 18),
    ("S00", "T88", 19),
    ("V00", "Y99", 20),
    ("Z00", "Z99", 21),
]

ICD9_CHAPTER_RANGES = [
    ("001", "139", 1),
    ("140", "239", 2),
    ("240", "279", 4),
    ("280", "289", 3),
    ("290", "319", 5),
    ("320", "389", 6),
    ("390", "459", 9),
    ("460", "519", 10),
    ("520", "579", 11),
    ("580", "629", 14),
    ("630", "679", 15),
    ("680", "709", 12),
    ("710", "739", 13),
    ("740", "759", 17),
    ("760", "779", 16),
    ("780", "799", 18),
    ("800", "999", 19),
]

ICD10_CHAPTER_NAMES = {
    1: "infectious_parasitic",
    2: "neoplasms",
    3: "blood_diseases",
    4: "endocrine_metabolic",
    5: "mental_disorders",
    6: "nervous_system",
    7: "eye_diseases",
    8: "ear_diseases",
    9: "circulatory",
    10: "respiratory",
    11: "digestive",
    12: "skin_diseases",
    13: "musculoskeletal",
    14: "genitourinary",
    15: "pregnancy_childbirth",
    16: "perinatal",
    17: "congenital",
    18: "symptoms_signs",
    19: "injury_poisoning",
    20: "external_causes",
    21: "health_status_factors",
}

EXPECTED_LAB_NAMES = [
    "albumin",
    "alt",
    "anion_gap",
    "ast",
    "bicarbonate",
    "bilirubin_total",
    "bun",
    "calcium",
    "chloride",
    "creatinine",
    "glucose",
    "hematocrit",
    "hemoglobin",
    "inr",
    "lactate",
    "magnesium",
    "phosphate",
    "platelet",
    "potassium",
    "pt",
    "ptt",
    "sodium",
    "wbc",
]


def map_icd10_to_chapter(icd_code):
    if icd_code is None or len(icd_code) == 0:
        return None
    code = icd_code.upper().strip()
    for low, high, chapter in ICD10_CHAPTER_RANGES:
        if low <= code <= high:
            return chapter
    return None


def map_icd9_to_chapter(icd_code):
    if icd_code is None or len(icd_code) == 0:
        return None
    code = icd_code.strip().zfill(3)
    for low, high, chapter in ICD9_CHAPTER_RANGES:
        if low <= code <= high:
            return chapter
    return None


def map_icd_to_chapter(icd_code, icd_version):
    if icd_code is None:
        return None
    if icd_version == 10:
        return map_icd10_to_chapter(icd_code)
    elif icd_version == 9:
        return map_icd9_to_chapter(icd_code)
    return None


def has_parquet_files(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    if not fs.exists(hadoop_path):
        return False
    statuses = fs.listStatus(hadoop_path)
    for status in statuses:
        name = status.getPath().getName()
        if name.endswith(".parquet") or name.endswith(".snappy.parquet"):
            return True
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Build Gold Analytical Dataset from Silver Layer"
    )
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    parser.add_argument(
        "--include-eicu",
        action="store_true",
        default=False,
        help="Union eICU harmonized data into Gold (external validation)",
    )
    parser.add_argument(
        "--include-notes",
        action="store_true",
        default=False,
        help="Left join note embeddings (requires silver/note_embeddings)",
    )
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    print(f"[INFO] Starting build_gold_dataset job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")
    print(f"[INFO] Include eICU: {args.include_eicu}")
    print(f"[INFO] Include notes: {args.include_notes}")

    builder = SparkSession.builder.appName("GoldLayer_BuildDataset")
    if args.env == "local":
        builder = builder.master("local[*]")
    builder = builder.config("spark.sql.shuffle.partitions", "200").config(
        "spark.sql.parquet.enableVectorizedReader", "false"
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    start_time = time.time()

    # ──────────────────────────────────────────────
    # STEP 1: Load admissions base table
    # ──────────────────────────────────────────────
    print("\n[STEP 1] Loading admissions base table...")
    admissions_path = f"{base_path}/silver/admissions"
    df_base = spark.read.parquet(admissions_path).select(
        col("hadm_id").cast("long").alias("hadm_id"),
        col("subject_id").cast("long").alias("subject_id"),
        col("age").cast("int").alias("age"),
        col("gender"),
        col("duration_days").cast("int").alias("duration_days"),
        col("event_flag_mortality").cast("int").alias("event_flag_mortality"),
        col("event_flag_readmission").cast("int").alias("event_flag_readmission"),
        col("admittime"),
        col("dischtime"),
        col("admityear").cast("int").alias("admityear"),
    )
    base_count = df_base.count()
    print(f"[METRIC] Base admissions count: {base_count}")

    df = df_base

    # ──────────────────────────────────────────────
    # STEP 2: Left join vitals from chartevents_agg
    # ──────────────────────────────────────────────
    print("\n[STEP 2] Left joining vitals (chartevents_agg)...")
    vitals_path = f"{base_path}/silver/chartevents_agg"
    vitals_cols = [
        "hadm_id",
        "sbp_mean",
        "sbp_min",
        "sbp_max",
        "spo2_mean",
        "hr_mean",
        "temperature_mean",
    ]
    try:
        if has_parquet_files(spark, vitals_path):
            df_vitals = spark.read.parquet(vitals_path).select(
                col("hadm_id").cast("long").alias("hadm_id"),
                col("sbp_mean"),
                col("sbp_min"),
                col("sbp_max"),
                col("spo2_mean"),
                col("hr_mean"),
                col("temperature_mean"),
            )
            vitals_count = df_vitals.count()
            print(f"[METRIC] Vitals rows: {vitals_count}")

            df = df.join(df_vitals, on="hadm_id", how="left")
            join_count = df.count()
            print(f"[METRIC] Row count after vitals join: {join_count}")

            missing_vitals = df.agg(
                count("*").alias("rows"),
                sum(col("sbp_mean").isNull().cast("int")).alias("missing_sbp"),
                sum(col("spo2_mean").isNull().cast("int")).alias("missing_spo2"),
                sum(col("hr_mean").isNull().cast("int")).alias("missing_hr"),
                sum(col("temperature_mean").isNull().cast("int")).alias("missing_temp"),
            ).collect()[0]
            print(
                f"[METRIC] Missing vitals: "
                f"SBP {missing_vitals['missing_sbp']}/{missing_vitals['rows']}, "
                f"SpO2 {missing_vitals['missing_spo2']}/{missing_vitals['rows']}, "
                f"HR {missing_vitals['missing_hr']}/{missing_vitals['rows']}, "
                f"Temp {missing_vitals['missing_temp']}/{missing_vitals['rows']}"
            )
        else:
            print(
                f"[WARN] No parquet files found at {vitals_path}, adding null vitals columns"
            )
            for vc in vitals_cols[1:]:
                df = df.withColumn(vc, lit(None).cast("double"))
    except Exception as exc:
        print(f"[WARN] Vitals data not available, adding null columns: {exc}")
        for vc in vitals_cols[1:]:
            df = df.withColumn(vc, lit(None).cast("double"))

    # ──────────────────────────────────────────────
    # STEP 3: Pivot labs from long → wide, left join
    # ──────────────────────────────────────────────
    print("\n[STEP 3] Pivoting labs (long → wide) and left joining...")
    labs_path = f"{base_path}/silver/labs_agg"
    try:
        if has_parquet_files(spark, labs_path):
            df_labs = spark.read.parquet(labs_path).select(
                col("hadm_id").cast("long").alias("hadm_id"),
                col("lab_name"),
                col("lab_mean"),
            )
            labs_count = df_labs.count()
            labs_admissions = df_labs.select("hadm_id").distinct().count()
            print(f"[METRIC] Labs long-format rows: {labs_count}")
            print(f"[METRIC] Distinct admissions with labs: {labs_admissions}")

            lab_names = [
                row["lab_name"]
                for row in df_labs.select("lab_name").distinct().collect()
            ]
            lab_names.sort()
            print(f"[METRIC] Lab features ({len(lab_names)}): {lab_names}")

            df_labs_wide = (
                df_labs.groupBy("hadm_id").pivot("lab_name").agg(first("lab_mean"))
            )
            df_labs_wide_count = df_labs_wide.count()
            print(f"[METRIC] Labs wide-format rows: {df_labs_wide_count}")

            df = df.join(df_labs_wide, on="hadm_id", how="left")
            join_count = df.count()
            print(f"[METRIC] Row count after labs join: {join_count}")

            missing_labs = (
                df.agg(
                    sum(col(lab_names[0]).isNull().cast("int")).alias("missing")
                ).collect()[0]["missing"]
                if lab_names
                else 0
            )
            print(
                f"[METRIC] Missing first lab ({lab_names[0] if lab_names else 'N/A'}): "
                f"{missing_labs}/{base_count}"
            )
        else:
            print(
                f"[WARN] No parquet files found at {labs_path}, adding null lab columns"
            )
            for ln in EXPECTED_LAB_NAMES:
                df = df.withColumn(ln, lit(None).cast("double"))
    except Exception as exc:
        print(f"[WARN] Labs data not available, adding null columns: {exc}")
        for ln in EXPECTED_LAB_NAMES:
            df = df.withColumn(ln, lit(None).cast("double"))

    # ──────────────────────────────────────────────
    # STEP 4: One-hot ICD chapters, left join
    # ──────────────────────────────────────────────
    print("\n[STEP 4] One-hot encoding ICD chapters and left joining...")
    diagnoses_path = f"{base_path}/silver/diagnoses"
    df_diag = spark.read.parquet(diagnoses_path).select(
        col("hadm_id").cast("long").alias("hadm_id"),
        col("icd_code"),
        col("icd_version").cast("int").alias("icd_version"),
    )
    diag_count = df_diag.count()
    print(f"[METRIC] Diagnoses rows: {diag_count}")

    icd_chapter_udf = udf(map_icd_to_chapter, IntegerType())
    df_diag_chapter = df_diag.withColumn(
        "icd_chapter",
        icd_chapter_udf(col("icd_code"), col("icd_version")),
    ).filter(col("icd_chapter").isNotNull())

    chapter_count = df_diag_chapter.count()
    print(f"[METRIC] Diagnoses with mapped chapter: {chapter_count}")

    print("[METRIC] ICD chapter distribution (top 25):")
    df_diag_chapter.groupBy("icd_chapter").count().orderBy(col("count").desc()).show(
        25, truncate=False
    )

    df_diag_onehot = df_diag_chapter.select("hadm_id", "icd_chapter").distinct()

    chapters_in_data = [
        row["icd_chapter"]
        for row in df_diag_onehot.select("icd_chapter").distinct().collect()
    ]
    chapters_in_data.sort()
    print(f"[METRIC] Chapters present in data: {chapters_in_data}")

    for ch in chapters_in_data:
        ch_name = ICD10_CHAPTER_NAMES.get(ch, f"chap_{ch:02d}")
        df_diag_onehot = df_diag_onehot.withColumn(
            f"icd10_chap_{ch:02d}_{ch_name}",
            when(col("icd_chapter") == ch, 1).otherwise(0),
        )

    df_diag_onehot = df_diag_onehot.drop("icd_chapter")

    df_diag_agg = df_diag_onehot.groupBy("hadm_id").agg(
        *[sum(col(c)).alias(c) for c in df_diag_onehot.columns if c != "hadm_id"]
    )

    df_diag_agg = df_diag_agg.select(
        col("hadm_id"),
        *[
            when(col(c) > 0, 1).otherwise(0).alias(c)
            for c in df_diag_agg.columns
            if c != "hadm_id"
        ],
    )

    onehot_count = df_diag_agg.count()
    print(f"[METRIC] One-hot diagnosis rows: {onehot_count}")

    df = df.join(df_diag_agg, on="hadm_id", how="left")
    join_count = df.count()
    print(f"[METRIC] Row count after diagnoses join: {join_count}")

    diag_onehot_cols = [c for c in df_diag_agg.columns if c != "hadm_id"]
    if diag_onehot_cols:
        missing_diag = df.agg(
            sum(col(diag_onehot_cols[0]).isNull().cast("int")).alias("missing")
        ).collect()[0]["missing"]
        print(
            f"[METRIC] Missing ICD one-hot ({diag_onehot_cols[0]}): "
            f"{missing_diag}/{base_count}"
        )

    # ──────────────────────────────────────────────
    # STEP 5: (Optional) Left join note embeddings
    # ──────────────────────────────────────────────
    if args.include_notes:
        print("\n[STEP 5] Left joining note embeddings...")
        notes_emb_path = f"{base_path}/silver/note_embeddings"
        try:
            df_notes_emb = spark.read.parquet(notes_emb_path)
            emb_cols = [c for c in df_notes_emb.columns if c.startswith("note_emb_")]
            df_notes_emb = df_notes_emb.select(
                col("hadm_id").cast("long").alias("hadm_id"), *emb_cols
            )
            notes_emb_count = df_notes_emb.count()
            print(f"[METRIC] Note embeddings rows: {notes_emb_count}")

            df = df.join(df_notes_emb, on="hadm_id", how="left")
            join_count = df.count()
            print(f"[METRIC] Row count after notes join: {join_count}")
        except Exception as exc:
            print(f"[WARN] Note embeddings not available, skipping: {exc}")
    else:
        print("\n[STEP 5] Skipping note embeddings (--include-notes not set)")

    # ──────────────────────────────────────────────
    # STEP 6: (Optional) Union eICU harmonized
    # ──────────────────────────────────────────────
    if args.include_eicu:
        print("\n[STEP 6] Union eICU harmonized data...")
        eicu_path = f"{base_path}/silver/eicu_harmonized"
        try:
            df_eicu = spark.read.parquet(eicu_path)
            eicu_count = df_eicu.count()
            print(f"[METRIC] eICU harmonized rows: {eicu_count}")

            df_eicu_gold = df_eicu.select(
                col("stay_id_eicu").cast("long").alias("hadm_id"),
                lit(None).cast("long").alias("subject_id"),
                col("age").cast("int").alias("age"),
                col("gender"),
                lit(None).cast("int").alias("duration_days"),
                col("event_flag_mortality").cast("int").alias("event_flag_mortality"),
                lit(0).cast("int").alias("event_flag_readmission"),
                lit(None).cast("timestamp").alias("admittime"),
                lit(None).cast("timestamp").alias("dischtime"),
                lit(None).cast("int").alias("admityear"),
                col("sbp_mean"),
                col("sbp_min"),
                col("sbp_max"),
                col("spo2_mean"),
                col("hr_mean"),
                col("temperature_mean"),
            )

            for c in df.columns:
                if c not in df_eicu_gold.columns:
                    df_eicu_gold = df_eicu_gold.withColumn(c, lit(None))

            df_eicu_gold = df_eicu_gold.select(df.columns)

            df = df.unionByName(df_eicu_gold, allowMissingColumns=True)
            union_count = df.count()
            print(f"[METRIC] Row count after eICU union: {union_count}")
        except Exception as exc:
            print(f"[WARN] eICU data not available, skipping: {exc}")
    else:
        print("\n[STEP 6] Skipping eICU union (--include-eicu not set)")

    # ──────────────────────────────────────────────
    # STEP 7: Add temporal split column
    # ──────────────────────────────────────────────
    print("\n[STEP 7] Adding temporal split column...")
    df = df.withColumn(
        "split",
        when(col("admityear") < 2019, "train")
        .when(col("admityear") == 2019, "val")
        .when(col("admityear").isNotNull(), "test")
        .otherwise("test_external"),
    )

    split_dist = df.groupBy("split").count().orderBy("split")
    print("[METRIC] Split distribution:")
    split_dist.show(truncate=False)

    # ──────────────────────────────────────────────
    # STEP 8: Drop intermediate columns, write output
    # ──────────────────────────────────────────────
    print("\n[STEP 8] Writing Gold dataset...")
    output_path = f"{base_path}/gold/analytical_dataset"

    df_output = df.drop("admittime", "dischtime")

    df_output.write.mode("overwrite").partitionBy("split").option(
        "compression", "snappy"
    ).parquet(output_path)

    total_count = df_output.count()
    distinct_hadm = df_output.select("hadm_id").distinct().count()
    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"[RESULT] Gold dataset written to: {output_path}")
    print(f"[RESULT] Total rows: {total_count}")
    print(f"[RESULT] Distinct hadm_id: {distinct_hadm}")
    print(f"[RESULT] Total columns: {len(df_output.columns)}")
    print(f"[RESULT] Columns: {df_output.columns}")
    print(f"[RESULT] Elapsed time: {elapsed:.1f}s")

    print(f"\n[METRIC] Event rates by split:")
    df_output.groupBy("split").agg(
        count("*").alias("rows"),
        avg(col("event_flag_mortality")).alias("mortality_rate"),
        sum(col("event_flag_mortality")).alias("mortality_count"),
        avg(col("event_flag_readmission")).alias("readmission_rate"),
        sum(col("event_flag_readmission")).alias("readmission_count"),
    ).orderBy("split").show(truncate=False)

    print("[INFO] build_gold_dataset job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
