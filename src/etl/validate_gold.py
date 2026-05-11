import argparse

from pyspark.sql import SparkSession
import builtins

from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max,
    min,
    sum as spark_sum,
)


GOLD_REQUIRED_COLUMNS = [
    "hadm_id",
    "subject_id",
    "age",
    "gender",
    "duration_days",
    "event_flag_mortality",
    "event_flag_readmission",
    "admityear",
    "split",
]

GOLD_FEATURE_GROUPS = {
    "vitals_mean": [
        "sbp_mean",
        "spo2_mean",
        "hr_mean",
        "temperature_mean",
    ],
    "vitals_min_max": [
        "sbp_min",
        "sbp_max",
    ],
    "vitals_count": [
        "sbp_count",
        "spo2_count",
        "hr_count",
        "temperature_count",
    ],
    "labs_mean": [
        "albumin_mean",
        "alt_mean",
        "anion_gap_mean",
        "ast_mean",
        "bicarbonate_mean",
        "bilirubin_total_mean",
        "bun_mean",
        "calcium_mean",
        "chloride_mean",
        "creatinine_mean",
        "glucose_mean",
        "hematocrit_mean",
        "hemoglobin_mean",
        "inr_mean",
        "lactate_mean",
        "magnesium_mean",
        "phosphate_mean",
        "platelet_mean",
        "potassium_mean",
        "pt_mean",
        "ptt_mean",
        "sodium_mean",
        "wbc_mean",
    ],
    "labs_min": [
        "albumin_min",
        "alt_min",
        "anion_gap_min",
        "ast_min",
        "bicarbonate_min",
        "bilirubin_total_min",
        "bun_min",
        "calcium_min",
        "chloride_min",
        "creatinine_min",
        "glucose_min",
        "hematocrit_min",
        "hemoglobin_min",
        "inr_min",
        "lactate_min",
        "magnesium_min",
        "phosphate_min",
        "platelet_min",
        "potassium_min",
        "pt_min",
        "ptt_min",
        "sodium_min",
        "wbc_min",
    ],
    "labs_max": [
        "albumin_max",
        "alt_max",
        "anion_gap_max",
        "ast_max",
        "bicarbonate_max",
        "bilirubin_total_max",
        "bun_max",
        "calcium_max",
        "chloride_max",
        "creatinine_max",
        "glucose_max",
        "hematocrit_max",
        "hemoglobin_max",
        "inr_max",
        "lactate_max",
        "magnesium_max",
        "phosphate_max",
        "platelet_max",
        "potassium_max",
        "pt_max",
        "ptt_max",
        "sodium_max",
        "wbc_max",
    ],
    "icd_chapters": [
        c
        for c in [
            "icd10_chap_01_infectious_parasitic",
            "icd10_chap_02_neoplasms",
            "icd10_chap_03_blood_diseases",
            "icd10_chap_04_endocrine_metabolic",
            "icd10_chap_05_mental_disorders",
            "icd10_chap_06_nervous_system",
            "icd10_chap_07_eye_diseases",
            "icd10_chap_08_ear_diseases",
            "icd10_chap_09_circulatory",
            "icd10_chap_10_respiratory",
            "icd10_chap_11_digestive",
            "icd10_chap_12_skin_diseases",
            "icd10_chap_13_musculoskeletal",
            "icd10_chap_14_genitourinary",
            "icd10_chap_15_pregnancy_childbirth",
            "icd10_chap_16_perinatal",
            "icd10_chap_17_congenital",
            "icd10_chap_18_symptoms_signs",
            "icd10_chap_19_injury_poisoning",
            "icd10_chap_20_external_causes",
            "icd10_chap_21_health_status_factors",
        ]
    ],
    "note_embeddings": [f"note_emb_{i}" for i in range(1, 129)],
}


def validate_gold(spark, base_path):
    gold_path = f"{base_path}/gold/analytical_dataset"
    print(f"[INFO] Reading Gold dataset from: {gold_path}")

    df = spark.read.parquet(gold_path)
    total_count = df.count()
    print(f"[METRIC] Total rows: {total_count}")

    if total_count == 0:
        print("[FAIL] Gold dataset is empty")
        return False

    columns = set(df.columns)
    missing_required = [c for c in GOLD_REQUIRED_COLUMNS if c not in columns]
    if missing_required:
        print(f"[FAIL] Missing required columns: {missing_required}")
        return False
    print("[PASS] Required columns exist")

    print(f"[METRIC] Total columns: {len(df.columns)}")

    distinct_hadm = df.select("hadm_id").distinct().count()
    duplicate_count = total_count - distinct_hadm
    print(f"[METRIC] Distinct hadm_id: {distinct_hadm}")
    print(f"[METRIC] Duplicate hadm_id: {duplicate_count}")
    if duplicate_count > 0:
        print("[WARN] Duplicate hadm_id found (expected for eICU external validation)")
    else:
        print("[PASS] No duplicate hadm_id")

    null_exprs = [
        spark_sum(col(c).isNull().cast("int")).alias(c) for c in GOLD_REQUIRED_COLUMNS
    ]
    nulls = df.agg(count("*").alias("rows"), *null_exprs).collect()[0]
    for c in GOLD_REQUIRED_COLUMNS:
        null_count = nulls[c]
        print(f"[METRIC] Null {c}: {null_count} / {nulls['rows']}")

    print("\n[INFO] Split distribution:")
    df.groupBy("split").agg(
        count("*").alias("rows"),
        countDistinct("hadm_id").alias("distinct_hadm"),
    ).orderBy("split").show(truncate=False)

    print("[INFO] Event rates by split:")
    df.groupBy("split").agg(
        count("*").alias("rows"),
        avg(col("event_flag_mortality")).alias("mortality_rate"),
        spark_sum(col("event_flag_mortality")).alias("mortality_count"),
        avg(col("event_flag_readmission")).alias("readmission_rate"),
        spark_sum(col("event_flag_readmission")).alias("readmission_count"),
    ).orderBy("split").show(truncate=False)

    print("[INFO] Temporal split validation:")
    for split_name in ["train", "val", "test"]:
        split_df = df.filter(col("split") == split_name)
        if split_df.count() > 0:
            year_stats = split_df.agg(
                count("*").alias("rows"),
                min("admityear").alias("min_year"),
                max("admityear").alias("max_year"),
            ).collect()[0]
            print(
                f"  {split_name}: rows={year_stats['rows']}, "
                f"admityear=[{year_stats['min_year']}, {year_stats['max_year']}]"
            )

    for group_name, feature_cols in GOLD_FEATURE_GROUPS.items():
        existing_cols = [c for c in feature_cols if c in columns]
        if existing_cols:
            null_exprs_group = [
                spark_sum(col(c).isNull().cast("int")).alias(c) for c in existing_cols
            ]
            nulls_group = df.agg(count("*").alias("rows"), *null_exprs_group).collect()[
                0
            ]
            missing_total = builtins.sum(nulls_group[c] for c in existing_cols)
            total_cells = nulls_group["rows"] * len(existing_cols)
            missing_rate = (missing_total / total_cells * 100) if total_cells > 0 else 0
            print(
                f"\n[INFO] Feature group '{group_name}': {len(existing_cols)}/{len(feature_cols)} columns present, missing rate: {missing_rate:.2f}%"
            )
            if len(existing_cols) <= 10:
                for c in existing_cols:
                    print(f"  {c}: {nulls_group[c]} / {nulls_group['rows']} null")
        else:
            print(
                f"\n[WARN] Feature group '{group_name}': 0/{len(feature_cols)} columns found (not yet generated)"
            )

    print("\n[INFO] Gold schema:")
    df.printSchema()

    return True


def main():
    parser = argparse.ArgumentParser(description="Validate Gold Analytical Dataset")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    builder = SparkSession.builder.appName("ValidateGoldDataset")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Validating Gold Dataset in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    passed = validate_gold(spark, base_path)

    spark.stop()
    if not passed:
        raise RuntimeError("Gold validation failed")
    print("\n[PASS] Gold validation completed successfully")


if __name__ == "__main__":
    main()
