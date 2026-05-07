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
    "vitals": [
        "sbp_mean",
        "sbp_min",
        "sbp_max",
        "spo2_mean",
        "hr_mean",
        "temperature_mean",
    ],
    "labs": [
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
    ],
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
                f"\n[INFO] Feature group '{group_name}': {len(existing_cols)} columns, missing rate: {missing_rate:.2f}%"
            )
            for c in existing_cols:
                print(f"  {c}: {nulls_group[c]} / {nulls_group['rows']} null")
        else:
            print(f"\n[WARN] Feature group '{group_name}': no columns found")

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
