import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, min, max


def main():
    parser = argparse.ArgumentParser(description="Inspect Gold Analytical Dataset")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    parser.add_argument(
        "--split",
        default=None,
        help="Filter by split (train, val, test, test_external). Default: all",
    )
    parser.add_argument(
        "--limit", type=int, default=20, help="Number of rows to show (default: 20)"
    )
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"

    builder = SparkSession.builder.appName("InspectGoldDataset")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    gold_path = f"{base_path}/gold/analytical_dataset"
    print(f"[INFO] Reading Gold dataset from: {gold_path}")
    df = spark.read.parquet(gold_path)

    if args.split:
        df = df.filter(col("split") == args.split)
        print(f"[INFO] Filtered to split={args.split}")

    total = df.count()
    print(f"\n{'=' * 70}")
    print(f"  GOLD DATASET OVERVIEW")
    print(f"{'=' * 70}")
    print(f"  Rows: {total:,}")
    print(f"  Columns: {len(df.columns)}")

    print(f"\n{'=' * 70}")
    print(f"  SCHEMA")
    print(f"{'=' * 70}")
    df.printSchema()

    print(f"\n{'=' * 70}")
    print(f"  SAMPLE DATA ({args.limit} rows)")
    print(f"{'=' * 70}")
    df.show(args.limit, truncate=False)

    print(f"\n{'=' * 70}")
    print(f"  STATISTICS (numeric columns)")
    print(f"{'=' * 70}")
    df.select(
        "age",
        "duration_days",
        "event_flag_mortality",
        "event_flag_readmission",
        "sbp_mean",
        "spo2_mean",
        "hr_mean",
        "temperature_mean",
        "creatinine",
        "sodium",
        "wbc",
        "hemoglobin",
    ).summary("count", "mean", "min", "25%", "50%", "75%", "max").show(truncate=False)

    print(f"\n{'=' * 70}")
    print(f"  SPLIT DISTRIBUTION")
    print(f"{'=' * 70}")
    df.groupBy("split").agg(
        count("*").alias("rows"),
        avg(col("event_flag_mortality")).alias("mortality_rate"),
        avg(col("event_flag_readmission")).alias("readmission_rate"),
        min(col("admityear")).alias("min_year"),
        max(col("admityear")).alias("max_year"),
    ).orderBy("split").show(truncate=False)

    print(f"\n{'=' * 70}")
    print(f"  NULL COUNTS (top 20 columns with most nulls)")
    print(f"{'=' * 70}")
    null_counts = []
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            null_counts.append((c, null_count, round(null_count / total * 100, 2)))
    null_counts.sort(key=lambda x: -x[1])
    if null_counts:
        print(f"  {'Column':<40} {'Null':>10} {'Rate':>8}")
        print(f"  {'-' * 40} {'-' * 10} {'-' * 8}")
        for name, cnt, rate in null_counts[:20]:
            print(f"  {name:<40} {cnt:>10,} {rate:>7.2f}%")
    else:
        print("  No nulls found!")

    print(f"\n{'=' * 70}")
    print(f"  ICD CHAPTER COVERAGE (train split)")
    print(f"{'=' * 70}")
    train_df = df.filter(col("split") == "train")
    icd_cols = [c for c in df.columns if c.startswith("icd10_chap_")]
    if icd_cols and train_df.count() > 0:
        for c in icd_cols:
            positive = train_df.filter(col(c) == 1).count()
            rate = round(positive / train_df.count() * 100, 2)
            name = c.replace("icd10_chap_", "")
            print(f"  {name:<40} {positive:>8,} ({rate:>5.2f}%)")

    spark.stop()
    print(f"\n[INFO] Done.")


if __name__ == "__main__":
    main()
