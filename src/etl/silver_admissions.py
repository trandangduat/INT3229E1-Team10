import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    datediff,
    lag,
    max,
    min,
    sum,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.window import Window


def main():
    parser = argparse.ArgumentParser(description="MIMIC-IV Silver Layer: Admissions")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    if args.env == "local":
        base_path = "data"
    else:
        base_path = "hdfs://master10:9000/user/dis/data"

    print(f"[INFO] Starting silver_admissions job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    builder = SparkSession.builder.appName("SilverLayer_Admissions")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    admissions_path = f"{base_path}/bronze/mimic_iv/admissions"
    patients_path = f"{base_path}/bronze/mimic_iv/patients"
    output_path = f"{base_path}/silver/admissions"

    print("[INFO] Reading Bronze data...")
    df_adm_raw = spark.read.parquet(admissions_path)
    df_pat_raw = spark.read.parquet(patients_path)

    adm_input_count = df_adm_raw.count()
    pat_input_count = df_pat_raw.count()
    print(f"[METRIC] Raw admissions count: {adm_input_count}")
    print(f"[METRIC] Raw patients count: {pat_input_count}")

    df_adm = df_adm_raw.select(
        col("subject_id").cast("long").alias("subject_id"),
        col("hadm_id").cast("long").alias("hadm_id"),
        to_timestamp("admittime").alias("admittime"),
        to_timestamp("dischtime").alias("dischtime"),
        to_timestamp("deathtime").alias("deathtime"),
        col("hospital_expire_flag").cast("int").alias("event_flag_mortality"),
    )

    df_pat = df_pat_raw.select(
        col("subject_id").cast("long").alias("subject_id"),
        col("gender"),
        col("anchor_age").cast("int"),
        col("anchor_year").cast("int"),
    )

    invalid_adm_count = df_adm.filter(
        col("subject_id").isNull()
        | col("hadm_id").isNull()
        | col("admittime").isNull()
        | col("dischtime").isNull()
    ).count()
    invalid_pat_count = df_pat.filter(
        col("subject_id").isNull()
        | col("anchor_age").isNull()
        | col("anchor_year").isNull()
    ).count()
    print(f"[METRIC] Admissions rows with invalid required casts: {invalid_adm_count}")
    print(f"[METRIC] Patients rows with invalid required casts: {invalid_pat_count}")

    print("[INFO] Joining admissions and patients...")
    df_joined = df_adm.join(df_pat, on="subject_id", how="inner")

    joined_count = df_joined.count()
    print(f"[METRIC] Count after join: {joined_count}")

    df_transformed = (
        df_joined.withColumn("admityear", year(col("admittime")))
        .withColumn("duration_days", datediff(col("dischtime"), col("admittime")))
        .withColumn("age", col("anchor_age") + (col("admityear") - col("anchor_year")))
    )

    print("[INFO] Applying clinical filters (age >= 18, duration_days >= 1)...")
    df_filtered = df_transformed.filter(
        col("subject_id").isNotNull()
        & col("hadm_id").isNotNull()
        & col("admittime").isNotNull()
        & col("dischtime").isNotNull()
        & col("admityear").isNotNull()
        & (col("age") >= 18)
        & (col("duration_days") >= 1)
    )

    print("[INFO] Computing 30-day readmission flag...")
    patient_window = Window.partitionBy("subject_id").orderBy("admittime")
    df_with_next = (
        df_filtered.withColumn(
            "next_admittime", lag("admittime", -1).over(patient_window)
        )
        .withColumn(
            "days_to_readmission",
            datediff(col("next_admittime"), col("dischtime")),
        )
        .withColumn(
            "event_flag_readmission",
            when(
                col("days_to_readmission").isNotNull()
                & (col("days_to_readmission") >= 0)
                & (col("days_to_readmission") <= 30),
                1,
            ).otherwise(0),
        )
        .drop("next_admittime", "days_to_readmission")
    )

    df_silver = df_with_next

    final_count = df_silver.count()
    print(f"[METRIC] Final Silver admissions count: {final_count}")

    print("[INFO] --- VALIDATION METRICS ---")
    avg_mort = df_silver.agg(avg(col("event_flag_mortality"))).collect()[0][0]
    mortality_rate = (avg_mort or 0.0) * 100
    print(f"[METRIC] Mortality Rate: {mortality_rate:.2f}%")

    avg_readm = df_silver.agg(avg(col("event_flag_readmission"))).collect()[0][0]
    readmission_rate = (avg_readm or 0.0) * 100
    print(f"[METRIC] 30-day Readmission Rate: {readmission_rate:.2f}%")

    duration_stats = df_silver.agg(
        min(col("duration_days")).alias("min_days"),
        max(col("duration_days")).alias("max_days"),
        avg(col("duration_days")).alias("avg_days"),
    ).collect()[0]
    avg_days = duration_stats["avg_days"] or 0.0
    print(
        f"[METRIC] Duration Days -> Min: {duration_stats['min_days']}, Max: {duration_stats['max_days']}, Avg: {avg_days:.2f}"
    )

    output_quality = df_silver.agg(
        count("*").alias("rows"),
        sum(col("event_flag_mortality").isNull().cast("int")).alias(
            "missing_mortality"
        ),
    ).collect()[0]
    print(
        f"[METRIC] Missing mortality flags: {output_quality['missing_mortality']} / {output_quality['rows']}"
    )

    print("[METRIC] Distribution by admityear (Top 5):")
    df_silver.groupBy("admityear").count().orderBy("admityear", ascending=False).show(5)

    print(f"[INFO] Writing to Silver Layer at: {output_path}")
    df_silver.write.mode("overwrite").partitionBy("admityear").option(
        "compression", "snappy"
    ).parquet(output_path)

    print("[INFO] silver_admissions job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
