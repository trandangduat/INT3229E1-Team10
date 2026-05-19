import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    ceil,
    col,
    count,
    datediff,
    greatest,
    lead,
    lit,
    lower,
    max,
    min,
    sum,
    to_date,
    to_timestamp,
    unix_timestamp,
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
        col("admission_type"),
        col("insurance"),
        col("marital_status"),
        col("race"),
        col("discharge_location"),
    )

    df_pat = df_pat_raw.select(
        col("subject_id").cast("long").alias("subject_id"),
        col("gender"),
        col("anchor_age").cast("int"),
        col("anchor_year").cast("int"),
        to_date("dod").alias("dod"),
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
        .withColumn("index_time", col("dischtime"))
    )

    patient_window = Window.partitionBy("subject_id").orderBy("admittime", "hadm_id")
    df_readmission_lookup = (
        df_transformed.filter(
            col("subject_id").isNotNull()
            & col("hadm_id").isNotNull()
            & col("admittime").isNotNull()
        )
        .withColumn("next_admittime", lead("admittime", 1).over(patient_window))
        .select("hadm_id", "next_admittime")
    )

    print("[INFO] Applying post-discharge cohort filters...")
    df_filtered = df_transformed.filter(
        col("subject_id").isNotNull()
        & col("hadm_id").isNotNull()
        & col("admittime").isNotNull()
        & col("dischtime").isNotNull()
        & col("admityear").isNotNull()
        & (col("age") >= 18)
        & (col("duration_days") >= 1)
        & col("deathtime").isNull()
        & (
            col("discharge_location").isNull()
            | (
                ~lower(col("discharge_location")).contains("expire")
                & ~lower(col("discharge_location")).contains("deceased")
                & ~lower(col("discharge_location")).contains("died")
            )
        )
    )

    df_filtered = df_filtered.join(df_readmission_lookup, on="hadm_id", how="left")

    print("[INFO] Computing 30-day readmission flag...")
    df_with_next = (
        df_filtered.withColumn(
            "hours_to_readmission",
            (unix_timestamp(col("next_admittime")) - unix_timestamp(col("dischtime")))
            / 3600.0,
        )
        .withColumn(
            "event_flag_readmission",
            when(
                col("hours_to_readmission").isNotNull()
                & (col("hours_to_readmission") > 0)
                & (col("hours_to_readmission") <= 30 * 24),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "readmission_time_days",
            when(
                col("event_flag_readmission") == 1,
                greatest(ceil(col("hours_to_readmission") / 24.0), lit(1)),
            ).otherwise(30),
        )
        .withColumn(
            "days_to_death_after_discharge",
            datediff(col("dod"), col("dischtime")),
        )
        .withColumn(
            "event_flag_mortality",
            when(
                col("days_to_death_after_discharge").isNotNull()
                & (col("days_to_death_after_discharge") >= 0)
                & (col("days_to_death_after_discharge") <= 365),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "mortality_time_days",
            when(
                col("event_flag_mortality") == 1,
                greatest(col("days_to_death_after_discharge"), lit(1)),
            ).otherwise(365),
        )
        .withColumn(
            "mortality_time_months",
            col("mortality_time_days") / 30.4375,
        )
        .withColumn("readmission_event_30d", col("event_flag_readmission"))
        .withColumn("mortality_event_12m", col("event_flag_mortality"))
        .drop(
            "next_admittime",
            "hours_to_readmission",
            "days_to_death_after_discharge",
        )
    )

    df_silver = df_with_next

    final_count = df_silver.count()
    print(f"[METRIC] Final Silver admissions count: {final_count}")

    print("[INFO] --- VALIDATION METRICS ---")
    avg_mort = df_silver.agg(avg(col("event_flag_mortality"))).collect()[0][0]
    mortality_rate = (avg_mort or 0.0) * 100
    print(f"[METRIC] 12-month Mortality Rate: {mortality_rate:.2f}%")

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
        sum(col("event_flag_readmission").isNull().cast("int")).alias(
            "missing_readmission"
        ),
        sum(col("event_flag_mortality").isNull().cast("int")).alias(
            "missing_mortality"
        ),
        sum(col("readmission_time_days").isNull().cast("int")).alias(
            "missing_readmission_time"
        ),
        sum(col("mortality_time_days").isNull().cast("int")).alias(
            "missing_mortality_time"
        ),
    ).collect()[0]
    print(
        f"[METRIC] Missing labels: readmission {output_quality['missing_readmission']} / {output_quality['rows']}, mortality {output_quality['missing_mortality']} / {output_quality['rows']}"
    )
    print(
        f"[METRIC] Missing label times: readmission {output_quality['missing_readmission_time']} / {output_quality['rows']}, mortality {output_quality['missing_mortality_time']} / {output_quality['rows']}"
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
