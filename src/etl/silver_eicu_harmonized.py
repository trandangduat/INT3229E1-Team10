import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max, min, sum, when


def aggregate_vital(name, source_col, low, high):
    valid_value = when(
        (col(source_col) >= low) & (col(source_col) <= high), col(source_col)
    )
    return [
        avg(valid_value).alias(f"{name}_mean"),
        min(valid_value).alias(f"{name}_min"),
        max(valid_value).alias(f"{name}_max"),
        sum(valid_value.isNotNull().cast("int")).alias(f"{name}_count"),
    ]


def main():
    parser = argparse.ArgumentParser(description="eICU Silver Layer: Harmonized Vitals")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    if args.env == "local":
        base_path = "data"
    else:
        base_path = "hdfs://master10:9000/user/dis/data"

    print(f"[INFO] Starting silver_eicu_harmonized job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    builder = SparkSession.builder.appName("SilverLayer_eICU_Harmonized")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    patient_path = f"{base_path}/bronze/eicu/patient"
    vital_path = f"{base_path}/bronze/eicu/vitalPeriodic"
    output_path = f"{base_path}/silver/eicu_harmonized"

    print(f"[INFO] Reading eICU patient from: {patient_path}")
    print(f"[INFO] Reading eICU vitalPeriodic from: {vital_path}")
    df_patient_raw = spark.read.parquet(patient_path)
    df_vital_raw = spark.read.parquet(vital_path)

    patient_count = df_patient_raw.count()
    vital_count = df_vital_raw.count()
    print(f"[METRIC] Raw eICU patient count: {patient_count}")
    print(f"[METRIC] Raw eICU vitalPeriodic count: {vital_count}")

    df_patient = df_patient_raw.select(
        col("patientunitstayid").cast("long").alias("stay_id_eicu"),
        col("patienthealthsystemstayid").cast("long").alias("healthsystem_stay_id"),
        col("uniquepid"),
        col("hospitalid").cast("int").alias("hospitalid"),
        col("gender"),
        col("age").cast("int").alias("age"),
        col("unitdischargeoffset").cast("int").alias("unitdischargeoffset"),
        when(col("unitdischargestatus") == "Expired", 1)
        .otherwise(0)
        .alias("event_flag_mortality"),
    )

    df_vital = df_vital_raw.select(
        col("patientunitstayid").cast("long").alias("stay_id_eicu"),
        col("observationoffset").cast("int").alias("observationoffset"),
        col("systemicsystolic").cast("double").alias("sbp"),
        col("sao2").cast("double").alias("spo2"),
        col("heartrate").cast("double").alias("hr"),
        col("temperature").cast("double").alias("temperature"),
    )

    invalid_patient_count = df_patient.filter(
        col("stay_id_eicu").isNull() | col("hospitalid").isNull()
    ).count()
    invalid_vital_count = df_vital.filter(
        col("stay_id_eicu").isNull() | col("observationoffset").isNull()
    ).count()
    print(f"[METRIC] Invalid patient keys: {invalid_patient_count}")
    print(f"[METRIC] Invalid vital keys: {invalid_vital_count}")

    df_vital_24h = df_vital.filter(
        col("stay_id_eicu").isNotNull()
        & col("observationoffset").isNotNull()
        & (col("observationoffset") >= 0)
        & (col("observationoffset") < 1440)
    )
    vital_24h_count = df_vital_24h.count()
    print(f"[METRIC] eICU vital rows in first 24h: {vital_24h_count}")

    agg_exprs = []
    agg_exprs.extend(aggregate_vital("sbp", "sbp", 40, 300))
    agg_exprs.extend(aggregate_vital("spo2", "spo2", 50, 100))
    agg_exprs.extend(aggregate_vital("hr", "hr", 20, 250))
    agg_exprs.extend(aggregate_vital("temperature", "temperature", 25, 45))

    df_vital_agg = df_vital_24h.groupBy("stay_id_eicu").agg(*agg_exprs)
    stays_with_vitals = df_vital_agg.count()
    print(f"[METRIC] eICU stays with 24h vitals: {stays_with_vitals}")

    df_silver = df_patient.filter(
        col("stay_id_eicu").isNotNull() & col("hospitalid").isNotNull()
    ).join(df_vital_agg, on="stay_id_eicu", how="left")

    final_count = df_silver.count()
    print(f"[METRIC] Final eICU harmonized count: {final_count}")

    missing = df_silver.agg(
        count("*").alias("rows"),
        sum(col("sbp_mean").isNull().cast("int")).alias("missing_sbp"),
        sum(col("spo2_mean").isNull().cast("int")).alias("missing_spo2"),
        sum(col("hr_mean").isNull().cast("int")).alias("missing_hr"),
        sum(col("temperature_mean").isNull().cast("int")).alias("missing_temperature"),
    ).collect()[0]
    print(
        "[METRIC] Missing vitals: "
        f"SBP {missing['missing_sbp']}/{missing['rows']}, "
        f"SpO2 {missing['missing_spo2']}/{missing['rows']}, "
        f"HR {missing['missing_hr']}/{missing['rows']}, "
        f"Temperature {missing['missing_temperature']}/{missing['rows']}"
    )

    print("[METRIC] Distribution by hospitalid:")
    df_silver.groupBy("hospitalid").count().orderBy("hospitalid").show(20)

    print(f"[INFO] Writing eICU harmonized data to: {output_path}")
    df_silver.write.mode("overwrite").partitionBy("hospitalid").option(
        "compression", "snappy"
    ).parquet(output_path)

    print("[INFO] silver_eicu_harmonized job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
