import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    max,
    min,
    sum,
    to_timestamp,
    when,
)


VITAL_ITEMIDS = {
    "sbp": [220050, 220179],
    "spo2": [220277],
    "hr": [220045],
    "temperature": [223761, 223762],
}

VITAL_RANGES = {
    "sbp": (40, 300),
    "spo2": (50, 100),
    "hr": (20, 250),
    "temperature": (25, 45),
}


def aggregate_vital(name):
    low, high = VITAL_RANGES[name]
    value = when(
        (col("vital_name") == name)
        & (col("valuenum") >= low)
        & (col("valuenum") <= high),
        col("valuenum"),
    )
    return [
        avg(value).alias(f"{name}_mean"),
        min(value).alias(f"{name}_min"),
        max(value).alias(f"{name}_max"),
        sum(value.isNotNull().cast("int")).alias(f"{name}_count"),
    ]


def main():
    parser = argparse.ArgumentParser(description="MIMIC-IV Silver Layer: 24h Vitals")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    if args.env == "local":
        base_path = "data"
    else:
        base_path = "hdfs://master10:9000/user/dis/data"

    print(f"[INFO] Starting silver_vitals_mimic job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    builder = SparkSession.builder.appName("SilverLayer_MIMIC_Vitals")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    chartevents_path = f"{base_path}/bronze/mimic_iv/chartevents"
    admissions_path = f"{base_path}/silver/admissions"
    output_path = f"{base_path}/silver/chartevents_agg"

    print(f"[INFO] Reading chartevents from: {chartevents_path}")
    print(f"[INFO] Reading Silver admissions from: {admissions_path}")
    df_chart_raw = spark.read.parquet(chartevents_path)
    df_adm = spark.read.parquet(admissions_path).select(
        col("hadm_id").cast("long").alias("hadm_id"),
        col("admittime"),
        col("dischtime"),
        col("admityear"),
    )

    input_count = df_chart_raw.count()
    print(f"[METRIC] Raw chartevents count: {input_count}")

    itemid_to_name = []
    for name, itemids in VITAL_ITEMIDS.items():
        for itemid in itemids:
            itemid_to_name.append((itemid, name))
    mapping_df = spark.createDataFrame(itemid_to_name, ["itemid", "vital_name"])

    all_itemids = [itemid for itemids in VITAL_ITEMIDS.values() for itemid in itemids]
    df_chart = df_chart_raw.select(
        col("subject_id").cast("long").alias("subject_id"),
        col("hadm_id").cast("long").alias("hadm_id"),
        col("stay_id").cast("long").alias("stay_id"),
        col("itemid").cast("int").alias("itemid"),
        to_timestamp("charttime").alias("charttime"),
        col("valuenum").cast("double").alias("valuenum"),
    ).filter(col("itemid").isin(all_itemids))

    df_chart = df_chart.withColumn(
        "valuenum",
        when(col("itemid") == 223761, (col("valuenum") - 32) * 5 / 9).otherwise(
            col("valuenum")
        ),
    )

    vital_item_count = df_chart.count()
    print(f"[METRIC] Rows after vital itemid filter: {vital_item_count}")

    df_named = df_chart.join(mapping_df, on="itemid", how="inner")
    print("[METRIC] Vital rows by type before 24h filter:")
    df_named.groupBy("vital_name").count().orderBy("vital_name").show()

    df_joined = df_named.join(df_adm, on="hadm_id", how="inner")
    df_24h = df_joined.filter(
        col("charttime").isNotNull()
        & col("admittime").isNotNull()
        & (col("charttime") >= col("admittime"))
        & (col("charttime") < expr("admittime + INTERVAL 24 HOURS"))
    )

    window_count = df_24h.count()
    print(f"[METRIC] Rows in first 24h admission window: {window_count}")

    agg_exprs = []
    for name in VITAL_ITEMIDS:
        agg_exprs.extend(aggregate_vital(name))

    df_silver = df_24h.groupBy("hadm_id", "admityear").agg(*agg_exprs)
    final_count = df_silver.count()
    print(f"[METRIC] Admissions with MIMIC 24h vitals: {final_count}")

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

    print(f"[INFO] Writing MIMIC vitals aggregate to: {output_path}")
    df_silver.write.mode("overwrite").partitionBy("admityear").option(
        "compression", "snappy"
    ).parquet(output_path)

    print("[INFO] silver_vitals_mimic job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
