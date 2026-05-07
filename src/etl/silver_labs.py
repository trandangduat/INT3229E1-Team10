import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    max,
    min,
    to_timestamp,
)


LAB_ITEMIDS = {
    "hematocrit": [51221],
    "hemoglobin": [51222],
    "platelet": [51265],
    "wbc": [51301],
    "creatinine": [50912, 52546],
    "bun": [51006, 52647],
    "sodium": [50983, 52623],
    "potassium": [50971, 52610],
    "chloride": [50902, 52535],
    "bicarbonate": [50882],
    "anion_gap": [50868],
    "glucose": [50931, 52569],
    "calcium": [50893],
    "magnesium": [50960],
    "phosphate": [50970],
    "inr": [51237, 51675],
    "pt": [51274],
    "ptt": [51275, 52923],
    "alt": [50861],
    "ast": [50878],
    "bilirubin_total": [50885, 53089],
    "albumin": [50862, 53085],
    "lactate": [50813, 52442, 53154],
}


def main():
    parser = argparse.ArgumentParser(description="MIMIC-IV Silver Layer: 24h Labs")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    print(f"[INFO] Starting silver_labs job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    builder = SparkSession.builder.appName("SilverLayer_MIMIC_Labs")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    labevents_path = f"{base_path}/bronze/mimic_iv/labevents"
    admissions_path = f"{base_path}/silver/admissions"
    output_path = f"{base_path}/silver/labs_agg"

    print(f"[INFO] Reading labevents from: {labevents_path}")
    print(f"[INFO] Reading Silver admissions from: {admissions_path}")
    df_lab_raw = spark.read.parquet(labevents_path)
    df_adm = spark.read.parquet(admissions_path).select(
        col("hadm_id").cast("long").alias("hadm_id"),
        col("admittime"),
        col("admityear"),
    )

    input_count = df_lab_raw.count()
    print(f"[METRIC] Raw labevents count: {input_count}")

    itemid_to_name = []
    for name, itemids in LAB_ITEMIDS.items():
        for itemid in itemids:
            itemid_to_name.append((itemid, name))
    mapping_df = spark.createDataFrame(itemid_to_name, ["itemid", "lab_name"])
    all_itemids = [itemid for itemids in LAB_ITEMIDS.values() for itemid in itemids]

    df_lab = df_lab_raw.select(
        col("subject_id").cast("long").alias("subject_id"),
        col("hadm_id").cast("long").alias("hadm_id"),
        col("itemid").cast("int").alias("itemid"),
        to_timestamp("charttime").alias("charttime"),
        col("valuenum").cast("double").alias("valuenum"),
    ).filter(
        col("hadm_id").isNotNull()
        & col("itemid").isin(all_itemids)
        & col("valuenum").isNotNull()
    )

    filtered_count = df_lab.count()
    print(f"[METRIC] Rows after lab itemid/value filter: {filtered_count}")

    df_named = df_lab.join(mapping_df, on="itemid", how="inner")
    print("[METRIC] Lab rows by feature before 24h filter:")
    df_named.groupBy("lab_name").count().orderBy("lab_name").show(100, truncate=False)

    df_24h = df_named.join(df_adm, on="hadm_id", how="inner").filter(
        col("charttime").isNotNull()
        & col("admittime").isNotNull()
        & (col("charttime") >= col("admittime"))
        & (col("charttime") < expr("admittime + INTERVAL 24 HOURS"))
    )
    window_count = df_24h.count()
    print(f"[METRIC] Rows in first 24h admission window: {window_count}")

    df_silver = df_24h.groupBy("hadm_id", "admityear", "lab_name").agg(
        avg("valuenum").alias("lab_mean"),
        min("valuenum").alias("lab_min"),
        max("valuenum").alias("lab_max"),
        count("valuenum").alias("lab_count"),
    )

    final_count = df_silver.count()
    admission_count = df_silver.select("hadm_id").distinct().count()
    print(f"[METRIC] Admission-lab feature rows: {final_count}")
    print(f"[METRIC] Admissions with 24h lab features: {admission_count}")

    print("[METRIC] Lab feature coverage:")
    df_silver.groupBy("lab_name").agg(count("*").alias("admissions_with_lab")).orderBy(
        "lab_name"
    ).show(100, truncate=False)

    print(f"[INFO] Writing labs aggregate to: {output_path}")
    df_silver.repartition("admityear").write.mode("overwrite").partitionBy(
        "admityear"
    ).option("compression", "snappy").parquet(output_path)

    print("[INFO] silver_labs job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
