import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, trim, upper, when


def main():
    parser = argparse.ArgumentParser(description="MIMIC-IV Silver Layer: Diagnoses")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    if args.env == "local":
        base_path = "data"
    else:
        base_path = "hdfs://master10:9000/user/dis/data"

    print(f"[INFO] Starting silver_diagnoses job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    builder = SparkSession.builder.appName("SilverLayer_Diagnoses")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronze_diagnoses_path = f"{base_path}/bronze/mimic_iv/diagnoses_icd"
    admissions_path = f"{base_path}/silver/admissions"
    output_path = f"{base_path}/silver/diagnoses"

    print(f"[INFO] Reading Bronze diagnoses from: {bronze_diagnoses_path}")
    print(f"[INFO] Reading Silver admissions cohort from: {admissions_path}")
    df_raw = spark.read.parquet(bronze_diagnoses_path)
    df_adm = (
        spark.read.parquet(admissions_path)
        .select(col("hadm_id").cast("long").alias("hadm_id"))
        .distinct()
    )

    input_count = df_raw.count()
    cohort_count = df_adm.count()
    print(f"[METRIC] Raw diagnoses count: {input_count}")
    print(f"[METRIC] Silver admissions cohort distinct hadm_id: {cohort_count}")

    df_cast = df_raw.select(
        col("subject_id").cast("long").alias("subject_id"),
        col("hadm_id").cast("long").alias("hadm_id"),
        col("seq_num").cast("int").alias("seq_num"),
        upper(trim(col("icd_code"))).alias("icd_code"),
        col("icd_version").cast("int").alias("icd_version"),
    )

    invalid_required_count = df_cast.filter(
        col("subject_id").isNull()
        | col("hadm_id").isNull()
        | col("seq_num").isNull()
        | col("icd_code").isNull()
        | (col("icd_code") == "")
        | col("icd_version").isNull()
    ).count()
    print(f"[METRIC] Rows with invalid required fields: {invalid_required_count}")

    df_clean = df_cast.filter(
        col("subject_id").isNotNull()
        & col("hadm_id").isNotNull()
        & col("seq_num").isNotNull()
        & col("icd_code").isNotNull()
        & (col("icd_code") != "")
        & col("icd_version").isNotNull()
    )

    clean_count = df_clean.count()
    print(f"[METRIC] Rows after null/empty filter: {clean_count}")

    df_cohort = df_clean.join(df_adm, on="hadm_id", how="inner")
    cohort_match_count = df_cohort.count()
    orphan_count = clean_count - cohort_match_count
    print(f"[METRIC] Rows after cohort filter: {cohort_match_count}")
    print(f"[METRIC] Orphan rows removed (hadm_id not in admissions): {orphan_count}")

    df_silver = df_cohort.withColumn(
        "is_primary_diagnosis", when(col("seq_num") == 1, 1).otherwise(0)
    ).withColumn("primary_icd_code", when(col("seq_num") == 1, col("icd_code")))

    final_count = df_silver.count()
    print(f"[METRIC] Final Silver diagnoses count: {final_count}")

    metrics = df_silver.agg(
        countDistinct("hadm_id").alias("distinct_hadm_id"),
        countDistinct("icd_code").alias("distinct_icd_code"),
        sum(col("is_primary_diagnosis")).alias("primary_diagnosis_rows"),
    ).collect()[0]
    print(f"[METRIC] Distinct admissions with diagnoses: {metrics['distinct_hadm_id']}")
    print(f"[METRIC] Distinct ICD codes: {metrics['distinct_icd_code']}")
    print(f"[METRIC] Primary diagnosis rows: {metrics['primary_diagnosis_rows']}")

    print("[METRIC] ICD version distribution:")
    df_silver.groupBy("icd_version").count().orderBy("icd_version").show()

    print(f"[INFO] Writing Silver diagnoses to: {output_path}")
    df_silver.write.mode("overwrite").option("compression", "snappy").parquet(
        output_path
    )

    print("[INFO] silver_diagnoses job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
