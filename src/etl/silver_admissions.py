import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, year, when, avg, min, max, count

def main():
    parser = argparse.ArgumentParser(description="MIMIC-IV Silver Layer: Admissions")
    parser.add_argument("env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)")
    args = parser.parse_args()

    # Thiết lập base path
    if args.env == "local":
        base_path = "data"
    else:
        # Use the production HDFS base path consistent with ingestion scripts and docs
        base_path = "hdfs://master10:9000/user/dis/data"

    print(f"[INFO] Starting silver_admissions job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    # Khởi tạo Spark Session
    builder = SparkSession.builder.appName("SilverLayer_Admissions")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Định nghĩa đường dẫn
    admissions_path = f"{base_path}/bronze/mimic_iv/admissions"
    patients_path = f"{base_path}/bronze/mimic_iv/patients"
    output_path = f"{base_path}/silver/admissions"

    # 1. Đọc dữ liệu từ Bronze
    print("[INFO] Reading Bronze data...")
    df_adm_raw = spark.read.parquet(admissions_path)
    df_pat_raw = spark.read.parquet(patients_path)

    adm_input_count = df_adm_raw.count()
    print(f"[METRIC] Raw admissions count: {adm_input_count}")

    # 2. Cast kiểu dữ liệu và chuẩn bị bảng admissions
    df_adm = df_adm_raw.select(
        col("subject_id").cast("long"),
        col("hadm_id").cast("long"),
        to_timestamp("admittime").alias("admittime"),
        to_timestamp("dischtime").alias("dischtime"),
        to_timestamp("deathtime").alias("deathtime"),
        col("hospital_expire_flag").cast("int").alias("event_flag_mortality")
    )

    # 3. Cast kiểu dữ liệu và chuẩn bị bảng patients
    df_pat = df_pat_raw.select(
        col("subject_id").cast("long"),
        col("gender"),
        col("anchor_age").cast("int"),
        col("anchor_year").cast("int")
    )

    # 4. Join admissions và patients
    print("[INFO] Joining admissions and patients...")
    df_joined = df_adm.join(df_pat, on="subject_id", how="inner")
    
    joined_count = df_joined.count()
    print(f"[METRIC] Count after join: {joined_count}")

    # 5. Transformation Logic (Tính tuổi, thời gian, nhãn)
    df_transformed = df_joined \
        .withColumn("admityear", year(col("admittime"))) \
        .withColumn("duration_days", datediff(col("dischtime"), col("admittime"))) \
        .withColumn("age", col("anchor_age") + (col("admityear") - col("anchor_year")))

    # 6. Filter theo rule lâm sàng
    print("[INFO] Applying clinical filters (age >= 18, duration_days >= 1)...")
    df_silver = df_transformed.filter(
        (col("age") >= 18) & 
        (col("duration_days") >= 1)
    )

    final_count = df_silver.count()
    print(f"[METRIC] Final Silver admissions count: {final_count}")

    # 7. Validation Metrics
    print("[INFO] --- VALIDATION METRICS ---")
    
    # Tỉ lệ tử vong
    avg_mort = df_silver.agg(avg(col("event_flag_mortality"))).collect()[0][0]
    mortality_rate = (avg_mort or 0.0) * 100
    print(f"[METRIC] Mortality Rate: {mortality_rate:.2f}%")

    # Thống kê duration_days
    duration_stats = df_silver.agg(
        min(col("duration_days")).alias("min_days"),
        max(col("duration_days")).alias("max_days"),
        avg(col("duration_days")).alias("avg_days")
    ).collect()[0]
    print(f"[METRIC] Duration Days -> Min: {duration_stats['min_days']}, Max: {duration_stats['max_days']}, Avg: {duration_stats['avg_days']:.2f}")

    # Phân phối theo năm (admityear)
    print("[METRIC] Distribution by admityear (Top 5):")
    df_silver.groupBy("admityear").count().orderBy("admityear", ascending=False).show(5)

    # 8. Write to Silver Layer
    print(f"[INFO] Writing to Silver Layer at: {output_path}")
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("admityear") \
        .option("compression", "snappy") \
        .parquet(output_path)

    print("[INFO] silver_admissions job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()