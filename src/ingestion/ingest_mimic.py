import sys

from pyspark.sql import SparkSession


def main():
    # Sử dụng tham số dòng lệnh để xác định môi trường (local hay hdfs)
    env = sys.argv[1] if len(sys.argv) > 1 else "local"

    # 1. Khởi tạo Spark Session
    builder = SparkSession.builder.appName("Bronze_Ingestion_MIMIC_IV")
    if env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()

    # Thiết lập đường dẫn dựa trên môi trường
    if env == "local":
        base_input_path = "data/raw"
        base_output_path = "data/bronze/mimic_iv"
    else:
        # Đường dẫn HDFS theo log bạn cung cấp
        base_input_path = "hdfs://master10:9000/user/dis/data/raw_data/mimic"
        base_output_path = "hdfs://master10:9000/user/dis/data/bronze/mimic_iv"

    print(f"Running in {env} mode.")
    print(f"Input base path: {base_input_path}")
    print(f"Output base path: {base_output_path}")

    # Danh sách các bảng cần ingest và thư mục của nó
    tables = [
        {"name": "admissions", "path": "hosp/admissions.csv"},
        {"name": "patients", "path": "hosp/patients.csv"},
        {"name": "diagnoses_icd", "path": "hosp/diagnoses_icd.csv"},
        {"name": "labevents", "path": "hosp/labevents.csv"},
        {"name": "d_items", "path": "icu/d_items.csv"},
    ]

    for table in tables:
        table_name = table["name"]
        file_path = f"{base_input_path}/{table['path']}"
        output_dir = f"{base_output_path}/{table_name}"

        print(f"--- Processing table: {table_name} ---")

        try:
            # Bronze Rule: Immutable, No inferSchema, Read everything as string (default inferSchema=False handles this, but we explicitly specify it).
            # Không sử dụng schema cứng vì ở Bronze chúng ta muốn Raw data.
            # inferSchema=False sẽ load tất cả các field như StringType.
            df = spark.read.csv(file_path, header=True, inferSchema=False)

            # Lưu trữ với Parquet và nén Snappy
            df.write.parquet(output_dir, mode="overwrite", compression="snappy")
            print(f"[SUCCESS] Saved {table_name} to {output_dir}")

        except Exception as e:
            print(f"[ERROR] Failed to process {table_name}. Reason: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
