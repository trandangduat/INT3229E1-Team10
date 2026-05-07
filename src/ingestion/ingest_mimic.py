import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser(description="Bronze ingestion for MIMIC-IV tables")
    parser.add_argument(
        "env",
        nargs="?",
        default="local",
        choices=["local", "hdfs"],
        help="Execution environment (local or hdfs)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Optional table names to ingest. If omitted, all configured MIMIC-IV tables are ingested.",
    )
    args = parser.parse_args()
    env = args.env

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
        {"name": "chartevents", "path": "icu/chartevents.csv"},
        {"name": "d_labitems", "path": "hosp/d_labitems.csv"},
    ]

    table_names = {table["name"] for table in tables}
    if args.tables:
        unknown_tables = sorted(set(args.tables) - table_names)
        if unknown_tables:
            raise ValueError(
                f"Unknown table(s): {unknown_tables}. Available tables: {sorted(table_names)}"
            )
        tables = [table for table in tables if table["name"] in set(args.tables)]

    print(f"Tables to ingest: {[table['name'] for table in tables]}")

    failed_tables = []
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
            failed_tables.append(table_name)

    if failed_tables:
        spark.stop()
        raise RuntimeError(f"Failed to ingest table(s): {failed_tables}")

    spark.stop()


if __name__ == "__main__":
    main()
