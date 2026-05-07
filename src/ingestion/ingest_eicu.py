import sys

from pyspark.sql import SparkSession


def main():
    env = sys.argv[1] if len(sys.argv) > 1 else "local"

    builder = SparkSession.builder.appName("Bronze_Ingestion_eICU")
    if env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
# Khởi tạo Spark Session
    
    if env == "local":
        base_input_path = "data/raw"
        base_output_path = "data/bronze/eicu"
    else:
        base_input_path = "hdfs://master10:9000/user/dis/data/raw_data/eICU"
        base_output_path = "hdfs://master10:9000/user/dis/data/bronze/eicu"

    print(f"Running in {env} mode.")
    print(f"Input base path: {base_input_path}")
    print(f"Output base path: {base_output_path}")

    # Danh sách các bảng eICU cần ingest
    tables = ["patient", "vitalPeriodic", "diagnosis", "medication"]

    for table_name in tables:
        file_path = f"{base_input_path}/{table_name}.csv"
        output_dir = f"{base_output_path}/{table_name}"

        print(f"--- Processing table: {table_name} ---")

        try:
            # Bronze Rule: Immutable. No rename or drop column yet.
            df = spark.read.csv(file_path, header=True, inferSchema=False)

            df.write.parquet(output_dir, mode="overwrite", compression="snappy")
            print(f"[SUCCESS] Saved {table_name} to {output_dir}")

        except Exception as e:
            print(f"[ERROR] Failed to process {table_name}. Reason: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
