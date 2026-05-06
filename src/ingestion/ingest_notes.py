import sys
from pyspark.sql import SparkSession


def main():
    env = sys.argv[1] if len(sys.argv) > 1 else "local"

    # Khởi tạo Spark Session
    builder = SparkSession.builder.appName("Bronze_Ingestion_Notes")
    if env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()

    if env == "local":
        file_path = "data/raw/discharge.csv"
        output_dir = "data/bronze/mimic_iv_note/discharge"
    else:
        # Giả sử file discharge.csv nằm trong note/
        file_path = "hdfs://master10:9000/data/raw_data/mimic/note/discharge.csv"
        output_dir = "hdfs://master10:9000/data/bronze/mimic_iv_note/discharge"

    print(f"Running in {env} mode.")
    print(f"Input path: {file_path}")
    print(f"Output path: {output_dir}")

    try:
        # Cấu hình multiline và escape string để không vỡ dòng text
        df = spark.read.csv(
            file_path, header=True, inferSchema=False, multiLine=True, escape='"'
        )

        df.write.parquet(output_dir, mode="overwrite", compression="snappy")
        print(f"[SUCCESS] Saved discharge notes to {output_dir}")

    except Exception as e:
        print(f"[ERROR] Failed to process notes. Reason: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
