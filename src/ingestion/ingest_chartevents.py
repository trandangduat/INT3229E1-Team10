from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# 1. Khởi tạo Spark Session ở chế độ Local
spark = (
    SparkSession.builder.appName("Bronze_Ingestion_Chartevents")
    .master("local[*]")
    .getOrCreate()
)

# 2. KHAI BÁO SCHEMA CỨNG (Theo yêu cầu Design Spec để tránh OOM do inferSchema)
chartevents_schema = StructType(
    [
        StructField("subject_id", IntegerType(), True),
        StructField("hadm_id", IntegerType(), True),
        StructField("stay_id", IntegerType(), True),
        StructField("caregiver_id", IntegerType(), True),
        StructField("charttime", TimestampType(), True),
        StructField("storetime", TimestampType(), True),
        StructField("itemid", IntegerType(), True),
        StructField("value", StringType(), True),
        StructField("valuenum", FloatType(), True),
        StructField("valueuom", StringType(), True),
        StructField("warning", IntegerType(), True),
    ]
)

# 3. ĐỌC DỮ LIỆU CSV (Bronze Layer Ingestion)
input_path = "/home/jovyan/data/raw/chartevents_sample.csv"

# Không dùng inferSchema=True, truyền schema cứng vào
df_raw = spark.read.csv(input_path, header=True, schema=chartevents_schema)

print("--- SCHEMA CỦA DỮ LIỆU ĐÃ ĐỌC ---")
df_raw.printSchema()

print("--- 5 DÒNG DỮ LIỆU ĐẦU TIÊN ---")
df_raw.show(5)

# 4. GHI DỮ LIỆU SANG PARQUET + SNAPPY (Quy định của dự án)
output_path = "/home/jovyan/data/bronze/mimic_iv/chartevents"

# Ghi ra định dạng cột phân tán Parquet với thuật toán nén Snappy
df_raw.write.parquet(output_path, mode="overwrite", compression="snappy")

print(f"Hoàn thành Ingestion! Dữ liệu Parquet đã được lưu tại: {output_path}")

# Dừng Spark
spark.stop()
