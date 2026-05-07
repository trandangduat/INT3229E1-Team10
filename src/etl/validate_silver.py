import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum


SILVER_TABLES = {
    "admissions": {
        "path": "silver/admissions",
        "required_columns": [
            "subject_id",
            "hadm_id",
            "admittime",
            "dischtime",
            "admityear",
            "duration_days",
            "age",
            "event_flag_mortality",
        ],
        "required_non_null": [
            "subject_id",
            "hadm_id",
            "admittime",
            "dischtime",
            "admityear",
        ],
        "distinct_key": "hadm_id",
    },
    "chartevents_agg": {
        "path": "silver/chartevents_agg",
        "required_columns": [
            "hadm_id",
            "admityear",
            "sbp_mean",
            "spo2_mean",
            "hr_mean",
            "temperature_mean",
        ],
        "required_non_null": ["hadm_id", "admityear"],
        "distinct_key": "hadm_id",
    },
    "diagnoses": {
        "path": "silver/diagnoses",
        "required_columns": [
            "subject_id",
            "hadm_id",
            "seq_num",
            "icd_code",
            "icd_version",
            "is_primary_diagnosis",
        ],
        "required_non_null": [
            "subject_id",
            "hadm_id",
            "seq_num",
            "icd_code",
            "icd_version",
        ],
        "distinct_key": "hadm_id",
    },
    "labs_agg": {
        "path": "silver/labs_agg",
        "required_columns": [
            "hadm_id",
            "admityear",
            "lab_name",
            "lab_mean",
            "lab_min",
            "lab_max",
            "lab_count",
        ],
        "required_non_null": ["hadm_id", "admityear", "lab_name", "lab_mean"],
        "distinct_key": "hadm_id",
    },
    "eicu_harmonized": {
        "path": "silver/eicu_harmonized",
        "required_columns": [
            "stay_id_eicu",
            "hospitalid",
            "event_flag_mortality",
            "sbp_mean",
            "spo2_mean",
            "hr_mean",
            "temperature_mean",
        ],
        "required_non_null": ["stay_id_eicu", "hospitalid"],
        "distinct_key": "stay_id_eicu",
    },
}


def hdfs_success_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    success_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"{path}/_SUCCESS")
    fs = success_path.getFileSystem(hadoop_conf)
    return fs.exists(success_path)


def hdfs_path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    return fs.exists(hadoop_path)


def validate_table(spark, base_path, table_name, config):
    path = f"{base_path}/{config['path']}"
    print(f"\n=== {table_name} ===")
    print(f"[INFO] Path: {path}")

    if not hdfs_path_exists(spark, path):
        print("[FAIL] Path does not exist")
        return False

    if not hdfs_success_exists(spark, path):
        print("[FAIL] Missing _SUCCESS")
        return False
    print("[PASS] _SUCCESS exists")

    try:
        df = spark.read.parquet(path)
    except Exception as exc:
        print(f"[FAIL] Unable to read Parquet output: {exc}")
        return False
    row_count = df.count()
    print(f"[METRIC] Row count: {row_count}")
    if row_count == 0:
        print("[FAIL] Table is empty")
        return False

    columns = set(df.columns)
    missing_columns = [
        column for column in config["required_columns"] if column not in columns
    ]
    if missing_columns:
        print(f"[FAIL] Missing required columns: {missing_columns}")
        return False
    print("[PASS] Required columns exist")

    null_exprs = [
        sum(col(column).isNull().cast("int")).alias(column)
        for column in config["required_non_null"]
    ]
    nulls = df.agg(count("*").alias("rows"), *null_exprs).collect()[0]
    for column in config["required_non_null"]:
        print(f"[METRIC] Null {column}: {nulls[column]} / {nulls['rows']}")
        if nulls[column] != 0:
            print(f"[FAIL] Required column has nulls: {column}")
            return False
    print("[PASS] Required non-null checks")

    distinct_key_count = df.select(config["distinct_key"]).distinct().count()
    print(f"[METRIC] Distinct {config['distinct_key']}: {distinct_key_count}")

    print("[INFO] Schema:")
    df.printSchema()
    return True


def validate_relationships(spark, base_path):
    print("\n=== relationships ===")
    admissions_path = f"{base_path}/silver/admissions"
    admissions = spark.read.parquet(admissions_path).select("hadm_id").distinct()
    admissions_count = admissions.count()
    print(f"[METRIC] Silver admissions distinct hadm_id: {admissions_count}")

    for table_name in ["chartevents_agg", "diagnoses", "labs_agg"]:
        path = f"{base_path}/silver/{table_name}"
        if not hdfs_path_exists(spark, path):
            print(f"[WARN] Skipping relationship check for missing table: {table_name}")
            continue
        table = spark.read.parquet(path).select("hadm_id").distinct()
        orphan_count = table.join(admissions, on="hadm_id", how="left_anti").count()
        print(f"[METRIC] {table_name} hadm_id not found in admissions: {orphan_count}")
        if orphan_count == 0:
            print(f"[PASS] {table_name} hadm_id subset of admissions")
        else:
            print(f"[FAIL] {table_name} has orphan hadm_id")


def main():
    parser = argparse.ArgumentParser(description="Validate Silver Layer outputs")
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    builder = SparkSession.builder.appName("ValidateSilverLayer")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Validating Silver Layer in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    results = []
    for table_name, config in SILVER_TABLES.items():
        results.append(
            (table_name, validate_table(spark, base_path, table_name, config))
        )

    if any(table == "admissions" and passed for table, passed in results):
        validate_relationships(spark, base_path)

    print("\n=== summary ===")
    all_passed = True
    for table_name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {table_name}")
        all_passed = all_passed and passed

    spark.stop()
    if not all_passed:
        raise RuntimeError("Silver validation failed")


if __name__ == "__main__":
    main()
