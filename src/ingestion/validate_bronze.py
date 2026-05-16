import argparse
import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class TableSpec:
    dataset: str
    table: str
    raw_path: str
    bronze_path: str
    multiline: bool = False


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    return fs.exists(hadoop_path)


def read_raw_csv(spark, path, multiline=False):
    reader = spark.read.option("header", True).option("inferSchema", False)
    if multiline:
        reader = reader.option("multiLine", True).option("escape", '"')
    return reader.csv(path)


def read_bronze_parquet(spark, path):
    return spark.read.parquet(path)


def validate_table(spark, spec):
    result = {
        "dataset": spec.dataset,
        "table": spec.table,
        "raw_path": spec.raw_path,
        "bronze_path": spec.bronze_path,
        "raw_exists": False,
        "bronze_exists": False,
        "success_exists": False,
        "raw_rows": None,
        "bronze_rows": None,
        "raw_columns": None,
        "bronze_columns": None,
        "columns_match": False,
        "all_bronze_string": False,
        "rows_match": False,
        "status": "FAIL",
        "error": None,
    }

    try:
        result["raw_exists"] = path_exists(spark, spec.raw_path)
        result["bronze_exists"] = path_exists(spark, spec.bronze_path)
        result["success_exists"] = path_exists(spark, f"{spec.bronze_path}/_SUCCESS")

        if not result["raw_exists"]:
            result["error"] = "Raw CSV path does not exist"
            return result

        if not result["bronze_exists"]:
            result["error"] = "Bronze Parquet path does not exist"
            return result

        raw_df = read_raw_csv(spark, spec.raw_path, spec.multiline)
        bronze_df = read_bronze_parquet(spark, spec.bronze_path)

        result["raw_rows"] = raw_df.count()
        result["bronze_rows"] = bronze_df.count()
        result["raw_columns"] = raw_df.columns
        result["bronze_columns"] = bronze_df.columns

        result["rows_match"] = result["raw_rows"] == result["bronze_rows"]
        result["columns_match"] = result["raw_columns"] == result["bronze_columns"]
        result["all_bronze_string"] = all(
            field.dataType.simpleString() == "string"
            for field in bronze_df.schema.fields
        )

        if (
            result["rows_match"]
            and result["columns_match"]
            and result["all_bronze_string"]
        ):
            result["status"] = "PASS"
        else:
            result["status"] = "FAIL"

        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result


def build_table_specs(base_path, include_note=True):
    raw_base = f"{base_path}/raw_data"
    bronze_base = f"{base_path}/bronze"

    specs = [
        TableSpec(
            dataset="mimic_iv",
            table="admissions",
            raw_path=f"{raw_base}/mimic/hosp/admissions.csv",
            bronze_path=f"{bronze_base}/mimic_iv/admissions",
        ),
        TableSpec(
            dataset="mimic_iv",
            table="patients",
            raw_path=f"{raw_base}/mimic/hosp/patients.csv",
            bronze_path=f"{bronze_base}/mimic_iv/patients",
        ),
        TableSpec(
            dataset="mimic_iv",
            table="diagnoses_icd",
            raw_path=f"{raw_base}/mimic/hosp/diagnoses_icd.csv",
            bronze_path=f"{bronze_base}/mimic_iv/diagnoses_icd",
        ),
        TableSpec(
            dataset="mimic_iv",
            table="labevents",
            raw_path=f"{raw_base}/mimic/hosp/labevents.csv",
            bronze_path=f"{bronze_base}/mimic_iv/labevents",
        ),
        TableSpec(
            dataset="mimic_iv",
            table="d_items",
            raw_path=f"{raw_base}/mimic/icu/d_items.csv",
            bronze_path=f"{bronze_base}/mimic_iv/d_items",
        ),
        TableSpec(
            dataset="mimic_iv",
            table="chartevents",
            raw_path=f"{raw_base}/mimic/icu/chartevents.csv",
            bronze_path=f"{bronze_base}/mimic_iv/chartevents",
        ),
        TableSpec(
            dataset="eicu",
            table="patient",
            raw_path=f"{raw_base}/eICU/patient.csv",
            bronze_path=f"{bronze_base}/eicu/patient",
        ),
        TableSpec(
            dataset="eicu",
            table="vitalPeriodic",
            raw_path=f"{raw_base}/eICU/vitalPeriodic.csv",
            bronze_path=f"{bronze_base}/eicu/vitalPeriodic",
        ),
        TableSpec(
            dataset="eicu",
            table="diagnosis",
            raw_path=f"{raw_base}/eICU/diagnosis.csv",
            bronze_path=f"{bronze_base}/eicu/diagnosis",
        ),
        TableSpec(
            dataset="eicu",
            table="medication",
            raw_path=f"{raw_base}/eICU/medication.csv",
            bronze_path=f"{bronze_base}/eicu/medication",
        ),
    ]

    if include_note:
        specs.append(
            TableSpec(
                dataset="mimic_iv_note",
                table="discharge",
                raw_path=f"{raw_base}/mimic/note/discharge.csv",
                bronze_path=f"{bronze_base}/mimic_iv_note/discharge",
                multiline=True,
            )
        )

    return specs


def filter_specs(specs, datasets, tables):
    if datasets:
        allowed = set(datasets)
        specs = [spec for spec in specs if spec.dataset in allowed]

    if tables:
        allowed = set(tables)
        specs = [spec for spec in specs if spec.table in allowed]

    return specs


def print_result(result):
    print(f"\n===== {result['dataset']}.{result['table']} =====")
    print(f"Status: {result['status']}")
    print(f"Raw path: {result['raw_path']}")
    print(f"Bronze path: {result['bronze_path']}")
    print(f"Raw exists: {result['raw_exists']}")
    print(f"Bronze exists: {result['bronze_exists']}")
    print(f"_SUCCESS exists: {result['success_exists']}")
    print(f"Raw rows: {result['raw_rows']}")
    print(f"Bronze rows: {result['bronze_rows']}")
    print(f"Rows match: {result['rows_match']}")
    print(f"Raw columns: {len(result['raw_columns'] or [])}")
    print(f"Bronze columns: {len(result['bronze_columns'] or [])}")
    print(f"Columns match: {result['columns_match']}")
    print(f"Bronze all string columns: {result['all_bronze_string']}")

    if (
        result["raw_columns"]
        and result["bronze_columns"]
        and not result["columns_match"]
    ):
        print("Raw columns:")
        print(",".join(result["raw_columns"]))
        print("Bronze columns:")
        print(",".join(result["bronze_columns"]))

    if result["error"]:
        print(f"Error: {result['error']}")


def main():
    parser = argparse.ArgumentParser(description="Validate Bronze ingestion outputs.")
    parser.add_argument(
        "--base-path",
        default="hdfs://master10:9000/user/dis/data",
        help="Base HDFS data path. Default: hdfs://master10:9000/user/dis/data",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["mimic_iv", "eicu", "mimic_iv_note"],
        help="Dataset to validate. Can be repeated. Default: all datasets.",
    )
    parser.add_argument(
        "--table",
        action="append",
        help="Table to validate. Can be repeated. Default: all tables.",
    )
    parser.add_argument(
        "--skip-note",
        action="store_true",
        help="Skip MIMIC-IV-Note validation if discharge.csv is not ready yet.",
    )

    args = parser.parse_args()

    spark = SparkSession.builder.appName("Validate_Bronze_Ingestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    specs = build_table_specs(args.base_path, include_note=not args.skip_note)
    specs = filter_specs(specs, args.dataset, args.table)

    if not specs:
        print("No tables selected for validation.")
        spark.stop()
        return 1

    print(f"Base path: {args.base_path}")
    print(f"Tables selected: {len(specs)}")

    results = []
    for spec in specs:
        result = validate_table(spark, spec)
        results.append(result)
        print_result(result)

    total = len(results)
    passed = sum(1 for result in results if result["status"] == "PASS")
    failed = total - passed

    print("\n===== SUMMARY =====")
    print(f"Total tables: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed:
        print("Failed tables:")
        for result in results:
            if result["status"] != "PASS":
                print(
                    f"- {result['dataset']}.{result['table']}: {result['error'] or 'validation mismatch'}"
                )

    spark.stop()
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
