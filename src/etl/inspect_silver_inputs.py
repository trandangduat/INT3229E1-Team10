import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower


def show_table(spark, name, path):
    print(f"\n=== {name} ===")
    try:
        df = spark.read.parquet(path)
        print("columns:", df.columns)
        print("count:", df.count())
        df.show(5, truncate=80)
    except Exception as exc:
        print(f"ERROR: {exc}")


def main():
    parser = argparse.ArgumentParser(
        description="Inspect Bronze inputs for Silver mapping"
    )
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    builder = SparkSession.builder.appName("InspectSilverInputs")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if args.env == "local":
        base = "data/bronze"
    else:
        base = "hdfs://master10:9000/user/dis/data/bronze"

    paths = {
        "d_items": f"{base}/mimic_iv/d_items",
        "d_labitems": f"{base}/mimic_iv/d_labitems",
        "chartevents": f"{base}/mimic_iv/chartevents",
        "labevents": f"{base}/mimic_iv/labevents",
        "eicu_patient": f"{base}/eicu/patient",
        "eicu_vitalPeriodic": f"{base}/eicu/vitalPeriodic",
        "eicu_diagnosis": f"{base}/eicu/diagnosis",
        "eicu_medication": f"{base}/eicu/medication",
    }

    for name, path in paths.items():
        show_table(spark, name, path)

    d_items = spark.read.parquet(paths["d_items"])
    print("\n=== d_items vital candidates ===")
    for pattern in [
        "systolic|blood pressure|arterial pressure",
        "spo2|oxygen saturation|o2 saturation",
        "heart rate|pulse",
        "temperature|temp",
    ]:
        print(f"\n-- pattern: {pattern}")
        d_items.where(lower(col("label")).rlike(pattern)).select(
            "itemid", "label", "abbreviation", "linksto", "category", "unitname"
        ).show(100, truncate=False)

    labevents = spark.read.parquet(paths["labevents"])
    print("\n=== labevents itemid top counts ===")
    labevents.groupBy("itemid").count().orderBy(col("count").desc()).show(
        50, truncate=False
    )

    try:
        d_labitems = spark.read.parquet(paths["d_labitems"])
        print("\n=== d_labitems feature candidates ===")
        for pattern in [
            "creatinine|urea nitrogen|bun|glucose|sodium|potassium|chloride|bicarbonate",
            "hemoglobin|hematocrit|platelet|white blood|wbc|inr|ptt|pt ",
            "albumin|bilirubin|lactate|magnesium|calcium|phosphate",
        ]:
            print(f"\n-- pattern: {pattern}")
            d_labitems.where(lower(col("label")).rlike(pattern)).select(
                "itemid", "label", "fluid", "category"
            ).show(200, truncate=False)

        print("\n=== labevents top itemids with labels ===")
        labevents.groupBy("itemid").count().join(
            d_labitems, on="itemid", how="left"
        ).select("itemid", "label", "fluid", "category", "count").orderBy(
            col("count").desc()
        ).show(100, truncate=False)
    except Exception as exc:
        print(f"\n=== d_labitems unavailable ===\n{exc}")

    spark.stop()


if __name__ == "__main__":
    main()
