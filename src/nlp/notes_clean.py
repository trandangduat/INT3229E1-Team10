import argparse

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, size, trim


MEDICAL_STOPWORDS = [
    "admission",
    "discharge",
    "date",
    "name",
    "patient",
    "hospital",
]


def first_existing_column(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def main():
    parser = argparse.ArgumentParser(
        description="MIMIC-IV-Note Silver Layer: Clean Notes"
    )
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    args = parser.parse_args()

    if args.env == "local":
        base_path = "data"
    else:
        base_path = "hdfs://master10:9000/user/dis/data"

    print(f"[INFO] Starting notes_clean job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    builder = SparkSession.builder.appName("SilverLayer_NotesClean")
    if args.env == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"{base_path}/bronze/mimic_iv_note/discharge"
    output_path = f"{base_path}/silver/notes_clean"

    print(f"[INFO] Reading Bronze notes from: {input_path}")
    df_raw = spark.read.parquet(input_path)
    input_count = df_raw.count()
    print(f"[METRIC] Raw notes count: {input_count}")

    text_col = first_existing_column(df_raw.columns, ["text", "note_text"])
    if text_col is None:
        raise ValueError("Expected a note text column named 'text' or 'note_text'.")

    selected_cols = []
    for column_name in [
        "note_id",
        "subject_id",
        "hadm_id",
        "note_type",
        "charttime",
        "storetime",
    ]:
        if column_name in df_raw.columns:
            selected_cols.append(col(column_name))

    df_notes = df_raw.select(
        *selected_cols,
        col(text_col).alias("note_text_raw"),
    )

    df_clean = df_notes.withColumn(
        "note_text_clean",
        trim(
            regexp_replace(
                regexp_replace(lower(col("note_text_raw")), r"\[\*\*.*?\*\*\]", " "),
                r"\s+",
                " ",
            )
        ),
    ).filter(col("note_text_clean") != "")

    tokenizer = RegexTokenizer(
        inputCol="note_text_clean",
        outputCol="tokens_raw",
        pattern="[^a-z0-9]+",
        gaps=True,
        minTokenLength=2,
    )
    tokenized = tokenizer.transform(df_clean)

    remover = StopWordsRemover(
        inputCol="tokens_raw",
        outputCol="tokens",
        stopWords=StopWordsRemover.loadDefaultStopWords("english") + MEDICAL_STOPWORDS,
    )
    df_silver = (
        remover.transform(tokenized)
        .drop("tokens_raw")
        .withColumn("token_count", size(col("tokens")))
    )

    final_count = df_silver.count()
    print(f"[METRIC] Clean notes count: {final_count}")

    token_stats = df_silver.select("token_count").summary(
        "min", "25%", "50%", "75%", "max"
    )
    print("[METRIC] Token count distribution:")
    token_stats.show(truncate=False)

    if "hadm_id" in df_silver.columns:
        admission_count = (
            df_silver.select("hadm_id")
            .where(col("hadm_id").isNotNull())
            .distinct()
            .count()
        )
        print(f"[METRIC] Admissions with clean notes: {admission_count}")

    print(f"[INFO] Writing clean notes to: {output_path}")
    df_silver.write.mode("overwrite").option("compression", "snappy").parquet(
        output_path
    )

    print("[INFO] notes_clean job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
