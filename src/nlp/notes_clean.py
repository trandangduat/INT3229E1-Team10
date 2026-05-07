import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    lower,
    regexp_replace,
    size,
    split,
    trim,
    udf,
)
from pyspark.sql.types import ArrayType, StringType

ENGLISH_STOPWORDS = [
    "a",
    "an",
    "the",
    "and",
    "or",
    "but",
    "if",
    "in",
    "on",
    "at",
    "to",
    "for",
    "of",
    "with",
    "by",
    "from",
    "is",
    "was",
    "are",
    "were",
    "been",
    "be",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "shall",
    "can",
    "this",
    "that",
    "these",
    "those",
    "it",
    "its",
    "not",
    "no",
    "nor",
    "as",
    "so",
    "than",
    "too",
    "very",
    "just",
    "about",
    "above",
    "after",
    "again",
    "all",
    "also",
    "am",
    "any",
    "because",
    "before",
    "being",
    "between",
    "both",
    "during",
    "each",
    "few",
    "further",
    "get",
    "got",
    "here",
    "how",
    "into",
    "more",
    "most",
    "my",
    "now",
    "only",
    "other",
    "our",
    "out",
    "over",
    "own",
    "same",
    "she",
    "he",
    "him",
    "her",
    "his",
    "they",
    "them",
    "their",
    "then",
    "there",
    "through",
    "under",
    "until",
    "up",
    "us",
    "we",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "you",
    "your",
    "me",
    "i",
    "s",
    "t",
    "d",
    "ll",
    "ve",
    "re",
]

MEDICAL_STOPWORDS = [
    "admission",
    "discharge",
    "date",
    "name",
    "patient",
    "hospital",
    "hospitalname",
    "md",
    "patientname",
    "namepattern",
    "clip",
    "dr",
    "report",
    "completed",
    "signed",
    "electronic",
]

STOPWORDS = list(set(ENGLISH_STOPWORDS + MEDICAL_STOPWORDS))


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

    clean_count = df_clean.count()
    print(f"[METRIC] Notes after PHI strip and empty filter: {clean_count}")

    df_tokens = df_clean.withColumn(
        "tokens_raw", split(col("note_text_clean"), r"[^a-z0-9]+")
    ).withColumn(
        "tokens_filtered",
        expr("filter(tokens_raw, x -> length(x) >= 2)"),
    )

    stopword_set = set(STOPWORDS)
    stopwords_broadcast = spark.sparkContext.broadcast(stopword_set)

    def remove_stopwords(tokens):
        sw = stopwords_broadcast.value
        if tokens is None:
            return None
        return [t for t in tokens if t not in sw]

    remove_stopwords_udf = udf(remove_stopwords, ArrayType(StringType()))

    df_silver = (
        df_tokens.withColumn("tokens", remove_stopwords_udf(col("tokens_filtered")))
        .drop("tokens_raw", "tokens_filtered")
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
