import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, lit, size
from pyspark.ml.feature import Word2VecModel


def main():
    parser = argparse.ArgumentParser(
        description="Generate note embeddings from Word2Vec model"
    )
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    parser.add_argument(
        "--vector-size",
        type=int,
        default=128,
        help="Embedding dimension (default: 128)",
    )
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    print(f"[INFO] Starting note_embeddings job in {args.env.upper()} mode")
    print(f"[INFO] Base path: {base_path}")

    builder = SparkSession.builder.appName("NLP_NoteEmbeddings")
    if args.env == "local":
        builder = builder.master("local[*]")
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    start_time = time.time()

    input_path = f"{base_path}/silver/notes_clean"
    model_path = f"{base_path}/silver/word2vec_model"
    output_path = f"{base_path}/silver/note_embeddings"

    print(f"[INFO] Loading Word2Vec model from: {model_path}")
    model = Word2VecModel.load(model_path)

    print(f"[INFO] Reading clean notes from: {input_path}")
    df = spark.read.parquet(input_path).select(
        col("hadm_id").cast("long").alias("hadm_id"),
        col("tokens"),
    )
    note_count = df.count()
    print(f"[METRIC] Total notes: {note_count}")

    df_valid = df.filter(col("tokens").isNotNull() & (size(col("tokens")) > 0))
    valid_count = df_valid.count()
    print(f"[METRIC] Notes with valid tokens: {valid_count}")

    print("[INFO] Generating document embeddings...")
    df_emb = model.transform(df_valid).select("hadm_id", "note_embedding")

    df_emb_count = df_emb.count()
    print(f"[METRIC] Embeddings generated: {df_emb_count}")

    emb_col = df_emb.select("note_embedding").first()[0]
    actual_dim = len(emb_col) if emb_col else 0
    print(f"[METRIC] Embedding dimension: {actual_dim}")

    print(f"[INFO] Writing note embeddings to: {output_path}")
    df_emb.write.mode("overwrite").option("compression", "snappy").parquet(output_path)

    elapsed = time.time() - start_time
    print(f"[METRIC] Elapsed time: {elapsed:.1f}s")
    print("[INFO] note_embeddings job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
