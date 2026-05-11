import argparse
import time
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
from pyspark.ml.feature import Word2Vec


def log(msg):
    print(msg, flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Train Word2Vec on MIMIC-IV-Note cleaned tokens"
    )
    parser.add_argument(
        "env", choices=["local", "hdfs"], help="Execution environment (local or hdfs)"
    )
    parser.add_argument("--vector-size", type=int, default=128)
    parser.add_argument("--min-count", type=int, default=5)
    parser.add_argument("--window-size", type=int, default=5)
    parser.add_argument("--max-iter", type=int, default=1)
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    log(f"[INFO] Starting train_word2vec job in {args.env.upper()} mode")
    log(
        f"[INFO] vectorSize={args.vector_size}, minCount={args.min_count}, windowSize={args.window_size}, maxIter={args.max_iter}"
    )

    builder = SparkSession.builder.appName("NLP_TrainWord2Vec")
    if args.env == "local":
        builder = builder.master("local[*]")
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    start_time = time.time()

    input_path = f"{base_path}/silver/notes_clean"
    model_path = f"{base_path}/silver/word2vec_model"

    log(f"[INFO] Reading clean notes from: {input_path}")
    df = spark.read.parquet(input_path)

    token_count = df.count()
    log(f"[METRIC] Total notes: {token_count}")

    df_tokens = df.filter(col("tokens").isNotNull() & (size(col("tokens")) > 0))
    valid_count = df_tokens.count()
    log(f"[METRIC] Notes with valid tokens: {valid_count}")

    log("[INFO] Training Word2Vec... (this may take a few minutes)")
    log("[INFO] Check progress at http://master10:4040 → Stages tab")

    t1 = time.time()
    w2v = Word2Vec(
        vectorSize=args.vector_size,
        minCount=args.min_count,
        windowSize=args.window_size,
        maxIter=args.max_iter,
        inputCol="tokens",
        outputCol="note_embedding",
        seed=42,
    )
    model = w2v.fit(df_tokens)
    train_elapsed = time.time() - t1
    log(f"[METRIC] Word2Vec training completed in {train_elapsed:.1f}s")

    log(f"[INFO] Saving model to: {model_path}")
    model.write().overwrite().save(model_path)

    vocab_size = len(model.getVectors().collect())
    elapsed = time.time() - start_time
    log(f"[METRIC] Vocabulary size: {vocab_size}")
    log(f"[METRIC] Total elapsed time: {elapsed:.1f}s")
    log("[INFO] train_word2vec job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
