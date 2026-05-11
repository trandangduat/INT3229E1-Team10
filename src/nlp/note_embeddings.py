import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.feature import Word2VecModel

DIM = 128


def log(msg):
    print(msg, flush=True)


def vector_to_array(v):
    if v is None:
        return None
    return v.toArray().tolist()


vector_to_array_udf = udf(vector_to_array, ArrayType(DoubleType()))


def main():
    parser = argparse.ArgumentParser(
        description="Generate note embeddings using trained Word2Vec model"
    )
    parser.add_argument("env", choices=["local", "hdfs"])
    parser.add_argument("--dim", type=int, default=128)
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    log(f"[INFO] Starting note_embeddings job in {args.env.upper()} mode")
    log(f"[INFO] Dimension: {args.dim}")

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

    log(f"[STEP 1/4] Reading clean notes from: {input_path}")
    df = spark.read.parquet(input_path).select(
        col("hadm_id").cast("long").alias("hadm_id"),
        col("tokens"),
    )
    note_count = df.count()
    log(f"[METRIC] Total notes: {note_count}")

    log(f"[STEP 2/4] Filtering valid tokens...")
    df_valid = df.filter(col("tokens").isNotNull() & (size(col("tokens")) > 0))
    valid_count = df_valid.count()
    log(f"[METRIC] Notes with valid tokens: {valid_count}")

    log(
        f"[STEP 3/4] Loading Word2Vec model and generating {args.dim}-dim embeddings..."
    )
    t1 = time.time()
    model = Word2VecModel.load(model_path)
    df_emb = model.transform(df_valid)

    # Flatten VectorUDT into separate columns note_emb_1, note_emb_2 ...
    df_emb = df_emb.withColumn("emb_array", vector_to_array_udf(col("note_embedding")))

    select_cols = ["hadm_id"] + [
        col("emb_array").getItem(i).alias(f"note_emb_{i + 1}") for i in range(args.dim)
    ]
    df_emb_final = df_emb.select(*select_cols)

    emb_count = df_emb_final.count()
    t2 = time.time()
    log(f"[METRIC] Embeddings generated: {emb_count} in {t2 - t1:.1f}s")

    log("[STEP 3b/4] Verifying sample embeddings...")
    rows = df_emb_final.limit(3).collect()
    for r in rows:
        vec = [r[f"note_emb_{i + 1}"] for i in range(5)]
        log(f"  hadm_id={r['hadm_id']}, sample_vec={vec}")

    log(f"[STEP 4/4] Writing note embeddings to: {output_path}")
    df_emb_final.write.mode("overwrite").option("compression", "snappy").parquet(
        output_path
    )

    elapsed = time.time() - start_time
    log(f"[METRIC] Total elapsed time: {elapsed:.1f}s")
    log("[INFO] note_embeddings job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
