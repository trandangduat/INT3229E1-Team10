import argparse
import hashlib
import struct
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, udf
from pyspark.sql.types import ArrayType, DoubleType


DIM = 128


def log(msg):
    print(msg, flush=True)


def token_to_vector(token):
    h = hashlib.md5(token.encode("utf-8")).digest()
    result = []
    for i in range(DIM):
        seed_bytes = h + struct.pack("<I", i)
        hash_val = int(hashlib.sha256(seed_bytes).hexdigest(), 16)
        val = (hash_val % 20001 - 10000) / 10000.0
        result.append(val)
    return result


def make_hash_embedding(tokens):
    if not tokens:
        return [0.0] * DIM
    vecs = [token_to_vector(t) for t in tokens]
    n = len(vecs)
    return [sum(v[i] for v in vecs) / n for i in range(DIM)]


hash_embedding_udf = udf(make_hash_embedding, ArrayType(DoubleType()))


def main():
    parser = argparse.ArgumentParser(
        description="Generate note embeddings using hash-based approach (no training needed)"
    )
    parser.add_argument("env", choices=["local", "hdfs"])
    parser.add_argument("--dim", type=int, default=128)
    args = parser.parse_args()

    base_path = "data" if args.env == "local" else "hdfs://master10:9000/user/dis/data"
    log(f"[INFO] Starting note_embeddings job in {args.env.upper()} mode")
    log(f"[INFO] Dimension: {args.dim}")

    builder = SparkSession.builder.appName("NLP_NoteEmbeddings_Hash")
    if args.env == "local":
        builder = builder.master("local[*]")
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    start_time = time.time()

    input_path = f"{base_path}/silver/notes_clean"
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

    log(f"[STEP 3/4] Generating {args.dim}-dim hash embeddings (no training needed)...")
    t1 = time.time()
    df_emb = df_valid.withColumn("note_embedding", hash_embedding_udf(col("tokens")))
    df_emb = df_emb.select("hadm_id", "note_embedding")

    emb_count = df_emb.count()
    t2 = time.time()
    log(f"[METRIC] Embeddings generated: {emb_count} in {t2 - t1:.1f}s")

    log("[STEP 3b/4] Verifying sample embeddings...")
    rows = df_emb.limit(3).collect()
    for r in rows:
        vec = r["note_embedding"]
        log(f"  hadm_id={r['hadm_id']}, dim={len(vec)}, sample_vec={vec[:5]}")

    log(f"[STEP 4/4] Writing note embeddings to: {output_path}")
    df_emb.write.mode("overwrite").option("compression", "snappy").parquet(output_path)

    elapsed = time.time() - start_time
    log(f"[METRIC] Total elapsed time: {elapsed:.1f}s")
    log("[INFO] note_embeddings job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
