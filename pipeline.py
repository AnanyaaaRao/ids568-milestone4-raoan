"""
pipeline.py
Distributed feature engineering pipeline using PySpark.

Usage:
    # Small correctness test first
    python pipeline.py --input test_data/ --output test_output/ --mode local --workers 1

    # Local baseline (single thread)
    python pipeline.py --input data/ --output output_local/ --mode local --workers 1

    # Distributed (4 threads)
    python pipeline.py --input data/ --output output_dist/ --mode distributed --workers 4

Metrics are saved to <output>/metrics_<mode>.json
Output hashes saved to <output>/output_hashes.json  (reproducibility verification)
"""

import os, time, hashlib, json, argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkConf

# ── CLI ──────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="Distributed feature engineering with PySpark")
parser.add_argument("--input",   required=True,  help="Input directory (parquet files)")
parser.add_argument("--output",  default="output/", help="Output directory")
parser.add_argument("--mode",    default="local", choices=["local", "distributed"],
                    help="local = 1 thread baseline; distributed = N parallel threads")
parser.add_argument("--workers", type=int, default=4,
                    help="Number of parallel worker threads (used in distributed mode)")
args = parser.parse_args()

os.makedirs(args.output, exist_ok=True)

# ── Build Spark master URL from --mode and --workers ─────────────────────────
# local[1]  → single-threaded baseline (no parallelism)
# local[N]  → N parallel executor threads, simulating a distributed cluster
if args.mode == "distributed":
    master = f"local[{args.workers}]"
    shuffle_partitions = args.workers * 4   # e.g. 16 for 4 workers
else:
    master = "local[1]"
    shuffle_partitions = 4

conf = (SparkConf()
        .setMaster(master)
        .setAppName(f"milestone4-{args.mode}-w{args.workers}")
        .set("spark.sql.shuffle.partitions",   str(shuffle_partitions))
        .set("spark.sql.adaptive.enabled",     "true")
        .set("spark.driver.memory",            "4g")
        .set("spark.executor.memory",          "2g")
        .set("spark.eventLog.enabled",         "false"))

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"[INFO] PySpark started  master={master}  shuffle_partitions={shuffle_partitions}")

# ── Load & cache ─────────────────────────────────────────────────────────────

t_start = time.time()
print(f"\n[INFO] Reading parquet from '{args.input}' ...")

df = spark.read.parquet(args.input)

# Repartition by user_id before caching so the user-level groupBy reuses
# the partition layout and avoids a full second shuffle.
df = df.repartition(shuffle_partitions, "user_id").cache()

total_rows = df.count()   # materialise cache
print(f"[INFO] Loaded & cached {total_rows:,} rows  ({time.time()-t_start:.1f}s)")

# Sub-frame reused by all three aggregations
purchases = (df.filter(F.col("event_type") == "purchase")
               .withColumn("spend", F.col("price") * F.col("quantity")))

# ── User features ─────────────────────────────────────────────────────────────

print("\n[INFO] Computing user features ...")
t0 = time.time()

uf = df.groupBy("user_id").agg(
    F.count("event_id")                                                 .alias("total_events"),
    F.sum(F.when(F.col("event_type") == "purchase",    1).otherwise(0)).alias("purchase_count"),
    F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_add_count"),
    F.countDistinct("product_id")                                       .alias("unique_products"),
    F.countDistinct("category")                                         .alias("unique_categories"),
    F.countDistinct("session_id")                                       .alias("session_count"),
    F.avg("rating")                                                     .alias("avg_rating_given"),
)
sp = purchases.groupBy("user_id").agg(
    F.sum("spend").alias("total_spend"),
    F.avg("spend").alias("avg_order_value"),
)
uf = (uf.join(sp, on="user_id", how="left")
        .fillna(0, subset=["total_spend", "avg_order_value"])
        .withColumn("purchase_rate",
                    F.col("purchase_count") / F.col("total_events")))

uf.write.mode("overwrite").parquet(os.path.join(args.output, "user_features"))
uf_rows = spark.read.parquet(os.path.join(args.output, "user_features")).count()
print(f"[INFO] User features: {uf_rows:,} rows  ({time.time()-t0:.1f}s)")

# ── Product features ──────────────────────────────────────────────────────────

print("\n[INFO] Computing product features ...")
t0 = time.time()

pf = df.groupBy("product_id", "category").agg(
    F.count("event_id")                                                 .alias("total_interactions"),
    F.sum(F.when(F.col("event_type") == "view",        1).otherwise(0)).alias("view_count"),
    F.sum(F.when(F.col("event_type") == "purchase",    1).otherwise(0)).alias("purchase_count"),
    F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_count"),
    F.avg("price")                                                      .alias("avg_price"),
    F.countDistinct("user_id")                                          .alias("unique_buyers"),
    F.avg("rating")                                                     .alias("avg_rating"),
)
rv = purchases.groupBy("product_id").agg(F.sum("spend").alias("total_revenue"))
pf = (pf.join(rv, on="product_id", how="left")
        .fillna(0, subset=["total_revenue"])
        .withColumn("conversion_rate",
                    F.col("purchase_count") /
                    F.when(F.col("view_count") == 0, 1).otherwise(F.col("view_count"))))

pf.write.mode("overwrite").parquet(os.path.join(args.output, "product_features"))
pf_rows = spark.read.parquet(os.path.join(args.output, "product_features")).count()
print(f"[INFO] Product features: {pf_rows:,} rows  ({time.time()-t0:.1f}s)")

# ── Category features ─────────────────────────────────────────────────────────

print("\n[INFO] Computing category features ...")
t0 = time.time()

cf = df.groupBy("category").agg(
    F.count("event_id")                                                 .alias("total_events"),
    F.sum(F.when(F.col("event_type") == "purchase",    1).otherwise(0)).alias("purchase_count"),
    F.avg("price")                                                      .alias("avg_price"),
    F.countDistinct("user_id")                                          .alias("unique_users"),
)
cr = purchases.groupBy("category").agg(F.sum("spend").alias("total_revenue"))
cf = (cf.join(cr, on="category", how="left")
        .fillna(0, subset=["total_revenue"]))

cf.write.mode("overwrite").parquet(os.path.join(args.output, "category_features"))
cf_rows = spark.read.parquet(os.path.join(args.output, "category_features")).count()
print(f"[INFO] Category features: {cf_rows:,} rows  ({time.time()-t0:.1f}s)")

# ── Collect shuffle & memory metrics from Spark ───────────────────────────────

sc = spark.sparkContext
status = sc.statusTracker()

shuffle_read_bytes = shuffle_write_bytes = peak_spill_bytes = 0
for sid in status.getStageIds():
    info = status.getStageInfo(sid)
    if info:
        shuffle_read_bytes  += info.shuffleReadBytes
        shuffle_write_bytes += info.shuffleWriteBytes
        peak_spill_bytes     = max(peak_spill_bytes, info.memoryBytesSpilled)

shuffle_read_mb  = round(shuffle_read_bytes  / 1e6, 2)
shuffle_write_mb = round(shuffle_write_bytes / 1e6, 2)
peak_spill_mb    = round(peak_spill_bytes    / 1e6, 2)

try:
    driver_heap_mb = round(
        sc._jvm.Runtime.getRuntime().totalMemory() / 1e6, 1)
except Exception:
    driver_heap_mb = "N/A"

total_runtime = round(time.time() - t_start, 2)

# ── Print & save metrics ──────────────────────────────────────────────────────

print(f"\n{'='*56}")
print(f" METRICS — {args.mode.upper()} MODE  (master={master})")
print(f"{'='*56}")
print(f" Rows processed        : {total_rows:,}")
print(f" Total runtime         : {total_runtime} s")
print(f" Workers               : {args.workers}")
print(f" Shuffle partitions    : {shuffle_partitions}")
print(f" Shuffle read          : {shuffle_read_mb} MB")
print(f" Shuffle write         : {shuffle_write_mb} MB")
print(f" Peak memory spilled   : {peak_spill_mb} MB")
print(f" Driver heap           : {driver_heap_mb} MB")
print(f"{'='*56}")

metrics = {
    "mode":                  args.mode,
    "master":                master,
    "workers":               args.workers,
    "shuffle_partitions":    shuffle_partitions,
    "rows_processed":        total_rows,
    "runtime_seconds":       total_runtime,
    "shuffle_read_mb":       shuffle_read_mb,
    "shuffle_write_mb":      shuffle_write_mb,
    "peak_spill_mb":         peak_spill_mb,
    "driver_heap_mb":        driver_heap_mb,
    "user_feature_rows":     uf_rows,
    "product_feature_rows":  pf_rows,
    "category_feature_rows": cf_rows,
}
metrics_path = os.path.join(args.output, f"metrics_{args.mode}.json")
with open(metrics_path, "w") as f:
    json.dump(metrics, f, indent=2)
print(f"[INFO] Metrics → {metrics_path}")

# ── Output hash verification ──────────────────────────────────────────────────
# SHA-256 over sorted (filename, size) pairs — stable across identical runs.

def hash_parquet_dir(dirpath: str) -> str:
    """Fingerprint all parquet files in a directory by name + byte size."""
    h = hashlib.sha256()
    entries = sorted(
        (fname, os.path.getsize(os.path.join(root, fname)))
        for root, _, fnames in os.walk(dirpath)
        for fname in fnames if fname.endswith(".parquet")
    )
    for fname, size in entries:
        h.update(f"{fname}:{size}".encode())
    return h.hexdigest()

print("\n[INFO] Computing output hashes ...")
hashes = {}
for name in ["user_features", "product_features", "category_features"]:
    h = hash_parquet_dir(os.path.join(args.output, name))
    hashes[name] = h
    print(f"  {name:25s} : {h[:24]}...")

hash_path = os.path.join(args.output, "output_hashes.json")
with open(hash_path, "w") as f:
    json.dump({"mode": args.mode, "workers": args.workers,
               "runtime_seconds": total_runtime, "hashes": hashes}, f, indent=2)
print(f"[INFO] Hashes → {hash_path}")

print("\n[INFO] Pipeline complete!")
spark.stop()
