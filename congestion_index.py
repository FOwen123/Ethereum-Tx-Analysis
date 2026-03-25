# Script 2: Daily congestion index
# Tracks day-by-day network stress throughout August 2025
# Key finding: ATH price vs gas price relationship, gas limit raise effect

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("CongestionIndex")
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

S3_BUCKET = "midterm-big-data-project"
INPUT_PATH = f"s3://{S3_BUCKET}/cleaned/ethereum/"
OUTPUT_PATH = f"s3://{S3_BUCKET}/results/congestion_index/"

# Max block gas limit in August 2025 (raised to 45M in June 2025)
BLOCK_GAS_LIMIT = 45_000_000
# Average blocks per day on Ethereum PoS (~7200 blocks/day at 12s slot time)
BLOCKS_PER_DAY = 7200

print("=== Loading cleaned Ethereum data ===")
df = spark.read.parquet(INPUT_PATH)

# -------------------------------------------------------
# 1. Daily congestion index
# -------------------------------------------------------
daily = df.groupBy("date").agg(
    F.count("hash").alias("tx_count"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.max("gas_fee_gwei").alias("max_gas_gwei"),
    F.min("gas_fee_gwei").alias("min_gas_gwei"),
    F.percentile_approx("gas_fee_gwei", 0.50).alias("median_gas_gwei"),
    F.percentile_approx("gas_fee_gwei", 0.95).alias("p95_gas_gwei"),
    F.sum("receipt_gas_used").alias("total_gas_used"),
    F.avg("tx_cost_gwei").alias("avg_tx_cost_gwei"),
    F.sum("tx_cost_gwei").alias("total_fees_gwei"),
)

# Congestion index = actual gas used / theoretical max gas per day
# Value near 1.0 = fully congested, near 0 = low activity
daily = daily.withColumn(
    "congestion_index",
    F.col("total_gas_used") / (BLOCK_GAS_LIMIT * BLOCKS_PER_DAY)
).withColumn(
    "congestion_level",
    F.when(F.col("congestion_index") >= 0.85, "critical")
     .when(F.col("congestion_index") >= 0.65, "high")
     .when(F.col("congestion_index") >= 0.40, "medium")
     .otherwise("low")
).orderBy("date")

print("--- Daily congestion index (August 2025) ---")
daily.select(
    "date", "tx_count", "avg_gas_gwei",
    "congestion_index", "congestion_level"
).show(31, truncate=False)

daily.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}daily_index/")

# -------------------------------------------------------
# 2. Most congested days
# -------------------------------------------------------
top_days = daily.orderBy(F.col("congestion_index").desc()).limit(10)
print("--- Top 10 most congested days ---")
top_days.show(truncate=False)

top_days.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}top_congested_days/")

# -------------------------------------------------------
# 3. Gas price distribution — shows effect of 45M gas limit raise
# -------------------------------------------------------
# Bucket gas prices to show distribution
buckets = df.withColumn(
    "gas_bucket",
    F.when(F.col("gas_fee_gwei") < 1,   "< 1 gwei")
     .when(F.col("gas_fee_gwei") < 3,   "1-3 gwei")
     .when(F.col("gas_fee_gwei") < 5,   "3-5 gwei")
     .when(F.col("gas_fee_gwei") < 10,  "5-10 gwei")
     .when(F.col("gas_fee_gwei") < 20,  "10-20 gwei")
     .when(F.col("gas_fee_gwei") < 50,  "20-50 gwei")
     .otherwise("> 50 gwei")
).groupBy("gas_bucket").agg(
    F.count("hash").alias("tx_count")
).orderBy("gas_bucket")

print("--- Gas price distribution ---")
buckets.show(truncate=False)

buckets.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}gas_distribution/")

# -------------------------------------------------------
# 4. Weekly summary — week over week trend
# -------------------------------------------------------
weekly = df.withColumn(
    "week",
    F.weekofyear("block_timestamp")
).groupBy("week").agg(
    F.count("hash").alias("tx_count"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.sum("receipt_gas_used").alias("total_gas_used"),
).orderBy("week")

print("--- Weekly summary ---")
weekly.show(truncate=False)

weekly.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}weekly_summary/")

print(f"=== congestion_index.py complete. Results at {OUTPUT_PATH} ===")
spark.stop()