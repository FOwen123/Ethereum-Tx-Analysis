# Script 1: Peak hour congestion analysis
# Finds which hours of the day had highest gas prices and transaction volume
# August 2025 Ethereum data

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("PeakHoursAnalysis")
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

S3_BUCKET = "midterm-big-data-project"
INPUT_PATH = f"s3://{S3_BUCKET}/cleaned/ethereum/"
OUTPUT_PATH = f"s3://{S3_BUCKET}/results/peak_hours/"

print("=== Loading cleaned Ethereum data ===")
df = spark.read.parquet(INPUT_PATH)

# -------------------------------------------------------
# 1. Hourly aggregation — avg gas price, tx count, avg fee
# -------------------------------------------------------
hourly = df.groupBy("hour_of_day").agg(
    F.count("hash").alias("tx_count"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.percentile_approx("gas_fee_gwei", 0.95).alias("p95_gas_gwei"),
    F.avg("tx_cost_gwei").alias("avg_tx_cost_gwei"),
    F.sum("receipt_gas_used").alias("total_gas_used")
).orderBy("hour_of_day")

# Add congestion score: normalized combination of gas price + tx volume
w = Window.orderBy(F.lit(1))
max_gas = hourly.agg(F.max("avg_gas_gwei")).collect()[0][0]
max_tx   = hourly.agg(F.max("tx_count")).collect()[0][0]

hourly = hourly.withColumn(
    "congestion_score",
    (F.col("avg_gas_gwei") / max_gas * 0.6 +
     F.col("tx_count") / max_tx * 0.4) * 100
)

print("--- Hourly congestion (UTC) ---")
hourly.orderBy(F.col("congestion_score").desc()).show(24, truncate=False)

hourly.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}hourly_congestion/")

# -------------------------------------------------------
# 2. Day-of-week aggregation
# -------------------------------------------------------
# day_of_week: 1=Sunday, 2=Monday, ..., 7=Saturday (Spark default)
dow_map = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
           5: "Thursday", 6: "Friday", 7: "Saturday"}

dow_expr = F.create_map([F.lit(k) for pair in
    [(k, v) for k, v in dow_map.items()] for k in pair])

daily = df.groupBy("day_of_week").agg(
    F.count("hash").alias("tx_count"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.avg("tx_cost_gwei").alias("avg_tx_cost_gwei"),
).orderBy("day_of_week")

print("--- Day of week congestion ---")
daily.show(7, truncate=False)

daily.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}daily_congestion/")

# -------------------------------------------------------
# 3. Heatmap matrix: hour x day_of_week
# -------------------------------------------------------
heatmap = df.groupBy("hour_of_day", "day_of_week").agg(
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.count("hash").alias("tx_count")
).orderBy("day_of_week", "hour_of_day")

print("--- Hour x Day heatmap matrix ---")
heatmap.show(50, truncate=False)

heatmap.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}heatmap_matrix/")

# -------------------------------------------------------
# 4. Top 5 most congested hours overall
# -------------------------------------------------------
top5 = hourly.orderBy(F.col("congestion_score").desc()).limit(5)
print("--- Top 5 peak congestion hours (UTC) ---")
top5.show(truncate=False)

top5.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}top5_peak_hours/")

print(f"=== peak_hours.py complete. Results at {OUTPUT_PATH} ===")
spark.stop()