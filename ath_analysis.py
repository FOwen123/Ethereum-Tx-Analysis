# Script 3: ETH ATH correlation analysis
# August 2025: ETH hit ~$4,897 ATH, daily txns hit all-time high ~1.7M
# BTC whale dumped 24k BTC for ETH on Aug 24
# Gas limit raised to 45M — fees dropped despite record activity
# This script finds the on-chain fingerprint of these events

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ATHAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

S3_BUCKET = "midterm-big-data-project"
INPUT_PATH = f"s3://{S3_BUCKET}/cleaned/ethereum/"
OUTPUT_PATH = f"s3://{S3_BUCKET}/results/ath_analysis/"

# Key dates in August 2025 to highlight in analysis
ATH_DATE      = "2025-08-24"   # BTC whale dump + ETH surge, ~ATH period
HIGH_ACTIVITY = "2025-08-25"   # Typically highest activity follows price spike

print("=== Loading cleaned Ethereum data ===")
df = spark.read.parquet(INPUT_PATH)

# -------------------------------------------------------
# 1. Block-level activity — finds exact congestion spikes
# -------------------------------------------------------
block_level = df.groupBy("block_number", "date").agg(
    F.count("hash").alias("tx_count"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.sum("receipt_gas_used").alias("block_gas_used"),
    F.max("gas_fee_gwei").alias("max_gas_gwei"),
).withColumn(
    "block_utilization",
    F.col("block_gas_used") / 45_000_000   # 45M gas limit
)

# Top 20 most congested blocks of August
top_blocks = block_level.orderBy(
    F.col("avg_gas_gwei").desc()
).limit(20)

print("--- Top 20 highest gas price blocks ---")
top_blocks.show(truncate=False)

top_blocks.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}top_congested_blocks/")

# -------------------------------------------------------
# 2. Hourly activity on ATH day vs average day
# -------------------------------------------------------
# ATH day hourly breakdown
ath_hourly = df.filter(F.col("date") == ATH_DATE) \
    .groupBy("hour_of_day").agg(
        F.count("hash").alias("tx_count"),
        F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
        F.sum("receipt_gas_used").alias("total_gas_used"),
    ).orderBy("hour_of_day")

print(f"--- Hourly breakdown on ATH day ({ATH_DATE}) ---")
ath_hourly.show(24, truncate=False)

ath_hourly.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}ath_day_hourly/")

# Average hourly across all of August
avg_hourly = df.groupBy("hour_of_day").agg(
    F.avg("gas_fee_gwei").alias("avg_gas_gwei_august"),
    (F.count("hash") / 31).alias("avg_tx_per_hour"),
).orderBy("hour_of_day")

avg_hourly.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}avg_hourly_august/")

# -------------------------------------------------------
# 3. Transaction value analysis — large value txns on ATH day
#    High value transfers = whale activity / institutional flows
# -------------------------------------------------------
high_value = df.filter(
    (F.col("date") == ATH_DATE) &
    (F.col("value_wei") > 1e18)   # > 1 ETH
).agg(
    F.count("hash").alias("large_tx_count"),
    F.avg("value_wei").alias("avg_value_wei"),
    F.sum("value_wei").alias("total_value_wei"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
)

print(f"--- High value transactions on ATH day ({ATH_DATE}) ---")
high_value.show(truncate=False)

high_value.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}ath_high_value_txns/")

# -------------------------------------------------------
# 4. Daily tx count trend — shows the ATH spike in context
#    Key finding: txns hit all-time high ~1.7M/day in August 2025
# -------------------------------------------------------
daily_volume = df.groupBy("date").agg(
    F.count("hash").alias("daily_tx_count"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.avg("tx_cost_gwei").alias("avg_tx_cost_gwei"),
    F.sum("receipt_gas_used").alias("total_gas_used"),
).orderBy("date")

# Add 7-day rolling average for trend line
window_7d = Window.orderBy("date").rowsBetween(-6, 0)
daily_volume = daily_volume.withColumn(
    "rolling_7d_avg_gas",
    F.avg("avg_gas_gwei").over(window_7d)
).withColumn(
    "rolling_7d_tx_count",
    F.avg("daily_tx_count").over(window_7d)
)

print("--- Daily transaction volume + gas trend ---")
daily_volume.show(31, truncate=False)

daily_volume.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}daily_volume_trend/")

# -------------------------------------------------------
# 5. Gas fee paradox — record txns but lower fees
#    Due to gas limit raise from 30M to 45M in June 2025
# -------------------------------------------------------
fee_paradox = df.groupBy("date").agg(
    F.count("hash").alias("tx_count"),
    F.avg("gas_fee_gwei").alias("avg_gas_gwei"),
    F.avg("tx_cost_gwei").alias("avg_cost_gwei"),
    (F.sum("receipt_gas_used") / 45_000_000 / 7200).alias("avg_block_utilization")
).orderBy("date")

print("--- Fee paradox: high activity vs low fees ---")
fee_paradox.select(
    "date", "tx_count", "avg_gas_gwei",
    "avg_cost_gwei", "avg_block_utilization"
).show(31, truncate=False)

fee_paradox.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{OUTPUT_PATH}fee_paradox/")

print(f"=== ath_analysis.py complete. Results at {OUTPUT_PATH} ===")
spark.stop()