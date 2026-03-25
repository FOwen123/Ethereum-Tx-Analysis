# preprocess.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("BlockchainPreprocess")
    # Read Parquet TIMESTAMP(NANOS, true) as LONG (nanoseconds) on Spark.
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)

S3_BUCKET = "midterm-big-data-project"
CHAIN = "ethereum"
REQUIRED_COLUMNS = {
    "block_timestamp",
    "block_number",
    "hash",
    "from_address",
    "to_address",
    "gas",
    "gas_price",
    "receipt_gas_used",
    "value_wei",
}

print(f"Processing {CHAIN}...")
df = spark.read.parquet(f"s3://{S3_BUCKET}/raw/{CHAIN}/")

missing = REQUIRED_COLUMNS.difference(set(df.columns))
if missing:
    raise ValueError(f"Missing required columns in raw parquet: {sorted(missing)}")

print("Raw schema check passed.")
df.printSchema()
print(f"Raw rows: {df.count():,}")

# Normalize block_timestamp for downstream hour/day/date functions.
block_ts_type = dict(df.dtypes).get("block_timestamp")
if block_ts_type in ("bigint", "long"):
    df = df.withColumn(
        "block_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("block_timestamp") / F.lit(1_000_000_000))),
    )
elif block_ts_type not in ("timestamp",):
    raise ValueError(f"Unsupported block_timestamp type: {block_ts_type}")

# 1. Drop nulls in critical columns
df = df.dropna(subset=["block_timestamp", "gas_price", "receipt_gas_used"])

# 2. Filter out zero/negative gas
df = df.filter((F.col("gas_price") > 0) & (F.col("receipt_gas_used") > 0))

# 3. Add derived columns
df = df.withColumn("hour_of_day", F.hour("block_timestamp")) \
       .withColumn("day_of_week", F.dayofweek("block_timestamp")) \
       .withColumn("date", F.to_date("block_timestamp")) \
       .withColumn("gas_fee_gwei", F.col("gas_price") / 1e9) \
       .withColumn("tx_cost_gwei",
           (F.col("gas_price") * F.col("receipt_gas_used")) / 1e9) \
       .withColumn("chain", F.lit(CHAIN))

# 4. Write cleaned data back to S3
df.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/cleaned/{CHAIN}/")
print(f"Cleaned rows: {df.count():,}")
print(f"Done: {CHAIN}")

spark.stop()