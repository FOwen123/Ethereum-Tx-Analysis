# transfer.py
import boto3
import time
from google.cloud import bigquery
from google.oauth2 import service_account

# --- config ---
PROJECT_ID    = "storied-precept-469907-j7"
S3_BUCKET     = "midterm-big-data-project"
S3_PREFIX     = "raw/ethereum/"
CREDS_FILE    = "/home/ubuntu/gcp-key.json"   # GCP service account key

# --- clients ---
creds = service_account.Credentials.from_service_account_file(CREDS_FILE)
bq    = bigquery.Client(project=PROJECT_ID, credentials=creds)
s3    = boto3.client("s3")

QUERY = """
SELECT
  block_timestamp,
  block_number,
  `hash`,
  from_address,
  to_address,
  gas,
  gas_price,
  receipt_gas_used,
  CAST(`value` AS FLOAT64) AS value_wei
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE DATE(block_timestamp) BETWEEN '2025-08-01' AND '2025-08-31'
"""

def transfer():
    print("Running BigQuery export...")
    query_job = bq.query(QUERY)

    # Wait for completion without calling getQueryResults (can fail for huge results).
    while query_job.state != "DONE":
        time.sleep(2)
        query_job.reload()

    if query_job.error_result:
        raise RuntimeError(f"BigQuery job failed: {query_job.error_result}")
    if query_job.destination is None:
        raise RuntimeError("BigQuery job has no destination table to read from.")

    # Read from the query destination table to avoid large-response API limits.
    df_iter = bq.list_rows(query_job.destination).to_dataframe_iterable()

    for i, chunk in enumerate(df_iter):
        filename = f"transactions_chunk_{i:04d}.parquet"
        local_path = f"/tmp/{filename}"

        # save chunk as parquet (smaller than CSV, faster to process in Spark)
        chunk.to_parquet(local_path, index=False)
        print(f"Uploading chunk {i} → s3://{S3_BUCKET}/{S3_PREFIX}{filename}")

        s3.upload_file(local_path, S3_BUCKET, f"{S3_PREFIX}{filename}")
        print(f"Chunk {i} done — {len(chunk):,} rows")

if __name__ == "__main__":
    transfer()