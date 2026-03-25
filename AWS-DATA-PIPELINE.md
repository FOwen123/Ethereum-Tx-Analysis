# Big Data Project — Ethereum Congestion Analysis

### Complete Step-by-Step Guide

**Dataset:** Ethereum August 2025 transactions from BigQuery  
**Stack:** BigQuery → EC2 → S3 → EMR Spark → S3 results → AI Agent

---

## PART 1 — AWS IAM Setup

### 1.1 Create EC2 role (lets EC2 talk to S3 and EMR)

1. AWS Console → **IAM** → **Roles** → **Create role**
2. Trusted entity → **AWS service** → Use case → **EC2** → Next
3. Attach these policies:
   - `AmazonS3FullAccess`
   - `AmazonEMRFullAccessPolicy_v2`
   - `AmazonEC2FullAccess`
   - `IAMFullAccess`
4. Name it `ec2-s3-role` → **Create role**

### 1.2 Create EMR service role (lets EMR provision EC2)

1. **IAM** → **Roles** → **Create role**
2. Trusted entity → **AWS service** → Use case → **EMR** → select **EMR** → Next
3. It auto-attaches `AmazonEMRServicePolicy_v2` — also add:
   - `AmazonEC2FullAccess`
4. Name it `EMR-service-role` → **Create role**

### 1.3 Create EMR EC2 instance profile (lets EMR nodes read/write S3)

1. **IAM** → **Roles** → **Create role**
2. Trusted entity → **AWS service** → Use case → **EMR** → select **EMR Role for EC2** → Next
3. Attach:
   - `AmazonS3FullAccess`
   - `AmazonEMRFullAccessPolicy_v2`
4. Name it `EMR_EC2_DefaultRole` → **Create role**

---

## PART 2 — S3 Bucket

1. AWS Console → **S3** → **Create bucket**
2. Name: `yourname-bigdata-project` (globally unique)
3. Region: `us-east-1`
4. Everything else default → **Create bucket**

---

## PART 3 — EC2 Setup

### 3.1 Launch instance

1. AWS Console → **EC2** → **Launch instance**

| Field                | Value                                          |
| -------------------- | ---------------------------------------------- |
| Name                 | `bigdata-transfer`                             |
| AMI                  | Ubuntu Server 24.04 LTS                        |
| Instance type        | `t2.micro`                                     |
| Key pair             | Create new → `bigdata-key` → `.pem` → Download |
| IAM instance profile | `ec2-s3-role`                                  |
| Storage              | 20GB gp2                                       |

### 3.2 SSH into EC2

```bash
# Fix key permissions (Linux/WSL only)
chmod 400 ~/Downloads/bigdata-key.pem

# Connect
ssh -i ~/Downloads/bigdata-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
```

> On Windows: EC2 Console → select instance → **Connect** → **EC2 Instance Connect**

### 3.3 Install dependencies on EC2

```bash
sudo apt update && sudo apt install -y python3-pip tmux

pip3 install \
  google-cloud-bigquery \
  google-cloud-bigquery-storage \
  google-auth \
  boto3 \
  pandas \
  pyarrow \
  tqdm
```

### 3.4 Verify S3 access

```bash
aws s3 ls s3://yourname-bigdata-project/
```

---

## PART 4 — GCP Setup

### 4.1 Enable BigQuery API

1. Go to `console.cloud.google.com` → create a project
2. Search **BigQuery API** → **Enable**

### 4.2 Create service account

1. **IAM & Admin** → **Service Accounts** → **+ Create Service Account**
2. Name: `bigdata-reader`
3. Attach: `BigQuery Data Viewer` + `BigQuery Job User`

### 4.3 Download JSON key

1. Click `bigdata-reader` → **Keys** → **Add Key** → **JSON** → **Create**
2. Rename downloaded file to `gcp-key.json`

### 4.4 Copy key to EC2

Run from your **local terminal**:

```bash
scp -i ~/Downloads/bigdata-key.pem \
    ~/Downloads/gcp-key.json \
    ubuntu@YOUR_EC2_PUBLIC_IP:/home/ubuntu/gcp-key.json
```

---

## PART 5 — Transfer Data (BigQuery → S3)

### 5.1 Create transfer.py on EC2

```bash
nano transfer.py
```

### 5.2 Run inside tmux

```bash
tmux new -s transfer
python3 transfer.py

# If disconnected, re-attach:
tmux attach -t transfer
```

### 5.3 Verify data landed

```bash
aws s3 ls s3://yourname-bigdata-project/raw/ethereum/ --human-readable
```

### 5.4 Delete GCP key after transfer

```bash
rm /home/ubuntu/gcp-key.json
```

---

## PART 6 — EMR Cluster Setup

### 6.1 Find available instance types

```bash
aws ec2 describe-instance-type-offerings \
  --location-type availability-zone \
  --filters Name=instance-type,Values=m5.xlarge,m4.xlarge,m5.2xlarge \
  --region us-east-1 \
  --query "InstanceTypeOfferings[*].[InstanceType,Location]" \
  --output table
```

### 6.2 Find subnet for your availability zone

```bash
aws ec2 describe-subnets \
  --region us-east-1 \
  --query "Subnets[*].[SubnetId,AvailabilityZone]" \
  --output table
```

Pick the subnet ID matching an availability zone from step 6.1.

### 6.3 Create EMR cluster in console

AWS Console → **EMR** → **Create cluster**:

| Field                 | Value                 |
| --------------------- | --------------------- |
| Name                  | `bigdata-spark`       |
| EMR release           | `emr-7.0.0`           |
| Application           | Spark only            |
| Primary instance type | `m5.xlarge`           |
| Core instance type    | `m5.xlarge`           |
| Core instance count   | `2`                   |
| EC2 subnet            | Subnet from step 6.2  |
| Key pair              | `bigdata-key`         |
| Service role          | `EMR-service-role`    |
| Instance profile      | `EMR_EC2_DefaultRole` |

Wait until cluster status shows **Waiting**.

### 6.4 Get cluster ID

```bash
aws emr list-clusters --active \
  --query "Clusters[*].[Id,Name,Status.State]" \
  --output table
```

---

## PART 7 — Spark Scripts

### 7.1 preprocess.py

```bash
nano preprocess.py
```

### 7.2 peak_hours.py

```bash
nano peak_hours.py
```

### 7.3 congestion_index.py

```bash
nano congestion_index.py
```

### 7.4 ath_analysis.py

```bash
nano ath_analysis.py
```

---

## PART 8 — Upload Scripts and Submit EMR Steps

### 8.1 Upload all scripts to S3

```bash
aws s3 cp preprocess.py       s3://yourname-bigdata-project/scripts/preprocess.py
aws s3 cp peak_hours.py       s3://yourname-bigdata-project/scripts/peak_hours.py
aws s3 cp congestion_index.py s3://yourname-bigdata-project/scripts/congestion_index.py
aws s3 cp ath_analysis.py     s3://yourname-bigdata-project/scripts/ath_analysis.py
```

### 8.2 Submit steps (one at a time, wait for COMPLETED before next)

```bash
# Step 1 — Preprocess
aws emr add-steps \
  --cluster-id YOUR_CLUSTER_ID \
  --steps Type=Spark,Name="Preprocess",ActionOnFailure=CONTINUE,\
Args=[s3://yourname-bigdata-project/scripts/preprocess.py]

# Step 2 — Peak hours
aws emr add-steps \
  --cluster-id YOUR_CLUSTER_ID \
  --steps Type=Spark,Name="PeakHours",ActionOnFailure=CONTINUE,\
Args=[s3://yourname-bigdata-project/scripts/peak_hours.py]

# Step 3 — Congestion index
aws emr add-steps \
  --cluster-id YOUR_CLUSTER_ID \
  --steps Type=Spark,Name="CongestionIndex",ActionOnFailure=CONTINUE,\
Args=[s3://yourname-bigdata-project/scripts/congestion_index.py]

# Step 4 — ATH analysis
aws emr add-steps \
  --cluster-id YOUR_CLUSTER_ID \
  --steps Type=Spark,Name="ATHAnalysis",ActionOnFailure=CONTINUE,\
Args=[s3://yourname-bigdata-project/scripts/ath_analysis.py]
```

### 8.3 Check step status

```bash
# Check all steps
aws emr list-steps \
  --cluster-id YOUR_CLUSTER_ID \
  --query "Steps[*].[Id,Name,Status.State]" \
  --output table

# Check one step
aws emr describe-step \
  --cluster-id YOUR_CLUSTER_ID \
  --step-id YOUR_STEP_ID \
  --query "Step.Status.State"
```

| State       | Meaning                                         |
| ----------- | ----------------------------------------------- |
| `PENDING`   | Waiting — check cluster is in `WAITING` state   |
| `RUNNING`   | Currently executing                             |
| `COMPLETED` | Done — submit next step                         |
| `FAILED`    | Check stderr in EMR console → Steps → Log files |

### 8.4 Verify results

```bash
aws s3 ls s3://yourname-bigdata-project/results/ --recursive --human-readable
```

---

## PART 9 — Download Results to EC2

```bash
mkdir -p ~/results
aws s3 cp s3://yourname-bigdata-project/results/ ~/results/ --recursive
ls ~/results/
```

---

## PART 10 — Cleanup

```bash
# Terminate EMR cluster
aws emr terminate-clusters --cluster-ids YOUR_CLUSTER_ID

# Stop EC2 instance (AWS Console → EC2 → Stop)
# Delete S3 bucket if no longer needed (AWS Console → S3 → Delete)
```

---

## Common Errors

| Error                                    | Fix                                                                 |
| ---------------------------------------- | ------------------------------------------------------------------- |
| `Illegal Parquet type: TIMESTAMP(NANOS)` | Add 3 spark config lines to SparkSession (already in scripts above) |
| `INSTANCE_TYPE_NOT_SUPPORTED`            | Run step 6.1, pick available type, set subnet in step 6.2           |
| `AccessDenied on S3`                     | Add `AmazonS3FullAccess` to `EMR_EC2_DefaultRole`                   |
| `insufficient EC2 permissions`           | Add `AmazonEC2FullAccess` to `EMR-service-role`                     |
| `Step stuck on PENDING`                  | Cluster must be in `WAITING` state first                            |
| `Permission denied (publickey)`          | Run `chmod 400 bigdata-key.pem`                                     |

---

_Last updated: March 2026_
