# Big Data - Ethereum Congestion Analysis

This project analyzes Ethereum congestion (August 2025) with an EMR Spark pipeline and a local Streamlit AI app.

`bigdata-result/` is the local copy of outputs generated from cleaned data processing on Amazon EMR.

## 1) Prerequisites

- Linux/macOS terminal (or WSL on Windows)
- Python 3.12+ (managed by `uv`)
- AWS/GCP credentials only if you want to rerun the full cloud pipeline
- OpenAI API key for the chat feature in `agent.py`

## 2) Install `uv`

Install `uv` using Astral's official installer:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then restart your shell (or source your shell profile) and verify:

```bash
uv --version
```

## 3) Project Setup

From this folder (`Big-Data/`), sync dependencies from `pyproject.toml` and `uv.lock`:

```bash
uv sync
```

This creates `.venv/` and installs all required packages, including:

- `streamlit`
- `pandas`
- `openai`
- `pyspark`
- `boto3`
- `google-cloud-bigquery`

## 4) Configure Environment Variables

Create `.env` in `Big-Data/`:

```env
OPENAI_API_KEY=your_api_key_here
```

`agent.py` loads `.env` automatically from the project folder.

## 5) Data Folder Requirement

Keep the EMR result folder at:

```text
Big-Data/bigdata-result/
```

The app reads `part-*.csv` files recursively from this folder. Current expected structure includes:

- `bigdata-result/peak_hours/...`
- `bigdata-result/congestion_index/...`
- `bigdata-result/ath_analysis/...`

## 6) Run the Streamlit App

Run with `uv` (recommended):

```bash
uv run streamlit run agent.py
```

After startup, open the local URL shown by Streamlit (usually `http://localhost:8501`).

## 7) Optional: Run Full Pipeline Scripts

If you want to regenerate data in AWS/EMR, these are the real scripts in this repo:

- `transfer.py` (BigQuery -> S3 raw parquet)
- `preprocess.py` (clean/enrich parquet)
- `peak_hours.py` (hourly and weekly congestion insights)
- `congestion_index.py` (daily index + distributions)
- `ath_analysis.py` (ATH-day and fee paradox analysis)

## 8) Troubleshooting

- Missing API key error:
  - Ensure `.env` contains `OPENAI_API_KEY=...`
- No data loaded warning:
  - Confirm `bigdata-result/` exists in this folder and contains `part-*.csv` files
- Dependency issues:
  - Re-run `uv sync`

## References

- [uv documentation](https://docs.astral.sh/uv/)
