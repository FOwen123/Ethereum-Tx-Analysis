# agent.py
# Ethereum Congestion Analysis — AI Agent
# Runs locally, reads downloaded CSV files, answers questions via ChatGPT

import os
import glob
import pandas as pd
import streamlit as st
from pathlib import Path
from openai import OpenAI

# ── Local env loader (.env) ───────────────────────────────────────────────────
def load_local_env(env_file: Path) -> None:
    """Load KEY=VALUE pairs from a local .env file into process env."""
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

# Load .env from the same folder as this script.
PROJECT_DIR = Path(__file__).resolve().parent
load_local_env(PROJECT_DIR / ".env")

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Ethereum Congestion Agent",
    page_icon="⛽",
    layout="wide"
)

# ── Styling ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0f0f0f; }
    .stApp { background-color: #0f0f0f; }
    h1 { color: #00ff88; font-family: monospace; }
    .metric-card {
        background: #1a1a1a;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 16px;
        margin: 4px;
    }
    .stChatMessage { background-color: #1a1a1a; }
</style>
""", unsafe_allow_html=True)

# ── Load data ──────────────────────────────────────────────────────────────────
@st.cache_data
def load_all_data(base_path: str) -> dict[str, pd.DataFrame]:
    """Load all CSV files from the results directory."""
    dataframes = {}
    pattern = os.path.join(base_path, "**", "part-*.csv")
    files = glob.glob(pattern, recursive=True)

    for filepath in files:
        # Use folder name as key
        parts = Path(filepath).parts
        # Find the meaningful folder name (skip 'part-*' file)
        folder = parts[-2]  # e.g. 'hourly_congestion', 'daily_index'
        try:
            df = pd.read_csv(filepath)
            dataframes[folder] = df
            print(f"Loaded {folder}: {len(df)} rows, cols: {list(df.columns)}")
        except Exception as e:
            print(f"Error loading {filepath}: {e}")

    return dataframes

# ── Summarize data for context ─────────────────────────────────────────────────
def build_data_summary(dataframes: dict[str, pd.DataFrame]) -> str:
    """Build a concise summary of all loaded dataframes for the AI context."""
    summary = "AVAILABLE DATASETS:\n\n"
    for name, df in dataframes.items():
        summary += f"Table: {name}\n"
        summary += f"  Rows: {len(df)}\n"
        summary += f"  Columns: {list(df.columns)}\n"
        summary += f"  Sample data:\n{df.head(3).to_string()}\n\n"
    return summary

def get_relevant_data(question: str, dataframes: dict[str, pd.DataFrame]) -> str:
    """Pick the most relevant dataframes based on keywords in the question."""
    question_lower = question.lower()
    relevant = {}

    # Use explicit intent-first routing for common ambiguity.
    if "peak hour" in question_lower:
        matched_tables = {"hourly_congestion"}
    elif "peak day" in question_lower or "most congested day" in question_lower:
        matched_tables = {"daily_index"}
    else:
        matched_tables = set()

    keyword_map = {
        "peak": ["hourly_congestion", "top5_peak_hours", "heatmap_matrix"],
        "hour": ["hourly_congestion", "heatmap_matrix"],
        "heatmap": ["heatmap_matrix"],
        "day": ["daily_index", "daily_congestion", "daily_volume_trend"],
        "daily": ["daily_index", "daily_congestion", "daily_volume_trend"],
        "congestion": ["daily_index", "hourly_congestion", "top_congested_days"],
        "gas": ["hourly_congestion", "daily_index", "gas_distribution", "fee_paradox"],
        "fee": ["fee_paradox", "gas_distribution"],
        "ath": ["ath_day_hourly", "ath_high_value_txns", "daily_volume_trend"],
        "august 24": ["ath_day_hourly", "ath_high_value_txns"],
        "whale": ["ath_day_hourly", "ath_high_value_txns"],
        "week": ["weekly_summary"],
        "trend": ["daily_volume_trend", "weekly_summary", "fee_paradox"],
        "transaction": ["daily_volume_trend", "hourly_congestion", "fee_paradox"],
        "volume": ["daily_volume_trend", "weekly_summary"],
        "worst": ["top_congested_days", "top5_peak_hours"],
        "top": ["top_congested_days", "top5_peak_hours"],
        "distribution": ["gas_distribution"],
        "paradox": ["fee_paradox"],
        "block": ["top_congested_blocks"],
    }

    # Find relevant tables
    for keyword, tables in keyword_map.items():
        if keyword in question_lower:
            matched_tables.update(tables)

    # If no keyword match, include all tables (general question)
    if not matched_tables:
        matched_tables = set(dataframes.keys())

    for table in matched_tables:
        if table in dataframes:
            relevant[table] = dataframes[table]

    # Build context string
    context = ""
    for name, df in relevant.items():
        context += f"\nTable '{name}' ({len(df)} rows):\n"
        context += df.to_string(max_rows=50) + "\n"

    return context

# ── ChatGPT API call ───────────────────────────────────────────────────────────
def ask_chatgpt(
    question: str,
    data_context: str,
    data_summary: str,
    chat_history: list
) -> str:
    """Send question + data context to ChatGPT and get an answer."""
    client = OpenAI(api_key=st.session_state.api_key)

    system_prompt = f"""You are an expert data analyst specializing in Ethereum blockchain network analysis.
You have access to processed Ethereum transaction data from August 2025.

Key context about this dataset:
- Data period: August 2025
- ETH hit its cycle ATH (~$4,897) around August 24, 2025
- A major BTC whale dumped 24k BTC and converted to ETH on August 24
- Ethereum's gas limit was raised from 30M to 45M in June 2025
- Despite record ~1.7M daily transactions, fees were lower due to the gas limit raise
- Gas prices are in gwei units
- day_of_week: 1=Sunday, 2=Monday, 3=Tuesday, 4=Wednesday, 5=Thursday, 6=Friday, 7=Saturday
- hour_of_day is in UTC timezone

{data_summary}

When answering:
- Be specific and reference actual numbers from the data
- Highlight interesting patterns or anomalies
- Connect findings to the ETH ATH event where relevant
- Keep answers concise but insightful
- If asked to compare, show the actual values side by side
- Format numbers clearly (e.g. 1.5 gwei, 450K transactions)
- For "peak hour" questions, use `hourly_congestion` and rank by `congestion_score` (not just tx_count).
- For "peak day" questions, use `daily_index` and rank by `congestion_index`.
- If multiple tables conflict, explain the conflict and name the metric definition you used.

RELEVANT DATA FOR THIS QUESTION:
{data_context}
"""

    messages = []
    # Add full chat history to keep a continuous conversation.
    for msg in chat_history:
        messages.append({"role": msg["role"], "content": msg["content"]})

    # Add current question
    messages.append({"role": "user", "content": question})

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            *messages,
        ],
        max_tokens=1024,
        temperature=0.2,
    )

    return (response.choices[0].message.content or "").strip()

# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("⛽ Ethereum Congestion Analysis Agent")
st.markdown("*Ask questions about Ethereum network congestion in August 2025*")

# Fixed config from local project files.
if "api_key" not in st.session_state:
    st.session_state.api_key = os.getenv("OPENAI_API_KEY", "")
data_path = str(PROJECT_DIR / "bigdata-result")

# Load data
if data_path and os.path.exists(data_path):
    dataframes = load_all_data(data_path)
    data_summary = build_data_summary(dataframes)

    with st.expander("Loaded tables", expanded=False):
        for name in dataframes.keys():
            st.markdown(f"- `{name}` ({len(dataframes[name])} rows)")
else:
    st.warning(f"Data folder not found: `{data_path}`")
    dataframes = {}
    data_summary = ""

# Quick stats row
if dataframes:
    col1, col2, col3, col4 = st.columns(4)
    try:
        daily = dataframes.get("daily_index", pd.DataFrame())
        hourly = dataframes.get("hourly_congestion", pd.DataFrame())

        with col1:
            if "tx_count" in daily.columns:
                st.metric("Total Transactions", f"{daily['tx_count'].sum():,.0f}")
        with col2:
            if "avg_gas_gwei" in hourly.columns:
                st.metric("Avg Gas Price", f"{hourly['avg_gas_gwei'].mean():.2f} gwei")
        with col3:
            if "congestion_index" in daily.columns:
                peak_day = daily.loc[daily["congestion_index"].idxmax(), "date"]
                st.metric("Peak Congestion Day", str(peak_day))
        with col4:
            if "hour_of_day" in hourly.columns and "congestion_score" in hourly.columns:
                peak_hour = hourly.loc[hourly["congestion_score"].idxmax(), "hour_of_day"]
                st.metric("Peak Hour (UTC)", f"{int(peak_hour):02d}:00")
    except Exception:
        pass

st.divider()

# Suggested prompts (main page)
st.markdown("**Suggested questions:**")
suggested_questions = [
    "What was the peak congestion hour in August 2025?",
    "Which day had the highest gas prices?",
    "What happened to gas prices on August 24 (ATH day)?",
    "Show me the fee paradox — high transactions but low fees",
    "Which hours are most congested on weekdays vs weekends?",
    "What were the top 5 most congested days?",
    "How did transaction volume trend over August?",
    "What was the average gas price in gwei?",
]
q_col1, q_col2 = st.columns(2)
for i, q in enumerate(suggested_questions):
    target_col = q_col1 if i % 2 == 0 else q_col2
    with target_col:
        if st.button(q, use_container_width=True, key=f"suggested-{i}"):
            st.session_state.pending_question = q

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Always render chat input so it doesn't disappear after button clicks.
typed_prompt = st.chat_input("Ask about Ethereum congestion in August 2025...")
prompt = typed_prompt
if not prompt and "pending_question" in st.session_state:
    prompt = st.session_state.pending_question
    del st.session_state.pending_question

if prompt:
    if not dataframes:
        st.error("No data loaded — make sure bigdata-result exists in this project folder")
    elif not hasattr(st.session_state, "api_key") or not st.session_state.api_key:
        st.error("OPENAI_API_KEY is missing. Add it to .env and rerun.")
    else:
        # Show user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Get relevant data and ask ChatGPT
        with st.chat_message("assistant"):
            with st.spinner("Analyzing data..."):
                try:
                    data_context = get_relevant_data(prompt, dataframes)
                    response = ask_chatgpt(
                        prompt,
                        data_context,
                        data_summary,
                        st.session_state.messages[:-1]
                    )
                    st.markdown(response)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response
                    })
                except Exception as e:
                    st.error(f"Error: {e}")