from __future__ import annotations

import csv
import math
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEEP_LEARNING_DIR = Path(__file__).resolve().parent
PEAK_HOURS_DIR = REPO_ROOT / "bigdata-result" / "peak_hours"
OUTPUT_PATH = DEEP_LEARNING_DIR / "model_input" / "task5_features.csv"


def load_part_csv(directory: Path) -> list[dict[str, str]]:
    part_files = sorted(directory.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No part-*.csv files found in {directory}")

    rows: list[dict[str, str]] = []
    for path in part_files:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows.extend(reader)
    return rows


def require_columns(rows: list[dict[str, str]], required: list[str], label: str) -> None:
    if not rows:
        raise ValueError(f"{label} is empty")
    available = set(rows[0].keys())
    missing = [column for column in required if column not in available]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def parse_int(value: str, column: str) -> int:
    try:
        return int(float(value))
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {column}: {value}") from exc


def parse_float(value: str, column: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {column}: {value}") from exc


def assert_unique_keys(
    rows: list[dict[str, object]], keys: list[str], label: str
) -> None:
    seen: set[tuple[object, ...]] = set()
    duplicates: list[dict[str, object]] = []

    for row in rows:
        key = tuple(row[column] for column in keys)
        if key in seen:
            duplicates.append({column: row[column] for column in keys})
        seen.add(key)

    if duplicates:
        raise ValueError(f"{label} has duplicate keys on {keys}: {duplicates[:10]}")


def normalize_heatmap(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                "hour_of_day": parse_int(row["hour_of_day"], "hour_of_day"),
                "day_of_week": parse_int(row["day_of_week"], "day_of_week"),
                "target_avg_gas_gwei": parse_float(
                    row["avg_gas_gwei"], "avg_gas_gwei"
                ),
                "cell_tx_count": parse_int(row["tx_count"], "tx_count"),
            }
        )
    return normalized


def normalize_hourly(rows: list[dict[str, str]]) -> dict[int, dict[str, object]]:
    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        normalized_rows.append(
            {
                "hour_of_day": parse_int(row["hour_of_day"], "hour_of_day"),
                "hour_tx_count": parse_int(row["tx_count"], "tx_count"),
                "hour_p95_gas_gwei": parse_float(
                    row["p95_gas_gwei"], "p95_gas_gwei"
                ),
                "hour_avg_tx_cost_gwei": parse_float(
                    row["avg_tx_cost_gwei"], "avg_tx_cost_gwei"
                ),
                "hour_total_gas_used": parse_float(
                    row["total_gas_used"], "total_gas_used"
                ),
            }
        )
    assert_unique_keys(normalized_rows, ["hour_of_day"], "hourly_congestion")
    return {int(row["hour_of_day"]): row for row in normalized_rows}


def normalize_daily(rows: list[dict[str, str]]) -> dict[int, dict[str, object]]:
    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        normalized_rows.append(
            {
                "day_of_week": parse_int(row["day_of_week"], "day_of_week"),
                "day_tx_count": parse_int(row["tx_count"], "tx_count"),
                "day_avg_tx_cost_gwei": parse_float(
                    row["avg_tx_cost_gwei"], "avg_tx_cost_gwei"
                ),
            }
        )
    assert_unique_keys(normalized_rows, ["day_of_week"], "daily_congestion")
    return {int(row["day_of_week"]): row for row in normalized_rows}


def format_value(value: object) -> str:
    if isinstance(value, float):
        return repr(value)
    return str(value)


def main() -> None:
    heatmap_raw = load_part_csv(PEAK_HOURS_DIR / "heatmap_matrix")
    hourly_raw = load_part_csv(PEAK_HOURS_DIR / "hourly_congestion")
    daily_raw = load_part_csv(PEAK_HOURS_DIR / "daily_congestion")

    require_columns(
        heatmap_raw,
        ["hour_of_day", "day_of_week", "avg_gas_gwei", "tx_count"],
        "heatmap_matrix",
    )
    require_columns(
        hourly_raw,
        [
            "hour_of_day",
            "tx_count",
            "p95_gas_gwei",
            "avg_tx_cost_gwei",
            "total_gas_used",
        ],
        "hourly_congestion",
    )
    require_columns(
        daily_raw,
        ["day_of_week", "tx_count", "avg_tx_cost_gwei"],
        "daily_congestion",
    )

    heatmap = normalize_heatmap(heatmap_raw)
    hourly = normalize_hourly(hourly_raw)
    daily = normalize_daily(daily_raw)

    assert_unique_keys(heatmap, ["hour_of_day", "day_of_week"], "heatmap_matrix")

    expected_heatmap_rows = 24 * 7
    if len(heatmap) != expected_heatmap_rows:
        raise ValueError(f"Expected {expected_heatmap_rows} heatmap rows, found {len(heatmap)}")

    merged_rows: list[dict[str, object]] = []
    day_columns = [f"day_{value}" for value in range(1, 8)]

    for row in sorted(heatmap, key=lambda item: (item["day_of_week"], item["hour_of_day"])):
        hour_key = int(row["hour_of_day"])
        day_key = int(row["day_of_week"])

        if hour_key not in hourly:
            raise ValueError(f"Missing hourly_congestion row for hour_of_day={hour_key}")
        if day_key not in daily:
            raise ValueError(f"Missing daily_congestion row for day_of_week={day_key}")

        merged = {
            "hour_of_day": hour_key,
            "day_of_week": day_key,
            "target_avg_gas_gwei": row["target_avg_gas_gwei"],
            "cell_tx_count": row["cell_tx_count"],
            "hour_tx_count": hourly[hour_key]["hour_tx_count"],
            "hour_p95_gas_gwei": hourly[hour_key]["hour_p95_gas_gwei"],
            "hour_avg_tx_cost_gwei": hourly[hour_key]["hour_avg_tx_cost_gwei"],
            "hour_total_gas_used": hourly[hour_key]["hour_total_gas_used"],
            "day_tx_count": daily[day_key]["day_tx_count"],
            "day_avg_tx_cost_gwei": daily[day_key]["day_avg_tx_cost_gwei"],
            "hour_sin": math.sin(2 * math.pi * hour_key / 24.0),
            "hour_cos": math.cos(2 * math.pi * hour_key / 24.0),
        }

        for column in day_columns:
            merged[column] = 1 if column == f"day_{day_key}" else 0

        merged_rows.append(merged)

    required_model_columns = [
        "hour_of_day",
        "day_of_week",
        "target_avg_gas_gwei",
        "cell_tx_count",
        "hour_tx_count",
        "hour_p95_gas_gwei",
        "hour_avg_tx_cost_gwei",
        "hour_total_gas_used",
        "day_tx_count",
        "day_avg_tx_cost_gwei",
        "hour_sin",
        "hour_cos",
        *day_columns,
    ]

    for row in merged_rows:
        for column in required_model_columns:
            if column not in row:
                raise ValueError(f"Missing column in merged row: {column}")
            if row[column] is None:
                raise ValueError(f"Null value found in merged row column: {column}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=required_model_columns)
        writer.writeheader()
        for row in merged_rows:
            writer.writerow({key: format_value(value) for key, value in row.items()})

    print(f"Saved merged dataset to {OUTPUT_PATH}")
    print(f"Rows: {len(merged_rows)}")
    print(f"Columns: {len(required_model_columns)}")
    print("Columns:")
    for column in required_model_columns:
        print(f" - {column}")


if __name__ == "__main__":
    main()
