from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import csv
import random
import statistics as stats

from addition import ROWS_AMOUNT, CATEGORIES, rand_float, Category, ensure_dir


def _safe_stdev(values: list[float]) -> float:
    return stats.stdev(values) if len(values) >= 2 else 0.0


def make_source_csvs(count: int, base_name: str, data_dir: str | Path = "data") -> None:
    data_path = Path(data_dir)
    ensure_dir(data_path)

    for i in range(1, count + 1):
        file_path = data_path / f"{base_name}_{i}.csv"
        with file_path.open(mode="w", newline="") as f:
            writer = csv.writer(f)
            for _ in range(ROWS_AMOUNT):
                category = random.choice(CATEGORIES).value
                writer.writerow([category, rand_float()])


def summarize_file_stats(filename: str, data_dir: str | Path = "data") -> dict[Category, tuple[float, float]]:
    data_path = Path(data_dir) / filename
    grouped: dict[Category, list[float]] = defaultdict(list)

    with data_path.open(encoding="utf-8", mode="r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            cat = Category(row[0].strip())
            value = float(row[1].strip())
            grouped[cat].append(value)

    return {cat: (stats.median(values), _safe_stdev(values)) for cat, values in grouped.items()}


def save_primary_results(
    summaries: list[dict[Category, tuple[float, float]]],
    results_dir: str | Path = "results",
) -> None:
    out_dir = Path(results_dir)
    ensure_dir(out_dir)

    for i, per_file in enumerate(summaries, start=1):
        out_path = out_dir / f"result_{i}.csv"
        with out_path.open(mode="w", newline="") as f:
            writer = csv.writer(f)
            for category, (median, stdev) in per_file.items():
                writer.writerow([category.value, median, stdev])


def combine_medians(result_files_count: int, results_dir: str | Path = "results") -> None:
    results_path = Path(results_dir)
    ensure_dir(results_path)

    medians_by_category: dict[Category, list[float]] = defaultdict(list)

    for i in range(1, result_files_count + 1):
        file_path = results_path / f"result_{i}.csv"
        with file_path.open(encoding="utf-8", mode="r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                cat = Category(row[0].strip())
                medians_by_category[cat].append(float(row[1]))

    final_path = results_path / "RESULT.csv"
    with final_path.open(mode="w", newline="") as f:
        writer = csv.writer(f)
        for cat, values in medians_by_category.items():
            writer.writerow([cat.value, stats.median(values), _safe_stdev(values)])
