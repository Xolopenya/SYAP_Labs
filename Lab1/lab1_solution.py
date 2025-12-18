import os
import csv
import random
import statistics
from concurrent.futures import ProcessPoolExecutor

LETTERS = ["A", "B", "C", "D"]
FILES_COUNT = 5
ROWS_PER_FILE = 50
VALUE_MIN = 1.0
VALUE_MAX = 100.0

DATA_DIR = "data"
OUT_DIR = "results"


def ensure_empty_dir(folder):
    os.makedirs(folder, exist_ok=True)
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            os.remove(path)


def stdev_or_zero(values):
    return statistics.stdev(values) if len(values) >= 2 else 0.0  # иначе StatisticsError [web:4]


def generate_files():
    paths = []
    for i in range(1, FILES_COUNT + 1):
        path = os.path.join(DATA_DIR, f"sample_{i}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for _ in range(ROWS_PER_FILE):
                w.writerow([random.choice(LETTERS), random.uniform(VALUE_MIN, VALUE_MAX)])
        paths.append(path)
    return paths


def compute_stats_for_file(path):
    groups = {}  # letter -> list[float]

    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        for row in r:
            if not row:
                continue
            letter = row[0].strip()
            value = float(row[1])
            if letter in LETTERS:
                groups.setdefault(letter, []).append(value)

    result = {}  # letter -> (median, stdev)
    for letter, values in groups.items():
        result[letter] = (statistics.median(values), stdev_or_zero(values))
    return result


def save_primary(out_path, stats_map):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for letter in LETTERS:
            if letter in stats_map:
                med, sd = stats_map[letter]
                w.writerow([letter, med, sd])


def aggregate(primary_paths, final_path):
    medians = {}  # letter -> list[median]

    for p in primary_paths:
        with open(p, "r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            for row in r:
                if not row:
                    continue
                letter = row[0].strip()
                median_value = float(row[1])
                if letter in LETTERS:
                    medians.setdefault(letter, []).append(median_value)

    with open(final_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for letter in LETTERS:
            vals = medians.get(letter, [])
            if not vals:
                continue
            w.writerow([letter, statistics.median(vals), stdev_or_zero(vals)])


def main():
    ensure_empty_dir(DATA_DIR)
    ensure_empty_dir(OUT_DIR)

    sources = generate_files()

    # Параллельная обработка файлов [web:24]
    with ProcessPoolExecutor() as ex:
        computed = list(ex.map(compute_stats_for_file, sources))

    primary_paths = []
    for i, stats_map in enumerate(computed, start=1):
        out_path = os.path.join(OUT_DIR, f"result_{i}.csv")
        save_primary(out_path, stats_map)
        primary_paths.append(out_path)

    aggregate(primary_paths, os.path.join(OUT_DIR, "FINAL.csv"))


if __name__ == "__main__":
    main()
