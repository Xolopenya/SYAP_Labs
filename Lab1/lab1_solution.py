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


def clear_folder(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            os.remove(path)


def safe_stdev(values):
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values)


def generate_files():
    paths = []

    for i in range(1, FILES_COUNT + 1):
        path = os.path.join(DATA_DIR, "sample_" + str(i) + ".csv")
        f = open(path, "w", encoding="utf-8", newline="")
        w = csv.writer(f)

        for _ in range(ROWS_PER_FILE):
            letter = random.choice(LETTERS)
            value = random.uniform(VALUE_MIN, VALUE_MAX)
            w.writerow([letter, value])

        f.close()
        paths.append(path)

    return paths


def process_file(src_path, out_path):
    groups = {}

    f = open(src_path, "r", encoding="utf-8", newline="")
    r = csv.reader(f)

    for row in r:
        if not row:
            continue

        letter = row[0].strip()
        if letter not in LETTERS:
            continue

        try:
            value = float(row[1])
        except:
            continue

        if letter not in groups:
            groups[letter] = []
        groups[letter].append(value)

    f.close()

    out = open(out_path, "w", encoding="utf-8", newline="")
    w = csv.writer(out)

    for letter in LETTERS:
        if letter in groups and len(groups[letter]) > 0:
            med = statistics.median(groups[letter])
            sd = safe_stdev(groups[letter])
            w.writerow([letter, med, sd])

    out.close()


def process_file_task(task):
    # task = (src_path, out_path)
    process_file(task[0], task[1])
    return task[1]  # вернём путь к результату


def aggregate(result_paths, final_path):
    medians = {}

    for p in result_paths:
        f = open(p, "r", encoding="utf-8", newline="")
        r = csv.reader(f)

        for row in r:
            if not row:
                continue

            letter = row[0].strip()
            if letter not in LETTERS:
                continue

            try:
                median_value = float(row[1])
            except:
                continue

            if letter not in medians:
                medians[letter] = []
            medians[letter].append(median_value)

        f.close()

    out = open(final_path, "w", encoding="utf-8", newline="")
    w = csv.writer(out)

    for letter in LETTERS:
        if letter in medians and len(medians[letter]) > 0:
            med = statistics.median(medians[letter])
            sd = safe_stdev(medians[letter])
            w.writerow([letter, med, sd])

    out.close()


def main():
    clear_folder(DATA_DIR)
    clear_folder(OUT_DIR)

    sources = generate_files()

    tasks = []
    for i, src in enumerate(sources, start=1):
        out_path = os.path.join(OUT_DIR, "result_" + str(i) + ".csv")
        tasks.append((src, out_path))

    with ProcessPoolExecutor() as ex:
        result_paths = list(ex.map(process_file_task, tasks))

    aggregate(result_paths, os.path.join(OUT_DIR, "FINAL.csv"))


if __name__ == "__main__":
    main()
