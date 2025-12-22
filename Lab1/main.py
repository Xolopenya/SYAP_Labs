import csv
from concurrent.futures import ProcessPoolExecutor as Pool

from addition import clear_dir, ensure_dir
from process import make_source_csvs, summarize_file_stats, save_primary_results, combine_medians


def run() -> None:
    print("Подготовка папок data/ и results/...")

    ensure_dir("data")
    ensure_dir("results")
    clear_dir("data")
    clear_dir("results")

    base_name = input("Введите базовое имя файлов (без .csv), например test: ").strip()
    file_count = int(input("Введите количество генерируемых файлов: ").strip())

    print(f"Генерация {file_count} файлов в папку ./data ...")
    make_source_csvs(count=file_count, base_name=base_name)

    input_files = [f"{base_name}_{i}.csv" for i in range(1, file_count + 1)]

    print("Параллельная обработка файлов (медиана и стандартное отклонение по категориям)...")
    with Pool() as executor:
        summaries = list(executor.map(summarize_file_stats, input_files))

    print("Сохранение результатов первичной обработки в ./results/result_*.csv ...")
    save_primary_results(summaries)

    print("Вторичная обработка: агрегирование медиан в ./results/RESULT.csv ...")
    combine_medians(result_files_count=file_count)

    print("Готово. Итоговый файл: ./results/RESULT.csv")


if __name__ == "__main__":
    run()
