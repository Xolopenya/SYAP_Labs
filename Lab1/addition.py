import random
from enum import Enum
from pathlib import Path


class Category(Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"


CATEGORIES = [*Category]
FLOAT_MIN = 1.0
FLOAT_MAX = 100.0
ROWS_AMOUNT = 30


def rand_float() -> float:
    return random.uniform(FLOAT_MIN, FLOAT_MAX)


def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def clear_dir(folder_path: str | Path) -> None:
    folder = Path(folder_path)
    if not folder.is_dir():
        print(f"Папка {folder_path} не существует")
        return

    for entry in folder.iterdir():
        try:
            if entry.is_file():
                entry.unlink()
        except Exception as e:
            print(f"Ошибка при удалении файла {entry}. {e}")
