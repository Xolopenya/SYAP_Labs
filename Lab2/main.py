import sys
from translator import translate


def main() -> None:
    default_inputs = ["./script_1.cpp", "./script_2.cpp", "./script_3.cpp"]

    if len(sys.argv) == 1:
        for src in default_inputs:
            translate(src, f"{src}__OUTPUT.py")
        return

    for src in sys.argv[1:]:
        translate(src, f"{src}__OUTPUT.py")


if __name__ == "__main__":
    main()
