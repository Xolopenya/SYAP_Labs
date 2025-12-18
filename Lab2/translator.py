import re

def _translate_condition(expr: str) -> str:
    expr = expr.replace("&&", "and").replace("||", "or")
    expr = re.sub(r'\btrue\b', "True", expr)
    expr = re.sub(r'\bfalse\b', "False", expr)
    expr = re.sub(r'!\s*', 'not ', expr)
    return expr

def _translate_cout(payload: str) -> str:
    # cout << "a=" << a << endl;  -> print("a=", a)
    payload = payload.replace("endl", "")
    parts = [p.strip() for p in payload.split("<<") if p.strip()]
    return "print(" + ", ".join(parts) + ")" if parts else "print()"

PATTERNS = [
    # include / using namespace
    (re.compile(r'^\s*#include\s+<.*>\s*$'), lambda m, s: ""),
    (re.compile(r'^\s*using\s+namespace\s+std\s*;\s*$'), lambda m, s: ""),

    # int main() { to def main():
    (re.compile(r'^\s*int\s+main\s*\(\s*\)\s*\{\s*$'), lambda m, s: "def main():"),

    # Функции без аргументов
    (re.compile(r'^\s*(?:int|double|bool|string|void)\s+([A-Za-z_]\w*)\s*\(\s*\)\s*\{\s*$'),
     lambda m, s: f"def {m.group(1)}():"),

    # return 0
    (re.compile(r'^\s*return\s+0\s*;\s*$'), lambda m, s: "return"),

    # return expr
    (re.compile(r'^\s*return\s+(.+?)\s*;\s*$'),
     lambda m, s: f"return {m.group(1).strip()}"),

    # cout << ...
    (re.compile(r'^\s*cout\s*<<\s*(.+?)\s*;\s*$'), lambda m, s: _translate_cout(m.group(1))),

    # cin >> x
    (re.compile(r'^\s*cin\s*>>\s*([A-Za-z_]\w*)\s*;\s*$'), lambda m, s: f"{m.group(1)} = int(input())"),

    # for (int i = 0; i < N; i++) {
    (re.compile(
        r'^\s*for\s*\(\s*int\s+([A-Za-z_]\w*)\s*=\s*0\s*;\s*\1\s*<\s*(.+?)\s*;\s*\1\+\+\s*\)\s*\{\s*$'
    ), lambda m, s: f"for {m.group(1)} in range({m.group(2).strip()}):"),

    # if (...) { / else {
    (re.compile(r'^\s*if\s*\(\s*(.+?)\s*\)\s*\{\s*$'), lambda m, s: f"if {_translate_condition(m.group(1))}:"),
    (re.compile(r'^\s*else\s*\{\s*$'), lambda m, s: "else:"),

    # Объявления переменных с инициализацией
    (re.compile(r'^\s*int\s+([A-Za-z_]\w*)\s*=\s*(.+?)\s*;\s*$'), lambda m, s: f"{m.group(1)} = {m.group(2).strip()}"),
    (re.compile(r'^\s*double\s+([A-Za-z_]\w*)\s*=\s*(.+?)\s*;\s*$'), lambda m, s: f"{m.group(1)} = {m.group(2).strip()}"),
    (re.compile(r'^\s*bool\s+([A-Za-z_]\w*)\s*=\s*(true|false)\s*;\s*$'), lambda m, s: f"{m.group(1)} = {m.group(2) == 'true'}"),
    (re.compile(r'^\s*string\s+([A-Za-z_]\w*)\s*=\s*(.+?)\s*;\s*$'), lambda m, s: f"{m.group(1)} = {m.group(2).strip()}"),

    # Объявления переменных без инициализации
    (re.compile(r'^\s*int\s+([A-Za-z_]\w*)\s*;\s*$'), lambda m, s: f"{m.group(1)} = 0"),
    (re.compile(r'^\s*double\s+([A-Za-z_]\w*)\s*;\s*$'), lambda m, s: f"{m.group(1)} = 0.0"),
    (re.compile(r'^\s*bool\s+([A-Za-z_]\w*)\s*;\s*$'), lambda m, s: f"{m.group(1)} = False"),
    (re.compile(r'^\s*string\s+([A-Za-z_]\w*)\s*;\s*$'), lambda m, s: f"{m.group(1)} = ''"),

    # i++ i--
    (re.compile(r'^\s*([A-Za-z_]\w*)\s*\+\+\s*;\s*$'), lambda m, s: f"{m.group(1)} += 1"),
    (re.compile(r'^\s*([A-Za-z_]\w*)\s*\-\-\s*;\s*$'), lambda m, s: f"{m.group(1)} -= 1"),

    # Вызов функции без аргументов
    (re.compile(r'^\s*([A-Za-z_]\w*)\s*\(\s*\)\s*;\s*$'), lambda m, s: f"{m.group(1)}()"),

    # присваивание x = ...
    (re.compile(r'^\s*([A-Za-z_]\w*)\s*=\s*(.+?)\s*;\s*$'), lambda m, s: f"{m.group(1)} = {m.group(2).strip()}"),
]


def _post_patch(line: str) -> str:
    # true/false в присваиваниях/любых выражениях
    line = re.sub(r'\btrue\b', 'True', line)
    line = re.sub(r'\bfalse\b', 'False', line)

    # (double)x to float(x)
    line = re.sub(r'\(double\)\s*([A-Za-z_]\w*)', r'float(\1)', line)

    # (double) n
    line = re.sub(r'\(double\)\s*\(\s*([A-Za-z_]\w*)\s*\)', r'float(\1)', line)

    return line


def convert_cpp_to_python(text: str) -> str:
    out_lines: list[str] = []
    indent = 0

    for raw in text.splitlines():
        src = raw.rstrip()

        if re.match(r'^\s*}\s*$', src):
            indent = max(0, indent - 1)
            continue

        converted = None
        for rx, fn in PATTERNS:
            m = rx.match(src)
            if m:
                converted = fn(m, src)
                break

        if converted is None:
            if src.strip() == "":
                converted = ""
            elif src.strip().startswith("//"):
                converted = "# " + src.strip()[2:].strip()
            else:
                converted = "# TODO: unsupported: " + src.strip()

        converted = _post_patch(converted)

        out_lines.append(("    " * indent + converted) if converted != "" else "")

        if re.search(r'\{\s*$', src) and converted.rstrip().endswith(":"):
            indent += 1

    # Авто-вызов main
    out_lines.append("")
    out_lines.append("if __name__ == '__main__':")
    out_lines.append("    main()")
    return "\n".join(out_lines) + "\n"


def translate(src_path: str, dst_path: str) -> None:
    with open(src_path, "r", encoding="utf-8") as f:
        cpp_code = f.read()

    py_code = convert_cpp_to_python(cpp_code)

    with open(dst_path, "w", encoding="utf-8") as f:
        f.write(py_code)
