from flask import Flask, render_template, request, jsonify
import json
import logging
import io
import csv

from kafka_producer import send_to_kafka
from validators import validate_data, validate_table_structure

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/send", methods=["POST"])
def send_data():
    try:
        data = request.get_json()

        validation_error = validate_data(data)
        if validation_error:
            return jsonify({"success": False, "error": validation_error}), 400

        table_error = validate_table_structure(data["table"], data["columns"])
        if table_error:
            return jsonify({"success": False, "error": table_error}), 400

        success, message = send_to_kafka(data)

        if success:
            logger.info(f"Успешно отправлено: {data['table']}")
            return jsonify(
                {
                    "success": True,
                    "message": f" Таблица '{data['table']}' отправлена в Kafka ({len(data['rows'])} строк)",
                }
            ), 200

        logger.error(f"Ошибка отправки: {message}")
        return jsonify({"success": False, "error": message}), 500

    except Exception as e:
        logger.exception("Ошибка /api/send")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/send-csv", methods=["POST"])
def send_csv():
    try:
        table = (request.form.get("table") or "").strip()
        f = request.files.get("file")

        if not table:
            return jsonify({"success": False, "error": "Название таблицы не указано"}), 400
        if not f:
            return jsonify({"success": False, "error": "CSV файл не выбран"}), 400

        # убирает BOM, который добавляет Excel
        content = f.stream.read().decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(content))

        if not reader.fieldnames:
            return jsonify({"success": False, "error": "Не найдены заголовки CSV"}), 400

        columns = []
        for name in reader.fieldnames:
            name = (name or "").strip()
            if not name:
                continue
            col_type = "INTEGER" if name.lower() in ("id",) else "TEXT"
            columns.append({"name": name, "type": col_type})

        rows = []
        for r in reader:
            row_obj = {}
            for col in columns:
                key = col["name"]
                raw = r.get(key)
                val = (raw if raw is not None else "").strip()

                if col["type"] == "INTEGER":
                    row_obj[key] = int(val) if val != "" else None
                else:
                    row_obj[key] = val

            if any(v not in (None, "") for v in row_obj.values()):
                rows.append(row_obj)

        payload = {"table": table, "columns": columns, "rows": rows}

        validation_error = validate_data(payload)
        if validation_error:
            return jsonify({"success": False, "error": validation_error}), 400

        table_error = validate_table_structure(payload["table"], payload["columns"])
        if table_error:
            return jsonify({"success": False, "error": table_error}), 400

        ok, msg = send_to_kafka(payload)
        if not ok:
            return jsonify({"success": False, "error": msg}), 500

        logger.info(f"Успешно отправлено (CSV): {table}, rows={len(rows)}")
        return jsonify({"success": True, "message": f"CSV отправлен: {table} ({len(rows)} строк)"}), 200

    except Exception as e:
        logger.exception("Ошибка /api/send-csv")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/validate-json", methods=["POST"])
def validate_json():
    try:
        json_str = request.get_json().get("json", "")
        json.loads(json_str)
        return jsonify({"valid": True}), 200
    except Exception:
        return jsonify({"valid": False, "error": "Некорректный JSON"}), 400


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    print("Producer подключен к Kafka: localhost:9092")
    print("Запуск Flask приложения...")
    print("Откройте браузер на http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
