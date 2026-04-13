from flask import Flask, request, jsonify
from scraper_engine import scrape_all
from scoring_engine import compute_conformity
from db import init_db, save
from excel_processor import enrich_excel
import sqlite3
import os

app = Flask(__name__)
init_db()

@app.route("/scrape", methods=["POST"])
def scrape():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "No JSON received"}), 400

        name = data.get("name", "")
        city = data.get("city", "")

        if not name or not city:
            return jsonify({"error": "Missing name or city"}), 400

        results = scrape_all(name, city)
        result = compute_conformity(results)

        result["name"] = name
        result["city"] = city

        save(result)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/enrich", methods=["POST"])
def enrich():
    input_file = request.json["input"]
    output_file = request.json["output"]

    enrich_excel(input_file, output_file)

    return jsonify({"status": "done"})

@app.route("/stats")
def stats():
    conn = sqlite3.connect("data.db")
    c = conn.cursor()

    total = c.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    avg = c.execute("SELECT AVG(confidence) FROM results").fetchone()[0]

    return jsonify({
        "total": total,
        "avg_confidence": round(avg or 0, 1)
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
