from flask import Flask, request, jsonify
import asyncio
from scraper_engine import scrape_all
from scoring_engine import compute_conformity
from db import init_db, save
from excel_processor import enrich_excel

app = Flask(__name__)
init_db()

@app.route("/scrape", methods=["POST"])
def scrape():
    data = request.json
    name = data["name"]
    city = data["city"]

    results = asyncio.run(scrape_all(name, city))
    result = compute_conformity(results)

    result["name"] = name
    result["city"] = city

    save(result)

    return jsonify(result)

@app.route("/enrich", methods=["POST"])
def enrich():
    input_file = request.json["input"]
    output_file = request.json["output"]

    asyncio.run(enrich_excel(input_file, output_file))

    return jsonify({"status": "done"})

@app.route("/stats")
def stats():
    import sqlite3
    conn = sqlite3.connect("data.db")
    c = conn.cursor()

    total = c.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    avg = c.execute("SELECT AVG(confidence) FROM results").fetchone()[0]

    return jsonify({
        "total": total,
        "avg_confidence": round(avg or 0, 1)
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
