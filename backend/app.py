"""
SyndicPro Scanner — API Backend
Recherche automatique de contacts pour syndicats tunisiens.
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from scraper_engine import scrape_all
from scoring_engine import compute_conformity
from db import init_db, save, get_all, get_stats, delete_all
from excel_processor import enrich_excel
import os
import io
import csv

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

init_db()

API_KEY = os.environ.get("API_KEY", "")  # optionnel — laisser vide pour désactiver


def check_key():
    """Vérifie la clé API si configurée."""
    if not API_KEY:
        return True
    return request.headers.get("X-Api-Key") == API_KEY


# ── Recherche d'un syndic ──────────────────────────────────────────────────────

@app.route("/scrape", methods=["POST"])
def scrape():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401

    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    city = (data.get("city") or "").strip()

    if not name or not city:
        return jsonify({"error": "Les champs 'name' et 'city' sont obligatoires"}), 400

    try:
        raw_results = scrape_all(name, city)
        result = compute_conformity(raw_results)
        result["name"] = name
        result["city"] = city
        save(result)
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"Erreur scrape({name}, {city}): {e}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


# ── Statistiques ───────────────────────────────────────────────────────────────

@app.route("/stats")
def stats():
    try:
        return jsonify(get_stats())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Liste des résultats ────────────────────────────────────────────────────────

@app.route("/results")
def results():
    try:
        limit      = min(int(request.args.get("limit", 200)), 500)
        offset     = int(request.args.get("offset", 0))
        only_found = request.args.get("found", "0") == "1"
        rows = get_all(limit=limit, offset=offset, only_found=only_found)
        return jsonify({"results": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Export CSV ─────────────────────────────────────────────────────────────────

@app.route("/export/csv")
def export_csv():
    try:
        rows = get_all(limit=5000)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            "id", "name", "city", "phone", "email", "website",
            "all_phones", "all_emails", "confidence", "sources_hit", "created_at"
        ])
        writer.writeheader()
        writer.writerows(rows)
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8-sig")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="syndicats_contacts.csv"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Enrichissement Excel ───────────────────────────────────────────────────────

@app.route("/enrich", methods=["POST"])
def enrich():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401

    if "file" not in request.files:
        return jsonify({"error": "Fichier Excel manquant (champ 'file')"}), 400

    uploaded = request.files["file"]
    if not uploaded.filename.endswith((".xlsx", ".xls")):
        return jsonify({"error": "Format non supporté. Utilisez .xlsx ou .xls"}), 400

    try:
        result_bytes = enrich_excel(uploaded)
        return send_file(
            io.BytesIO(result_bytes),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="syndicats_enrichis.xlsx"
        )
    except Exception as e:
        app.logger.error(f"Erreur enrich: {e}")
        return jsonify({"error": f"Erreur traitement Excel: {str(e)}"}), 500


# ── Supprimer tous les résultats ───────────────────────────────────────────────

@app.route("/results/clear", methods=["POST"])
def clear_results():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    delete_all()
    return jsonify({"status": "ok", "message": "Base de données vidée"})


# ── Health check ───────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "2.0"})


# ── Frontend pages ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/dashboard")
def dashboard_page():
    return app.send_static_file("dashboard.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
