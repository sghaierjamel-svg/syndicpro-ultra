"""
SyndicPro Scanner — API Backend v4
Nouveautés : enrichissement Excel asynchrone, cache, context générique.
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from scraper_engine import scrape_all, get_rne_candidates
from scoring_engine import compute_conformity
from db import (init_db, save, get_all, count_all, get_stats, delete_all,
                seed_from_list, set_cache, job_create, job_update, job_get,
                get_result, update_result, invalidate_cache)
from excel_processor import enrich_excel
import os
import io
import csv
import base64
import uuid
import threading
import logging
import openpyxl
import traceback

app    = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)
logging.basicConfig(level=logging.INFO)

init_db()

API_KEY = os.environ.get("API_KEY", "")


def check_key():
    if not API_KEY:
        return True
    return request.headers.get("X-Api-Key") == API_KEY


# ── Recherche ─────────────────────────────────────────────────────────────────

@app.route("/scrape", methods=["POST"])
def scrape():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401

    body    = request.get_json(silent=True) or {}
    name    = (body.get("name")    or "").strip()
    city    = (body.get("city")    or "").strip()
    rne_id  = (body.get("rne_id")  or "").strip()
    context = (body.get("context") or "").strip()

    if not name or not city:
        return jsonify({"error": "Les champs 'name' et 'city' sont obligatoires"}), 400

    try:
        raw        = scrape_all(name, city, rne_id=rne_id, context=context)
        from_cache = len(raw) == 1 and raw[0].get("from_cache", False)
        result     = compute_conformity(raw)
        result["name"] = name
        result["city"] = city
        if from_cache:
            result["from_cache"] = True
        save(result)
        # Mettre en cache si des contacts ont été trouvés
        if result.get("found") or result.get("president"):
            set_cache(name, city, result)
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"Erreur scrape({name},{city}): {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


# ── Candidats RNE (sélection manuelle) ────────────────────────────────────────

@app.route("/rne/candidates")
def rne_candidates():
    name = (request.args.get("name") or "").strip()
    city = (request.args.get("city") or "").strip()
    if not name or not city:
        return jsonify({"error": "Paramètres 'name' et 'city' requis"}), 400
    try:
        candidates = get_rne_candidates(name, city)
        return jsonify({"candidates": candidates})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Stats ──────────────────────────────────────────────────────────────────────

@app.route("/stats")
def stats():
    try:
        return jsonify(get_stats())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Résultats ──────────────────────────────────────────────────────────────────

@app.route("/results")
def results():
    try:
        limit      = min(int(request.args.get("limit", 200)), 500)
        offset     = int(request.args.get("offset", 0))
        only_found = request.args.get("found", "0") == "1"
        rows  = get_all(limit=limit, offset=offset, only_found=only_found)
        total = count_all(only_found=only_found)
        return jsonify({"results": rows, "count": total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Modifier un résultat manuellement ─────────────────────────────────────────

@app.route("/results/<int:row_id>", methods=["PUT"])
def update_result_route(row_id):
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    body = request.get_json(silent=True) or {}
    update_result(row_id, **body)
    return jsonify({"status": "ok"})


@app.route("/results/<int:row_id>", methods=["GET"])
def get_result_route(row_id):
    row = get_result(row_id)
    if not row:
        return jsonify({"error": "Résultat introuvable"}), 404
    return jsonify(row)


@app.route("/results/<int:row_id>/rescrape", methods=["POST"])
def rescrape_result(row_id):
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    row = get_result(row_id)
    if not row:
        return jsonify({"error": "Résultat introuvable"}), 404
    name   = row["name"]
    city   = row["city"]
    rne_id = row.get("rne_id", "")
    invalidate_cache(name, city)
    try:
        raw    = scrape_all(name, city, rne_id=rne_id, context="")
        result = compute_conformity(raw)
        result["name"] = name
        result["city"] = city
        save(result)
        if result.get("found") or result.get("president"):
            set_cache(name, city, result)
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"Erreur rescrape({name},{city}): {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


# ── Export CSV ─────────────────────────────────────────────────────────────────

@app.route("/export/csv")
def export_csv():
    try:
        rows = get_all(limit=5000)
        out  = io.StringIO()
        w    = csv.DictWriter(out, fieldnames=[
            "id","name","city","phone","email","website",
            "all_phones","all_emails","confidence","sources_hit",
            "president","address","created_at"
        ], extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
        out.seek(0)
        return send_file(
            io.BytesIO(out.getvalue().encode("utf-8-sig")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="contacts.csv"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Export Excel ──────────────────────────────────────────────────────────────

@app.route("/export/excel")
def export_excel_db():
    try:
        only_found = request.args.get("found", "0") == "1"
        rows = get_all(limit=5000, only_found=only_found)
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Contacts"

        headers = ["ID", "Nom Résidence", "Ville", "Téléphone", "Email", "Site Web",
                   "Président / Gérant", "Adresse", "Tous les tél.", "Tous les emails",
                   "Confiance (%)", "Sources", "Vérifié", "Notes", "Date"]
        ws.append(headers)

        # Style entête
        from openpyxl.styles import Font, PatternFill, Alignment
        for cell in ws[1]:
            cell.font      = Font(bold=True, color="FFFFFF")
            cell.fill      = PatternFill("solid", fgColor="1E40AF")
            cell.alignment = Alignment(horizontal="center")

        for r in rows:
            members_str = ""
            if r.get("members"):
                members_str = ", ".join(
                    f"{m.get('nom','')} ({m.get('qualite','')})"
                    for m in r["members"]
                )
            ws.append([
                r.get("id", ""),
                r.get("name", ""),
                r.get("city", ""),
                r.get("phone", ""),
                r.get("email", ""),
                r.get("website", ""),
                members_str or r.get("president", ""),
                r.get("address", ""),
                r.get("all_phones", ""),
                r.get("all_emails", ""),
                r.get("confidence", 0),
                r.get("sources_hit", ""),
                "Oui" if r.get("verified") else "",
                r.get("notes", ""),
                r.get("created_at", "")[:16] if r.get("created_at") else "",
            ])

        # Ajuster largeur colonnes
        for col in ws.columns:
            max_len = max((len(str(cell.value or "")) for cell in col), default=0)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 45)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return send_file(
            buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="contacts_syndicpro.xlsx"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Enrichissement Excel ASYNCHRONE ───────────────────────────────────────────

def _run_excel_job(job_id: str, file_bytes: bytes, context: str):
    """Traitement en arrière-plan — s'exécute dans un thread daemon."""
    try:
        def progress(cur, total):
            job_update(job_id, status="running", progress=cur, total=total)

        result_bytes = enrich_excel(
            io.BytesIO(file_bytes),
            progress_callback=progress,
            context=context
        )
        b64 = base64.b64encode(result_bytes).decode()
        job_update(job_id, status="done", result_b64=b64)
    except Exception as e:
        job_update(job_id, status="error", error=str(e))


@app.route("/enrich/start", methods=["POST"])
def enrich_start():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    if "file" not in request.files:
        return jsonify({"error": "Fichier Excel manquant"}), 400

    uploaded = request.files["file"]
    if not uploaded.filename.lower().endswith((".xlsx", ".xls")):
        return jsonify({"error": "Format non supporté (.xlsx / .xls requis)"}), 400

    context    = (request.form.get("context") or "").strip()
    file_bytes = uploaded.read()
    job_id     = str(uuid.uuid4())
    job_create(job_id)

    t = threading.Thread(target=_run_excel_job,
                         args=(job_id, file_bytes, context),
                         daemon=True)
    t.start()
    return jsonify({"job_id": job_id})


@app.route("/enrich/status/<job_id>")
def enrich_status(job_id):
    job = job_get(job_id)
    if not job:
        return jsonify({"error": "Job inconnu"}), 404
    # Ne pas renvoyer le fichier dans le status
    return jsonify({
        "status":   job["status"],
        "progress": job["progress"],
        "total":    job["total"],
        "error":    job.get("error", ""),
    })


@app.route("/enrich/download/<job_id>")
def enrich_download(job_id):
    job = job_get(job_id)
    if not job:
        return jsonify({"error": "Job inconnu"}), 404
    if job["status"] != "done":
        return jsonify({"error": "Fichier pas encore prêt", "status": job["status"]}), 202
    try:
        result_bytes = base64.b64decode(job["result_b64"])
        return send_file(
            io.BytesIO(result_bytes),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="contacts_enrichis.xlsx"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Ancienne route synchrone conservée pour compatibilité (max ~5 lignes)
@app.route("/enrich", methods=["POST"])
def enrich():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    if "file" not in request.files:
        return jsonify({"error": "Fichier Excel manquant"}), 400
    uploaded = request.files["file"]
    if not uploaded.filename.lower().endswith((".xlsx", ".xls")):
        return jsonify({"error": "Format non supporté"}), 400
    try:
        result_bytes = enrich_excel(uploaded)
        return send_file(
            io.BytesIO(result_bytes),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="contacts_enrichis.xlsx"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Import seed RNE ────────────────────────────────────────────────────────────

@app.route("/import/seed", methods=["POST"])
def import_seed():
    if "file" not in request.files:
        return jsonify({"error": "Fichier Excel manquant"}), 400
    uploaded = request.files["file"]
    try:
        wb = openpyxl.load_workbook(uploaded)
        ws = wb.active

        header_row = None
        headers    = []
        for i, row in enumerate(ws.iter_rows(min_row=1, max_row=5, values_only=True), 1):
            if any(str(v or '').strip() for v in row):
                header_row = i
                headers    = [str(v or '').strip() for v in row]
                break

        if not header_row:
            return jsonify({"error": "Entêtes non trouvées"}), 400

        def col(frags):
            for frag in frags:
                for i, h in enumerate(headers):
                    if frag.lower() in h.lower():
                        return i
            return None

        name_col = col(["Nom Résidence", "Nom Residence", "Nom", "Dénomination"])
        city_col = col(["Ville", "City"])
        gov_col  = col(["Gouvernorat"])
        rne_col  = col(["ID RNE", "RNE", "Identifiant"])

        if name_col is None:
            return jsonify({"error": "Colonne 'Nom' introuvable"}), 400

        syndics = []
        for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
            name = str(row[name_col] or "").strip()
            city = str(row[city_col] if city_col is not None else "").strip() or \
                   str(row[gov_col]  if gov_col  is not None else "").strip()
            rne  = str(row[rne_col] if rne_col is not None else "").strip()
            if name and city:
                syndics.append({"name": name, "city": city, "rne_id": rne})

        inserted = seed_from_list(syndics)
        return jsonify({"status": "ok", "inserted": inserted, "total": len(syndics)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Admin ──────────────────────────────────────────────────────────────────────

@app.route("/results/clear", methods=["POST"])
def clear_results():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    delete_all()
    return jsonify({"status": "ok"})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "4.0"})


# ── Diagnostic RNE ────────────────────────────────────────────────────────────

@app.route("/debug/rne")
def debug_rne():
    """Teste la connexion RNE et retourne un rapport détaillé."""
    import os
    from scraper_engine import _get_rne_token, src_rne_entite

    rne_username = os.environ.get("RNE_USERNAME", "")
    rne_password = os.environ.get("RNE_PASSWORD", "")
    rne_token_env = os.environ.get("RNE_TOKEN", "")

    report = {
        "env": {
            "RNE_USERNAME":  rne_username[:4] + "***" if rne_username else "(non défini)",
            "RNE_PASSWORD":  "***" if rne_password else "(non défini)",
            "RNE_TOKEN":     rne_token_env[:8] + "…" if rne_token_env else "(non défini)",
        },
        "token_obtenu": False,
        "token_preview": "",
        "test_email": "",
        "test_rne_id": "1735882881",   # ID RNE de test (syndic connu)
        "erreur": "",
    }

    try:
        token = _get_rne_token()
        if token:
            report["token_obtenu"] = True
            report["token_preview"] = token[:8] + "…"
        else:
            report["erreur"] = "Token vide — vérifiez RNE_USERNAME et RNE_PASSWORD"
            return jsonify(report)
    except Exception as e:
        report["erreur"] = f"Erreur lors de _get_rne_token() : {e}"
        return jsonify(report)

    # Appel brut à l'API pour voir tous les champs retournés
    try:
        import requests as _req
        r = _req.get(
            f"https://www.registre-entreprises.tn/api/rne-api/front-office/entites/{report['test_rne_id']}",
            headers={
                "Authorization": f"Bearer {token}",
                "Referer": "https://www.registre-entreprises.tn/",
                "Accept": "application/json",
            },
            timeout=15
        )
        report["http_status"] = r.status_code
        if r.status_code == 200 and r.text:
            raw = r.json()
            # Afficher tous les champs de premier niveau (valeurs tronquées)
            report["champs_disponibles"] = {
                k: (str(v)[:80] if v not in (None, "", [], {}) else "(vide)")
                for k, v in raw.items()
            }
            # Chercher tout champ contenant "email" ou "mail" ou "tel" ou "gsm"
            report["champs_contact"] = {
                k: str(v)
                for k, v in raw.items()
                if any(x in k.lower() for x in ["email","mail","tel","gsm","phone","contact"])
            }
        else:
            report["erreur"] = f"HTTP {r.status_code} — {r.text[:200]}"
    except Exception as e:
        report["erreur"] = f"Erreur appel direct : {e}"

    return jsonify(report)


# ── Pages frontend ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/dashboard")
def dashboard_page():
    return app.send_static_file("dashboard.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
