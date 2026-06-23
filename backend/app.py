"""
SyndicPro Scanner — API Backend v4
Nouveautés : enrichissement Excel asynchrone, cache, context générique.
"""

import pathlib, re as _re

def _load_env_local():
    """Charge backend/.env.local dans os.environ si le fichier existe."""
    env_path = pathlib.Path(__file__).parent / ".env.local"
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = _re.match(r'^([A-Z_][A-Z0-9_]*)=(.*)$', line)
            if m:
                key, val = m.group(1), m.group(2).strip().strip('"').strip("'")
                if key not in os.environ:  # ne pas écraser les vraies variables d'env
                    os.environ[key] = val

import os  # os doit être importé avant l'appel
_load_env_local()

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from scraper_engine import scrape_all, get_rne_candidates, src_truecaller_query, _ar_to_latin, _is_arabic
from scoring_engine import compute_conformity
from db import (init_db, save, get_all, count_all, get_stats, delete_all,
                seed_from_list, set_cache, job_create, job_update, job_get,
                get_result, update_result, invalidate_cache, get_email_contacts,
                get_pipeline_stats, update_pipeline_status, get_contacts_by_status,
                start_sequences, get_due_seq_emails, pause_sequence, record_tracking_event,
                get_meta, set_meta, get_conn)
from excel_processor import enrich_excel
from email_agent import (TEMPLATES, build_email, send_email,
                         start_campaign, get_campaign_status,
                         build_seq_email, send_sequence_step, start_seq_worker,
                         BASE_URL)
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
start_seq_worker()


def _do_track_sync():
    """Une passe de synchronisation tracking (appelable seule ou en boucle)."""
    import requests
    api_url = "https://www.syndicpro.tn/api/st/events"
    api_key = os.environ.get("SCANNER_API_KEY", "")
    since  = get_meta("track_sync_since")
    params = {"since": since} if since else {}
    resp = requests.get(
        api_url, params=params,
        headers={"X-Scanner-Key": api_key},
        timeout=10
    )
    if resp.ok:
        events = resp.json().get("events", [])
        new_since = since
        for ev in events:
            record_tracking_event(ev["track_id"], ev["event"])
            new_since = ev["created_at"]
        if new_since != since:
            set_meta("track_sync_since", new_since)
            logging.info(f"[TrackSync] {len(events)} événements synchronisés")


def _sync_tracking_from_syndicpro():
    """Poller toutes les 5 min — premier tick immédiat au démarrage."""
    import time as _time
    # Premier sync immédiat (sans attendre 5 min)
    try:
        _do_track_sync()
    except Exception as e:
        logging.warning(f"[TrackSync] Erreur init : {e}")
    while True:
        _time.sleep(300)
        try:
            _do_track_sync()
        except Exception as e:
            logging.warning(f"[TrackSync] Erreur : {e}")


_sync_thread = threading.Thread(target=_sync_tracking_from_syndicpro, daemon=True)
_sync_thread.start()

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
        # Ajouter translittération latine pour chaque membre arabe
        for m in result.get("members", []):
            if _is_arabic(m.get("nom", "")):
                m["nom_latin"] = _ar_to_latin(m["nom"])
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


# ── Cache invalidation ────────────────────────────────────────────────────────

@app.route("/cache/invalidate", methods=["POST"])
def cache_invalidate():
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    city = (body.get("city") or "").strip()
    if name and city:
        invalidate_cache(name, city)
        return jsonify({"ok": True})
    return jsonify({"error": "name + city requis"}), 400


# ── Stats ──────────────────────────────────────────────────────────────────────

@app.route("/stats")
def stats():
    try:
        return jsonify(get_stats())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/dashboard/stats")
def dashboard_stats():
    """Stats analytiques complètes pour le command center."""
    try:
        conn = get_conn()

        # Pipeline
        ps = get_pipeline_stats()

        # Distribution villes (top 12)
        cities_raw = conn.execute(
            """SELECT city, COUNT(*) as n FROM results
               WHERE city IS NOT NULL AND city != ''
               GROUP BY city ORDER BY n DESC LIMIT 12"""
        ).fetchall()
        cities = [{"city": r["city"], "n": r["n"]} for r in cities_raw]

        # Distribution scores
        sc = conn.execute("""
            SELECT
              SUM(CASE WHEN lead_score = 0              THEN 1 ELSE 0 END) as s0,
              SUM(CASE WHEN lead_score BETWEEN 1 AND 30 THEN 1 ELSE 0 END) as s30,
              SUM(CASE WHEN lead_score BETWEEN 31 AND 60 THEN 1 ELSE 0 END) as s60,
              SUM(CASE WHEN lead_score > 60             THEN 1 ELSE 0 END) as s100
            FROM results
        """).fetchone()
        scores = {"0": sc["s0"], "1-30": sc["s30"], "31-60": sc["s60"], "61+": sc["s100"]}

        # Nouveaux via RNE sync
        rne_new = conn.execute(
            "SELECT COUNT(*) as n FROM results WHERE notes LIKE '%sync RNE%'"
        ).fetchone()["n"]

        rne_new_emailed = conn.execute(
            """SELECT COUNT(*) as n FROM results
               WHERE notes LIKE '%sync RNE%' AND email != '' AND email IS NOT NULL"""
        ).fetchone()["n"]

        # Séquence par étape
        seq_steps = conn.execute("""
            SELECT seq_step, COUNT(*) as n FROM results
            WHERE seq_started_at IS NOT NULL AND seq_started_at != ''
            GROUP BY seq_step ORDER BY seq_step
        """).fetchall()
        seq_by_step = {r["seq_step"]: r["n"] for r in seq_steps}

        # Ouvertures par étape (depuis seq_log)
        opens_raw = conn.execute("""
            SELECT sl.step, COUNT(DISTINCT sl.result_id) as n
            FROM seq_log sl
            WHERE sl.opened = 1
            GROUP BY sl.step
        """).fetchall()
        opens_by_step = {r["step"]: r["n"] for r in opens_raw}

        # Syndics récents (date_creation non vide)
        recent = conn.execute(
            """SELECT COUNT(*) as n FROM results
               WHERE date_creation >= '2026-01-01'"""
        ).fetchone()["n"]

        conn.close()

        return jsonify({
            "pipeline":       ps,
            "cities":         cities,
            "scores":         scores,
            "rne_new":        rne_new,
            "rne_new_emailed": rne_new_emailed,
            "seq_by_step":    seq_by_step,
            "opens_by_step":  opens_by_step,
            "recent_2026":    recent,
        })
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
        for i, row in enumerate(ws.iter_rows(min_row=1, max_row=10, values_only=True), 1):
            non_empty = [str(v or '').strip() for v in row if str(v or '').strip()]
            if len(non_empty) >= 2:
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
    import os, requests as _req
    from scraper_engine import _get_rne_token

    rne_username  = os.environ.get("RNE_USERNAME", "")
    rne_password  = os.environ.get("RNE_PASSWORD", "")
    rne_token_env = os.environ.get("RNE_TOKEN", "")

    report = {
        "env": {
            "RNE_USERNAME": rne_username[:4] + "***" if rne_username else "(non défini)",
            "RNE_PASSWORD": "***" if rne_password else "(non défini)",
            "RNE_TOKEN":    rne_token_env[:8] + "…" if rne_token_env else "(non défini)",
        },
        "token_obtenu": False,
        "token_preview": "",
        "erreur": "",
    }

    try:
        token = _get_rne_token()
        if not token:
            report["erreur"] = "Token vide — vérifiez RNE_USERNAME et RNE_PASSWORD"
            return jsonify(report)
        report["token_obtenu"] = True
        report["token_preview"] = token[:8] + "…"
    except Exception as e:
        report["erreur"] = f"_get_rne_token() : {e}"
        return jsonify(report)

    hdrs = {
        "Authorization": f"Bearer {token}",
        "Referer": "https://www.registre-entreprises.tn/",
        "Accept": "application/json",
    }

    # Étape 1 : trouver un vrai rne_id via l'API PUBLIQUE (sans token, param correct)
    rne_id_test = request.args.get("rne_id", "").strip()
    if not rne_id_test:
        try:
            rs = _req.get(
                "https://www.registre-entreprises.tn/api/rne-api/front-office/shortEntites",
                params={"denominationLatin": "syndic", "size": 5},
                headers={"Referer": "https://www.registre-entreprises.tn/",
                         "Accept": "application/json",
                         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"},
                timeout=15
            )
            report["search_status"] = rs.status_code
            report["search_raw_preview"] = rs.text[:300]
            if rs.status_code == 200 and rs.text.strip():
                body = rs.json()
                # La réponse est {"registres": [...], "nombreTotal": N}
                items = body.get("registres") or (body if isinstance(body, list) else [])
                if items:
                    rne_id_test = items[0].get("identifiantUnique", "")
                    report["search_sample"] = str(items[0])[:400]
        except Exception as e:
            report["erreur_search"] = str(e)

    report["test_rne_id"] = rne_id_test or "(aucun trouvé)"

    if not rne_id_test:
        report["erreur"] = "Aucun rne_id valide trouvé. Ajoutez ?rne_id=XXXX dans l'URL."
        return jsonify(report)

    # Étape 2 : appel /entites/{id} et affichage brut
    try:
        r = _req.get(
            f"https://www.registre-entreprises.tn/api/rne-api/front-office/entites/{rne_id_test}",
            headers=hdrs, timeout=15
        )
        report["http_status"] = r.status_code
        if r.status_code == 200 and r.text:
            raw = r.json()
            report["champs_disponibles"] = {
                k: (str(v)[:100] if v not in (None, "", [], {}) else "(vide)")
                for k, v in raw.items()
            }
            report["champs_contact"] = {
                k: str(v)
                for k, v in raw.items()
                if any(x in k.lower() for x in ["email","mail","tel","gsm","phone","contact","adresse"])
            }
        else:
            report["erreur"] = f"HTTP {r.status_code} — {r.text[:300]}"
    except Exception as e:
        report["erreur"] = f"Erreur appel /entites : {e}"

    return jsonify(report)


# ── Test scrape complet (debug) ───────────────────────────────────────────────

@app.route("/debug/scrape")
def debug_scrape():
    """
    Lance scrape_all + compute_conformity sur un syndic de test et retourne
    le détail de chaque source (ce qu'elle a trouvé ou non).
    Paramètres : ?name=...&city=...&rne_id=... (rne_id optionnel)
    """
    name   = (request.args.get("name")   or "SYNDIC DES COPROPRIETAIRES DE LA RESIDENCE EL YASSAMINE").strip()
    city   = (request.args.get("city")   or "Sousse").strip()
    rne_id = (request.args.get("rne_id") or "").strip()

    from scraper_engine import scrape_all
    from scoring_engine import compute_conformity

    try:
        raw    = scrape_all(name, city, rne_id=rne_id, context="syndic")
        result = compute_conformity(raw)

        phase1, phase2 = [], []
        for r in raw:
            src = r.get("source", "?")
            entry = {
                "source":    src,
                "phones":    r.get("phones", []),
                "emails":    r.get("emails", []),
                "rne_id":    r.get("rne_id_found", ""),
                "president": r.get("president", ""),
                "members":   [m.get("nom","") for m in r.get("members", [])],
                "noms_cherches": r.get("noms_cherches", []),
            }
            if src == "member_contact":
                phase2.append(entry)
            else:
                phase1.append(entry)

        return jsonify({
            "input":        {"name": name, "city": city, "rne_id": rne_id},
            "phase1":       phase1,
            "phase2_membres": phase2,
            "resultat":     {
                "phone":       result.get("phone"),
                "email":       result.get("email"),
                "president":   result.get("president"),
                "members":     result.get("members", []),
                "global_conf": result.get("global_conf"),
                "sources_hit": result.get("sources_hit"),
                "found":       result.get("found"),
            }
        })
    except Exception as e:
        import traceback
        return jsonify({"erreur": str(e), "trace": traceback.format_exc()}), 500


# ── Truecaller — auth + test ──────────────────────────────────────────────────

@app.route("/admin/truecaller-otp", methods=["POST"])
def truecaller_otp():
    """
    Étape 1 : demande d'envoi OTP Truecaller sur le numéro de téléphone fourni.
    Body JSON : {"phone": "+21699xxxxxx"}
    Truecaller envoie un SMS avec un code à 6 chiffres.
    """
    import requests as _req
    body  = request.get_json(silent=True) or {}
    phone = (body.get("phone") or "").strip()
    if not phone:
        return jsonify({"error": "Champ 'phone' manquant"}), 400

    # Numéro au format E.164
    if not phone.startswith("+"):
        phone = "+216" + phone.lstrip("0")

    try:
        from phonenumbers import parse as _parse_phone
        pn = _parse_phone(phone, None)
        r = _req.post(
            "https://account-asia-south1.truecaller.com/v2/sendOnboardingOtp",
            json={
                "countryCode": "TN",
                "dialingCode": 216,
                "phoneNumber": str(pn.national_number),
                "region": "region-2",
                "sequenceNo": 2,
                "installationDetails": {
                    "app": {"buildVersion": 5, "majorVersion": 11, "minorVersion": 7, "store": "GOOGLE_PLAY"},
                    "device": {"deviceId": "a1b2c3d4e5f6g7h8", "language": "en", "manufacturer": "Samsung",
                               "model": "Galaxy S21", "osName": "Android", "osVersion": "10", "mobileServices": ["GMS"]},
                    "language": "en",
                },
            },
            headers={
                "content-type":   "application/json; charset=UTF-8",
                "accept-encoding": "gzip",
                "user-agent":     "Truecaller/11.75.5 (Android;10)",
                "clientsecret":   "lvc22mp3l1sfv6ujg83rd17btt",
            },
            timeout=10
        )
        return jsonify({"status": r.status_code, "body": r.text[:400]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin/truecaller-verify", methods=["POST"])
def truecaller_verify():
    """
    Étape 2 : vérification OTP → retourne le Bearer token.
    Body JSON : {"phone": "+21699xxxxxx", "otp": "123456"}
    Copiez le token retourné dans la variable TRUECALLER_TOKEN sur Render.
    """
    import requests as _req
    body  = request.get_json(silent=True) or {}
    phone = (body.get("phone") or "").strip()
    otp   = (body.get("otp")   or "").strip()
    if not phone or not otp:
        return jsonify({"error": "Champs 'phone' et 'otp' requis"}), 400

    if not phone.startswith("+"):
        phone = "+216" + phone.lstrip("0")

    try:
        from phonenumbers import parse as _parse_phone
        pn = _parse_phone(phone, None)
        r = _req.post(
            "https://account-asia-south1.truecaller.com/v1/verifyOnboardingOtp",
            json={
                "countryCode": "TN",
                "dialingCode": 216,
                "phoneNumber": str(pn.national_number),
                "requestId":   body.get("requestId", ""),
                "token":       otp,
            },
            headers={
                "content-type":   "application/json; charset=UTF-8",
                "accept-encoding": "gzip",
                "user-agent":     "Truecaller/11.75.5 (Android;10)",
                "clientsecret":   "lvc22mp3l1sfv6ujg83rd17btt",
            },
            timeout=10
        )
        body_json = {}
        try:
            body_json = r.json()
        except Exception:
            pass

        token = (body_json.get("installationId") or body_json.get("token") or
                 body_json.get("access_token") or body_json.get("accessToken") or "")
        return jsonify({
            "status":    r.status_code,
            "token":     token,
            "raw":       r.text[:600],
            "action":    "Copiez 'token' dans la variable TRUECALLER_TOKEN sur Render" if token else "",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin/truecaller-test")
def truecaller_test():
    """
    Teste le token Truecaller stocké dans TRUECALLER_TOKEN.
    Paramètre : ?q=nom (ex: ?q=Lassad+Zitouni)
    """
    from scraper_engine import src_truecaller_query
    token = os.environ.get("TRUECALLER_TOKEN", "").strip()
    q = (request.args.get("q") or "Lassad Zitouni").strip()
    if not token:
        return jsonify({
            "error":   "Variable TRUECALLER_TOKEN non définie",
            "action":  "1) POST /admin/truecaller-otp  2) POST /admin/truecaller-verify  3) Coller le token dans Render"
        }), 400
    result = src_truecaller_query(q)
    return jsonify({
        "query":        q,
        "token_preview": token[:8] + "…",
        "phones":       result.get("phones", []),
        "found":        bool(result.get("phones")),
    })


# ── Agent Email ───────────────────────────────────────────────────────────────

@app.route("/email/templates")
def email_templates():
    """Liste les templates disponibles."""
    result = [{"id": k, "name": v["name"], "subject": v["subject"]}
              for k, v in TEMPLATES.items()]
    return jsonify({"templates": result})


@app.route("/email/preview", methods=["POST"])
def email_preview():
    """Retourne un aperçu HTML du template pour un contact donné."""
    body        = request.get_json(silent=True) or {}
    template_id = body.get("template_id", "prospection")
    contact     = body.get("contact", {"name": "Résidence El Yasmine", "city": "Tunis"})
    try:
        subject, html = build_email(template_id, contact)
        return jsonify({"subject": subject, "html": html})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/email/send/<int:row_id>", methods=["POST"])
def email_send_one(row_id):
    """Envoie un email à un contact spécifique."""
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401

    row = get_result(row_id)
    if not row:
        return jsonify({"error": "Contact introuvable"}), 404

    to_email = row.get("email", "").strip()
    if not to_email:
        return jsonify({"error": "Ce contact n'a pas d'email"}), 400

    body        = request.get_json(silent=True) or {}
    template_id = body.get("template_id", "prospection")
    try:
        subject, html = build_email(template_id, row)
        ok = send_email(to_email, subject, html)
        if ok:
            update_result(row_id, email_sent=1, email_sent_at="", email_template=template_id, email_status="sent")
            return jsonify({"status": "sent", "to": to_email})
        return jsonify({"error": "Échec envoi (vérifiez la config RESEND/SMTP)"}), 500
    except Exception as e:
        app.logger.error(f"[email_send] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/email/campaign/start", methods=["POST"])
def email_campaign_start():
    """Démarre une campagne email sur tous les contacts avec email."""
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401

    body          = request.get_json(silent=True) or {}
    template_id   = body.get("template_id", "prospection")
    only_unsent   = body.get("only_unsent", True)
    min_confidence= float(body.get("min_confidence", 0))

    contacts = get_email_contacts(only_unsent=only_unsent, min_confidence=min_confidence)
    if not contacts:
        return jsonify({"error": "Aucun contact éligible"}), 400

    result = start_campaign(contacts, template_id)
    if "error" in result:
        return jsonify(result), 409
    return jsonify(result)


@app.route("/email/campaign/status")
def email_campaign_status():
    """Retourne l'état de la campagne en cours."""
    return jsonify(get_campaign_status())


@app.route("/email/contacts")
def email_contacts_list():
    """Retourne les contacts éligibles à l'envoi email."""
    only_unsent    = request.args.get("only_unsent", "1") == "1"
    min_confidence = float(request.args.get("min_confidence", 0))
    contacts = get_email_contacts(only_unsent=only_unsent, min_confidence=min_confidence)
    return jsonify({"contacts": contacts, "count": len(contacts)})


# ── Pipeline commercial ────────────────────────────────────────────────────────

@app.route("/api/pipeline/stats")
def pipeline_stats():
    try:
        return jsonify(get_pipeline_stats())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pipeline/contacts")
def pipeline_contacts():
    """Contacts groupés par statut pipeline."""
    try:
        status = request.args.get("status", "prospect")
        limit  = int(request.args.get("limit", 200))
        rows   = get_contacts_by_status(status, limit)
        return jsonify({"contacts": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pipeline/<int:row_id>/status", methods=["POST"])
def pipeline_update_status(row_id):
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    body   = request.get_json(silent=True) or {}
    status = (body.get("status") or "").strip()
    valid  = {"prospect", "emailed", "opened", "interested", "demo", "client", "lost"}
    if status not in valid:
        return jsonify({"error": f"Statut invalide. Valeurs : {', '.join(sorted(valid))}"}), 400
    try:
        update_pipeline_status(row_id, status)
        return jsonify({"ok": True, "id": row_id, "status": status})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Séquences automatiques ─────────────────────────────────────────────────────

@app.route("/api/sequence/start", methods=["POST"])
def sequence_start():
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    body = request.get_json(silent=True) or {}
    ids  = body.get("ids", [])
    if not ids or not isinstance(ids, list):
        return jsonify({"error": "Fournir 'ids': liste d'IDs"}), 400
    try:
        started = start_sequences(ids)
        return jsonify({"started": started, "total": len(ids)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sequence/send-due", methods=["POST"])
def sequence_send_due():
    """Déclenche manuellement l'envoi de tous les emails dus."""
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    try:
        due  = get_due_seq_emails()
        sent = 0
        errs = 0
        for c in due:
            step = c.get("seq_step", 1)
            ok   = send_sequence_step(step, c, BASE_URL)
            if ok:
                sent += 1
            else:
                errs += 1
        return jsonify({"due": len(due), "sent": sent, "errors": errs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sequence/status")
def sequence_status():
    """Résumé de l'avancement des séquences."""
    try:
        from db import get_conn
        conn = get_conn()
        rows = conn.execute("""
            SELECT seq_step, COUNT(*) as n
            FROM results
            WHERE seq_started_at != '' AND seq_paused = 0
            GROUP BY seq_step
        """).fetchall()
        conn.close()
        return jsonify({"steps": {r["seq_step"]: r["n"] for r in rows}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sequence/<int:row_id>/pause", methods=["POST"])
def sequence_pause(row_id):
    if not check_key():
        return jsonify({"error": "Clé API invalide"}), 401
    try:
        pause_sequence(row_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Prospects (contacts avec email, triés par score) ──────────────────────────

@app.route("/api/prospects")
def api_prospects():
    try:
        from db import get_conn
        conn = get_conn()
        rows = conn.execute("""
            SELECT id, name, city, email, phone, president, members,
                   address, confidence, lead_score, sources_hit,
                   seq_step, seq_started_at, pipeline_status,
                   email_opens, email_clicks
            FROM results
            WHERE email IS NOT NULL AND email != ''
            ORDER BY lead_score DESC, confidence DESC
        """).fetchall()
        conn.close()
        result = []
        for r in rows:
            row = dict(r)
            try:
                row["members"] = json.loads(row["members"]) if row["members"] else []
            except Exception:
                row["members"] = []
            result.append(row)
        return jsonify({"prospects": result, "count": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Lead scoring ───────────────────────────────────────────────────────────────

@app.route("/api/leads/score-all", methods=["POST"])
def leads_score_all():
    try:
        from lead_scorer import run_scoring_all
        result = run_scoring_all()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Tracking pixel (open + click) ──────────────────────────────────────────────

_TRANSPARENT_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)


@app.route("/track/open/<track_id>")
def track_open(track_id):
    try:
        record_tracking_event(track_id, "open")
    except Exception:
        pass
    return send_file(
        io.BytesIO(_TRANSPARENT_GIF),
        mimetype="image/gif",
        max_age=0,
    )


@app.route("/track/click/<track_id>")
def track_click(track_id):
    redirect_url = request.args.get("url", "https://www.syndicpro.tn")
    try:
        record_tracking_event(track_id, "click")
    except Exception:
        pass
    from flask import redirect as flask_redirect
    return flask_redirect(redirect_url)


# ── Pages frontend ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return app.send_static_file("index.html")


# ── RNE Sync + Enrichissement automatique ─────────────────────────────────────

_rne_sync_state = {
    "phase":        "idle",   # idle | sync | enrich | done | error
    "sync_done":    False,
    "sync_result":  None,
    "enrich_total": 0,
    "enrich_done":  0,
    "enrich_emails": 0,
    "enrich_phones": 0,
    "current_name": "",
    "error":        "",
}
_rne_sync_lock = threading.Lock()


def _rne_full_pipeline():
    """Phase 1 : sync RNE → Phase 2 : enrichissement automatique des nouveaux."""
    log = logging.getLogger("app")

    # ── Phase 1 : Détection ───────────────────────────────────────────────────
    with _rne_sync_lock:
        _rne_sync_state["phase"] = "sync"

    try:
        from rne_sync import run_sync
        sync_result = run_sync()
    except Exception as e:
        log.error(f"[RNE Sync] {e}")
        with _rne_sync_lock:
            _rne_sync_state.update({"phase": "error", "error": str(e)})
        return

    with _rne_sync_lock:
        _rne_sync_state["sync_done"]   = True
        _rne_sync_state["sync_result"] = sync_result

    new_count = sync_result.get("new", 0)
    upd_count = sync_result.get("updated", 0)
    log.info(f"[RNE Sync] Phase 1 terminée — {new_count} nouveaux, {upd_count} mis à jour")

    # ── Phase 2 : Enrichissement (nouveaux + flaggés à re-enrichir) ───────────
    conn = get_conn()
    todo = conn.execute(
        """SELECT id, name, city, rne_id FROM results
           WHERE rne_id IS NOT NULL AND rne_id != ''
             AND (email IS NULL OR email = '')
           ORDER BY id DESC"""
    ).fetchall()
    conn.close()
    todo = [dict(r) for r in todo]

    if not todo:
        with _rne_sync_lock:
            _rne_sync_state["phase"] = "done"
        return

    with _rne_sync_lock:
        _rne_sync_state.update({"phase": "enrich", "enrich_total": len(todo),
                                  "enrich_done": 0, "enrich_emails": 0, "enrich_phones": 0})

    for i, row in enumerate(todo):
        with _rne_sync_lock:
            _rne_sync_state["current_name"] = row["name"][:50]

        try:
            invalidate_cache(row["name"], row["city"])
            raw    = scrape_all(row["name"], row["city"], rne_id=row["rne_id"], context="")
            result = compute_conformity(raw)
            result["name"] = row["name"]
            result["city"] = row["city"]
            save(result)
            got_email = bool(result.get("email"))
            got_phone = bool(result.get("phone"))
            with _rne_sync_lock:
                _rne_sync_state["enrich_done"]  = i + 1
                if got_email: _rne_sync_state["enrich_emails"] += 1
                if got_phone: _rne_sync_state["enrich_phones"] += 1
        except Exception as e:
            log.warning(f"[Enrich] {row['name'][:40]}: {e}")
            with _rne_sync_lock:
                _rne_sync_state["enrich_done"] = i + 1

        import time as _time
        _time.sleep(1.5)

    with _rne_sync_lock:
        _rne_sync_state["phase"] = "done"

    log.info(f"[RNE Sync] Pipeline terminé — "
             f"{_rne_sync_state['enrich_emails']} emails, "
             f"{_rne_sync_state['enrich_phones']} tél")


@app.route("/api/rne/sync", methods=["POST"])
def rne_sync_start():
    with _rne_sync_lock:
        if _rne_sync_state["phase"] in ("sync", "enrich"):
            return jsonify({"ok": False, "error": "Pipeline déjà en cours"})
        _rne_sync_state.update({
            "phase": "idle", "sync_done": False, "sync_result": None,
            "enrich_total": 0, "enrich_done": 0, "enrich_emails": 0,
            "enrich_phones": 0, "current_name": "", "error": "",
        })

    threading.Thread(target=_rne_full_pipeline, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/rne/sync/status")
def rne_sync_status():
    with _rne_sync_lock:
        return jsonify(dict(_rne_sync_state))


@app.route("/api/nouveautes")
def api_nouveautes():
    """Rapport des nouveautés : nouveaux RNE, multi-gestionnaires, MAJ détectées."""
    conn = get_conn()

    # 1. Nouveaux syndics RNE (détectés via sync)
    new_rows = conn.execute("""
        SELECT id, name, city, email, phone, lead_score, rne_sync_at, date_creation,
               seq_started_at, seq_step, pipeline_status
        FROM results
        WHERE notes LIKE '%sync RNE%'
        ORDER BY rne_sync_at DESC
    """).fetchall()
    new_syndics = [dict(r) for r in new_rows]

    # 2. Multi-gestionnaires (même email → plusieurs résidences)
    multi_rows = conn.execute("""
        SELECT email, COUNT(*) as n,
               GROUP_CONCAT(id, '||') as ids,
               GROUP_CONCAT(name, '||') as noms,
               GROUP_CONCAT(city, '||') as villes,
               GROUP_CONCAT(lead_score, '||') as scores
        FROM results
        WHERE email IS NOT NULL AND email != '' AND email NOT LIKE '%xxx%'
        GROUP BY email
        HAVING n > 1
        ORDER BY n DESC
        LIMIT 30
    """).fetchall()

    multi = []
    for r in multi_rows:
        ids    = (r["ids"]    or "").split("||")
        noms   = (r["noms"]   or "").split("||")
        villes = (r["villes"] or "").split("||")
        scores = (r["scores"] or "").split("||")
        residences = [
            {"id": ids[i], "name": noms[i], "city": villes[i],
             "score": int(scores[i]) if scores[i].isdigit() else 0}
            for i in range(len(ids))
        ]
        multi.append({
            "email":       r["email"],
            "count":       r["n"],
            "residences":  residences,
            "total_score": sum(x["score"] for x in residences),
        })

    # 3. Syndics flaggés mise à jour RNE
    updated_rows = conn.execute("""
        SELECT id, name, city, email, phone, rne_sync_at
        FROM results
        WHERE notes LIKE '%Mise à jour RNE%'
        ORDER BY rne_sync_at DESC
    """).fetchall()
    updated = [dict(r) for r in updated_rows]

    # 4. Résumé chiffres clés
    stats_new = {
        "total":      len(new_syndics),
        "with_email": sum(1 for r in new_syndics if r.get("email")),
        "no_email":   sum(1 for r in new_syndics if not r.get("email")),
        "seq_started":sum(1 for r in new_syndics if r.get("seq_started_at")),
    }
    last_sync = new_rows[0]["rne_sync_at"][:16] if new_rows else "—"

    conn.close()
    return jsonify({
        "new_syndics":  new_syndics,
        "multi":        multi,
        "updated":      updated,
        "stats_new":    stats_new,
        "last_sync":    last_sync,
    })


@app.route("/nouveautes")
def nouveautes_page():
    return app.send_static_file("nouveautes.html")


@app.route("/api/seq/start-new-rne", methods=["POST"])
def seq_start_new_rne():
    """Lance la séquence uniquement pour les syndics détectés via sync RNE
    qui ont un email et n'ont pas encore démarré de séquence."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT id FROM results
           WHERE notes LIKE '%sync RNE%'
             AND email IS NOT NULL AND email != ''
             AND (seq_started_at IS NULL OR seq_started_at = '')
             AND seq_paused = 0"""
    ).fetchall()
    conn.close()
    ids = [r["id"] for r in rows]
    if not ids:
        return jsonify({"ok": True, "started": 0, "message": "Aucun nouveau syndic éligible"})
    count = start_sequences(ids)
    return jsonify({"ok": True, "started": count})


@app.route("/api/seq/start-multi", methods=["POST"])
def seq_start_multi():
    """Lance une séquence multi-gestionnaire pour l'email donné.
    Sélectionne le contact principal (le plus récent), consolide les noms
    des résidences, et marque les autres comme gérés via multi-seq."""
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "email requis"}), 400

    conn = get_conn()
    rows = conn.execute(
        """SELECT id, name, city, president FROM results
           WHERE LOWER(email) = ? ORDER BY id DESC""",
        (email,)
    ).fetchall()

    if not rows:
        conn.close()
        return jsonify({"error": "Aucun contact trouvé pour cet email"}), 404

    count        = len(rows)
    main         = rows[0]
    others       = rows[1:]
    residences   = ", ".join(r["name"] for r in rows[:8])
    per_res      = round(99 / max(count, 1))

    # Mise à jour du contact principal
    conn.execute(
        """UPDATE results
           SET is_multi=1, multi_count=?, residences_str=?,
               seq_paused=0
           WHERE id=?""",
        (count, residences, main["id"])
    )
    # Marquer les autres résidences comme gérées via multi-seq (ne pas ré-envoyer)
    for r in others:
        conn.execute(
            """UPDATE results
               SET is_multi=1, multi_count=?, residences_str=?,
                   seq_paused=1,
                   notes=CASE WHEN notes NOT LIKE '%multi-seq%'
                               THEN notes || ' [géré via multi-seq]'
                               ELSE notes END
               WHERE id=?""",
            (count, residences, r["id"])
        )
    conn.commit()
    conn.close()

    started = start_sequences([main["id"]])
    return jsonify({
        "ok": True,
        "started": started,
        "main_id": main["id"],
        "count": count,
        "residences": residences,
    })


@app.route("/api/seq/new-rne/count")
def seq_new_rne_count():
    """Retourne le nombre de nouveaux syndics RNE éligibles à la séquence."""
    conn = get_conn()
    row = conn.execute(
        """SELECT COUNT(*) as n FROM results
           WHERE notes LIKE '%sync RNE%'
             AND email IS NOT NULL AND email != ''
             AND (seq_started_at IS NULL OR seq_started_at = '')
             AND seq_paused = 0"""
    ).fetchone()
    conn.close()
    return jsonify({"count": row["n"]})


@app.route("/dashboard")
def dashboard_page():
    return app.send_static_file("dashboard.html")


@app.route("/pipeline")
def pipeline_page():
    return app.send_static_file("pipeline.html")


@app.route("/prospects")
def prospects_page():
    return app.send_static_file("prospects.html")


@app.route("/api/email/test", methods=["POST"])
def email_test():
    """Envoie tous les templates (std + multi) à l'adresse demandée."""
    data = request.get_json(silent=True) or {}
    to   = (data.get("to") or "").strip()
    if not to:
        return jsonify({"error": "to requis"}), 400

    import uuid
    from email_agent import build_seq_email, send_email, SEQ_TEMPLATES, SEQ_MULTI_TEMPLATES

    contact_std = {"name": "Résidence El Marwa", "city": "Tunis", "president": "", "id": 0}
    contact_multi = {"name": "Résidence El Marwa", "city": "Tunis", "president": "",
                     "id": 0, "is_multi": 1, "multi_count": 5,
                     "residences_str": "El Marwa, La Colline, Jasmin, Safsaf, El Amal"}

    results = []
    for step in [1, 2, 3, 4]:
        subj, html = build_seq_email(step, contact_std, str(uuid.uuid4()), request.host_url)
        ok = send_email(to, f"[TEST std étape {step}] {subj}", html)
        results.append({"type": "std", "step": step, "subject": subj, "sent": ok})

    for step in [1, 2, 3, 4]:
        subj, html = build_seq_email(step, contact_multi, str(uuid.uuid4()), request.host_url)
        ok = send_email(to, f"[TEST multi étape {step}] {subj}", html)
        results.append({"type": "multi", "step": step, "subject": subj, "sent": ok})

    sent_count = sum(1 for r in results if r["sent"])
    return jsonify({"ok": True, "sent": sent_count, "total": len(results), "results": results})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
