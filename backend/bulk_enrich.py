"""
Enrichissement en masse — lit tous les syndics sans email et scrape chacun.
Lance en arrière-plan : nohup python bulk_enrich.py &
"""
import sys
import os
import time
import sqlite3
import logging

# Chemin absolu vers la DB
DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/tmp/bulk_enrich.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("bulk")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def fetch_todo():
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, name, city, rne_id FROM results "
        "WHERE (email IS NULL OR email = '') "
        "ORDER BY id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def save_result(data, row_id):
    conn = get_conn()
    phone     = data.get("phone", "") or ""
    email     = data.get("email", "") or ""
    website   = data.get("website", "") or ""
    all_phones = ", ".join(data.get("all_phones", []))
    all_emails = ", ".join(data.get("all_emails", []))
    sources   = ", ".join(data.get("sources_hit", []))
    confidence = data.get("global_conf", 0)
    found     = 1 if data.get("found") else 0
    president = data.get("president", "") or ""
    address   = data.get("address", "") or ""
    rne_id    = (data.get("rne_id") or data.get("rne_id_found") or "").strip()

    conn.execute("""
        UPDATE results SET
            phone=?, email=?, website=?, all_phones=?, all_emails=?,
            sources_hit=?, confidence=?, found=?, president=?, address=?,
            rne_id=CASE WHEN rne_id='' THEN ? ELSE rne_id END,
            created_at=CURRENT_TIMESTAMP
        WHERE id=?
    """, (phone, email, website, all_phones, all_emails,
          sources, confidence, found, president, address,
          rne_id, row_id))
    conn.commit()
    conn.close()

def main():
    from scraper_engine import scrape_all
    from scoring_engine import compute_conformity

    todo = fetch_todo()
    total = len(todo)
    log.info(f"=== Démarrage enrichissement — {total} syndics à traiter ===")

    found_email = 0
    found_phone = 0
    errors = 0

    for i, row in enumerate(todo, 1):
        name   = row["name"] or ""
        city   = row["city"] or ""
        rne_id = row["rne_id"] or ""
        row_id = row["id"]

        if not name:
            continue

        try:
            raw    = scrape_all(name, city, rne_id=rne_id, context="syndic")
            result = compute_conformity(raw)
            result["name"] = name
            result["city"] = city
            if rne_id:
                result["rne_id"] = rne_id
            save_result(result, row_id)

            got_email = bool(result.get("email"))
            got_phone = bool(result.get("phone"))
            if got_email: found_email += 1
            if got_phone: found_phone += 1

            log.info(
                f"[{i}/{total}] {name[:40]} | "
                f"email={'✅' if got_email else '❌'} "
                f"phone={'✅' if got_phone else '❌'} "
                f"sources={result.get('sources_hit', [])}"
            )
        except Exception as e:
            errors += 1
            log.error(f"[{i}/{total}] ERREUR {name}: {e}")

        # Pause pour éviter le rate-limiting
        time.sleep(2)

    log.info(
        f"=== TERMINÉ — emails: {found_email}/{total} | "
        f"téléphones: {found_phone}/{total} | erreurs: {errors} ==="
    )

if __name__ == "__main__":
    main()
