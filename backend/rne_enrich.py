"""
Enrichissement RNE — version correcte et complète.
- Charge .env.local (credentials RNE)
- Appelle src_rne_entite (email officiel) + src_rne_borne (président, adresse, tél)
- Passe par compute_conformity → confidence correcte
- Sauvegarde via db.save() → tous les champs corrects
- Met à jour lead_score à la fin via run_scoring_all()
"""
import os
import sys
import time
import logging

# ── 1. Charger .env.local AVANT tout import des modules ──────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE   = os.path.join(SCRIPT_DIR, ".env.local")

if os.path.exists(ENV_FILE):
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()
    print(f"✔ .env.local chargé — RNE: {os.environ.get('RNE_USERNAME','?')}")
else:
    print("⚠ .env.local introuvable")

import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("/tmp/rne_enrich.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("rne")

DB_PATH = os.path.join(SCRIPT_DIR, "data.db")


def fetch_todo():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, city, rne_id FROM results "
        "WHERE rne_id IS NOT NULL AND rne_id != '' "
        "AND (email IS NULL OR email = '') "
        "ORDER BY id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def main():
    from scraper_engine import src_rne_entite, src_rne_borne, short_name
    from scoring_engine import compute_conformity
    from db import save
    from lead_scorer import run_scoring_all

    todo  = fetch_todo()
    total = len(todo)
    log.info(f"=== Enrichissement RNE — {total} syndics restants ===")

    found_email = 0
    found_phone = 0
    errors      = 0

    for i, row in enumerate(todo, 1):
        rne_id = row["rne_id"]
        name   = row["name"] or ""
        city   = row["city"] or ""
        row_id = row["id"]

        raw_results = []

        try:
            data_entite, src = src_rne_entite(rne_id)
            if any(data_entite.values()):
                data_entite["source"] = src
                raw_results.append(data_entite)
        except Exception as e:
            log.warning(f"[{i}/{total}] rne_entite erreur {name}: {e}")
            errors += 1

        try:
            sn = short_name(name)
            data_borne, src = src_rne_borne(name, city, sn, rne_id)
            if any([data_borne.get("emails"), data_borne.get("phones"),
                    data_borne.get("president"), data_borne.get("address")]):
                data_borne["source"] = src
                raw_results.append(data_borne)
        except Exception as e:
            log.warning(f"[{i}/{total}] rne_borne erreur {name}: {e}")

        if not raw_results:
            # Rien trouvé — on saute sans écraser les données existantes
            log.info(f"[{i}/{total}] {name[:50]:<50} | email=❌ | tél=❌")
            time.sleep(1.5)
            continue

        try:
            result         = compute_conformity(raw_results)
            result["name"] = name
            result["city"] = city
            result["rne_id"] = rne_id
            save(result)

            got_email = bool(result.get("email"))
            got_phone = bool(result.get("phone"))
            if got_email: found_email += 1
            if got_phone: found_phone += 1

            log.info(
                f"[{i}/{total}] {name[:50]:<50} | "
                f"email={'✅ '+result['email'] if got_email else '❌':<40} | "
                f"conf={result.get('global_conf',0):.0f}% | "
                f"tél={'✅' if got_phone else '❌'}"
            )
        except Exception as e:
            log.error(f"[{i}/{total}] ERREUR save {name}: {e}")
            errors += 1

        time.sleep(1.5)

    # ── Recalculer le lead_score sur toute la DB ──────────────────────────────
    log.info("=== Calcul des lead scores... ===")
    try:
        stats = run_scoring_all()
        log.info(
            f"Lead scores mis à jour — {stats['scored']} contacts | "
            f"{stats['disqualified']} disqualifiés | "
            f"moyenne {stats['avg_score']} | top leads: {stats['top_leads']}"
        )
    except Exception as e:
        log.error(f"Erreur scoring: {e}")

    pct = round(found_email / total * 100) if total else 0
    log.info(
        f"\n=== TERMINÉ ===\n"
        f"  Emails    : {found_email}/{total} ({pct}%)\n"
        f"  Téléphones: {found_phone}/{total}\n"
        f"  Erreurs   : {errors}\n"
    )


if __name__ == "__main__":
    main()
