"""
Synchronisation RNE — Scanner Syndic
Détecte les nouveaux syndics créés dans le RNE et les syndics qui ont mis à jour
leurs informations (mise à jour annuelle obligatoire).
"""
import os
import time
import logging
import threading
import requests
from datetime import datetime

log = logging.getLogger("rne_sync")

RNE_BORNE_SEARCH = "https://www.registre-entreprises.tn/api/rne-api/front-office/shortEntites"
RNE_AUTH_URL     = "https://www.registre-entreprises.tn/api/rne-auth-api/oauth/token"

_token_cache = {"token": None, "expires_at": 0}
_token_lock  = threading.Lock()

# Mots-clés RNE pour les syndics de copropriété (du plus spécifique au plus général)
SYNDIC_KEYWORDS = [
    "syndic coproprietaires",
    "syndicat coproprietaires",
    "syndic residence",
    "niqabat mutasakini",
    "niqabat maliki",
    "syndic",
]

# Indicateurs qu'un nom est bien un syndic de copropriété
SYNDIC_MARKERS = ["SYNDIC", "SYNDICAT", "NIQABAT"]


# ── Authentification ──────────────────────────────────────────────────────────

def _get_token() -> str | None:
    with _token_lock:
        if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
            return _token_cache["token"]
        username = os.environ.get("RNE_USERNAME", "")
        password = os.environ.get("RNE_PASSWORD", "")
        if not username or not password:
            return None
        try:
            r = requests.post(
                RNE_AUTH_URL,
                data={
                    "grant_type":    "password",
                    "username":      username,
                    "password":      password,
                    "client_id":     "client",
                    "client_secret": "secret",
                },
                timeout=10,
            )
            if r.status_code == 200:
                data    = r.json()
                token   = data.get("access_token")
                exp_in  = data.get("expires_in", 3600)
                _token_cache["token"]      = token
                _token_cache["expires_at"] = time.time() + exp_in - 60
                return token
        except Exception as e:
            log.warning(f"[RNE Sync] Auth erreur: {e}")
        return None


def _headers() -> dict:
    h = {"Accept": "application/json"}
    token = _get_token()
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


# ── Recherche RNE ─────────────────────────────────────────────────────────────

def _search_keyword(keyword: str) -> dict:
    """
    Récupère tous les résultats RNE pour un mot-clé avec pagination complète.
    Retourne un dict {rne_id: {name, city, date_modif}}.
    """
    results = {}
    page = 0
    while True:
        try:
            r = requests.get(
                RNE_BORNE_SEARCH,
                params={"denominationLatin": keyword, "page": page, "limit": 94},
                headers=_headers(),
                timeout=12,
            )
            if r.status_code != 200:
                log.warning(f"[RNE Sync] '{keyword}' p{page} HTTP {r.status_code}")
                break
            body     = r.json()
            registres = body.get("registres", [])
            if not registres:
                break

            for e in registres:
                rne_id = e.get("identifiantUnique", "").strip()
                if not rne_id:
                    continue
                nom = (e.get("denominationLatin") or "").strip().upper()
                # Filtrer: ne garder que les vrais syndics de copropriété
                if not any(m in nom for m in SYNDIC_MARKERS):
                    continue
                # Ville — plusieurs champs possibles selon le type d'entité
                addr = e.get("adresseSiegeSocial") or {}
                city = (
                    (addr.get("gouvernorat") or addr.get("ville") or "") if isinstance(addr, dict) else ""
                ) or (e.get("villeFr") or e.get("villeSiegeFr") or e.get("ville") or "")
                city = city.strip()
                date_modif = (
                    e.get("dateModification") or
                    e.get("dateDerniereModification") or
                    e.get("dateEnregistrementModif") or
                    ""
                )
                results[rne_id] = {
                    "rne_id":     rne_id,
                    "name":       (e.get("denominationLatin") or "").strip(),
                    "city":       city,
                    "date_modif": date_modif,
                }

            if len(registres) < 94:
                break
            page += 1
            time.sleep(0.5)

        except Exception as ex:
            log.warning(f"[RNE Sync] '{keyword}' p{page} erreur: {ex}")
            break

    return results


def fetch_all_rne_syndics() -> dict:
    """Agrège les résultats de tous les mots-clés. Déduplique par rne_id."""
    all_results = {}
    for kw in SYNDIC_KEYWORDS:
        found = _search_keyword(kw)
        new_in_kw = len([k for k in found if k not in all_results])
        log.info(f"[RNE Sync] '{kw}' → {len(found)} résultats ({new_in_kw} nouveaux uniques)")
        all_results.update(found)
        time.sleep(1)
    return all_results


# ── Sync principal ────────────────────────────────────────────────────────────

def run_sync() -> dict:
    """
    Compare le RNE avec la DB Scanner Syndic.
    - Nouveaux syndics → insérés comme prospects
    - Syndics mis à jour (dateModif RNE > dernière sync) → flaggés pour re-enrichissement
    Retourne: {total_rne, new, updated, skipped, error?}
    """
    import db

    log.info("=== RNE Sync démarré ===")
    rne_syndics = fetch_all_rne_syndics()
    log.info(f"[RNE Sync] {len(rne_syndics)} syndics uniques sur le RNE")

    if not rne_syndics:
        return {"total_rne": 0, "new": 0, "updated": 0, "skipped": 0,
                "error": "RNE API inaccessible ou aucun résultat"}

    # Charger les entrées existantes avec un rne_id
    conn = db.get_conn()
    rows = conn.execute(
        "SELECT id, rne_id, rne_sync_at FROM results WHERE rne_id IS NOT NULL AND rne_id != ''"
    ).fetchall()
    existing = {row["rne_id"]: {"id": row["id"], "rne_sync_at": row["rne_sync_at"] or ""} for row in rows}
    conn.close()

    now_str       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_count     = 0
    updated_count = 0
    skipped_count = 0
    new_names     = []
    updated_names = []

    for rne_id, data in rne_syndics.items():
        if rne_id not in existing:
            _insert_new(db, data, now_str)
            new_count += 1
            new_names.append(data["name"][:50])
        else:
            ex = existing[rne_id]
            last_sync  = ex.get("rne_sync_at", "")[:10]   # YYYY-MM-DD
            date_modif = (data.get("date_modif") or "")[:10]

            # Mise à jour si RNE signale une modification plus récente que notre dernière vérification
            if date_modif and last_sync and date_modif > last_sync:
                _flag_updated(db, ex["id"], now_str)
                updated_count += 1
                updated_names.append(data["name"][:50])
            else:
                _touch_sync(db, ex["id"], now_str)
                skipped_count += 1

    log.info(
        f"=== RNE Sync terminé — "
        f"{new_count} nouveaux | {updated_count} mis à jour | {skipped_count} inchangés ==="
    )
    result = {
        "total_rne":     len(rne_syndics),
        "new":           new_count,
        "updated":       updated_count,
        "skipped":       skipped_count,
        "new_names":     new_names[:10],
        "updated_names": updated_names[:10],
    }
    return result


# ── Opérations DB ─────────────────────────────────────────────────────────────

def _insert_new(db_module, data: dict, now_str: str):
    conn = db_module.get_conn()
    try:
        conn.execute(
            """
            INSERT INTO results
                (name, city, rne_id, pipeline_status, found, rne_sync_at, notes, created_at)
            VALUES (?, ?, ?, 'prospect', 0, ?, 'Détecté automatiquement via sync RNE', ?)
            """,
            (data["name"], data.get("city", ""), data["rne_id"], now_str, now_str),
        )
        conn.commit()
    finally:
        conn.close()


def _flag_updated(db_module, row_id: int, now_str: str):
    conn = db_module.get_conn()
    try:
        conn.execute(
            """UPDATE results
               SET rne_sync_at = ?,
                   notes = 'Mise à jour RNE détectée — relancer enrichissement'
               WHERE id = ?""",
            (now_str, row_id),
        )
        conn.commit()
    finally:
        conn.close()


def _touch_sync(db_module, row_id: int, now_str: str):
    conn = db_module.get_conn()
    try:
        conn.execute("UPDATE results SET rne_sync_at = ? WHERE id = ?", (now_str, row_id))
        conn.commit()
    finally:
        conn.close()
