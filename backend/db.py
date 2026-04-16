"""
Gestion de la base de données SQLite — v4
Corrections : migration created_at, cache 24h, schéma complet.
"""

import sqlite3
import os
import json
import time
import logging
import threading

DB_PATH = os.environ.get("DB_PATH", "data.db")

# ── Colonnes attendues avec leur définition ALTER TABLE ───────────────────────
COLUMN_DEFS = {
    "phone":       "TEXT    DEFAULT ''",
    "email":       "TEXT    DEFAULT ''",
    "website":     "TEXT    DEFAULT ''",
    "all_phones":  "TEXT    DEFAULT ''",
    "all_emails":  "TEXT    DEFAULT ''",
    "sources_hit": "TEXT    DEFAULT ''",
    "confidence":  "REAL    DEFAULT 0",
    "found":       "INTEGER DEFAULT 0",
    "rne_id":      "TEXT    DEFAULT ''",
    "president":   "TEXT    DEFAULT ''",
    "members":     "TEXT    DEFAULT ''",
    "address":     "TEXT    DEFAULT ''",
    "created_at":  "TEXT    DEFAULT '1970-01-01 00:00:00'",
    "verified":    "INTEGER DEFAULT 0",
    "notes":       "TEXT    DEFAULT ''",
}
REQUIRED_COLUMNS = {"id", "name", "city"} | set(COLUMN_DEFS.keys())

# ── Cache mémoire (complété par SQLite pour la persistance) ───────────────────
_mem_cache: dict = {}
_cache_lock = threading.Lock()
CACHE_TTL = 24 * 3600   # 24 h


# ─────────────────────────────────────────────────────────────────────────────
#  Connexion
# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _existing_columns(cursor):
    rows = cursor.execute("PRAGMA table_info(results)").fetchall()
    return {row[1] for row in rows}


# ─────────────────────────────────────────────────────────────────────────────
#  init_db — crée ou migre le schéma
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    conn = get_conn()
    c = conn.cursor()

    # ── Table principale ──────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            city        TEXT NOT NULL,
            phone       TEXT DEFAULT '',
            email       TEXT DEFAULT '',
            website     TEXT DEFAULT '',
            all_phones  TEXT DEFAULT '',
            all_emails  TEXT DEFAULT '',
            sources_hit TEXT DEFAULT '',
            confidence  REAL DEFAULT 0,
            found       INTEGER DEFAULT 0,
            rne_id      TEXT DEFAULT '',
            president   TEXT DEFAULT '',
            members     TEXT DEFAULT '',
            address     TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            verified    INTEGER DEFAULT 0,
            notes       TEXT DEFAULT ''
        )
    """)
    conn.commit()

    # ── Migration : ajouter les colonnes manquantes ───────────────────────────
    existing = _existing_columns(c)
    missing  = REQUIRED_COLUMNS - existing

    if missing:
        logging.warning(f"[DB] Colonnes manquantes : {missing}")
        # Tenter ALTER TABLE pour chaque colonne manquante
        for col in missing:
            if col in ("id", "name", "city"):
                continue  # colonnes primaires non ajoutables via ALTER
            col_def = COLUMN_DEFS.get(col)
            if not col_def:
                continue
            try:
                c.execute(f"ALTER TABLE results ADD COLUMN {col} {col_def}")
                logging.info(f"[DB] Colonne ajoutée : {col}")
            except Exception as e:
                logging.warning(f"[DB] ALTER {col} échoué : {e}")
        conn.commit()

        # Vérification finale — si toujours manquant, recréer la table
        existing_after = _existing_columns(c)
        still_missing  = REQUIRED_COLUMNS - existing_after - {"id", "name", "city"}
        if still_missing:
            logging.warning(f"[DB] Recréation (irrémédiable : {still_missing})")
            _recreate_table(c)
            conn.commit()

    # ── Table cache ───────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_cache (
            key        TEXT PRIMARY KEY,
            result_json TEXT NOT NULL,
            expires_at  REAL NOT NULL
        )
    """)

    # ── Table jobs Excel async ────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS excel_jobs (
            job_id     TEXT PRIMARY KEY,
            status     TEXT DEFAULT 'pending',
            progress   INTEGER DEFAULT 0,
            total      INTEGER DEFAULT 0,
            result_b64 TEXT DEFAULT '',
            error      TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def _recreate_table(c):
    """Recrée la table results avec le schéma complet en préservant les données."""
    c.execute("ALTER TABLE results RENAME TO results_old")
    c.execute("""
        CREATE TABLE results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            city        TEXT NOT NULL,
            phone       TEXT DEFAULT '',
            email       TEXT DEFAULT '',
            website     TEXT DEFAULT '',
            all_phones  TEXT DEFAULT '',
            all_emails  TEXT DEFAULT '',
            sources_hit TEXT DEFAULT '',
            confidence  REAL DEFAULT 0,
            found       INTEGER DEFAULT 0,
            rne_id      TEXT DEFAULT '',
            president   TEXT DEFAULT '',
            members     TEXT DEFAULT '',
            address     TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            verified    INTEGER DEFAULT 0,
            notes       TEXT DEFAULT ''
        )
    """)
    old_cols = _existing_columns(c) & {
        "id","name","city","phone","email","website",
        "all_phones","all_emails","sources_hit","confidence",
        "found","rne_id","president","members","address","created_at",
        "verified","notes"
    }
    if old_cols:
        cols = ", ".join(old_cols)
        try:
            c.execute(f"INSERT INTO results ({cols}) SELECT {cols} FROM results_old")
        except Exception as e:
            logging.warning(f"[DB] Copie données : {e}")
    c.execute("DROP TABLE IF EXISTS results_old")


# ─────────────────────────────────────────────────────────────────────────────
#  Cache scraping (mémoire + SQLite)
# ─────────────────────────────────────────────────────────────────────────────

def _cache_key(name, city):
    return f"{name.lower().strip()}|{city.lower().strip()}"


def get_cache(name, city):
    """Retourne le résultat mis en cache s'il est encore valide."""
    key = _cache_key(name, city)
    now = time.time()

    # 1. Mémoire vive
    with _cache_lock:
        entry = _mem_cache.get(key)
        if entry and entry[1] > now:
            return entry[0]
        elif entry:
            del _mem_cache[key]

    # 2. SQLite
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT result_json, expires_at FROM scrape_cache WHERE key=?", (key,)
        ).fetchone()
        conn.close()
        if row and row["expires_at"] > now:
            result = json.loads(row["result_json"])
            with _cache_lock:
                _mem_cache[key] = (result, row["expires_at"])
            return result
    except Exception:
        pass
    return None


def set_cache(name, city, result):
    """Stocke le résultat 24h (mémoire + SQLite)."""
    key    = _cache_key(name, city)
    expiry = time.time() + CACHE_TTL
    with _cache_lock:
        _mem_cache[key] = (result, expiry)
    try:
        conn = get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO scrape_cache (key, result_json, expires_at) VALUES (?,?,?)",
            (key, json.dumps(result, ensure_ascii=False), expiry)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  CRUD résultats
# ─────────────────────────────────────────────────────────────────────────────

def _members_json(members):
    if not members:
        return ""
    try:
        return json.dumps(members, ensure_ascii=False)
    except Exception:
        return ""


def save(data):
    conn = get_conn()
    c    = conn.cursor()

    existing = c.execute(
        "SELECT id FROM results WHERE name=? AND city=?",
        (data["name"], data["city"])
    ).fetchone()

    rne_id = (data.get("rne_id") or data.get("rne_id_found") or "").strip()

    row = (
        data.get("phone",    ""),
        data.get("email",    ""),
        data.get("website",  ""),
        ", ".join(data.get("all_phones",  [])),
        ", ".join(data.get("all_emails",  [])),
        ", ".join(data.get("sources_hit", [])),
        data.get("global_conf", 0),
        1 if data.get("found") else 0,
        data.get("president", ""),
        _members_json(data.get("members", [])),
        data.get("address", ""),
        rne_id,
    )

    if existing:
        c.execute("""
            UPDATE results SET
                phone=?, email=?, website=?, all_phones=?, all_emails=?,
                sources_hit=?, confidence=?, found=?, president=?, members=?,
                address=?, rne_id=CASE WHEN rne_id='' THEN ? ELSE rne_id END,
                created_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, row + (existing["id"],))
    else:
        c.execute("""
            INSERT INTO results
                (phone, email, website, all_phones, all_emails, sources_hit,
                 confidence, found, president, members, address, rne_id, name, city)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, row + (data["name"], data["city"]))

    conn.commit()
    conn.close()


def _parse_row(row):
    d = dict(row)
    if d.get("members"):
        try:
            d["members"] = json.loads(d["members"])
        except Exception:
            d["members"] = []
    else:
        d["members"] = []
    return d


def get_all(limit=500, offset=0, only_found=False):
    conn = get_conn()
    c    = conn.cursor()
    q    = ("SELECT * FROM results WHERE found=1 ORDER BY confidence DESC, created_at DESC LIMIT ? OFFSET ?"
            if only_found else
            "SELECT * FROM results ORDER BY created_at DESC LIMIT ? OFFSET ?")
    rows = c.execute(q, (limit, offset)).fetchall()
    conn.close()
    return [_parse_row(r) for r in rows]


def count_all(only_found=False):
    conn  = get_conn()
    c     = conn.cursor()
    q     = "SELECT COUNT(*) FROM results WHERE found=1" if only_found else "SELECT COUNT(*) FROM results"
    total = c.execute(q).fetchone()[0]
    conn.close()
    return total


def get_stats():
    conn     = get_conn()
    c        = conn.cursor()
    total    = c.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    found    = c.execute("SELECT COUNT(*) FROM results WHERE found=1").fetchone()[0]
    avg_conf = c.execute("SELECT AVG(confidence) FROM results WHERE found=1").fetchone()[0]
    conn.close()
    return {
        "total":          total,
        "found":          found,
        "not_found":      total - found,
        "avg_confidence": round(avg_conf or 0, 1),
        "success_rate":   round((found / total * 100) if total else 0, 1),
    }


def seed_from_list(syndics):
    conn     = get_conn()
    c        = conn.cursor()
    inserted = 0
    for s in syndics:
        if not c.execute("SELECT id FROM results WHERE name=? AND city=?",
                         (s["name"], s["city"])).fetchone():
            c.execute(
                "INSERT INTO results (name, city, rne_id) VALUES (?,?,?)",
                (s["name"], s["city"], s.get("rne_id", ""))
            )
            inserted += 1
    conn.commit()
    conn.close()
    return inserted


def get_result(row_id):
    conn = get_conn()
    row  = conn.execute("SELECT * FROM results WHERE id=?", (row_id,)).fetchone()
    conn.close()
    return _parse_row(row) if row else None


def update_result(row_id, **kwargs):
    """Met à jour les champs d'un résultat (phone, email, website, notes, verified, etc.)."""
    if not kwargs:
        return
    # Champs autorisés pour la mise à jour manuelle
    allowed = {"phone","email","website","all_phones","all_emails",
               "president","address","notes","verified","confidence","found"}
    filtered = {k: v for k, v in kwargs.items() if k in allowed}
    if not filtered:
        return
    sets = ", ".join(f"{k}=?" for k in filtered)
    vals = list(filtered.values()) + [row_id]
    conn = get_conn()
    conn.execute(f"UPDATE results SET {sets} WHERE id=?", vals)
    conn.commit()
    conn.close()


def invalidate_cache(name, city):
    """Supprime l'entrée cache pour forcer un nouveau scraping."""
    key = _cache_key(name, city)
    with _cache_lock:
        _mem_cache.pop(key, None)
    try:
        conn = get_conn()
        conn.execute("DELETE FROM scrape_cache WHERE key=?", (key,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def delete_all():
    conn = get_conn()
    conn.execute("DELETE FROM results")
    conn.execute("DELETE FROM scrape_cache")
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  Jobs Excel asynchrones
# ─────────────────────────────────────────────────────────────────────────────

def job_create(job_id):
    conn = get_conn()
    conn.execute("INSERT OR REPLACE INTO excel_jobs (job_id) VALUES (?)", (job_id,))
    conn.commit()
    conn.close()


def job_update(job_id, **kwargs):
    if not kwargs:
        return
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [job_id]
    conn = get_conn()
    conn.execute(f"UPDATE excel_jobs SET {sets} WHERE job_id=?", vals)
    conn.commit()
    conn.close()


def job_get(job_id):
    conn = get_conn()
    row  = conn.execute("SELECT * FROM excel_jobs WHERE job_id=?", (job_id,)).fetchone()
    conn.close()
    return dict(row) if row else None
