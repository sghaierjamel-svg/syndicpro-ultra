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
    "phone":          "TEXT    DEFAULT ''",
    "email":          "TEXT    DEFAULT ''",
    "website":        "TEXT    DEFAULT ''",
    "all_phones":     "TEXT    DEFAULT ''",
    "all_emails":     "TEXT    DEFAULT ''",
    "sources_hit":    "TEXT    DEFAULT ''",
    "confidence":     "REAL    DEFAULT 0",
    "found":          "INTEGER DEFAULT 0",
    "rne_id":         "TEXT    DEFAULT ''",
    "president":      "TEXT    DEFAULT ''",
    "members":        "TEXT    DEFAULT ''",
    "address":        "TEXT    DEFAULT ''",
    "created_at":     "TEXT    DEFAULT '1970-01-01 00:00:00'",
    "verified":       "INTEGER DEFAULT 0",
    "notes":          "TEXT    DEFAULT ''",
    # Email agent columns
    "email_sent":     "INTEGER DEFAULT 0",
    "email_sent_at":  "TEXT    DEFAULT ''",
    "email_template": "TEXT    DEFAULT ''",
    "email_status":   "TEXT    DEFAULT ''",
    # Pipeline commercial
    "pipeline_status":  "TEXT    DEFAULT 'prospect'",
    "lead_score":       "INTEGER DEFAULT 0",
    "seq_step":         "INTEGER DEFAULT 0",
    "seq_started_at":   "TEXT    DEFAULT ''",
    "seq_paused":       "INTEGER DEFAULT 0",
    "seq_last_sent_at": "TEXT    DEFAULT ''",
    "email_opens":      "INTEGER DEFAULT 0",
    "email_clicks":     "INTEGER DEFAULT 0",
    # Synchronisation RNE
    "rne_sync_at":      "TEXT    DEFAULT ''",
    "date_creation":    "TEXT    DEFAULT ''",
    # Multi-gestionnaire
    "is_multi":         "INTEGER DEFAULT 0",
    "multi_count":      "INTEGER DEFAULT 0",
    "residences_str":   "TEXT    DEFAULT ''",
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
    conn.execute("PRAGMA synchronous=NORMAL")   # sécurisé avec WAL, 3× plus rapide
    conn.execute("PRAGMA cache_size=-8192")     # 8 MB de cache en RAM
    conn.execute("PRAGMA temp_store=MEMORY")    # résultats temporaires en RAM
    conn.execute("PRAGMA mmap_size=134217728")  # 128 MB memory-mapped I/O
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

    # ── Index pour les requêtes fréquentes du dashboard ──────────────────────
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_found       ON results(found)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_confidence  ON results(confidence DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_created_at  ON results(created_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_name_city   ON results(name, city)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_email       ON results(email)")
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

    # ── Table séquence email (log des envois) ─────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS seq_log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            result_id INTEGER NOT NULL,
            step      INTEGER NOT NULL,
            track_id  TEXT    NOT NULL UNIQUE,
            subject   TEXT    DEFAULT '',
            sent_at   TEXT    DEFAULT CURRENT_TIMESTAMP,
            opened    INTEGER DEFAULT 0,
            clicked   INTEGER DEFAULT 0,
            opened_at TEXT    DEFAULT ''
        )
    """)

    # ── Table événements tracking (ouvertures/clics) ──────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS track_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id   TEXT NOT NULL,
            event      TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(track_id, event)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_seq_log_result ON seq_log(result_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_track_events_tid ON track_events(track_id)")

    # ── Table meta (clé-valeur pour la persistance d'état) ────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT ''
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
        date_creation = (data.get("date_creation") or "").strip()
        c.execute("""
            UPDATE results SET
                phone    = CASE WHEN ? != '' THEN ? ELSE phone    END,
                email    = CASE WHEN ? != '' THEN ? ELSE email    END,
                website  = CASE WHEN ? != '' THEN ? ELSE website  END,
                all_phones  = CASE WHEN ? != '' THEN ? ELSE all_phones  END,
                all_emails  = CASE WHEN ? != '' THEN ? ELSE all_emails  END,
                sources_hit = CASE WHEN ? != '' THEN ? ELSE sources_hit END,
                confidence  = CASE WHEN ? > 0  THEN ? ELSE confidence  END,
                found    = CASE WHEN ? = 1  THEN 1  ELSE found    END,
                president= CASE WHEN ? != '' THEN ? ELSE president END,
                members  = CASE WHEN ? != '' THEN ? ELSE members  END,
                address  = CASE WHEN ? != '' THEN ? ELSE address  END,
                rne_id   = CASE WHEN rne_id = '' AND ? != '' THEN ? ELSE rne_id END,
                date_creation = CASE WHEN date_creation = '' AND ? != '' THEN ? ELSE date_creation END,
                city     = CASE WHEN (city IS NULL OR city = '') AND ? != '' THEN ? ELSE city END,
                created_at = CURRENT_TIMESTAMP
            WHERE id=?
        """, (
            row[0], row[0],             # phone
            row[1], row[1],             # email
            row[2], row[2],             # website
            row[3], row[3],             # all_phones
            row[4], row[4],             # all_emails
            row[5], row[5],             # sources_hit
            row[6], row[6],             # confidence
            row[7],                     # found
            row[8], row[8],             # president
            row[9], row[9],             # members
            row[10], row[10],           # address
            row[11], row[11],           # rne_id
            date_creation, date_creation,       # date_creation
            (data.get("city_rne") or ""), (data.get("city_rne") or ""),  # city
            existing["id"],
        ))
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
    conn       = get_conn()
    c          = conn.cursor()
    total      = c.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    found      = c.execute("SELECT COUNT(*) FROM results WHERE found=1").fetchone()[0]
    avg_conf   = c.execute("SELECT AVG(confidence) FROM results WHERE found=1").fetchone()[0]
    with_email = c.execute("SELECT COUNT(*) FROM results WHERE email!=''").fetchone()[0]
    emailed    = c.execute("SELECT COUNT(*) FROM results WHERE email_sent=1").fetchone()[0]
    conn.close()
    return {
        "total":          total,
        "found":          found,
        "not_found":      total - found,
        "avg_confidence": round(avg_conf or 0, 1),
        "success_rate":   round((found / total * 100) if total else 0, 1),
        "with_email":     with_email,
        "emailed":        emailed,
    }


def get_email_contacts(only_unsent=True, min_confidence=0):
    """Retourne les contacts avec email, filtrés pour la campagne."""
    conn = get_conn()
    c    = conn.cursor()
    q    = "SELECT * FROM results WHERE email != ''"
    params = []
    if only_unsent:
        q += " AND (email_sent IS NULL OR email_sent=0)"
    if min_confidence > 0:
        q += " AND confidence >= ?"
        params.append(min_confidence)
    q += " ORDER BY confidence DESC"
    rows = c.execute(q, params).fetchall()
    conn.close()
    return [_parse_row(r) for r in rows]


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
               "president","address","notes","verified","confidence","found",
               "email_sent","email_sent_at","email_template","email_status"}
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


# ─────────────────────────────────────────────────────────────────────────────
#  Pipeline commercial
# ─────────────────────────────────────────────────────────────────────────────

PIPELINE_STATUSES = ["prospect", "emailed", "opened", "interested", "demo", "client", "lost"]


def get_pipeline_stats() -> dict:
    conn = get_conn()
    c    = conn.cursor()
    total      = c.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    with_email = c.execute("SELECT COUNT(*) FROM results WHERE email != ''").fetchone()[0]
    with_phone = c.execute("SELECT COUNT(*) FROM results WHERE phone != ''").fetchone()[0]
    seq_active = c.execute("SELECT COUNT(*) FROM results WHERE seq_step > 0 AND seq_step < 4 AND seq_paused=0 AND pipeline_status NOT IN ('client','lost')").fetchone()[0]
    seq_done   = c.execute("SELECT COUNT(*) FROM results WHERE seq_step >= 4").fetchone()[0]
    total_opens  = c.execute("SELECT COALESCE(SUM(email_opens),0) FROM results").fetchone()[0]
    total_clicks = c.execute("SELECT COALESCE(SUM(email_clicks),0) FROM results").fetchone()[0]
    by_status  = {}
    for s in PIPELINE_STATUSES:
        by_status[s] = c.execute("SELECT COUNT(*) FROM results WHERE pipeline_status=?", (s,)).fetchone()[0]
    avg_score  = c.execute("SELECT COALESCE(AVG(lead_score),0) FROM results WHERE email != ''").fetchone()[0]
    clients    = by_status.get("client", 0)
    interested = by_status.get("interested", 0) + by_status.get("demo", 0)
    conn.close()
    return {
        "total": total,
        "with_email": with_email,
        "with_phone": with_phone,
        "seq_active": seq_active,
        "seq_done": seq_done,
        "total_opens": total_opens,
        "total_clicks": total_clicks,
        "by_status": by_status,
        "avg_score": round(avg_score, 1),
        "pipeline_value_min": clients * 59,
        "pipeline_value_max": (clients + interested) * 59,
    }


def update_pipeline_status(result_id: int, status: str):
    if status not in PIPELINE_STATUSES:
        return
    conn = get_conn()
    conn.execute("UPDATE results SET pipeline_status=? WHERE id=?", (status, result_id))
    conn.commit()
    conn.close()


def get_contacts_by_status(status: str, limit=200) -> list:
    conn  = get_conn()
    rows  = conn.execute(
        "SELECT * FROM results WHERE pipeline_status=? ORDER BY lead_score DESC, confidence DESC LIMIT ?",
        (status, limit)
    ).fetchall()
    conn.close()
    return [_parse_row(r) for r in rows]


def bulk_update_scores(scores: dict):
    """scores = {result_id: score}"""
    conn = get_conn()
    conn.executemany(
        "UPDATE results SET lead_score=? WHERE id=?",
        [(v, k) for k, v in scores.items()]
    )
    conn.commit()
    conn.close()


def get_all_for_scoring(limit=5000) -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, name, city, email, phone, president, confidence, pipeline_status FROM results LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
#  Séquence email
# ─────────────────────────────────────────────────────────────────────────────

SEQ_DELAYS = {1: 0, 2: 3 * 86400, 3: 7 * 86400, 4: 14 * 86400}  # secondes après start


def start_sequences(result_ids: list) -> int:
    """Démarre la séquence pour une liste de contacts (step=1, started_at=now)."""
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    started = 0
    for rid in result_ids:
        cur = conn.execute(
            """UPDATE results SET seq_step=1, seq_started_at=?, seq_last_sent_at=?,
               seq_paused=0, pipeline_status='emailed'
               WHERE id=? AND seq_step=0""",
            (now, now, rid)
        )
        started += cur.rowcount
    conn.commit()
    conn.close()
    return started


def get_due_seq_emails() -> list:
    """Retourne les contacts dont l'email de séquence courant est dû."""
    import time as _time
    now = _time.time()
    conn = get_conn()
    rows = conn.execute(
        """SELECT * FROM results
           WHERE seq_step IN (1,2,3)
             AND seq_paused = 0
             AND email != ''
             AND pipeline_status NOT IN ('client','lost')"""
    ).fetchall()
    conn.close()
    due = []
    for r in rows:
        d = _parse_row(r)
        step  = d.get("seq_step", 0)
        # délai à attendre AVANT d'envoyer le step courant (step 1 = 0s = immédiat)
        delay = SEQ_DELAYS.get(step, 99999)
        last_str = d.get("seq_last_sent_at") or d.get("seq_started_at") or ""
        if not last_str:
            continue
        try:
            import time as _t
            last_ts = _t.mktime(_t.strptime(last_str[:19], "%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue
        if now - last_ts >= delay:
            due.append(d)
    return due


def advance_seq_step(result_id: int, new_step: int):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute(
        "UPDATE results SET seq_step=?, seq_last_sent_at=? WHERE id=?",
        (new_step, now, result_id)
    )
    conn.commit()
    conn.close()


def pause_sequence(result_id: int):
    conn = get_conn()
    conn.execute("UPDATE results SET seq_paused=1 WHERE id=?", (result_id,))
    conn.commit()
    conn.close()


def log_seq_sent(result_id: int, step: int, track_id: str, subject: str):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO seq_log (result_id, step, track_id, subject) VALUES (?,?,?,?)",
        (result_id, step, track_id, subject)
    )
    conn.commit()
    conn.close()


def record_tracking_event(track_id: str, event: str) -> dict | None:
    """Enregistre un open/click et retourne les infos du contact. Idempotent."""
    conn = get_conn()
    # INSERT OR IGNORE : la contrainte UNIQUE(track_id,event) évite le double-comptage
    inserted = conn.execute(
        "INSERT OR IGNORE INTO track_events (track_id, event) VALUES (?,?)",
        (track_id, event)
    ).rowcount
    row = conn.execute("SELECT * FROM seq_log WHERE track_id=?", (track_id,)).fetchone()
    if row and inserted:   # on ne compte que si c'était vraiment nouveau
        result_id = row["result_id"]
        if event == "open":
            conn.execute(
                "UPDATE results SET email_opens=email_opens+1, "
                "pipeline_status=CASE WHEN pipeline_status IN ('prospect','emailed') THEN 'opened' "
                "ELSE pipeline_status END WHERE id=?",
                (result_id,)
            )
            conn.execute(
                "UPDATE seq_log SET opened=1, opened_at=CURRENT_TIMESTAMP WHERE track_id=?",
                (track_id,)
            )
        elif event == "click":
            conn.execute("UPDATE results SET email_clicks=email_clicks+1 WHERE id=?", (result_id,))
            conn.execute("UPDATE seq_log SET clicked=1 WHERE track_id=?", (track_id,))
    conn.commit()
    conn.close()
    return dict(row) if row else None


# ─────────────────────────────────────────────────────────────────────────────
#  Meta (clé-valeur persistant)
# ─────────────────────────────────────────────────────────────────────────────

def get_meta(key: str) -> str:
    conn = get_conn()
    row  = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else ""


def set_meta(key: str, value: str):
    conn = get_conn()
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()
