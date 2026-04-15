"""
Gestion de la base de données SQLite.
"""

import sqlite3
import os
import logging

DB_PATH = os.environ.get("DB_PATH", "data.db")

REQUIRED_COLUMNS = {
    "id", "name", "city", "phone", "email", "website",
    "all_phones", "all_emails", "sources_hit", "confidence",
    "found", "rne_id", "president", "address", "created_at"
}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _existing_columns(c):
    rows = c.execute("PRAGMA table_info(results)").fetchall()
    return {row[1] for row in rows}


def init_db():
    conn = get_conn()
    c = conn.cursor()

    # Créer la table complète si elle n'existe pas
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
            address     TEXT DEFAULT '',
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    # Vérifier si le schéma est à jour
    existing = _existing_columns(c)
    missing = REQUIRED_COLUMNS - existing

    if missing:
        logging.warning(f"Colonnes manquantes détectées : {missing} — migration en cours")
        # Option 1 : essayer d'ajouter les colonnes manquantes
        added = 0
        for col in missing:
            if col in ("id", "name", "city", "created_at"):
                continue  # ces colonnes ne peuvent pas être ajoutées via ALTER
            col_def = {
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
                "address":     "TEXT    DEFAULT ''",
            }.get(col)
            if col_def:
                try:
                    c.execute(f"ALTER TABLE results ADD COLUMN {col} {col_def}")
                    added += 1
                except Exception as e:
                    logging.warning(f"ALTER TABLE {col} : {e}")
        conn.commit()

        # Option 2 : si colonnes critiques toujours manquantes → recréer la table
        existing_after = _existing_columns(c)
        still_missing = REQUIRED_COLUMNS - existing_after - {"id", "name", "city", "created_at"}
        if still_missing:
            logging.warning(f"Recréation de la table (colonnes irrécupérables : {still_missing})")
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
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Copier les données récupérables
            old_cols = _existing_columns(c) & {"id","name","city","phone","email",
                                                "confidence","found","created_at"}
            cols_str = ", ".join(old_cols)
            try:
                c.execute(f"INSERT INTO results ({cols_str}) SELECT {cols_str} FROM results_old")
            except Exception:
                pass
            c.execute("DROP TABLE IF EXISTS results_old")
            conn.commit()

    conn.close()


def save(data):
    conn = get_conn()
    c = conn.cursor()

    existing = c.execute(
        "SELECT id FROM results WHERE name=? AND city=?",
        (data["name"], data["city"])
    ).fetchone()

    row = (
        data.get("phone", ""),
        data.get("email", ""),
        data.get("website", ""),
        ", ".join(data.get("all_phones", [])),
        ", ".join(data.get("all_emails", [])),
        ", ".join(data.get("sources_hit", [])),
        data.get("global_conf", 0),
        1 if data.get("found") else 0,
        data.get("president", ""),
        data.get("address", ""),
    )

    if existing:
        c.execute("""
            UPDATE results SET
                phone=?, email=?, website=?, all_phones=?, all_emails=?,
                sources_hit=?, confidence=?, found=?, president=?, address=?,
                created_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, row + (existing["id"],))
    else:
        c.execute("""
            INSERT INTO results
                (phone, email, website, all_phones, all_emails, sources_hit,
                 confidence, found, president, address, name, city)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, row + (data["name"], data["city"]))

    conn.commit()
    conn.close()


def get_all(limit=500, offset=0, only_found=False):
    conn = get_conn()
    c = conn.cursor()
    if only_found:
        rows = c.execute(
            "SELECT * FROM results WHERE found=1 ORDER BY confidence DESC, created_at DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
    else:
        rows = c.execute(
            "SELECT * FROM results ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats():
    conn = get_conn()
    c = conn.cursor()
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
    """Insère une liste de syndics sans scraping. Ignore les doublons."""
    conn = get_conn()
    c = conn.cursor()
    inserted = 0
    for s in syndics:
        existing = c.execute(
            "SELECT id FROM results WHERE name=? AND city=?",
            (s["name"], s["city"])
        ).fetchone()
        if not existing:
            c.execute(
                "INSERT INTO results (name, city, rne_id) VALUES (?,?,?)",
                (s["name"], s["city"], s.get("rne_id", ""))
            )
            inserted += 1
    conn.commit()
    conn.close()
    return inserted


def delete_all():
    conn = get_conn()
    conn.execute("DELETE FROM results")
    conn.commit()
    conn.close()
