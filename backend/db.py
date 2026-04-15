"""
Gestion de la base de données SQLite.
"""

import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", "data.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()
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
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migrations : ajouter les colonnes manquantes sur les bases existantes
    for sql in [
        "ALTER TABLE results ADD COLUMN website     TEXT    DEFAULT ''",
        "ALTER TABLE results ADD COLUMN all_phones  TEXT    DEFAULT ''",
        "ALTER TABLE results ADD COLUMN all_emails  TEXT    DEFAULT ''",
        "ALTER TABLE results ADD COLUMN sources_hit TEXT    DEFAULT ''",
        "ALTER TABLE results ADD COLUMN found       INTEGER DEFAULT 0",
    ]:
        try:
            c.execute(sql)
        except Exception:
            pass  # colonne déjà existante → on ignore
    conn.commit()
    conn.close()


def save(data):
    conn = get_conn()
    c = conn.cursor()

    # Dédoublonnage : mise à jour si déjà existant
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
    )

    if existing:
        c.execute("""
            UPDATE results SET
                phone=?, email=?, website=?, all_phones=?, all_emails=?,
                sources_hit=?, confidence=?, found=?,
                created_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, row + (existing["id"],))
    else:
        c.execute("""
            INSERT INTO results
                (phone, email, website, all_phones, all_emails, sources_hit,
                 confidence, found, name, city)
            VALUES (?,?,?,?,?,?,?,?,?,?)
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
    total     = c.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    found     = c.execute("SELECT COUNT(*) FROM results WHERE found=1").fetchone()[0]
    avg_conf  = c.execute("SELECT AVG(confidence) FROM results WHERE found=1").fetchone()[0]
    conn.close()
    return {
        "total":          total,
        "found":          found,
        "not_found":      total - found,
        "avg_confidence": round(avg_conf or 0, 1),
        "success_rate":   round((found / total * 100) if total else 0, 1),
    }


def delete_all():
    conn = get_conn()
    conn.execute("DELETE FROM results")
    conn.commit()
    conn.close()
