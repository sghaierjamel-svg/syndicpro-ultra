import sqlite3

def init_db():
    conn = sqlite3.connect("data.db")
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY,
        name TEXT,
        city TEXT,
        phone TEXT,
        email TEXT,
        confidence REAL
    )
    """)

    conn.commit()
    conn.close()

def save(data):
    conn = sqlite3.connect("data.db")
    c = conn.cursor()

    c.execute("""
    INSERT INTO results (name, city, phone, email, confidence)
    VALUES (?, ?, ?, ?, ?)
    """, (
        data["name"],
        data["city"],
        data["phone"],
        data["email"],
        data["global_conf"]
    ))

    conn.commit()
    conn.close()
