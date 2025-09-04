import sqlite3

DB_NAME = "spend.db"

def create_tables():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Payments table
    c.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        council TEXT,
        payment_date TEXT,
        supplier TEXT,
        description TEXT,
        category TEXT,
        amount_gbp REAL,
        invoice_ref TEXT,
        lat REAL,
        lon REAL,
        hash TEXT UNIQUE
    )
    """)

    # Helpful indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_council ON payments(council)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_supplier ON payments(supplier)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_hash ON payments(hash)")

    # Feedback table
    c.execute("""
    CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payment_id INTEGER,
        user_name TEXT,
        comment TEXT,
        rating INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_tables()
    print(f"{DB_NAME} ready.")
