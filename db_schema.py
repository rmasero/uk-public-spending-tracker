import sqlite3

DB_NAME = "spend.db"

def create_tables():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Payments table
    c.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        council TEXT NOT NULL,
        payment_date TEXT NOT NULL, -- ISO YYYY-MM-DD
        supplier TEXT,
        description TEXT,
        category TEXT,
        amount_gbp REAL NOT NULL DEFAULT 0,
        invoice_ref TEXT,
        lat REAL,
        lon REAL,
        hash TEXT NOT NULL
    );
    """)

    # Useful indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_council ON payments(council);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_supplier ON payments(supplier);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_ref);")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_hash ON payments(hash);")

    # Feedback table
    c.execute("""
    CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payment_id INTEGER NOT NULL,
        user_name TEXT,
        comment TEXT,
        rating INTEGER CHECK (rating BETWEEN 1 AND 5),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(payment_id) REFERENCES payments(id)
    );
    """)

    conn.commit()
    conn.close()
    print(f"{DB_NAME} ready with payments + feedback tables.")

if __name__ == "__main__":
    create_tables()
