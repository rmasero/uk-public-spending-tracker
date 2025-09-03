import sqlite3

DB_NAME = "spend.db"

def create_tables():
    """
    Creates the SQLite database and required tables if they do not exist.
    """
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Payments table
    c.execute('''
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
        hash TEXT
    )
    ''')

    # Feedback table
    c.execute('''
    CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payment_id INTEGER,
        user_name TEXT,
        comment TEXT,
        rating INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    conn.commit()
    conn.close()
    print(f"{DB_NAME} is ready with payments and feedback tables.")

# Auto-create database if run directly
if __name__ == "__main__":
    create_tables()
