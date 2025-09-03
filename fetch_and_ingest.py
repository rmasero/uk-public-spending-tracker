import sqlite3
import hashlib
from cleaning import normalize_record
from db_schema import create_tables
from geocode import geocode_address

DB_NAME = "spend.db"

def insert_records(records):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for r in records:
        norm = normalize_record(r)
        record_hash = hashlib.sha256(str(norm).encode()).hexdigest()
        
        lat, lon = geocode_address(norm['supplier'])
        
        c.execute('''
            INSERT INTO payments 
            (council, payment_date, supplier, description, category, amount_gbp, invoice_ref, lat, lon, hash)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        ''', (
            norm['council'], norm['payment_date'], norm['supplier'], norm['description'],
            norm['category'], norm['amount_gbp'], norm['invoice_ref'], lat, lon, record_hash
        ))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_tables()
    print("Ready to insert records.")
