import sqlite3
import hashlib
from typing import Iterable, Dict, List, Tuple

from cleaning import normalize_record
from db_schema import create_tables, DB_NAME
from geocode import geocode_address

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME)
    # Faster local SQLite with sane durability
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn

def _row_hash(norm: Dict) -> str:
    # Hash core business fields (stable across reimports)
    key = (
        norm.get("council", ""),
        norm.get("payment_date", ""),
        norm.get("supplier", ""),
        norm.get("description", ""),
        norm.get("category", ""),
        f"{float(norm.get('amount_gbp', 0.0)):.2f}",
        norm.get("invoice_ref", ""),
    )
    return hashlib.sha256("|".join(map(str, key)).encode("utf-8")).hexdigest()

def insert_records(records: Iterable[Dict], do_geocode: bool = False) -> Tuple[int, int]:
    """
    Insert records into payments. Returns (inserted, skipped)
    where skipped includes duplicates or rows without a parsable date.
    """
    create_tables()
    to_insert: List[Tuple] = []
    skipped = 0

    for r in records:
        norm = normalize_record(r)
        if not norm.get("payment_date"):
            skipped += 1
            continue
        h = _row_hash(norm)
        lat = lon = None
        if do_geocode and norm.get("supplier"):
            lat, lon = geocode_address(norm["supplier"])
        to_insert.append((
            norm["council"], norm["payment_date"], norm["supplier"], norm["description"],
            norm["category"], float(norm["amount_gbp"]), norm["invoice_ref"], lat, lon, h
        ))

    if not to_insert:
        return (0, skipped)

    conn = _connect()
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR IGNORE INTO payments
        (council, payment_date, supplier, description, category, amount_gbp, invoice_ref, lat, lon, hash)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, to_insert)
    inserted = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    conn.close()
    return (inserted, skipped)

if __name__ == "__main__":
    create_tables()
    print("Ready to insert records.")
