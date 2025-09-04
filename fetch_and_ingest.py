import sqlite3
import hashlib
from typing import List, Dict, Tuple

from cleaning import normalize_record
from db_schema import DB_NAME
from geocode import geocode_address

def _hash_norm(norm: Dict) -> str:
    key = (
        norm["council"],
        norm["payment_date"] or "",
        norm["supplier"],
        norm["description"],
        norm["category"],
        f'{norm["amount_gbp"]:.2f}',
        norm["invoice_ref"],
    )
    return hashlib.sha256("|".join(key).encode("utf-8")).hexdigest()

def insert_records(records: List[Dict], do_geocode: bool = False) -> Tuple[int, int]:
    """
    Insert normalized records into SQLite.
    Returns (inserted_count, skipped_count).
    """
    if not records:
        return 0, 0

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    inserted = 0
    skipped = 0

    for r in records:
        norm = normalize_record(r)
        h = _hash_norm(norm)

        lat, lon = (None, None)
        if do_geocode:
            lat, lon = geocode_address(norm["supplier"])

        try:
            c.execute(
                """
                INSERT INTO payments
                (council, payment_date, supplier, description, category, amount_gbp, invoice_ref, lat, lon, hash)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    norm["council"],
                    norm["payment_date"],
                    norm["supplier"],
                    norm["description"],
                    norm["category"],
                    norm["amount_gbp"],
                    norm["invoice_ref"],
                    lat,
                    lon,
                    h,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1  # duplicate via hash
        except Exception:
            skipped += 1  # bad row, skip and continue

    conn.commit()
    conn.close()
    return inserted, skipped
