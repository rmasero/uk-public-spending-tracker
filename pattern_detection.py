import sqlite3
from typing import Tuple, List

from db_schema import DB_NAME

def detect_anomalies(council: str) -> Tuple[List[tuple], List[tuple], List[tuple], List[tuple]]:
    """
    Returns 4 anomaly sets for a given council:
      - large payments (>£100k)
      - frequent monthly payments (>5 per supplier per month)
      - duplicate invoice references
      - payments without invoice reference
    """
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("""
        SELECT id, council, supplier, amount_gbp, payment_date
        FROM payments
        WHERE council = ? AND amount_gbp > 100000
        ORDER BY amount_gbp DESC
    """, (council,))
    large = c.fetchall()

    c.execute("""
        SELECT council, supplier, strftime('%Y-%m', payment_date) AS ym, COUNT(*) AS cnt, SUM(amount_gbp) AS total
        FROM payments
        WHERE council = ?
        GROUP BY council, supplier, ym
        HAVING cnt > 5
        ORDER BY cnt DESC
    """, (council,))
    frequent = c.fetchall()

    c.execute("""
        SELECT invoice_ref, COUNT(*) AS cnt, SUM(amount_gbp) AS total
        FROM payments
        WHERE council = ? AND invoice_ref IS NOT NULL AND TRIM(invoice_ref) <> ''
        GROUP BY invoice_ref
        HAVING cnt > 1
        ORDER BY cnt DESC
    """, (council,))
    dup_inv = c.fetchall()

    c.execute("""
        SELECT id, supplier, amount_gbp, payment_date, description
        FROM payments
        WHERE council = ? AND (invoice_ref IS NULL OR TRIM(invoice_ref) = '')
        ORDER BY payment_date DESC
    """, (council,))
    no_inv = c.fetchall()

    conn.close()
    return large, frequent, dup_inv, no_inv
