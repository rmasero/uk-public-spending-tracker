import time
from io import BytesIO
from typing import List, Tuple

import pandas as pd
import requests

from council_fetchers import FETCHERS
from councils_catalog import build_catalog, load_catalog

def fetch_new_council_csv(url: str, council_name: str, timeout: int = 10) -> list:
    """
    Generic CSV fetcher for councils without a custom parser.
    Returns a list of dicts with normalized keys.
    """
    # Respect custom fetchers if available
    if council_name in FETCHERS and callable(FETCHERS[council_name]):
        return FETCHERS[council_name]()

    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    content = r.content
    try:
        df = pd.read_csv(BytesIO(content))
    except UnicodeDecodeError:
        df = pd.read_csv(BytesIO(content), encoding="ISO-8859-1")

    # Flexible column mapping
    colmap = {
        "payment_date": ["payment_date", "date", "Payment Date", "Date"],
        "supplier": ["supplier", "Supplier", "Supplier Name", "supplier_name"],
        "description": ["description", "Description", "purpose", "Purpose"],
        "category": ["category", "Department", "Service Area", "Cost Centre", "ServiceArea"],
        "amount_gbp": ["amount", "Amount", "Amount Paid", "AmountPaid", "Net Amount", "Gross Amount"],
        "invoice_ref": ["invoice", "Invoice", "Invoice Ref", "InvoiceRef", "invoice_number", "Invoice Number"],
    }

    cols_lower = {c.lower(): c for c in df.columns}
    def pick(options):
        for o in options:
            if o.lower() in cols_lower:
                return cols_lower[o.lower()]
        return None

    c_date = pick(colmap["payment_date"])
    c_supplier = pick(colmap["supplier"])
    c_desc = pick(colmap["description"])
    c_cat = pick(colmap["category"])
    c_amt = pick(colmap["amount_gbp"])
    c_inv = pick(colmap["invoice_ref"])

    payments = []
    for _, row in df.iterrows():
        payments.append({
            "council": council_name,
            "payment_date": row.get(c_date) if c_date else None,
            "supplier": row.get(c_supplier) if c_supplier else "",
            "description": row.get(c_desc) if c_desc else "",
            "category": row.get(c_cat) if c_cat else "",
            "amount_gbp": row.get(c_amt) if c_amt else 0,
            "invoice_ref": row.get(c_inv) if c_inv else "",
        })
    return payments

def discover_new_councils() -> List[Tuple[str, str]]:
    """
    Return [(council_name, csv_url)] for all councils discovered via the cached or freshly-built catalog.
    """
    catalog = load_catalog()
    if not catalog:
        # Build with pagination (free CKAN API)
        catalog = build_catalog()

    pairs: List[Tuple[str, str]] = []
    for council, payload in catalog.items():
        for url in payload.get("csv_urls", []):
            pairs.append((council, url))
    return pairs
