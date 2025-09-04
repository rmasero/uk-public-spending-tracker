import time
from typing import List, Tuple, Dict, Any
from io import BytesIO

import pandas as pd
import requests

# --- Generic CSV normalization for councils without custom fetchers ---
def fetch_new_council_csv(url: str, council_name: str, timeout: int = 8) -> list:
    """
    Fetch a CSV and map common columns into our schema.
    """
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()

    # Try UTF-8 first, then fall back
    try:
        df = pd.read_csv(BytesIO(r.content))
    except UnicodeDecodeError:
        df = pd.read_csv(BytesIO(r.content), encoding="ISO-8859-1")
    except Exception:
        # Some councils publish semicolon-delimited
        df = pd.read_csv(BytesIO(r.content), sep=";")

    # Flexible column mapping
    colmap = {
        "payment_date": ["payment_date", "date", "Payment Date", "Date", "PaymentDate"],
        "supplier": ["supplier", "Supplier", "Supplier Name", "supplier_name", "SupplierName"],
        "description": ["description", "Description", "purpose", "Purpose", "Details"],
        "category": ["category", "Department", "Service Area", "Cost Centre", "ServiceArea", "Directorate"],
        "amount_gbp": ["amount", "Amount", "Amount Paid", "AmountPaid", "Net Amount", "NetAmount", "Gross Amount", "Value"],
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

# --- Discovery via data.gov.uk CKAN API (no caching) ---
CKAN_SEARCH = "https://data.gov.uk/api/3/action/package_search"
QUERY_TERMS = [
    '"payments to suppliers"',
    '"expenditure over 500"',
    '"spend over 500"',
    '"supplier payments"',
    '"payments over 250"',
    '"payments over 500"',
]
Q = " OR ".join(QUERY_TERMS)

def _page(q: str, rows: int, start: int, timeout: int = 20) -> Dict[str, Any]:
    r = requests.get(CKAN_SEARCH, params={"q": q, "rows": rows, "start": start}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def discover_new_councils(max_pages: int = 20, rows_per_page: int = 1000, sleep_between: float = 0.25) -> List[Tuple[str, str]]:
    """
    Returns (council_name, csv_url) pairs for all CSV resources that look like
    local authority spending datasets.
    """
    pairs: List[Tuple[str, str]] = []
    start = 0
    total = None

    for _ in range(max_pages):
        data = _page(Q, rows_per_page, start)
        result = data.get("result", {})
        if total is None:
            total = int(result.get("count", 0))
        packages = result.get("results", []) or []
        if not packages:
            break

        for pkg in packages:
            org = pkg.get("organization") or {}
            council = (org.get("title") or org.get("name") or "").strip() or (pkg.get("title") or "").strip()
            for res in pkg.get("resources", []) or []:
                fmt = str(res.get("format", "")).lower()
                url = res.get("url") or res.get("download_url")
                if fmt == "csv" and url:
                    pairs.append((council, url))

        start += rows_per_page
        if total is not None and start >= total:
            break
        time.sleep(sleep_between)

    # de-duplicate while preserving order
    seen = set()
    out = []
    for name, url in pairs:
        key = (name, url)
        if key not in seen:
            seen.add(key)
            out.append((name, url))
    return out
