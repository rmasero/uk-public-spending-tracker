import requests
import pandas as pd
from io import BytesIO
from typing import List, Tuple
from council_fetchers import FETCHERS

# Free public CKAN API for UK data
DATA_GOV_API_URL = (
    "https://ckan.publishing.service.gov.uk/api/3/action/package_search"
    "?q=payments+to+suppliers&rows=50"
)

def fetch_new_council_csv(url: str, council_name: str) -> list:
    """
    Generic CSV fetcher for councils without a custom parser.
    Returns a list of dicts with normalized keys.
    """
    if council_name in FETCHERS:
        return FETCHERS[council_name]()

    r = requests.get(url, timeout=30)
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
        "amount_gbp": ["amount", "Amount", "Amount Paid", "AmountPaid", "Net Amount"],
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
    Query data.gov.uk and return [(name, csv_url)]
    """
    r = requests.get(DATA_GOV_API_URL, timeout=30)
    r.raise_for_status()
    results = r.json().get("result", {}).get("results", [])
    discovered = []
    for pkg in results:
        name = pkg.get("title")
        for res in pkg.get("resources", []):
            if str(res.get("format", "")).lower() == "csv" and res.get("url"):
                discovered.append((name, res["url"]))
                break
    return discovered
