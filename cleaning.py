import re
from datetime import datetime
import pandas as pd

def clean_supplier(supplier):
    if supplier is None:
        return ""
    s = str(supplier).strip()
    s = re.sub(r"\s+", " ", s)
    return s.title()

def clean_amount(amount):
    if amount is None:
        return 0.0
    try:
        s = str(amount).replace("£", "").replace(",", "").strip()
        return float(s) if s else 0.0
    except Exception:
        return 0.0

# Returns YYYY-MM-DD or "" if unparseable
def clean_date(date_str):
    if date_str is None or str(date_str).strip() == "":
        return ""
    if isinstance(date_str, datetime):
        return date_str.strftime("%Y-%m-%d")
    s = str(date_str).strip()
    # Robust parse (UK-first)
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="raise")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%d %b %Y", "%d %B %Y"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                continue
    return ""

def clean_description(desc):
    if desc is None:
        return ""
    s = re.sub(r"\s+", " ", str(desc)).strip()
    return s[:2000]  # tame very long cells

def normalize_record(record):
    return {
        "council": str(record.get("council", "")).strip(),
        "payment_date": clean_date(record.get("payment_date")),
        "supplier": clean_supplier(record.get("supplier")),
        "description": clean_description(record.get("description")),
        "category": str(record.get("category", "")).strip(),
        "amount_gbp": clean_amount(record.get("amount_gbp")),
        "invoice_ref": str(record.get("invoice_ref", "")).strip(),
    }
