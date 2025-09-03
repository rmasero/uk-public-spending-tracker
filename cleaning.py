import re
from datetime import datetime

def clean_supplier(supplier):
    if not supplier:
        return ""
    return supplier.strip().title()

def clean_amount(amount):
    try:
        return float(str(amount).replace(',', '').replace('£',''))
    except:
        return 0.0

def clean_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
    except:
        try:
            return datetime.strptime(date_str, '%d/%m/%Y').date().isoformat()
        except:
            return date_str

def clean_description(desc):
    if not desc:
        return ""
    return re.sub(r'\s+', ' ', desc.strip())

def normalize_record(record):
    return {
        "council": record.get("council", "").strip(),
        "payment_date": clean_date(record.get("payment_date")),
        "supplier": clean_supplier(record.get("supplier")),
        "description": clean_description(record.get("description")),
        "category": record.get("category", "").strip(),
        "amount_gbp": clean_amount(record.get("amount_gbp")),
        "invoice_ref": record.get("invoice_ref", "").strip()
    }
