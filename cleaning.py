from datetime import datetime
from typing import Any, Dict, Optional

def clean_supplier(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()

def clean_amount(a: Any) -> float:
    try:
        s = str(a).replace(",", "").replace("£", "").strip()
        if s == "":
            return 0.0
        return float(s)
    except Exception:
        return 0.0

def _parse_date(ds: str) -> Optional[str]:
    # Try common formats
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%d %b %Y", "%Y-%m"):
        try:
            dt = datetime.strptime(ds.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    # Fallback: try month-only/unknown → keep as-is string if it looks like a year-month
    try:
        # Very loose attempt: if it parses as year only
        if len(ds.strip()) == 4 and ds.strip().isdigit():
            return f"{ds.strip()}-01-01"
    except Exception:
        pass
    return None

def clean_date(d: Any) -> Optional[str]:
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    ds = str(d).strip()
    if ds == "":
        return None
    # Excel-like integer serialized? (very rough)
    try:
        n = float(ds)
        # guard unlikely extremes
        if 35000 < n < 60000:
            base = datetime(1899, 12, 30)
            return (base).strftime("%Y-%m-%d")
    except Exception:
        pass
    return _parse_date(ds)

def normalize_record(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "council": (r.get("council") or "").strip(),
        "payment_date": clean_date(r.get("payment_date")),
        "supplier": clean_supplier(r.get("supplier")),
        "description": (r.get("description") or "").strip(),
        "category": (r.get("category") or "").strip(),
        "amount_gbp": clean_amount(r.get("amount_gbp")),
        "invoice_ref": (r.get("invoice_ref") or "").strip(),
    }
