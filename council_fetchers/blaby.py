import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

council_name = "Blaby"
csv_url = "https://www.blaby.gov.uk/open-data/payments.csv"  # Fallback/legacy

def fetch_payments():
    # Try static CSV first
    try:
        r = requests.get(csv_url, timeout=10)
        if r.ok and r.headers.get("content-type", "").startswith("text/csv"):
            df = pd.read_csv(BytesIO(r.content))
            if not df.empty:
                return _format_blaby(df)
    except Exception:
        pass

    # Scrape monthly CSVs
    page_url = "https://www.blaby.gov.uk/your-council/performance-and-budgets/payments-to-suppliers/"
    r = requests.get(page_url)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.select('a[href$=".csv"]')]
    all_payments = []
    for link in links:
        if not link.startswith("http"):
            link = "https://www.blaby.gov.uk" + link
        try:
            df = pd.read_csv(link)
            if not df.empty:
                all_payments.extend(_format_blaby(df))
        except Exception:
            continue
    return all_payments

def _format_blaby(df):
    # Column names may vary per file; best effort normalization
    return [
        {
            "council": council_name,
            "payment_date": row.get("Date") or row.get("Payment Date"),
            "supplier": row.get("Supplier"),
            "description": row.get("Purpose") or row.get("Description"),
            "category": row.get("Department", ""),
            "amount_gbp": row.get("Amount") or row.get("Amount (£)"),
            "invoice_ref": row.get("InvoiceRef", "") or row.get("Invoice Ref", "")
        }
        for _, row in df.iterrows()
    ]
