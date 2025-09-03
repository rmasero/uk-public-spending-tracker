import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

council_name = "Bristol"
csv_url = "https://data.gov.uk/dataset/bristol-city-payments.csv"  # Fallback

def fetch_payments():
    # Try static CSV first
    try:
        r = requests.get(csv_url, timeout=10)
        if r.ok and r.headers.get("content-type", "").startswith("text/csv"):
            df = pd.read_csv(BytesIO(r.content))
            if not df.empty:
                return _format_bristol(df)
    except Exception:
        pass

    # Scrape all CSVs from data.gov.uk
    page_url = "https://www.data.gov.uk/dataset/2dd91623-cbbc-4837-ba9f-1cfd1f38bb07/local-authority-spend-over-500-bristol-city-council"
    r = requests.get(page_url)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a["href"] for a in soup.select('a.resource-url-analytics[href$=".csv"]')]
    all_payments = []
    for link in links:
        try:
            df = pd.read_csv(link)
            if not df.empty:
                all_payments.extend(_format_bristol(df))
        except Exception:
            continue
    return all_payments

def _format_bristol(df):
    return [
        {
            "council": council_name,
            "payment_date": row.get("PaymentDate"),
            "supplier": row.get("SupplierName") or row.get("Supplier"),
            "description": row.get("Description"),
            "category": row.get("Department", ""),
            "amount_gbp": row.get("Amount") or row.get("AmountPaid"),
            "invoice_ref": row.get("TransactionNumber", "") or row.get("InvoiceNumber", "")
        }
        for _, row in df.iterrows()
    ]
