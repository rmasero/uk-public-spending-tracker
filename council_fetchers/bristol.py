import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import re

council_name = "Bristol"
csv_url = None  # Will be set after scraping

def get_monthly_csv_urls():
    """Scrape the data.gov.uk dataset page for Bristol for CSV links."""
    index_url = "https://www.data.gov.uk/dataset/2dd91623-cbbc-4837-ba9f-1cfd1f38bb07/local-authority-spend-over-500-bristol-city-council"
    r = requests.get(index_url)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [
        a['href']
        for a in soup.find_all("a", href=True)
        if ".csv" in a['href'] and "resources" in a['href']
    ]
    # Make absolute URLs if needed
    links = [
        l if l.startswith("http") else "https://www.data.gov.uk" + l
        for l in links
    ]
    return links

def fetch_payments():
    global csv_url
    csvs = get_monthly_csv_urls()
    payments = []
    for url in csvs:
        try:
            r = requests.get(url)
            df = pd.read_csv(BytesIO(r.content))
            csv_url = url  # Keep last valid as the "csv_url"
            for _, row in df.iterrows():
                payments.append({
                    "council": council_name,
                    "payment_date": row.get("PaymentDate"),
                    "supplier": row.get("SupplierName", row.get("Supplier")),
                    "description": row.get("Description"),
                    "category": row.get("Department", ""),
                    "amount_gbp": row.get("Amount"),
                    "invoice_ref": row.get("TransactionNumber", "")
                })
        except Exception:
            continue
    return payments
