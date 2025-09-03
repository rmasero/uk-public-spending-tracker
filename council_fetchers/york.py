import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

council_name = "City of York"
csv_url = None  # Will be set after scraping

def get_monthly_csv_urls():
    index_url = "https://data.yorkopendata.org/dataset/all-payments-to-suppliers"
    r = requests.get(index_url)
    soup = BeautifulSoup(r.text, "html.parser")
    links = [
        a['href']
        for a in soup.find_all("a", href=True)
        if a['href'].lower().endswith(".csv")
    ]
    links = [
        l if l.startswith("http") else "https://data.yorkopendata.org" + l
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
            csv_url = url
            for _, row in df.iterrows():
                payments.append({
                    "council": council_name,
                    "payment_date": row.get("PaymentDate", row.get("Date")),
                    "supplier": row.get("Supplier"),
                    "description": row.get("Description", ""),
                    "category": row.get("ServiceArea", row.get("Department", "")),
                    "amount_gbp": row.get("AmountPaid", row.get("Amount")),
                    "invoice_ref": row.get("InvoiceNumber", row.get("InvoiceRef", ""))
                })
        except Exception:
            continue
    return payments
