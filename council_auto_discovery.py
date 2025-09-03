import requests
import pandas as pd
from io import BytesIO
import sys
import os

# Add the current directory to Python path to find council_fetchers
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from council_fetchers import FETCHERS
except ImportError:
    # Fallback if council_fetchers can't be imported
    FETCHERS = {}

def fetch_new_council_csv(url, council_name):
    if council_name in FETCHERS:
        return FETCHERS[council_name]()
    
    r = requests.get(url)
    df = pd.read_csv(BytesIO(r.content))
    
    payments = []
    for _, row in df.iterrows():
        payments.append({
            "council": council_name,
            "payment_date": row.get("PaymentDate") or row.get("Date"),
            "supplier": row.get("Supplier") or row.get("Supplier Name"),
            "description": row.get("Description") or row.get("Purpose"),
            "category": row.get("Department") or row.get("ServiceArea",""),
            "amount_gbp": row.get("Amount") or row.get("AmountPaid"),
            "invoice_ref": row.get("InvoiceRef") or row.get("InvoiceNumber","")
        })
    return payments

def discover_new_councils(data_gov_api_url="https://data.gov.uk/api/3/action/package_search?q=spending+csv"):
    r = requests.get(data_gov_api_url)
    results = r.json().get("result", {}).get("results", [])
    discovered = []
    for res in results:
        name = res.get("title")
        resources = res.get("resources", [])
        csv_url = None
        for resrc in resources:
            if resrc.get("format","").lower() == "csv":
                csv_url = resrc.get("url")
                break
        if csv_url:
            discovered.append((name, csv_url))
    return discovered
