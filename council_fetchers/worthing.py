import pandas as pd
import requests
from io import BytesIO

council_name = "Worthing"
csv_url = "https://www.adur-worthing.gov.uk/open-data/payments.csv"

def fetch_payments():
    url = csv_url
    r = requests.get(url)
    df = pd.read_csv(BytesIO(r.content))
    payments = []
    for _, row in df.iterrows():
        payments.append({
            "council": council_name,
            "payment_date": row.get("Date"),
            "supplier": row.get("Supplier Name"),
            "description": row.get("Description"),
            "category": row.get("Department", ""),
            "amount_gbp": row.get("Amount"),
            "invoice_ref": row.get("Invoice Ref", "")
        })
    return payments
