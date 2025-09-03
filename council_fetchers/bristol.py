import pandas as pd
import requests
from io import BytesIO

def fetch_payments():
    url = "https://data.gov.uk/dataset/bristol-city-payments.csv"
    r = requests.get(url)
    df = pd.read_csv(BytesIO(r.content))
    
    payments = []
    for _, row in df.iterrows():
        payments.append({
            "council": "Bristol",
            "payment_date": row.get("PaymentDate"),
            "supplier": row.get("SupplierName"),
            "description": row.get("Description"),
            "category": row.get("Department", ""),
            "amount_gbp": row.get("Amount"),
            "invoice_ref": row.get("TransactionNumber", "")
        })
    return payments
