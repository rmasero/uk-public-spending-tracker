import pandas as pd
import requests
from io import BytesIO

def fetch_payments():
    url = "https://www.newcastle.gov.uk/open-data/payments.csv"
    r = requests.get(url)
    df = pd.read_csv(BytesIO(r.content))
    
    payments = []
    for _, row in df.iterrows():
        payments.append({
            "council": "Newcastle",
            "payment_date": row.get("PaymentDate"),
            "supplier": row.get("Supplier"),
            "description": row.get("Purpose"),
            "category": row.get("Department", ""),
            "amount_gbp": row.get("Amount"),
            "invoice_ref": row.get("InvoiceRef", "")
        })
    return payments
