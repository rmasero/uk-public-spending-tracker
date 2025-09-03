import pandas as pd
import requests
from io import BytesIO

def fetch_payments():
    url = "https://data.gov.uk/dataset/city-of-york-payments.csv"
    r = requests.get(url)
    df = pd.read_csv(BytesIO(r.content))
    
    payments = []
    for _, row in df.iterrows():
        payments.append({
            "council": "City of York",
            "payment_date": row.get("PaymentDate"),
            "supplier": row.get("Supplier"),
            "description": row.get("Description"),
            "category": row.get("ServiceArea", ""),
            "amount_gbp": row.get("AmountPaid"),
            "invoice_ref": row.get("InvoiceNumber", "")
        })
    return payments
