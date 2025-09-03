import pandas as pd
import requests
from io import BytesIO

council_name = "City of York"
csv_url = "https://data.gov.uk/dataset/city-of-york-payments.csv"

def fetch_payments():
    url = csv_url
    r = requests.get(url)
    df = pd.read_csv(BytesIO(r.content))
    payments = []
    for _, row in df.iterrows():
        payments.append({
            "council": council_name,
            "payment_date": row.get("PaymentDate"),
            "supplier": row.get("Supplier"),
            "description": row.get("Description"),
            "category": row.get("ServiceArea", ""),
            "amount_gbp": row.get("AmountPaid"),
            "invoice_ref": row.get("InvoiceNumber", "")
        })
    return payments
