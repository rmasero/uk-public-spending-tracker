import pandas as pd
import requests
from io import BytesIO, StringIO
from bs4 import BeautifulSoup
import os

council_name = "Blaby"
csv_url = "https://www.blaby.gov.uk/open-data/payments.csv"
payments_page = "https://www.blaby.gov.uk/your-council/performance-and-budgets/payments-to-suppliers/"

def _update_this_file(new_url):
    """Update this Python file with the discovered csv_url."""
    path = os.path.abspath(__file__)
    with open(path, "r") as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("csv_url ="):
            lines[i] = f'csv_url = "{new_url}"\n'
    with open(path, "w") as f:
        f.writelines(lines)

def _scrape_and_combine_csvs():
    r = requests.get(payments_page)
    soup = BeautifulSoup(r.content, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".csv" in href.lower():
            if not href.startswith("http"):
                href = "https://www.blaby.gov.uk" + href
            links.append(href)
    dfs = []
    for url in links:
        try:
            df = pd.read_csv(url)
            dfs.append(df)
        except Exception:
            continue
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

def fetch_payments():
    # Try direct CSV
    try:
        r = requests.get(csv_url, timeout=10)
        df = pd.read_csv(BytesIO(r.content))
        if not df.empty:
            return [
                {
                    "council": council_name,
                    "payment_date": row.get("Date"),
                    "supplier": row.get("Supplier"),
                    "description": row.get("Purpose"),
                    "category": row.get("Department", ""),
                    "amount_gbp": row.get("Amount"),
                    "invoice_ref": row.get("InvoiceRef", "")
                }
                for _, row in df.iterrows()
            ]
    except Exception:
        pass

    # Scrape for monthly CSVs
    df = _scrape_and_combine_csvs()
    if not df.empty:
        # Update this file to use the first monthly CSV as csv_url for next time
        _update_this_file(df.attrs.get('csv_url', csv_url))
        return [
            {
                "council": council_name,
                "payment_date": row.get("Date"),
                "supplier": row.get("Supplier"),
                "description": row.get("Purpose"),
                "category": row.get("Department", ""),
                "amount_gbp": row.get("Amount"),
                "invoice_ref": row.get("InvoiceRef", "")
            }
            for _, row in df.iterrows()
        ]
    return []
