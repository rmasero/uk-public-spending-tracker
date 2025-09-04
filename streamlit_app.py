# streamlit_app.py

import streamlit as st
import pandas as pd
import requests

from council_auto_discovery import fetch_new_council_csv
from db import insert_records, get_all_data


# --- Column mappings (normalise council CSVs into our DB schema) ---
COLUMN_MAPPINGS = {
    "supplier name": "supplier",
    "supplier": "supplier",
    "amount": "amount_gbp",
    "net amount": "amount_gbp",
    "value": "amount_gbp",
    "payment date": "payment_date",
    "date": "payment_date",
    "invoice number": "invoice_ref",
    "invoice no": "invoice_ref",
    "description": "description",
    "details": "description",
}


def normalise_dataframe(df, council_name):
    """Map council CSV columns into our standard schema."""
    df.columns = [c.lower().strip() for c in df.columns]

    # Rename where possible
    df = df.rename(columns={k: v for k, v in COLUMN_MAPPINGS.items() if k in df.columns})

    # Ensure all required columns exist
    keep_cols = ["supplier", "amount_gbp", "payment_date", "invoice_ref", "description"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = None

    df = df[keep_cols]
    df["council"] = council_name
    return df


def load_and_insert_new_councils():
    """Discover new council CSVs, fetch them, normalise, insert into DB."""
    st.info("🔍 Discovering councils and fetching CSVs…")
    council_csvs = fetch_new_council_csv()

    inserted_total = 0

    for i, (council, url) in enumerate(council_csvs, start=1):
        st.write(f"[{i}/{len(council_csvs)}] Processing {council} → {url}")

        try:
            # Timeout after 3 seconds per council fetch
            resp = requests.get(url, timeout=3)
            resp.raise_for_status()

            df = pd.read_csv(pd.compat.StringIO(resp.text))
            df = normalise_dataframe(df, council)

            records = df.to_dict(orient="records")
            insert_records(records)
            inserted_total += len(records)

            st.success(f"Inserted {len(records)} rows for {council}")

        except Exception as e:
            st.warning(f"⚠️ Skipping {council} due to error: {e}")

    st.success(f"🎉 Finished: {inserted_total} rows inserted into database.")


def main():
    st.title("UK Public Spending Tracker")

    st.info("Loading data automatically. This may take a few minutes…")

    # Run discovery + load at startup
    load_and_insert_new_councils()

    # Button to refresh/update
    if st.button("🔄 Update Data (may be slow)"):
        load_and_insert_new_councils()

    # Show data preview
    all_data = get_all_data()
    if all_data:
        df = pd.DataFrame(all_data)
        st.write("📊 Data preview:")
        st.dataframe(df.head(50))
    else:
        st.warning("No data available in the database.")


if __name__ == "__main__":
    main()
