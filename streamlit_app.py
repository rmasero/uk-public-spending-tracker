import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from fetch_and_ingest import insert_records
from db_schema import create_tables
from pattern_detection import detect_anomalies
from council_auto_discovery import discover_new_councils, fetch_new_council_csv
from geocode import geocode_address
import plotly.express as px
import glob
import importlib.util
import os
import time
import requests

DB_NAME = "spend.db"

# --------------------------
# Initialize database
# --------------------------
with st.spinner("Setting up database..."):
    create_tables()

# --------------------------
# Sidebar: Council selection
# --------------------------
st.sidebar.title("Public Spending Tracker")

# --------------------------
# Progress bar and status
# --------------------------
progress_text = "Starting up, please wait..."
progress_bar = st.sidebar.progress(0, text=progress_text)

# --------------------------
# Helper: Load council fetchers
# --------------------------
def load_predefined_councils():
    fetcher_files = glob.glob("council_fetchers/*.py")
    councils_loaded = []
    errors = []
    for i, file in enumerate(fetcher_files, 1):
        module_name = os.path.splitext(os.path.basename(file))[0]
        spec = importlib.util.spec_from_file_location(module_name, file)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            if hasattr(module, "COUNCIL_NAME") and hasattr(module, "CSV_URL"):
                council_name = module.COUNCIL_NAME
                csv_url = module.CSV_URL
                try:
                    progress_bar.progress(min(5 + int(i * (15/len(fetcher_files))), 20), text=f"Loading predefined: {council_name}")
                    records = fetch_new_council_csv(csv_url, council_name)
                    insert_records(records)
                    councils_loaded.append(council_name)
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                    errors.append((council_name, csv_url))
                    continue  # skip this, will retry later
        except Exception:
            continue
    return councils_loaded, errors

def try_failed_predefined(failed):
    retried = []
    for i, (council_name, csv_url) in enumerate(failed, 1):
        try:
            progress_bar.progress(min(20 + int(i * (10/max(1, len(failed)))), 30), text=f"Retrying: {council_name}")
            records = fetch_new_council_csv(csv_url, council_name)
            insert_records(records)
            retried.append((council_name, csv_url))
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            continue  # skip again silently
    return retried

def try_failed_new_councils(failed):
    retried = []
    for i, (council_name, csv_url) in enumerate(failed, 1):
        try:
            progress_bar.progress(min(45 + int(i * (5/max(1, len(failed)))), 50), text=f"Retrying new: {council_name}")
            records = fetch_new_council_csv(csv_url, council_name)
            insert_records(records)
            retried.append((council_name, csv_url))
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            continue  # skip again silently
    return retried

# 1. Load predefined councils from council_fetchers folder
progress_bar.progress(3, text="Loading predefined councils...")
predefined_loaded, predefined_failed = load_predefined_councils()
progress_bar.progress(22, text="Predefined councils loaded.")

# 2. Try failed predefined councils again (retry after all others)
progress_bar.progress(25, text="Retrying failed predefined councils...")
retried_predefined = try_failed_predefined(predefined_failed)
progress_bar.progress(30, text="Predefined retry done.")

# 3. Discover and ingest new councils automatically (after predefined)
progress_bar.progress(32, text="Discovering new councils...")
new_councils = discover_new_councils()
new_failed = []
if new_councils:
    for i, (council_name, csv_url) in enumerate(new_councils, 1):
        try:
            progress_bar.progress(33 + int(i * (12/len(new_councils))), text=f"Ingesting new: {council_name}")
            records = fetch_new_council_csv(csv_url, council_name)
            insert_records(records)
            time.sleep(0.05)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            new_failed.append((council_name, csv_url))
            continue
else:
    progress_bar.progress(45, text="No new councils discovered.")

# 4. Try failed new councils again after all others
progress_bar.progress(48, text="Retrying failed new councils...")
retried_new = try_failed_new_councils(new_failed)
progress_bar.progress(52, text="All councils loaded.")

# 5. Automatically look for more councils after all others
progress_bar.progress(55, text="Scanning for more councils...")
more_new_councils = discover_new_councils()
if more_new_councils:
    for i, (council_name, csv_url) in enumerate(more_new_councils, 1):
        try:
            progress_bar.progress(56 + int(i * (4/len(more_new_councils))), text=f"Loading more: {council_name}")
            records = fetch_new_council_csv(csv_url, council_name)
            insert_records(records)
            time.sleep(0.05)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            continue  # skip silently

progress_bar.progress(60, text="Council loading complete.")

# 6. Fetch list of councils from DB
progress_bar.progress(65, text="Fetching list of councils...")
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT DISTINCT council FROM payments")
councils = [row[0] for row in c.fetchall()]
conn.close()

if not councils:
    st.error("No councils found in database. Please check your data source.")
    st.stop()

selected_council = st.sidebar.selectbox("Select council", sorted(councils))

# 7. Filters
progress_bar.progress(70, text="Loading filters...")
st.sidebar.subheader("Filters")
start_date = st.sidebar.date_input("Start date", datetime(2023,1,1))
end_date = st.sidebar.date_input("End date", datetime.today())
supplier_search = st.sidebar.text_input("Supplier search")

# 8. Fetch filtered data
progress_bar.progress(75, text="Fetching payments data...")
conn = sqlite3.connect(DB_NAME)
query = "SELECT * FROM payments WHERE council = ? AND payment_date BETWEEN ? AND ?"
params = [selected_council, start_date.isoformat(), end_date.isoformat()]
if supplier_search:
    query += " AND supplier LIKE ?"
    params.append(f"%{supplier_search}%")

df = pd.read_sql_query(query, conn, params=params)
conn.close()
progress_bar.progress(80, text="Payments data loaded.")

# 9. Display summary stats
st.title(f"{selected_council} Public Spending")
st.markdown(f"Showing payments from {start_date} to {end_date}")
st.write(f"**Total payments:** £{df['amount_gbp'].sum():,.2f}")
st.write(f"**Number of transactions:** {len(df)}")

# 10. Top suppliers
progress_bar.progress(83, text="Calculating top suppliers...")
top_suppliers = df.groupby("supplier")['amount_gbp'].sum().sort_values(ascending=False).head(10).reset_index()
fig1 = px.bar(top_suppliers, x="supplier", y="amount_gbp", title="Top 10 Suppliers by Payment Amount")
st.plotly_chart(fig1)

# 11. Payments over time
progress_bar.progress(86, text="Processing payments over time...")
df['payment_date'] = pd.to_datetime(df['payment_date'])
payments_by_month = df.groupby(df['payment_date'].dt.to_period("M"))['amount_gbp'].sum().reset_index()
payments_by_month['payment_date'] = payments_by_month['payment_date'].dt.to_timestamp()
fig2 = px.line(payments_by_month, x="payment_date", y="amount_gbp", title="Payments Over Time")
st.plotly_chart(fig2)

# 12. Map visualization
progress_bar.progress(88, text="Preparing map visualization...")
df_map = df.dropna(subset=['lat','lon'])
if not df_map.empty:
    st.subheader("Payments Map")
    fig_map = px.scatter_mapbox(
        df_map, lat="lat", lon="lon", hover_name="supplier", hover_data=["amount_gbp","description"],
        color="amount_gbp", size="amount_gbp", zoom=8, mapbox_style="open-street-map"
    )
    st.plotly_chart(fig_map)

# 13. Anomaly detection with filters
progress_bar.progress(90, text="Detecting anomalies...")
st.subheader("Anomalies / Alerts")
anomaly_options = [
    "Large payments (>£100k)",
    "Frequent payments (>5 per month)",
    "Duplicate invoice numbers",
    "Payments without invoices",
    "Single supplier dominance"
]
selected_anomalies = st.multiselect("Select anomaly types to display", anomaly_options, default=anomaly_options)

conn = sqlite3.connect(DB_NAME)
c = conn.cursor()

# Large payments
if "Large payments (>£100k)" in selected_anomalies:
    c.execute("SELECT id, council, supplier, amount_gbp, payment_date FROM payments WHERE amount_gbp > 100000")
    large_df = pd.DataFrame(c.fetchall(), columns=["id","council","supplier","amount_gbp","payment_date"])
    if not large_df.empty:
        st.markdown("**Large payments (>£100k):**")
        st.dataframe(large_df[large_df['council']==selected_council])

# Frequent payments
if "Frequent payments (>5 per month)" in selected_anomalies:
    c.execute('''
        SELECT id, council, supplier, COUNT(*) as count, SUM(amount_gbp) as total_amount
        FROM payments
        GROUP BY council, supplier, strftime('%Y-%m', payment_date)
        HAVING count > 5
    ''')
    frequent_df = pd.DataFrame(c.fetchall(), columns=["id","council","supplier","count","total_amount"])
    if not frequent_df.empty:
        st.markdown("**Frequent payments (>5 per month):**")
        st.dataframe(frequent_df[frequent_df['council']==selected_council])

# Duplicate invoice numbers
if "Duplicate invoice numbers" in selected_anomalies:
    c.execute('''
        SELECT invoice_ref, COUNT(*) as cnt, SUM(amount_gbp) as total_amount
        FROM payments
        WHERE invoice_ref != ''
        GROUP BY invoice_ref
        HAVING cnt > 1
    ''')
    dup_df = pd.DataFrame(c.fetchall(), columns=["invoice_ref","count","total_amount"])
    if not dup_df.empty:
        st.markdown("**Duplicate invoice numbers:**")
        st.dataframe(dup_df)

# Payments without invoices
if "Payments without invoices" in selected_anomalies:
    c.execute("SELECT * FROM payments WHERE invoice_ref IS NULL OR invoice_ref = ''")
    missing_inv_df = pd.DataFrame(c.fetchall(), columns=["id","council","payment_date","supplier","description","category","amount_gbp","invoice_ref","lat","lon","hash"])
    if not missing_inv_df.empty:
        st.markdown("**Payments without invoices:**")
        st.dataframe(missing_inv_df[missing_inv_df['council']==selected_council])

# Single supplier dominance (>50% of total payments)
if "Single supplier dominance" in selected_anomalies:
    c.execute('''
        SELECT supplier, SUM(amount_gbp) as total_amount
        FROM payments
        WHERE council = ?
        GROUP BY supplier
        ORDER BY total_amount DESC
        LIMIT 1
    ''', (selected_council,))
    row = c.fetchone()
    if row:
        supplier, total_amount = row
        c.execute("SELECT SUM(amount_gbp) FROM payments WHERE council=?", (selected_council,))
        total_council = c.fetchone()[0]
        if total_council > 0 and (total_amount/total_council) > 0.5:
            st.markdown(f"**Warning:** Supplier `{supplier}` received more than 50% of total payments (£{total_amount:,.2f})")

conn.close()

progress_bar.progress(93, text="Anomaly detection complete.")

# 14. Citizen feedback
progress_bar.progress(95, text="Loading citizen feedback...")
st.subheader("Citizen Feedback")

conn = sqlite3.connect(DB_NAME)
c = conn.cursor()

# Submit feedback
with st.form("feedback_form"):
    payment_id = st.number_input("Payment ID to comment on", min_value=1, step=1)
    user_name = st.text_input("Your name")
    comment = st.text_area("Comment")
    rating = st.slider("Rating (1-5)", 1, 5, 3)
    submitted = st.form_submit_button("Submit Feedback")
    if submitted:
        c.execute("INSERT INTO feedback (payment_id,user_name,comment,rating) VALUES (?,?,?,?)",
                  (payment_id,user_name,comment,rating))
        conn.commit()
        st.success("Feedback submitted!")

# Display feedback
c.execute("SELECT * FROM feedback WHERE payment_id IN (SELECT id FROM payments WHERE council=?) ORDER BY created_at DESC", (selected_council,))
feedback_df = pd.DataFrame(c.fetchall(), columns=["id","payment_id","user_name","comment","rating","created_at"])
conn.close()

if not feedback_df.empty:
    st.dataframe(feedback_df)

progress_bar.progress(98, text="Citizen feedback loaded.")

# 15. CSV download
progress_bar.progress(100, text="All data loaded!")
csv_data = df.to_csv(index=False).encode('utf-8')
st.download_button(label="Download CSV", data=csv_data, file_name=f"{selected_council}_payments.csv", mime="text/csv")

# Remove the progress bar when done
progress_bar.empty()
