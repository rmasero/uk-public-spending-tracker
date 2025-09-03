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

DB_NAME = "spend.db"

# --------------------------
# Initialize database
# --------------------------
with st.spinner("Setting up database..."):
    create_tables()

st.sidebar.title("Public Spending Tracker")
progress_text = "Starting up, please wait..."
progress_bar = st.sidebar.progress(0, text=progress_text)

# --------------------------
# Load council fetchers (predefined councils) first
# --------------------------
progress_bar.progress(2, text="Locating predefined councils...")
COUNCIL_FETCHERS_DIR = "council_fetchers"
fetcher_files = sorted(
    [f for f in glob.glob(os.path.join(COUNCIL_FETCHERS_DIR, "*.py")) if not f.endswith("__init__.py")]
)
predefined_councils = []

# Import council_name, csv_url, and fetch_payments if available from each fetcher
for filepath in fetcher_files:
    try:
        spec = importlib.util.spec_from_file_location("fetcher", filepath)
        fetcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fetcher)
        council_name = getattr(fetcher, "council_name", None)
        csv_url = getattr(fetcher, "csv_url", None)
        # Store also the fetch_payments function if it exists
        fetch_payments_func = getattr(fetcher, "fetch_payments", None)
        if council_name and csv_url:
            predefined_councils.append(
                {
                    "council_name": council_name,
                    "csv_url": csv_url,
                    "fetcher_path": filepath,
                    "fetch_payments_func": fetch_payments_func,
                }
            )
    except Exception as e:
        # Don't show error, just skip
        pass

def try_fetch_predefined_council(council):
    """Try to fetch payments using custom fetch_payments, else fallback to generic CSV loader."""
    records = None
    try:
        if council["fetch_payments_func"]:
            # Custom fetcher
            records = council["fetch_payments_func"]()
        else:
            # Fallback to generic CSV loader
            records = fetch_new_council_csv(council["csv_url"], council["council_name"])
    except Exception:
        records = None
    return records

def try_fetch_new_council_csv(csv_url, council_name, timeout=15):
    try:
        return fetch_new_council_csv(csv_url, council_name, timeout=timeout)
    except Exception:
        return None

def load_council_data(councils, progress_start, progress_range, desc_prefix, predefined=False):
    failed = []
    total = len(councils)
    for i, council in enumerate(councils):
        pct = progress_start + int(i * (progress_range / max(1, total)))
        progress_bar.progress(pct, text=f"{desc_prefix} {council['council_name'] if predefined else council[0]}...")
        if predefined:
            records = try_fetch_predefined_council(council)
            name = council["council_name"]
            url = council["csv_url"]
        else:
            name, url = council
            records = try_fetch_new_council_csv(url, name)
        if records:
            insert_records(records)
        else:
            failed.append(council)
        time.sleep(0.05)
    return failed

# Load predefined councils first (custom fetchers take precedence)
failed_predefined = load_council_data(predefined_councils, 2, 23, "Loading predefined council:", predefined=True)

# --------------------------
# Discover and load new councils (round 1)
# --------------------------
progress_bar.progress(25, text="Discovering new councils...")
all_new_councils = discover_new_councils()
predef_names = set(c["council_name"] for c in predefined_councils)
new_councils = [(name, url) for name, url in all_new_councils if name not in predef_names]
failed_new = load_council_data(new_councils, 25, 15, "Loading new council:")

# --------------------------
# Retry failed councils only once after all others
# --------------------------
failed_all = failed_predefined + failed_new
if failed_all:
    progress_bar.progress(40, text="Retrying failed council loads...")
    if failed_predefined:
        # Retry predefined with custom logic
        failed_retry_predef = load_council_data(failed_predefined, 40, 3, "Retrying council:", predefined=True)
    else:
        failed_retry_predef = []
    # Retry new councils
    failed_retry_new = load_council_data(failed_new, 43, 2, "Retrying council:", predefined=False)
    failed_retry = failed_retry_predef + failed_retry_new
else:
    failed_retry = []

# --------------------------
# Loop: automatically look for more new councils after all others are loaded
# Only do this once more to avoid infinite loops (could be more if you want)
# --------------------------
progress_bar.progress(45, text="Scanning for additional councils...")
more_new_councils = discover_new_councils()
already_seen = set(list(predef_names) + [name for name, _ in new_councils] + [c["council_name"] if isinstance(c, dict) else c[0] for c in failed_all])
to_load = [(name, url) for name, url in more_new_councils if name not in already_seen]
load_council_data(to_load, 45, 5, "Loading additional council:")

progress_bar.progress(50, text="Fetching list of councils...")
# --------------------------
# Fetch list of councils from DB
# --------------------------
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT DISTINCT council FROM payments")
councils = [row[0] for row in c.fetchall()]
conn.close()

if not councils:
    st.error("No councils found in database. Please check your data source.")
    st.stop()

selected_council = st.sidebar.selectbox("Select council", sorted(councils))

# --------------------------
# Filters
# --------------------------
progress_bar.progress(55, text="Loading filters...")
st.sidebar.subheader("Filters")
start_date = st.sidebar.date_input("Start date", datetime(2023,1,1))
end_date = st.sidebar.date_input("End date", datetime.today())
supplier_search = st.sidebar.text_input("Supplier search")

# --------------------------
# Fetch filtered data
# --------------------------
progress_bar.progress(60, text="Fetching payments data...")
conn = sqlite3.connect(DB_NAME)
query = "SELECT * FROM payments WHERE council = ? AND payment_date BETWEEN ? AND ?"
params = [selected_council, start_date.isoformat(), end_date.isoformat()]
if supplier_search:
    query += " AND supplier LIKE ?"
    params.append(f"%{supplier_search}%")

df = pd.read_sql_query(query, conn, params=params)
conn.close()
progress_bar.progress(65, text="Payments data loaded.")

# --------------------------
# Display summary stats
# --------------------------
progress_bar.progress(70, text="Calculating summary statistics...")
st.title(f"{selected_council} Public Spending")
st.markdown(f"Showing payments from {start_date} to {end_date}")
st.write(f"**Total payments:** £{df['amount_gbp'].sum():,.2f}")
st.write(f"**Number of transactions:** {len(df)}")

# --------------------------
# Top suppliers
# --------------------------
progress_bar.progress(75, text="Calculating top suppliers...")
if not df.empty:
    top_suppliers = df.groupby("supplier")['amount_gbp'].sum().sort_values(ascending=False).head(10).reset_index()
    fig1 = px.bar(top_suppliers, x="supplier", y="amount_gbp", title="Top 10 Suppliers by Payment Amount")
    st.plotly_chart(fig1)

# --------------------------
# Payments over time
# --------------------------
progress_bar.progress(80, text="Processing payments over time...")
if not df.empty:
    df['payment_date'] = pd.to_datetime(df['payment_date'])
    payments_by_month = df.groupby(df['payment_date'].dt.to_period("M"))['amount_gbp'].sum().reset_index()
    payments_by_month['payment_date'] = payments_by_month['payment_date'].dt.to_timestamp()
    fig2 = px.line(payments_by_month, x="payment_date", y="amount_gbp", title="Payments Over Time")
    st.plotly_chart(fig2)

# --------------------------
# Map visualization
# --------------------------
progress_bar.progress(83, text="Preparing map visualization...")
df_map = df.dropna(subset=['lat','lon']) if not df.empty else pd.DataFrame()
if not df_map.empty:
    st.subheader("Payments Map")
    fig_map = px.scatter_mapbox(
        df_map, lat="lat", lon="lon", hover_name="supplier", hover_data=["amount_gbp","description"],
        color="amount_gbp", size="amount_gbp", zoom=8, mapbox_style="open-street-map"
    )
    st.plotly_chart(fig_map)

# --------------------------
# Anomaly detection with filters
# --------------------------
progress_bar.progress(85, text="Detecting anomalies...")
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
    missing_inv_df = pd.DataFrame(
        c.fetchall(),
        columns=["id","council","payment_date","supplier","description","category","amount_gbp","invoice_ref","lat","lon","hash"]
    )
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
        if total_council and total_amount and total_council > 0 and (total_amount/total_council) > 0.5:
            st.markdown(f"**Warning:** Supplier `{supplier}` received more than 50% of total payments (£{total_amount:,.2f})")

conn.close()
progress_bar.progress(90, text="Anomaly detection complete.")

# --------------------------
# Citizen feedback
# --------------------------
progress_bar.progress(92, text="Loading citizen feedback...")
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

# --------------------------
# CSV download
# --------------------------
progress_bar.progress(100, text="All data loaded!")
if not df.empty:
    csv_data = df.to_csv(index=False).encode('utf-8')
    st.download_button(label="Download CSV", data=csv_data, file_name=f"{selected_council}_payments.csv", mime="text/csv")

progress_bar.empty()
