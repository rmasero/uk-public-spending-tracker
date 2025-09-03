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
from council_fetchers import FETCHERS

DB_NAME = "spend.db"

# --------------------------
# Initialize database
# --------------------------
create_tables()

# --------------------------
# Automatically ingest all council fetcher data
# --------------------------
all_fetched_records = []
for council_name, fetch_func in FETCHERS.items():
    try:
        records = fetch_func()
        if records:
            all_fetched_records.extend(records)
            st.info(f"Fetched {len(records)} records from {council_name}")
    except Exception as e:
        st.warning(f"Failed to fetch records for {council_name}: {e}")

if all_fetched_records:
    insert_records(all_fetched_records)

# --------------------------
# Sidebar: Council selection
# --------------------------
st.sidebar.title("Public Spending Tracker")

# Discover and ingest new councils automatically
new_councils = discover_new_councils()
for council_name, csv_url in new_councils:
    records = fetch_new_council_csv(csv_url, council_name)
    insert_records(records)

# Fetch list of councils from DB
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT DISTINCT council FROM payments")
councils = [row[0] for row in c.fetchall()]
conn.close()

selected_council = st.sidebar.selectbox("Select council", sorted(councils))

# --------------------------
# Filters
# --------------------------
st.sidebar.subheader("Filters")
start_date = st.sidebar.date_input("Start date", datetime(2023,1,1))
end_date = st.sidebar.date_input("End date", datetime.today())
supplier_search = st.sidebar.text_input("Supplier search")

# --------------------------
# Fetch filtered data
# --------------------------
conn = sqlite3.connect(DB_NAME)
query = "SELECT * FROM payments WHERE council = ? AND payment_date BETWEEN ? AND ?"
params = [selected_council, start_date.isoformat(), end_date.isoformat()]
if supplier_search:
    query += " AND supplier LIKE ?"
    params.append(f"%{supplier_search}%")

df = pd.read_sql_query(query, conn, params=params)
conn.close()

# --------------------------
# Display summary stats
# --------------------------
st.title(f"{selected_council} Public Spending")
st.markdown(f"Showing payments from {start_date} to {end_date}")
st.write(f"**Total payments:** £{df['amount_gbp'].sum():,.2f}")
st.write(f"**Number of transactions:** {len(df)}")

# --------------------------
# Top suppliers
# --------------------------
top_suppliers = df.groupby("supplier")['amount_gbp'].sum().sort_values(ascending=False).head(10).reset_index()
fig1 = px.bar(top_suppliers, x="supplier", y="amount_gbp", title="Top 10 Suppliers by Payment Amount")
st.plotly_chart(fig1)

# --------------------------
# Payments over time
# --------------------------
df['payment_date'] = pd.to_datetime(df['payment_date'])
payments_by_month = df.groupby(df['payment_date'].dt.to_period("M"))['amount_gbp'].sum().reset_index()
payments_by_month['payment_date'] = payments_by_month['payment_date'].dt.to_timestamp()
fig2 = px.line(payments_by_month, x="payment_date", y="amount_gbp", title="Payments Over Time")
st.plotly_chart(fig2)

# --------------------------
# Map visualization
# --------------------------
df_map = df.dropna(subset=['lat','lon'])
if not df_map.empty:
   

