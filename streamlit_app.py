import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from uk_public_spending_tracker_final.fetch_and_ingest import insert_records
from uk_public_spending_tracker_final.db_schema import create_tables
from uk_public_spending_tracker_final.pattern_detection import detect_anomalies
from uk_public_spending_tracker_final.council_auto_discovery import discover_new_councils, fetch_new_council_csv
from uk_public_spending_tracker_final.geocode import geocode_address
import plotly.express as px

DB_NAME = "spend.db"

# --------------------------
# Initialize database
# --------------------------
create_tables()

# --------------------------
# Sidebar: Council selection
# --------------------------
st.sidebar.title("Public Spending Tracker")

# Discover and ingest new councils dynamically
if st.sidebar.button("Discover New Councils"):
    councils = discover_new_councils()
    for council_name, csv_url in councils:
        st.sidebar.write(f"Found: {council_name}")
        try:
            new_data = fetch_new_council_csv(csv_url)
            insert_records(new_data, DB_NAME)
            st.sidebar.success(f"Added data for {council_name}")
        except Exception as e:
            st.sidebar.error(f"Failed to fetch {council_name}: {e}")

# --------------------------
# Main Page
# --------------------------
st.title("UK Public Spending Tracker")

# Date filter
start_date = st.date_input("Start Date", datetime(2020, 1, 1))
end_date = st.date_input("End Date", datetime.now())

# Load data
conn = sqlite3.connect(DB_NAME)
query = f"""
SELECT c.name as council, s.amount, s.date, s.supplier, s.description, s.id
FROM spend s
JOIN councils c ON s.council_id = c.id
WHERE s.date BETWEEN '{start_date}' AND '{end_date}'
"""
df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    st.warning("No spending data available for the selected range.")
el
