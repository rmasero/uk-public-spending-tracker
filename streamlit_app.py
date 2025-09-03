import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import hashlib
from fetch_and_ingest import insert_records
from db_schema import create_tables
from pattern_detection import detect_anomalies
from council_auto_discovery import discover_new_councils, fetch_new_council_csv
from geocode import geocode_address
import plotly.express as px

# Import predefined council fetchers
try:
    from council_fetchers import FETCHERS
except ImportError:
    FETCHERS = {}

DB_NAME = "spend.db"

# --------------------------
# Helper functions for caching
# --------------------------
def get_last_update_time():
    """Get when data was last updated"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='update_log'")
        if not c.fetchone():
            # Create update log table if it doesn't exist
            c.execute('''
                CREATE TABLE update_log (
                    id INTEGER PRIMARY KEY,
                    council_name TEXT UNIQUE,
                    last_updated TIMESTAMP,
                    record_count INTEGER
                )
            ''')
            conn.commit()
            return None
        
        c.execute("SELECT MAX(last_updated) FROM update_log")
        result = c.fetchone()[0]
        return datetime.fromisoformat(result) if result else None
    finally:
        conn.close()

def log_council_update(council_name, record_count):
    """Log when a council was updated"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO update_log (council_name, last_updated, record_count)
        VALUES (?, ?, ?)
    ''', (council_name, datetime.now().isoformat(), record_count))
    conn.commit()
    conn.close()

def need_data_refresh():
    """Check if we need to refresh data (daily refresh)"""
    last_update = get_last_update_time()
    if not last_update:
        return True
    return datetime.now() - last_update > timedelta(days=1)

def get_processed_councils():
    """Get list of councils we've already processed"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT DISTINCT council_name FROM update_log")
    processed = {row[0] for row in c.fetchall()}
    conn.close()
    return processed

# --------------------------
# Initialize database
# --------------------------
create_tables()

# --------------------------
# Background data loading
# --------------------------
@st.cache_data(ttl=86400)  # Cache for 24 hours
def load_all_council_data():
    """Load all council data in background"""
    loading_status = []
    processed_councils = get_processed_councils()
    
    # 1. Load predefined councils first
    loading_status.append("Loading predefined councils...")
    for council_name, fetcher_func in FETCHERS.items():
        if council_name not in processed_councils or need_data_refresh():
            try:
                records = fetcher_func()
                if records:
                    insert_records(records)
                    log_council_update(council_name, len(records))
                    loading_status.append(f"✅ {council_name}: {len(records)} records")
                else:
                    loading_status.append(f"⚠️ {council_name}: No data available")
            except Exception as e:
                loading_status.append(f"❌ {council_name}: {str(e)}")
        else:
            loading_status.append(f"🔄 {council_name}: Using cached data")
    
    # 2. Discover and load new councils
    loading_status.append("Discovering additional councils...")
    try:
        new_councils = discover_new_councils()
        for council_name, csv_url in new_councils:
            # Skip if we've already processed this council recently
            if council_name not in processed_councils or need_data_refresh():
                try:
                    records = fetch_new_council_csv(csv_url, council_name)
                    if records:
                        insert_records(records)
                        log_council_update(council_name, len(records))
                        loading_status.append(f"✅ {council_name}: {len(records)} records (discovered)")
                    else:
                        loading_status.append(f"⚠️ {council_name}: No data available (discovered)")
                except Exception as e:
                    loading_status.append(f"❌ {council_name}: {str(e)} (discovered)")
            else:
                loading_status.append(f"🔄 {council_name}: Using cached data (discovered)")
                
    except Exception as e:
        loading_status.append(f"⚠️ Discovery API failed: {str(e)}")
    
    return loading_status

# Show loading progress
if need_data_refresh():
    with st.spinner("Loading and updating council data... This may take a few minutes on first run."):
        loading_status = load_all_council_data()
    
    # Show loading results in an expander (so it doesn't clutter the main app)
    with st.expander("📊 Data Loading Summary", expanded=False):
        for status in loading_status:
            st.write(status)
else:
    # Data is fresh, just load silently
    load_all_council_data()

# --------------------------
# Main App Interface
# --------------------------
st.sidebar.title("Public Spending Tracker")

# Get available councils from database
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT DISTINCT council FROM payments ORDER BY council")
councils = [row[0] for row in c.fetchall()]

# Show data freshness info
c.execute("SELECT COUNT(DISTINCT council) as councils, COUNT(*) as total_payments, MAX(payment_date) as latest_payment FROM payments")
stats = c.fetchone()
conn.close()

if not councils:
    st.error("No councils found in database. Please check your setup.")
    st.stop()

# Show quick stats
with st.sidebar.expander("📈 Database Stats"):
    st.write(f"**Councils:** {stats[0]}")
    st.write(f"**Total Payments:** {stats[1]:,}")
    st.write(f"**Latest Payment:** {stats[2]}")
    
    last_update = get_last_update_time()
    if last_update:
        st.write(f"**Last Updated:** {last_update.strftime('%Y-%m-%d %H:%M')}")

selected_council = st.sidebar.selectbox("Select council", councils)

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

if len(df) == 0:
    st.warning("No payment data found for the selected filters.")
    st.info("Try adjusting the date range or clearing the supplier search.")
    st.stop()

# Create columns for key metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Payments", f"£{df['amount_gbp'].sum():,.0f}")
with col2:
    st.metric("Transactions", f"{len(df):,}")
with col3:
    st.metric("Unique Suppliers", f"{df['supplier'].nunique():,}")
with col4:
    st.metric("Avg Payment", f"£{df['amount_gbp'].mean():,.0f}")

# --------------------------
# Top suppliers
# --------------------------
top_suppliers = df.groupby("supplier")['amount_gbp'].sum().sort_values(ascending=False).head(10).reset_index()
fig1 = px.bar(top_suppliers, x="supplier", y="amount_gbp", title="Top 10 Suppliers by Payment Amount")
fig1.update_xaxis(tickangle=45)
st.plotly_chart(fig1, use_container_width=True)

# --------------------------
# Payments over time
# --------------------------
df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
df_clean = df.dropna(subset=['payment_date'])

if len(df_clean) > 0:
    payments_by_month = df_clean.groupby(df_clean['payment_date'].dt.to_period("M"))['amount_gbp'].sum().reset_index()
    payments_by_month['payment_date'] = payments_by_month['payment_date'].dt.to_timestamp()
    fig2 = px.line(payments_by_month, x="payment_date", y="amount_gbp", title="Payments Over Time")
    st.plotly_chart(fig2, use_container_width=True)

# --------------------------
# Map visualization
# --------------------------
df_map = df.dropna(subset=['lat','lon'])
if not df_map.empty and len(df_map) > 0:
    st.subheader("📍 Payments Map")
    fig_map = px.scatter_mapbox(
        df_map, lat="lat", lon="lon", hover_name="supplier", hover_data=["amount_gbp","description"],
        color="amount_gbp", size="amount_gbp", zoom=8, mapbox_style="open-street-map",
        title=f"Geographic Distribution of Payments - {selected_council}"
    )
    st.plotly_chart(fig_map, use_container_width=True)

# --------------------------
# Anomaly detection
# --------------------------
st.subheader("🚨 Anomalies & Alerts")

conn = sqlite3.connect(DB_NAME)
c = conn.cursor()

tab1, tab2, tab3 = st.tabs(["Large Payments", "Frequent Payments", "Other Anomalies"])

with tab1:
    c.execute("SELECT council, supplier, amount_gbp, payment_date, description FROM payments WHERE council = ? AND amount_gbp > 100000 ORDER BY amount_gbp DESC", (selected_council,))
    large_payments = c.fetchall()
    if large_payments:
        large_df = pd.DataFrame(large_payments, columns=["Council","Supplier","Amount (£)","Date","Description"])
        st.dataframe(large_df, use_container_width=True)
    else:
        st.info("No payments over £100,000 found.")

with tab2:
    c.execute('''
        SELECT supplier, COUNT(*) as payment_count, SUM(amount_gbp) as total_amount
        FROM payments
        WHERE council = ?
        GROUP BY supplier
        HAVING payment_count > 10
        ORDER BY payment_count DESC
    ''', (selected_council,))
    frequent = c.fetchall()
    if frequent:
        freq_df = pd.DataFrame(frequent, columns=["Supplier","Payment Count","Total Amount (£)"])
        st.dataframe(freq_df, use_container_width=True)
    else:
        st.info("No suppliers with more than 10 payments found.")

with tab3:
    # Payments without invoice refs
    c.execute("SELECT COUNT(*) FROM payments WHERE council = ? AND (invoice_ref IS NULL OR invoice_ref = '')", (selected_council,))
    missing_invoices = c.fetchone()[0]
    
    # Single supplier dominance
    c.execute('''
        SELECT supplier, SUM(amount_gbp) as total_amount,
               (SUM(amount_gbp) * 100.0 / (SELECT SUM(amount_gbp) FROM payments WHERE council = ?)) as percentage
        FROM payments
        WHERE council = ?
        GROUP BY supplier
        ORDER BY total_amount DESC
        LIMIT 1
    ''', (selected_council, selected_council))
    
    top_supplier = c.fetchone()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Payments Missing Invoice Refs", f"{missing_invoices:,}")
    with col2:
        if top_supplier and top_supplier[2] > 50:
            st.metric("Top Supplier Dominance", f"{top_supplier[2]:.1f}%", delta="⚠️ High concentration")
        elif top_supplier:
            st.metric("Top Supplier Share", f"{top_supplier[2]:.1f}%")

conn.close()

# --------------------------
# Raw data table
# --------------------------
with st.expander("📋 Raw Payment Data", expanded=False):
    st.dataframe(df, use_container_width=True)
    
    # CSV download
    csv_data = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download CSV",
        data=csv_data,
        file_name=f"{selected_council.replace(' ', '_')}_payments_{start_date}_{end_date}.csv",
        mime="text/csv"
    )
