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

DB_NAME = "spend.db"

# --------------------------
# Initialize database
# --------------------------
create_tables()

# --------------------------
# Sidebar: Council selection
# --------------------------
st.sidebar.title("UK Public Spending Tracker")

# Try to discover and ingest new councils with progress indicator
with st.spinner("Discovering and loading council data..."):
    try:
        new_councils = discover_new_councils()
        
        if new_councils:
            # Limit to first 10 councils to avoid long loading times
            limited_councils = new_councils[:10]
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            successful_loads = 0
            total_councils = len(limited_councils)
            
            for i, (council_name, csv_url) in enumerate(limited_councils):
                try:
                    status_text.text(f"Processing {council_name}...")
                    records = fetch_new_council_csv(csv_url, council_name, timeout=10)
                    
                    if records:
                        insert_records(records)
                        successful_loads += 1
                        st.success(f"✅ Loaded {len(records)} records from {council_name}")
                    else:
                        st.warning(f"⚠️ No data found for {council_name}")
                        
                except Exception as e:
                    st.warning(f"⚠️ Skipped {council_name}: {str(e)[:100]}")
                
                # Update progress
                progress_bar.progress((i + 1) / total_councils)
            
            status_text.text(f"Completed! Successfully loaded {successful_loads}/{total_councils} councils")
            progress_bar.empty()
            status_text.empty()
        else:
            st.warning("No councils discovered from data.gov.uk API")
            
    except Exception as e:
        st.error(f"Error in council discovery: {e}")

# Fetch list of councils from DB
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT DISTINCT council FROM payments")
councils = [row[0] for row in c.fetchall()]
conn.close()

if not councils:
    st.error("No councils found in database. Please check the data sources.")
    st.info("You may need to manually add some council data to get started.")
    st.stop()

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

if len(df) == 0:
    st.warning("No payment data found for the selected council and date range.")
    st.info("Try selecting a different council or expanding the date range.")
else:
    st.write(f"**Total payments:** £{df['amount_gbp'].sum():,.2f}")
    st.write(f"**Number of transactions:** {len(df)}")

    # --------------------------
    # Top suppliers
    # --------------------------
    if len(df) > 0:
        top_suppliers = df.groupby("supplier")['amount_gbp'].sum().sort_values(ascending=False).head(10).reset_index()
        fig1 = px.bar(top_suppliers, x="supplier", y="amount_gbp", title="Top 10 Suppliers by Payment Amount")
        st.plotly_chart(fig1)

        # --------------------------
        # Payments over time
        # --------------------------
        df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
        df_valid_dates = df.dropna(subset=['payment_date'])
        
        if len(df_valid_dates) > 0:
            payments_by_month = df_valid_dates.groupby(df_valid_dates['payment_date'].dt.to_period("M"))['amount_gbp'].sum().reset_index()
            payments_by_month['payment_date'] = payments_by_month['payment_date'].dt.to_timestamp()
            fig2 = px.line(payments_by_month, x="payment_date", y="amount_gbp", title="Payments Over Time")
            st.plotly_chart(fig2)

        # --------------------------
        # Map visualization
        # --------------------------
        df_map = df.dropna(subset=['lat','lon'])
        if not df_map.empty:
            st.subheader("Payments Map")
            fig_map = px.scatter_mapbox(
                df_map, lat="lat", lon="lon", hover_name="supplier", hover_data=["amount_gbp","description"],
                color="amount_gbp", size="amount_gbp", zoom=8, mapbox_style="open-street-map"
            )
            st.plotly_chart(fig_map)

        # --------------------------
        # Anomaly detection
        # --------------------------
        st.subheader("Anomalies / Alerts")
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        # Large payments
        c.execute("SELECT id, council, supplier, amount_gbp, payment_date FROM payments WHERE amount_gbp > 100000 AND council = ?", (selected_council,))
        large_payments = c.fetchall()
        if large_payments:
            st.markdown("**Large payments (>£100k):**")
            large_df = pd.DataFrame(large_payments, columns=["id","council","supplier","amount_gbp","payment_date"])
            st.dataframe(large_df)

        conn.close()

        # --------------------------
        # CSV download
        # --------------------------
        csv_data = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV", 
            data=csv_data, 
            file_name=f"{selected_council}_payments.csv", 
            mime="text/csv"
        )
