import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from fetch_and_ingest import insert_records
from db_schema import create_tables
from pattern_detection import detect_anomalies
from council_auto_discovery import discover_new_councils, fetch_new_council_csv
from geocode import geocode_address
import council_fetchers
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
    st.subheader("Payments Map")
    fig_map = px.scatter_mapbox(
        df_map, lat="lat", lon="lon", hover_name="supplier", hover_data=["amount_gbp","description"],
        color="amount_gbp", size="amount_gbp", zoom=8, mapbox_style="open-street-map"
    )
    st.plotly_chart(fig_map)

# --------------------------
# Anomaly detection with filters
# --------------------------
st.subheader("Anomalies / Alerts")

# User filter selection
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

# --------------------------
# Citizen feedback
# --------------------------
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

# --------------------------
# CSV download
# --------------------------
csv_data = df.to_csv(index=False).encode('utf-8')
st.download_button(label="Download CSV", data=csv_data, file_name=f"{selected_council}_payments.csv", mime="text/csv")
