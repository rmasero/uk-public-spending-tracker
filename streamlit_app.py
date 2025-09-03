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
import plotly.graph_objects as go

# Import predefined council fetchers
try:
    from council_fetchers import FETCHERS
except ImportError:
    FETCHERS = {}

DB_NAME = "spend.db"

# --------------------------
# Enhanced fraud detection functions
# --------------------------
def detect_fraud_indicators(council_name):
    """Comprehensive fraud, waste, and corruption detection"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    indicators = {
        "high_risk": [],
        "medium_risk": [],
        "low_risk": [],
        "statistics": {}
    }
    
    # 1. LARGE PAYMENTS (>£100k) - High Risk
    c.execute("""
        SELECT supplier, amount_gbp, payment_date, description, invoice_ref
        FROM payments 
        WHERE council = ? AND amount_gbp > 100000
        ORDER BY amount_gbp DESC
    """, (council_name,))
    large_payments = c.fetchall()
    
    if large_payments:
        indicators["high_risk"].append({
            "type": "Large Payments",
            "severity": "HIGH RISK",
            "count": len(large_payments),
            "description": f"{len(large_payments)} payments over £100,000 detected",
            "details": large_payments[:10],  # Show top 10
            "total_value": sum([p[1] for p in large_payments])
        })
    
    # 2. DUPLICATE PAYMENTS - High Risk (same supplier, amount, date)
    c.execute("""
        SELECT supplier, amount_gbp, payment_date, COUNT(*) as duplicate_count,
               SUM(amount_gbp) as total_duplicated
        FROM payments 
        WHERE council = ?
        GROUP BY supplier, amount_gbp, payment_date
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, amount_gbp DESC
    """, (council_name,))
    duplicates = c.fetchall()
    
    if duplicates:
        indicators["high_risk"].append({
            "type": "Duplicate Payments",
            "severity": "HIGH RISK - POSSIBLE FRAUD",
            "count": len(duplicates),
            "description": f"{len(duplicates)} sets of duplicate payments found",
            "details": duplicates,
            "total_value": sum([d[4] for d in duplicates])
        })
    
    # 3. ROUND NUMBER BIAS - Medium Risk (suspicious round amounts)
    c.execute("""
        SELECT supplier, amount_gbp, payment_date, description
        FROM payments 
        WHERE council = ? AND (
            amount_gbp % 1000 = 0 OR 
            amount_gbp % 500 = 0 OR
            amount_gbp % 100 = 0
        ) AND amount_gbp > 1000
        ORDER BY amount_gbp DESC
    """, (council_name,))
    round_payments = c.fetchall()
    
    if len(round_payments) > 10:  # Only flag if there are many
        indicators["medium_risk"].append({
            "type": "Round Number Payments",
            "severity": "MEDIUM RISK",
            "count": len(round_payments),
            "description": f"{len(round_payments)} payments with suspicious round amounts",
            "details": round_payments[:20],
            "total_value": sum([p[1] for p in round_payments])
        })
    
    # 4. SINGLE SUPPLIER DOMINANCE - Medium Risk
    c.execute("""
        SELECT supplier, 
               COUNT(*) as payment_count,
               SUM(amount_gbp) as total_amount,
               (SUM(amount_gbp) * 100.0 / (SELECT SUM(amount_gbp) FROM payments WHERE council = ?)) as percentage
        FROM payments 
        WHERE council = ?
        GROUP BY supplier
        HAVING percentage > 25
        ORDER BY percentage DESC
    """, (council_name, council_name))
    dominant_suppliers = c.fetchall()
    
    if dominant_suppliers:
        indicators["medium_risk"].append({
            "type": "Supplier Dominance",
            "severity": "MEDIUM RISK",
            "count": len(dominant_suppliers),
            "description": f"{len(dominant_suppliers)} suppliers receiving >25% of total payments",
            "details": dominant_suppliers,
            "total_value": sum([s[2] for s in dominant_suppliers])
        })
    
    # 5. FREQUENT SMALL PAYMENTS (Splitting) - Medium Risk
    c.execute("""
        SELECT supplier, 
               COUNT(*) as frequency,
               AVG(amount_gbp) as avg_amount,
               SUM(amount_gbp) as total_amount
        FROM payments 
        WHERE council = ? AND amount_gbp BETWEEN 100 AND 9999
        GROUP BY supplier, strftime('%Y-%m', payment_date)
        HAVING frequency > 20 AND avg_amount < 5000
        ORDER BY frequency DESC
    """, (council_name,))
    frequent_small = c.fetchall()
    
    if frequent_small:
        indicators["medium_risk"].append({
            "type": "Payment Splitting",
            "severity": "MEDIUM RISK",
            "count": len(frequent_small),
            "description": f"{len(frequent_small)} cases of possible payment splitting detected",
            "details": frequent_small,
            "total_value": sum([f[3] for f in frequent_small])
        })
    
    # 6. MISSING DOCUMENTATION - Low Risk
    c.execute("""
        SELECT COUNT(*) as missing_invoice,
               SUM(amount_gbp) as total_missing_invoice
        FROM payments 
        WHERE council = ? AND (invoice_ref IS NULL OR invoice_ref = '' OR invoice_ref = 'N/A')
    """, (council_name,))
    missing_docs = c.fetchone()
    
    if missing_docs[0] > 0:
        indicators["low_risk"].append({
            "type": "Missing Documentation",
            "severity": "LOW RISK",
            "count": missing_docs[0],
            "description": f"{missing_docs[0]} payments without invoice references",
            "details": [],
            "total_value": missing_docs[1] or 0
        })
    
    # 7. WEEKEND/HOLIDAY PAYMENTS - Low Risk
    c.execute("""
        SELECT supplier, amount_gbp, payment_date, description
        FROM payments 
        WHERE council = ? AND (
            strftime('%w', payment_date) IN ('0', '6') OR  -- Weekend
            payment_date IN ('2023-12-25', '2023-12-26', '2024-01-01', '2024-12-25', '2024-12-26', '2025-01-01')  -- Holidays
        )
        ORDER BY amount_gbp DESC
    """, (council_name,))
    weekend_payments = c.fetchall()
    
    if weekend_payments:
        indicators["low_risk"].append({
            "type": "Off-Hours Payments",
            "severity": "LOW RISK",
            "count": len(weekend_payments),
            "description": f"{len(weekend_payments)} payments made on weekends/holidays",
            "details": weekend_payments[:10],
            "total_value": sum([p[1] for p in weekend_payments])
        })
    
    # Calculate overall statistics
    c.execute("SELECT COUNT(*), SUM(amount_gbp), COUNT(DISTINCT supplier) FROM payments WHERE council = ?", (council_name,))
    total_payments, total_value, total_suppliers = c.fetchone()
    
    indicators["statistics"] = {
        "total_payments": total_payments,
        "total_value": total_value,
        "total_suppliers": total_suppliers,
        "risk_score": len(indicators["high_risk"]) * 3 + len(indicators["medium_risk"]) * 2 + len(indicators["low_risk"])
    }
    
    conn.close()
    return indicators

# --------------------------
# Helper functions for caching (same as before)
# --------------------------
def get_last_update_time():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='update_log'")
        if not c.fetchone():
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
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO update_log (council_name, last_updated, record_count)
        VALUES (?, ?, ?)
    ''', (council_name, datetime.now().isoformat(), record_count))
    conn.commit()
    conn.close()

def need_data_refresh():
    last_update = get_last_update_time()
    if not last_update:
        return True
    return datetime.now() - last_update > timedelta(days=1)

def get_processed_councils():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT DISTINCT council_name FROM update_log")
    processed = {row[0] for row in c.fetchall()}
    conn.close()
    return processed

# --------------------------
# Initialize and load data
# --------------------------
create_tables()

@st.cache_data(ttl=86400)
def load_all_council_data():
    loading_status = []
    processed_councils = get_processed_councils()
    
    # Load predefined councils
    for council_name, fetcher_func in FETCHERS.items():
        if council_name not in processed_councils or need_data_refresh():
            try:
                records = fetcher_func()
                if records:
                    insert_records(records)
                    log_council_update(council_name, len(records))
                    loading_status.append(f"✅ {council_name}: {len(records)} records")
            except Exception as e:
                loading_status.append(f"❌ {council_name}: {str(e)}")
    
    # Discover new councils
    try:
        new_councils = discover_new_councils()
        for council_name, csv_url in new_councils:
            if council_name not in processed_councils or need_data_refresh():
                try:
                    records = fetch_new_council_csv(csv_url, council_name)
                    if records:
                        insert_records(records)
                        log_council_update(council_name, len(records))
                        loading_status.append(f"✅ {council_name}: {len(records)} records (discovered)")
                except Exception as e:
                    loading_status.append(f"❌ {council_name}: {str(e)} (discovered)")
    except Exception as e:
        loading_status.append(f"⚠️ Discovery failed: {str(e)}")
    
    return loading_status

# Load data
if need_data_refresh():
    with st.spinner("🔄 Loading council data..."):
        loading_status = load_all_council_data()
else:
    load_all_council_data()

# --------------------------
# Get available councils
# --------------------------
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT DISTINCT council FROM payments ORDER BY council")
councils = [row[0] for row in c.fetchall()]
conn.close()

if not councils:
    st.error("No councils found. Please check your data sources.")
    st.stop()

# --------------------------
# Main App Header
# --------------------------
st.title("🏛️ UK Public Spending Tracker")
st.markdown("**Transparency through data - Detecting waste, fraud, and corruption in public spending**")

# Quick stats
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT COUNT(DISTINCT council) as councils, COUNT(*) as payments, SUM(amount_gbp) as total_value FROM payments")
overall_stats = c.fetchone()
conn.close()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("📊 Councils", f"{overall_stats[0]}")
with col2:
    st.metric("💳 Total Payments", f"{overall_stats[1]:,}")
with col3:
    st.metric("💰 Total Value", f"£{overall_stats[2]:,.0f}")

# --------------------------
# Council Tabs
# --------------------------
if len(councils) > 10:
    # If too many councils, show dropdown instead of tabs
    selected_council = st.selectbox("🏛️ Select Council:", councils)
    council_tabs = [selected_council]
    tabs = [st.container()]
else:
    # Show as tabs if manageable number
    council_tabs = councils
    tabs = st.tabs([f"🏛️ {council}" for council in councils])

# --------------------------
# Display each council
# --------------------------
for tab, council_name in zip(tabs, council_tabs):
    with tab:
        st.header(f"{council_name} Spending Analysis")
        
        # Get council data
        conn = sqlite3.connect(DB_NAME)
        df = pd.read_sql_query("SELECT * FROM payments WHERE council = ?", conn, params=[council_name])
        conn.close()
        
        if df.empty:
            st.warning(f"No data available for {council_name}")
            continue
        
        # Council overview metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("💳 Payments", f"{len(df):,}")
        with col2:
            st.metric("💰 Total Value", f"£{df['amount_gbp'].sum():,.0f}")
        with col3:
            st.metric("🏢 Suppliers", f"{df['supplier'].nunique():,}")
        with col4:
            st.metric("📊 Avg Payment", f"£{df['amount_gbp'].mean():,.0f}")
        
        # FRAUD DETECTION SECTION
        st.subheader("🚨 Fraud, Waste & Corruption Analysis")
        
        indicators = detect_fraud_indicators(council_name)
        risk_score = indicators["statistics"]["risk_score"]
        
        # Risk Score Display
        if risk_score >= 8:
            st.error(f"🔴 **HIGH RISK COUNCIL** - Risk Score: {risk_score}")
        elif risk_score >= 4:
            st.warning(f"🟡 **MEDIUM RISK COUNCIL** - Risk Score: {risk_score}")
        else:
            st.success(f"🟢 **LOW RISK COUNCIL** - Risk Score: {risk_score}")
        
        # High Risk Indicators
        if indicators["high_risk"]:
            st.markdown("### 🔴 HIGH RISK INDICATORS")
            for indicator in indicators["high_risk"]:
                with st.expander(f"⚠️ {indicator['type']} - {indicator['severity']}", expanded=True):
                    st.write(indicator['description'])
                    st.metric("Total Value at Risk", f"£{indicator['total_value']:,.0f}")
                    
                    if indicator['details']:
                        details_df = pd.DataFrame(indicator['details'])
                        if indicator['type'] == "Large Payments":
                            details_df.columns = ["Supplier", "Amount", "Date", "Description", "Invoice Ref"]
                        elif indicator['type'] == "Duplicate Payments":
                            details_df.columns = ["Supplier", "Amount", "Date", "Duplicate Count", "Total Duplicated"]
                        
                        st.dataframe(details_df, use_container_width=True)
        
        # Medium Risk Indicators
        if indicators["medium_risk"]:
            st.markdown("### 🟡 MEDIUM RISK INDICATORS")
            for indicator in indicators["medium_risk"]:
                with st.expander(f"⚠️ {indicator['type']} - {indicator['severity']}", expanded=False):
                    st.write(indicator['description'])
                    st.metric("Total Value", f"£{indicator['total_value']:,.0f}")
                    
                    if indicator['details']:
                        details_df = pd.DataFrame(indicator['details'])
                        if indicator['type'] == "Supplier Dominance":
                            details_df.columns = ["Supplier", "Payment Count", "Total Amount", "Percentage"]
                        elif indicator['type'] == "Payment Splitting":
                            details_df.columns = ["Supplier", "Frequency", "Avg Amount", "Total Amount"]
                        
                        st.dataframe(details_df.head(10), use_container_width=True)
        
        # Low Risk Indicators
        if indicators["low_risk"]:
            with st.expander("🟢 Low Risk Indicators", expanded=False):
                for indicator in indicators["low_risk"]:
                    st.write(f"**{indicator['type']}**: {indicator['description']}")
                    if indicator['total_value'] > 0:
                        st.write(f"Value: £{indicator['total_value']:,.0f}")
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            # Top suppliers pie chart
            top_suppliers = df.groupby("supplier")['amount_gbp'].sum().sort_values(ascending=False).head(10)
            fig_pie = px.pie(
                values=top_suppliers.values,
                names=top_suppliers.index,
                title=f"Top 10 Suppliers - {council_name}"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Payment amounts distribution
            fig_hist = px.histogram(
                df, 
                x="amount_gbp", 
                title=f"Payment Distribution - {council_name}",
                nbins=50
            )
            fig_hist.update_xaxis(title="Payment Amount (£)")
            fig_hist.update_yaxis(title="Number of Payments")
            st.plotly_chart(fig_hist, use_container_width=True)
        
        # Timeline of payments
        df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
        df_clean = df.dropna(subset=['payment_date'])
        
        if len(df_clean) > 0:
            monthly_payments = df_clean.groupby(df_clean['payment_date'].dt.to_period("M"))['amount_gbp'].sum().reset_index()
            monthly_payments['payment_date'] = monthly_payments['payment_date'].dt.to_timestamp()
            
            fig_timeline = px.line(
                monthly_payments, 
                x="payment_date", 
                y="amount_gbp",
                title=f"Monthly Spending Timeline - {council_name}"
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Raw data
        with st.expander(f"📋 Raw Data - {council_name}", expanded=False):
            st.dataframe(df, use_container_width=True)
            
            csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name=f"{council_name.replace(' ', '_')}_payments.csv",
                mime="text/csv"
            )

# --------------------------
# Footer
# --------------------------
st.markdown("---")
st.markdown("**Data Sources**: UK Government Open Data, Council Websites | **Last Updated**: " + 
           (get_last_update_time().strftime('%Y-%m-%d %H:%M') if get_last_update_time() else 'Never'))
