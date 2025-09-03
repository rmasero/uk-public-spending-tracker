import streamlit as st
import sqlite3
import pandas as pd
import altair as alt
from db_schema import DB_NAME

st.set_page_config(page_title="UK Public Spending Tracker", layout="wide")

# --------------------------
# Database helpers
# --------------------------

def _connect():
    """Return a new SQLite connection (lightweight, safe per-query)."""
    return sqlite3.connect(DB_NAME, check_same_thread=False)

@st.cache_data(show_spinner=False)
def _query_df(sql: str, params: tuple = ()):
    """Run a query and return a DataFrame, cached by Streamlit."""
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)

# --------------------------
# UI
# --------------------------

st.title("UK Public Spending Tracker")

# Fetch councils
councils_df = _query_df("SELECT DISTINCT council FROM payments ORDER BY council")
councils = councils_df["council"].tolist()

selected_council = st.sidebar.selectbox("Select Council", councils)

# Fetch data for selected council
df = _query_df(
    "SELECT * FROM payments WHERE council = ? ORDER BY payment_date DESC",
    params=(selected_council,)
)

if df.empty:
    st.warning(f"No payments found for {selected_council}.")
    st.stop()

# Convert date column
df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")

# Sidebar filters
with st.sidebar:
    st.subheader("Filters")

    min_date, max_date = df["payment_date"].min(), df["payment_date"].max()
    date_range = st.date_input("Date range", [min_date, max_date])

    suppliers = ["All"] + sorted(df["supplier"].dropna().unique().tolist())
    supplier_filter = st.selectbox("Supplier", suppliers)

    categories = ["All"] + sorted(df["category"].dropna().unique().tolist())
    category_filter = st.selectbox("Category", categories)

# Apply filters
mask = (df["payment_date"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])))
if supplier_filter != "All":
    mask &= df["supplier"] == supplier_filter
if category_filter != "All":
    mask &= df["category"] == category_filter

filtered = df[mask]

# --------------------------
# Metrics
# --------------------------

total_spend = filtered["amount_gbp"].sum()
num_payments = len(filtered)
avg_payment = filtered["amount_gbp"].mean() if num_payments else 0

c1, c2, c3 = st.columns(3)
c1.metric("Total Spend (£)", f"{total_spend:,.2f}")
c2.metric("Number of Payments", f"{num_payments:,}")
c3.metric("Average Payment (£)", f"{avg_payment:,.2f}")

# --------------------------
# Charts
# --------------------------

st.subheader("Spending Over Time")
spend_over_time = (
    filtered.groupby("payment_date")["amount_gbp"]
    .sum()
    .reset_index()
)

if not spend_over_time.empty:
    chart = (
        alt.Chart(spend_over_time)
        .mark_line(point=True)
        .encode(
            x="payment_date:T",
            y="amount_gbp:Q",
            tooltip=["payment_date:T", "amount_gbp:Q"]
        )
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No spending data available for this date range.")

st.subheader("Top Suppliers")
top_suppliers = (
    filtered.groupby("supplier")["amount_gbp"]
    .sum()
    .nlargest(10)
    .reset_index()
)

if not top_suppliers.empty:
    bar_chart = (
        alt.Chart(top_suppliers)
        .mark_bar()
        .encode(
            x=alt.X("amount_gbp:Q", title="Spend (£)"),
            y=alt.Y("supplier:N", sort="-x"),
            tooltip=["supplier:N", "amount_gbp:Q"]
        )
    )
    st.altair_chart(bar_chart, use_container_width=True)
else:
    st.info("No supplier data available for this selection.")

# --------------------------
# Data Table
# --------------------------

st.subheader("Payments Data")
st.dataframe(
    filtered[["payment_date", "supplier", "description", "category", "amount_gbp", "invoice_ref"]],
    use_container_width=True
)

# Option to download data
@st.cache_data(show_spinner=False)
def convert_df(df: pd.DataFrame):
    return df.to_csv(index=False).encode("utf-8")

st.download_button(
    "Download data as CSV",
    convert_df(filtered),
    f"{selected_council}_payments.csv",
    "text/csv",
    key="download-csv"
)
