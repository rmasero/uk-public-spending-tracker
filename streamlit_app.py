import os
import glob
import importlib.util
import sqlite3
import time
from datetime import date
from typing import List, Tuple, Callable

import pandas as pd
import plotly.express as px
import streamlit as st

from db_schema import create_tables, DB_NAME
from fetch_and_ingest import insert_records
from council_auto_discovery import discover_new_councils, fetch_new_council_csv

st.set_page_config(page_title="UK Public Spending Tracker", layout="wide")
st.title("UK Public Spending Tracker")

# -------------------
# DB helpers
# -------------------
def _connect():
    return sqlite3.connect(DB_NAME)

def _query_df(sql: str, params: tuple = ()):
    conn = _connect()
    return pd.read_sql_query(sql, conn, params=params)

def _get_councils() -> List[str]:
    df = _query_df("SELECT DISTINCT council FROM payments ORDER BY council;")
    return df["council"].tolist()

# -------------------
# Fetcher loader
# -------------------
def _load_predefined_councils() -> List[Tuple[str, str, Callable]]:
    fetcher_dir = "council_fetchers"
    files = sorted([f for f in glob.glob(os.path.join(fetcher_dir, "*.py")) if not f.endswith("__init__.py")])
    items = []
    for path in files:
        spec = importlib.util.spec_from_file_location("cmod", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore
        name = getattr(mod, "council_name", os.path.basename(path).replace(".py", "").title())
        csv_url = getattr(mod, "csv_url", None)
        fetch_fn = getattr(mod, "fetch_payments", None)
        items.append((name, csv_url, fetch_fn))
    return items

# -------------------
# Refresh logic
# -------------------
def refresh_all_data(do_geocode: bool = False, progress=None):
    inserted_total = skipped_total = 0
    failed = []

    # 1) Predefined councils
    items = _load_predefined_councils()
    for i, (name, csv_url, fetch_fn) in enumerate(items, start=1):
        pct = int(5 + (i / max(1, len(items))) * 40)
        if progress:
            progress.progress(pct, text=f"Fetching: {name}")
        try:
            start = time.time()
            if callable(fetch_fn):
                records = fetch_fn()
            elif csv_url:
                records = fetch_new_council_csv(csv_url, name)
            else:
                records = []
            elapsed = time.time() - start
            if elapsed > 3.0:
                failed.append((name, csv_url, fetch_fn))
                continue
            ins, skip = insert_records(records, do_geocode=do_geocode)
            inserted_total += ins
            skipped_total += skip
        except Exception:
            failed.append((name, csv_url, fetch_fn))

    # 2) Discover new councils
    try:
        discovered = discover_new_councils()
    except Exception as e:
        discovered = []
        st.info(f"Discovery skipped: {e}")

    for j, (name, url) in enumerate(discovered, start=1):
        pct = int(50 + (j / max(1, len(discovered))) * 35)
        if progress:
            progress.progress(pct, text=f"Importing discovered: {name}")
        try:
            start = time.time()
            records = fetch_new_council_csv(url, name)
            elapsed = time.time() - start
            if elapsed > 3.0:
                failed.append((name, url, None))
                continue
            ins, skip = insert_records(records, do_geocode=False)  # discovery: no geocode
            inserted_total += ins
            skipped_total += skip
        except Exception:
            failed.append((name, url, None))

    # Retry failed councils once
    for (name, url, fetch_fn) in failed:
        if progress:
            progress.progress(90, text=f"Retrying {name}…")
        try:
            if callable(fetch_fn):
                records = fetch_fn()
            elif url:
                records = fetch_new_council_csv(url, name)
            else:
                records = []
            ins, skip = insert_records(records, do_geocode=do_geocode)
            inserted_total += ins
            skipped_total += skip
        except Exception:
            st.warning(f"Giving up on {name} after retry.")

    if progress:
        progress.progress(100, text="All done!")
    return inserted_total, skipped_total

# -------------------
# App start
# -------------------
with st.spinner("Setting up database…"):
    create_tables()

st.sidebar.header("Data controls")
st.sidebar.info("Data refresh runs automatically on start.\n\nYou can also force an update with geocoding (slower).")

geocode_refresh = st.sidebar.button("Refresh with geocoding (slow)")

if "data_loaded" not in st.session_state:
    # First run: auto refresh (no geocode)
    progress = st.sidebar.progress(0, text="Loading councils…")
    ins, skip = refresh_all_data(do_geocode=False, progress=progress)
    st.success(f"Startup load complete. Inserted {ins:,} new rows; skipped {skip:,}.")
    st.session_state.data_loaded = True

elif geocode_refresh:
    st.warning("Geocoding suppliers will be **slow** (free Nominatim). Please wait…")
    progress = st.sidebar.progress(0, text="Refreshing with geocoding…")
    ins, skip = refresh_all_data(do_geocode=True, progress=progress)
    st.success(f"Geocoded refresh complete. Inserted {ins:,} new rows; skipped {skip:,}.")

# -------------------
# Filters
# -------------------
st.sidebar.subheader("Filters")
councils = _get_councils()
if not councils:
    st.info("No data yet. Try refreshing again later.")
    st.stop()

sel_council = st.sidebar.selectbox("Council", councils, index=0)
start_date = st.sidebar.date_input("Start date", value=date(2023, 1, 1))
end_date = st.sidebar.date_input("End date", value=date.today())
supplier_query = st.sidebar.text_input("Supplier contains", "")

params = [sel_council, start_date.isoformat(), end_date.isoformat()]
sql = """
SELECT * FROM payments
WHERE council = ?
  AND payment_date BETWEEN ? AND ?
"""
if supplier_query.strip():
    sql += " AND lower(supplier) LIKE ?"
    params.append(f"%{supplier_query.lower()}%")

df = _query_df(sql, tuple(params))

# -------------------
# KPIs
# -------------------
cols = st.columns(3)
with cols[0]:
    st.metric("Total paid", f"£{df['amount_gbp'].sum():,.2f}")
with cols[1]:
    st.metric("Transactions", f"{len(df):,}")
with cols[2]:
    ts = pd.to_datetime(df["payment_date"], errors="coerce")
    if not ts.empty and ts.notna().any():
        st.metric("Date range", f"{ts.min().date()} → {ts.max().date()}")
    else:
        st.metric("Date range", "—")

# -------------------
# Charts
# -------------------
if df.empty:
    st.warning("No rows match your filters.")
else:
    left, right = st.columns(2)
    with left:
        sup = (
            df.groupby("supplier", dropna=False, as_index=False)["amount_gbp"]
            .sum()
            .sort_values("amount_gbp", ascending=False)
            .head(10)
        )
        st.plotly_chart(px.bar(sup, x="supplier", y="amount_gbp", title="Top suppliers (by £)"), use_container_width=True)

    with right:
        dt = pd.to_datetime(df["payment_date"], errors="coerce")
        df_time = (
            df.assign(payment_month=dt.dt.to_period("M").dt.to_timestamp())
            .groupby("payment_month", as_index=False)["amount_gbp"]
            .sum()
        )
        st.plotly_chart(px.line(df_time, x="payment_month", y="amount_gbp", title="Payments over time"), use_container_width=True)

    # Map if lat/lon available
    if {"lat", "lon"}.issubset(df.columns) and df[["lat", "lon"]].notna().any().any():
        figm = px.scatter_mapbox(
            df.dropna(subset=["lat", "lon"]),
            lat="lat",
            lon="lon",
            hover_name="supplier",
            hover_data={"amount_gbp": ":.2f", "description": True, "lat": False, "lon": False},
            size="amount_gbp",
            zoom=5,
            height=450,
        )
        figm.update_layout(mapbox_style="open-street-map", margin=dict(l=0, r=0, t=40, b=0), title="Geocoded payments")
        st.plotly_chart(figm, use_container_width=True)

# -------------------
# Anomalies / Alerts
# -------------------
with st.expander("Anomalies / Alerts", expanded=False):
    a1 = _query_df("""
        SELECT id, council, supplier, amount_gbp, payment_date
        FROM payments
        WHERE council = ? AND amount_gbp > 100000
        ORDER BY amount_gbp DESC
    """, (sel_council,))
    st.subheader("Large payments (> £100k)")
    st.dataframe(a1, use_container_width=True)

    a2 = _query_df("""
        SELECT council, supplier, strftime('%Y-%m', payment_date) AS ym, COUNT(*) AS cnt, SUM(amount_gbp) AS total
        FROM payments
        WHERE council = ?
        GROUP BY council, supplier, ym
        HAVING cnt > 5
        ORDER BY cnt DESC
    """, (sel_council,))
    st.subheader("Frequent monthly payments (>5)")
    st.dataframe(a2, use_container_width=True)

    a3 = _query_df("""
        SELECT invoice_ref, COUNT(*) AS cnt, SUM(amount_gbp) AS total
        FROM payments
        WHERE council = ? AND invoice_ref IS NOT NULL AND TRIM(invoice_ref) <> ''
        GROUP BY invoice_ref
        HAVING cnt > 1
        ORDER BY cnt DESC
    """, (sel_council,))
    st.subheader("Duplicate invoice references")
    st.dataframe(a3, use_container_width=True)

    a4 = _query_df("""
        SELECT id, supplier, amount_gbp, payment_date, description
        FROM payments
        WHERE council = ? AND (invoice_ref IS NULL OR TRIM(invoice_ref) = '')
        ORDER BY payment_date DESC
    """, (sel_council,))
    st.subheader("Payments without invoice reference")
    st.dataframe(a4, use_container_width=True)

    dom = _query_df("""
        WITH sums AS (
            SELECT supplier, SUM(amount_gbp) AS total
            FROM payments
            WHERE council = ?
            GROUP BY supplier
        ), grand AS (
            SELECT SUM(amount_gbp) AS gt FROM payments WHERE council = ?
        )
        SELECT s.supplier, s.total, g.gt, 100.0 * s.total / g.gt AS pct
        FROM sums s, grand g
        ORDER BY s.total DESC
        LIMIT 1
    """, (sel_council, sel_council))
    if not dom.empty and float(dom["pct"].iloc[0]) > 50.0:
        st.error(f"Supplier dominance: {dom['supplier'].iloc[0]} accounts for {dom['pct'].iloc[0]:.1f}% of spend (£{dom['total'].iloc[0]:,.0f}).")

# -------------------
# Feedback form
# -------------------
st.header("Citizen feedback")
with st.form("feedback"):
    pid = st.number_input("Payment ID", min_value=1, step=1)
    uname = st.text_input("Your name (optional)")
    comment = st.text_area("Comment")
    rating = st.slider("Rating", 1, 5, 3)
    submitted = st.form_submit_button("Submit feedback")
    if submitted:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO feedback (payment_id, user_name, comment, rating) VALUES (?,?,?,?)",
            (int(pid), uname.strip(), comment.strip(), int(rating)),
        )
        conn.commit()
        st.success("Thanks! Your feedback has been recorded.")

fb = _query_df("""
    SELECT f.created_at, f.payment_id, f.user_name, f.comment, f.rating
    FROM feedback f
    WHERE f.payment_id IN (SELECT id FROM payments WHERE council = ?)
    ORDER BY f.created_at DESC
    LIMIT 200
""", (sel_council,))
if not fb.empty:
    st.subheader("Recent feedback")
    st.dataframe(fb, use_container_width=True)

# -------------------
# CSV export
# -------------------
if not df.empty:
    st.download_button(
        "Download filtered CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{sel_council}_payments.csv",
        mime="text/csv",
    )
