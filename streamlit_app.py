# streamlit_app.py

import io
import os
import time
import sqlite3
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

import pandas as pd
import streamlit as st

# --- Use ONLY modules that actually exist in this repo ---
import fetch_and_ingest as ingest  # insert_records + geocode hook lives here
from fetch_and_ingest import insert_records
from db_schema import create_tables
from pattern_detection import detect_anomalies
from council_auto_discovery import discover_new_councils, fetch_new_council_csv

DB_NAME = "spend.db"


# --------------------------
# Helpers
# --------------------------
def run_once_per_session(key: str) -> bool:
    """Return True the first time per session for a given key."""
    if key not in st.session_state:
        st.session_state[key] = True
        return True
    return False


def fetch_records_with_timeout(url: str, council_name: str, timeout_secs: float = 3.0):
    """
    Call council_auto_discovery.fetch_new_council_csv(url, council_name)
    but enforce a wall-clock timeout so we can skip slow councils.
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fetch_new_council_csv, url, council_name)
        return fut.result(timeout=timeout_secs)


def safe_insert(records, geocode_enabled: bool):
    """
    Insert records using fetch_and_ingest.insert_records.
    If geocoding is disabled, monkey-patch ingest.geocode_address to a no-op.
    """
    original_geocoder = getattr(ingest, "geocode_address", None)

    try:
        if not geocode_enabled and original_geocoder is not None:
            # Disable geocoding for speed on the initial auto run
            ingest.geocode_address = lambda supplier: (None, None)

        insert_records(records)

    finally:
        # Restore original geocoder
        if original_geocoder is not None:
            ingest.geocode_address = original_geocoder


def ensure_db():
    # Create tables if needed
    create_tables()
    # Ensure DB file exists
    if not os.path.exists(DB_NAME):
        open(DB_NAME, "a").close()


def load_existing_dataframe(selected_council=None, date_from=None, date_to=None) -> pd.DataFrame:
    query = "SELECT council, payment_date, supplier, description, category, amount_gbp, invoice_ref, lat, lon FROM payments"
    clauses = []
    params = []

    if selected_council and selected_council != "All":
        clauses.append("council = ?")
        params.append(selected_council)

    if date_from:
        clauses.append("DATE(payment_date) >= DATE(?)")
        params.append(date_from)

    if date_to:
        clauses.append("DATE(payment_date) <= DATE(?)")
        params.append(date_to)

    if clauses:
        query += " WHERE " + " AND ".join(clauses)

    query += " ORDER BY DATE(payment_date) DESC"

    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()
    return df


def list_councils_in_db() -> list:
    conn = sqlite3.connect(DB_NAME)
    try:
        c = conn.cursor()
        c.execute("SELECT DISTINCT council FROM payments ORDER BY council ASC")
        rows = [r[0] for r in c.fetchall()]
    finally:
        conn.close()
    return rows


def discover_and_ingest(geocode_enabled: bool, max_pages_info: str = ""):
    """
    1) Discover councils + CSV URLs
    2) Fetch each council's records with a 3-second timeout
    3) Skip slow ones and retry once after the first pass
    4) Insert into DB (geocoding optional)
    """
    with st.status("Starting discovery…", state="running") as status:
        status.update(label="Discovering councils on data.gov.uk…")

        # Discover (uses your council_auto_discovery module)
        try:
            discovered = discover_new_councils()
        except Exception as e:
            st.error(f"Discovery failed: {e}")
            st.text(traceback.format_exc())
            return 0, 0, 0

        total = len(discovered)
        status.update(label=f"Discovery complete: {total} council CSVs found. Preparing to fetch…")

        progress = st.progress(0, text="Fetching & inserting data…")
        successes, failures, timeouts = 0, 0, 0
        retry_queue = []

        # First pass (skip anything that takes >3s)
        for idx, (council_name, url) in enumerate(discovered, start=1):
            progress.progress(min(idx / max(total, 1), 1.0),
                              text=f"[{idx}/{total}] {council_name} — fetching (3s timeout)…")

            start = time.time()
            try:
                records = fetch_records_with_timeout(url, council_name, timeout_secs=3.0)
                # insert
                safe_insert(records, geocode_enabled=geocode_enabled)
                successes += 1
            except FuturesTimeout:
                timeouts += 1
                retry_queue.append((council_name, url))
            except Exception as e:
                failures += 1
                st.write(f"Skipping {council_name} due to error: {e}")

            elapsed = time.time() - start
            # Small sleep to keep UI responsive
            if elapsed < 0.05:
                time.sleep(0.02)

        # Retry pass (once) for timeouts
        if retry_queue:
            status.update(label=f"Retrying {len(retry_queue)} timed-out councils (once)…")
            for idx, (council_name, url) in enumerate(retry_queue, start=1):
                progress.progress(min(idx / max(len(retry_queue), 1), 1.0),
                                  text=f"[retry {idx}/{len(retry_queue)}] {council_name} — fetching (3s timeout)…")
                try:
                    records = fetch_records_with_timeout(url, council_name, timeout_secs=3.0)
                    safe_insert(records, geocode_enabled=geocode_enabled)
                    successes += 1
                except Exception:
                    failures += 1  # if it times out again or errors, count as failure

        status.update(
            label=f"Done. Success: {successes}, Failed: {failures}, Timed out (not inserted): {timeouts}.",
            state="complete"
        )

    return successes, failures, timeouts


# --------------------------
# UI
# --------------------------
st.set_page_config(page_title="UK Public Spending Tracker", layout="wide")
st.title("UK Public Spending Tracker")

with st.sidebar:
    st.header("Controls")
    st.caption("Initial load runs automatically without geocoding for speed. "
               "Use the **Update & Geocode (slow)** button to enrich with coordinates.")
    auto_limit = st.number_input("Preview rows per council (UI only)", 50, 2000, 200, step=50)

# Prepare the DB
with st.spinner("Setting up database..."):
    ensure_db()

# Auto-run on first load (no geocoding)
if run_once_per_session("__bootstrapped__"):
    st.info("Auto-loading councils & payments (geocoding OFF for speed)…")
    succ, fail, tout = discover_and_ingest(geocode_enabled=False)
    st.success(f"Initial load complete. Success: {succ}, Failures: {fail}, Timeouts: {tout}.")
else:
    st.caption("Session active. Use the update button to refresh.")

# Update button (WITH geocoding, slow)
if st.button("🔄 Update & Geocode (slow)"):
    st.warning("This may be **slow** due to geocoding. Please keep the tab open; progress will be shown below.")
    succ, fail, tout = discover_and_ingest(geocode_enabled=True)
    st.success(f"Update complete. Success: {succ}, Failures: {fail}, Timeouts: {tout}.")

st.divider()

# --------------------------
# Data explorer
# --------------------------
st.subheader("Explore data")

# Filters
councils = ["All"] + list_councils_in_db()
left, right = st.columns(2)
with left:
    selected_council = st.selectbox("Council", councils, index=0)
with right:
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input("From", value=None)
    with col2:
        date_to = st.date_input("To", value=None)

# Load filtered data
df = load_existing_dataframe(
    selected_council=None if selected_council == "All" else selected_council,
    date_from=str(date_from) if date_from else None,
    date_to=str(date_to) if date_to else None,
)

# Show preview
if df.empty:
    st.warning("No data available yet for the selected filters.")
else:
    st.write(f"Showing {min(len(df), auto_limit)} of {len(df)} rows")
    st.dataframe(df.head(auto_limit), use_container_width=True)

    # Simple summaries
    with st.expander("Summary"):
        total_amount = df["amount_gbp"].fillna(0).sum() if "amount_gbp" in df.columns else 0
        st.write(f"**Payments:** {len(df):,} | **Total amount (shown rows)**: £{total_amount:,.2f}")

# --------------------------
# Anomaly detection (uses your pattern_detection.py)
# --------------------------
st.subheader("Pattern detection")
try:
    large, frequent = detect_anomalies()
    colA, colB = st.columns(2)
    with colA:
        st.write("**Large payments**")
        if large:
            st.dataframe(pd.DataFrame(large, columns=["council", "supplier", "amount_gbp"]))
        else:
            st.caption("No large payments flagged.")
    with colB:
        st.write("**Frequent payments**")
        if frequent:
            st.dataframe(pd.DataFrame(frequent, columns=["council", "supplier", "cnt"]))
        else:
            st.caption("No frequent payments flagged.")
except Exception as e:
    st.warning(f"Pattern detection unavailable: {e}")

# --------------------------
# CSV download for current filter
# --------------------------
st.subheader("Export")
if not df.empty:
    csv_data = df.to_csv(index=False).encode("utf-8")
    fname_council = (selected_council or "All").replace(" ", "_")
    st.download_button(
        label="Download current view as CSV",
        data=csv_data,
        file_name=f"{fname_council}_payments.csv",
        mime="text/csv"
    )

st.caption("Tip: Use the update button to refresh data and add geocodes (slow).")
