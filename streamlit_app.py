# streamlit_app.py (debug version)

import io
import os
import time
import sqlite3
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

import pandas as pd
import streamlit as st

import fetch_and_ingest as ingest
from fetch_and_ingest import insert_records
from db_schema import create_tables
from pattern_detection import detect_anomalies
from council_auto_discovery import discover_new_councils, fetch_new_council_csv

DB_NAME = "spend.db"


def run_once_per_session(key: str) -> bool:
    if key not in st.session_state:
        st.session_state[key] = True
        return True
    return False


def fetch_records_with_timeout(url: str, council_name: str, timeout_secs: float = 3.0):
    """Fetch records with timeout (returns list of dicts)."""
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fetch_new_council_csv, url, council_name)
        return fut.result(timeout=timeout_secs)


def safe_insert(records, geocode_enabled: bool):
    """Insert into DB with optional geocoding."""
    original_geocoder = getattr(ingest, "geocode_address", None)
    try:
        if not geocode_enabled and original_geocoder is not None:
            ingest.geocode_address = lambda supplier: (None, None)
        insert_records(records)
    finally:
        if original_geocoder is not None:
            ingest.geocode_address = original_geocoder


def ensure_db():
    create_tables()
    if not os.path.exists(DB_NAME):
        open(DB_NAME, "a").close()


def discover_and_ingest(geocode_enabled: bool):
    """Discover councils, fetch CSVs, insert into DB. Debug logging enabled."""
    with st.status("Discovering councils…", state="running") as status:
        try:
            discovered = discover_new_councils()
        except Exception as e:
            st.error(f"Discovery failed: {e}")
            st.text(traceback.format_exc())
            return 0, 0, 0

        total = len(discovered)
        status.update(label=f"Discovered {total} council CSVs. Starting fetch…")

        progress = st.progress(0)
        successes, failures, timeouts = 0, 0, 0
        error_logs = []

        for idx, (council_name, url) in enumerate(discovered, start=1):
            progress.progress(min(idx / max(total, 1), 1.0),
                              text=f"[{idx}/{total}] {council_name}")

            try:
                start = time.time()
                records = fetch_records_with_timeout(url, council_name, timeout_secs=3.0)

                safe_insert(records, geocode_enabled=geocode_enabled)
                successes += 1

            except FuturesTimeout:
                timeouts += 1
                error_logs.append({
                    "council": council_name,
                    "url": url,
                    "error": "Timeout after 3s",
                    "traceback": ""
                })
            except Exception as e:
                failures += 1
                tb = traceback.format_exc()
                st.error(f"❌ {council_name} failed: {e}")
                st.code(tb, language="python")

                # Log for download
                error_logs.append({
                    "council": council_name,
                    "url": url,
                    "error": str(e),
                    "traceback": tb
                })

        status.update(
            label=f"Done. Success: {successes}, Failures: {failures}, Timeouts: {timeouts}",
            state="complete"
        )

    # Save errors in session_state so user can download them
    st.session_state["error_logs"] = pd.DataFrame(error_logs)
    return successes, failures, timeouts


def list_councils_in_db():
    conn = sqlite3.connect(DB_NAME)
    try:
        c = conn.cursor()
        c.execute("SELECT DISTINCT council FROM payments ORDER BY council ASC")
        return [r[0] for r in c.fetchall()]
    finally:
        conn.close()


def load_existing_dataframe(selected_council=None):
    query = "SELECT council, payment_date, supplier, description, category, amount_gbp, invoice_ref, lat, lon FROM payments"
    params = []
    if selected_council and selected_council != "All":
        query += " WHERE council = ?"
        params.append(selected_council)

    conn = sqlite3.connect(DB_NAME)
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


# --------------------------
# Streamlit UI
# --------------------------
st.set_page_config(page_title="UK Public Spending Tracker", layout="wide")
st.title("UK Public Spending Tracker — Debug Mode")

ensure_db()

if run_once_per_session("__bootstrapped__"):
    st.info("Auto-loading councils & payments (geocoding OFF for speed)…")
    succ, fail, tout = discover_and_ingest(geocode_enabled=False)
    st.success(f"Initial load complete. Success: {succ}, Failures: {fail}, Timeouts: {tout}.")

if st.button("🔄 Update & Geocode (slow)"):
    succ, fail, tout = discover_and_ingest(geocode_enabled=True)
    st.success(f"Update complete. Success: {succ}, Failures: {fail}, Timeouts: {tout}.")

st.divider()
st.subheader("Error logs")

if "error_logs" in st.session_state and not st.session_state["error_logs"].empty:
    st.dataframe(st.session_state["error_logs"], use_container_width=True)
    st.download_button(
        "Download error log CSV",
        st.session_state["error_logs"].to_csv(index=False).encode("utf-8"),
        "error_log.csv",
        "text/csv"
    )
else:
    st.caption("No errors recorded yet.")
