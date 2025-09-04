# streamlit_app.py

import io
import os
import time
import json
import csv
import sqlite3
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

import pandas as pd
import requests
import streamlit as st

# --- Use only modules that exist in this repo ---
import fetch_and_ingest as ingest  # insert_records + optional geocode hook lives here
from fetch_and_ingest import insert_records
from db_schema import create_tables
from pattern_detection import detect_anomalies
from council_auto_discovery import discover_new_councils, fetch_new_council_csv
from council_fetchers import FETCHERS  # to detect custom fetchers

DB_NAME = "spend.db"

# =========================
# Error logging / reporting
# =========================
def new_error_record(**kwargs) -> dict:
    rec = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "council": None,
        "url": None,
        "stage": None,              # "preflight", "fetch", "insert", "retry_fetch", "retry_insert"
        "is_custom_fetcher": None,
        "http_status": None,
        "content_type": None,
        "content_length": None,
        "resolved_url": None,
        "error_type": None,
        "error_message": None,
        "traceback": None,
        "snippet": None,            # first bytes of response text (if HTML/error)
    }
    rec.update({k: v for k, v in kwargs.items() if k in rec or k == "extra"})
    return rec


def save_error_report(errors: list):
    if not errors:
        return None, None
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    base = f"/mnt/data/fetch_failures_{ts}"
    json_path = f"{base}.json"
    csv_path = f"{base}.csv"

    # JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    # CSV (flatten core keys)
    core_cols = [
        "timestamp", "council", "url", "stage", "is_custom_fetcher",
        "http_status", "content_type", "content_length", "resolved_url",
        "error_type", "error_message", "snippet"
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=core_cols)
        w.writeheader()
        for e in errors:
            row = {k: e.get(k) for k in core_cols}
            # trim very large snippet
            if row.get("snippet") and len(row["snippet"]) > 500:
                row["snippet"] = row["snippet"][:500] + " …"
            w.writerow(row)

    return json_path, csv_path


# =========================
# Infra helpers
# =========================
def run_once_per_session(key: str) -> bool:
    if key not in st.session_state:
        st.session_state[key] = True
        return True
    return False


def ensure_db():
    create_tables()
    if not os.path.exists(DB_NAME):
        open(DB_NAME, "a").close()


def list_councils_in_db() -> list:
    conn = sqlite3.connect(DB_NAME)
    try:
        c = conn.cursor()
        c.execute("SELECT DISTINCT council FROM payments ORDER BY council ASC")
        rows = [r[0] for r in c.fetchall()]
    finally:
        conn.close()
    return rows


def load_existing_dataframe(selected_council=None, date_from=None, date_to=None) -> pd.DataFrame:
    query = "SELECT council, payment_date, supplier, description, category, amount_gbp, invoice_ref, lat, lon FROM payments"
    clauses, params = [], []
    if selected_council and selected_council != "All":
        clauses.append("council = ?"); params.append(selected_council)
    if date_from:
        clauses.append("DATE(payment_date) >= DATE(?)"); params.append(date_from)
    if date_to:
        clauses.append("DATE(payment_date) <= DATE(?)"); params.append(date_to)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY DATE(payment_date) DESC"
    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()
    return df


def safe_insert(records, geocode_enabled: bool):
    """
    Insert records using fetch_and_ingest.insert_records.
    If geocoding is disabled, temporarily replace ingest.geocode_address with a no-op.
    """
    original_geocoder = getattr(ingest, "geocode_address", None)
    try:
        if not geocode_enabled and original_geocoder is not None:
            ingest.geocode_address = lambda supplier: (None, None)
        insert_records(records)
    finally:
        if original_geocoder is not None:
            ingest.geocode_address = original_geocoder


# =========================
# Debug / diagnostics
# =========================
def preflight_url(url: str, timeout_secs=3.0) -> dict:
    """
    Try HEAD, fall back to ranged GET. Capture headers and a small snippet.
    """
    info = {
        "http_status": None,
        "content_type": None,
        "content_length": None,
        "resolved_url": None,
        "snippet": None,
    }

    try:
        try:
            r = requests.head(url, allow_redirects=True, timeout=timeout_secs)
            info["http_status"] = r.status_code
            info["content_type"] = r.headers.get("Content-Type")
            info["content_length"] = r.headers.get("Content-Length")
            info["resolved_url"] = r.url
            # Some servers don't support HEAD; fall back if weird
            if r.status_code >= 400 or (info["content_type"] is None and info["content_length"] is None):
                raise Exception("HEAD not informative, trying GET")
        except Exception:
            r = requests.get(url, allow_redirects=True, timeout=timeout_secs, stream=True)
            info["http_status"] = r.status_code
            info["content_type"] = r.headers.get("Content-Type")
            info["content_length"] = r.headers.get("Content-Length")
            info["resolved_url"] = r.url
            try:
                snippet = r.raw.read(2048, decode_content=True)
            except Exception:
                snippet = r.content[:2048]
            # Safely decode snippet
            try:
                info["snippet"] = snippet.decode("utf-8", errors="replace")
            except Exception:
                info["snippet"] = str(snippet)[:2048]
    except Exception as e:
        info["http_status"] = None
        info["snippet"] = f"Preflight failed: {type(e).__name__}: {e}"
    return info


def fetch_records_with_timeout(url: str, council_name: str, timeout_secs: float = 3.0):
    """
    Wrap council_auto_discovery.fetch_new_council_csv with a wall-clock timeout.
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fetch_new_council_csv, url, council_name)
        return fut.result(timeout=timeout_secs)


# =========================
# Orchestration
# =========================
def discover_and_ingest(geocode_enabled: bool, debug_mode: bool, limit: int | None):
    """
    1) Discover councils + CSV URLs
    2) Fetch each council with a 3s timeout (skip slow), capture diagnostics on failure
    3) Retry once for timeouts after first pass
    4) Insert using insert_records (geocoding optional)
    Returns: (successes, failures, timeouts, errors_list)
    """
    errors = []

    with st.status("Starting discovery…", state="running") as status:
        try:
            discovered = discover_new_councils()
        except Exception as e:
            st.error(f"Discovery failed: {e}")
            st.text(traceback.format_exc())
            return 0, 0, 0, errors

        total = len(discovered)
        if limit and limit > 0:
            discovered = discovered[:limit]
            total = len(discovered)

        status.update(label=f"Discovery complete: {total} council CSVs found. Fetching…")

        progress = st.progress(0, text="Fetching & inserting data…")
        successes = failures = timeouts = 0
        retry_queue = []

        # Pass 1
        for idx, (council_name, url) in enumerate(discovered, start=1):
            is_custom = council_name in FETCHERS
            progress.progress(min(idx / max(total, 1), 1.0),
                              text=f"[{idx}/{total}] {council_name} — {'custom fetcher' if is_custom else 'CSV URL'} (3s timeout)…")

            try:
                start = time.time()
                recs = fetch_records_with_timeout(url, council_name, timeout_secs=3.0)
                safe_insert(recs, geocode_enabled=geocode_enabled)
                successes += 1
            except FuturesTimeout:
                timeouts += 1
                retry_queue.append((council_name, url))
                if debug_mode:
                    errors.append(new_error_record(
                        council=council_name, url=url, stage="fetch",
                        is_custom_fetcher=is_custom, error_type="Timeout",
                        error_message="Timed out after 3s"
                    ))
            except Exception as e:
                failures += 1
                err = new_error_record(
                    council=council_name, url=url, stage="fetch",
                    is_custom_fetcher=is_custom, error_type=type(e).__name__,
                    error_message=str(e), traceback=traceback.format_exc(),
                )
                # Try to capture HTTP info for non-custom path
                if not is_custom:
                    info = preflight_url(url, timeout_secs=3.0)
                    err.update(info)
                errors.append(err)

            # keep UI responsive
            elapsed = time.time() - start
            if elapsed < 0.02:
                time.sleep(0.01)

        # Retry timeouts once
        if retry_queue:
            status.update(label=f"Retrying {len(retry_queue)} timed-out councils once…")
            for idx, (council_name, url) in enumerate(retry_queue, start=1):
                is_custom = council_name in FETCHERS
                progress.progress(min(idx / max(len(retry_queue), 1), 1.0),
                                  text=f"[retry {idx}/{len(retry_queue)}] {council_name} — 3s timeout again if slow…")
                try:
                    recs = fetch_records_with_timeout(url, council_name, timeout_secs=3.0)
                    safe_insert(recs, geocode_enabled=geocode_enabled)
                    successes += 1
                except Exception as e:
                    failures += 1
                    err = new_error_record(
                        council=council_name, url=url, stage="retry_fetch",
                        is_custom_fetcher=is_custom, error_type=type(e).__name__,
                        error_message=str(e), traceback=traceback.format_exc(),
                    )
                    if not is_custom:
                        info = preflight_url(url, timeout_secs=3.0)
                        err.update(info)
                    errors.append(err)

        status.update(
            label=f"Done. Success: {successes}, Failed: {failures}, Timed out (not inserted): {timeouts}.",
            state="complete"
        )

    return successes, failures, timeouts, errors


# =========================
# UI
# =========================
st.set_page_config(page_title="UK Public Spending Tracker", layout="wide")
st.title("UK Public Spending Tracker")

with st.sidebar:
    st.header("Controls")
    st.caption("Initial load runs automatically *without geocoding* for speed. "
               "Use **Update & Geocode (slow)** to enrich with coordinates.")
    debug_mode = st.toggle("Debug mode (capture full error details)", value=True)
    limit = st.number_input("Debug: limit councils (0 = all)", min_value=0, max_value=5000, value=0, step=50)
    preview_rows = st.number_input("Preview rows to show", 50, 2000, 200, step=50)

# Prepare DB
with st.spinner("Setting up database…"):
    ensure_db()

# Auto-run on first load (no geocoding)
if run_once_per_session("__bootstrapped__"):
    st.info("Auto-loading councils & payments (geocoding OFF for speed)…")
    succ, fail, tout, errs = discover_and_ingest(
        geocode_enabled=False,
        debug_mode=debug_mode,
        limit=(None if limit == 0 else int(limit)),
    )
    st.success(f"Initial load complete. Success: {succ}, Failures: {fail}, Timeouts: {tout}.")
    st.session_state["last_errors"] = errs
else:
    st.caption("Session active. Use the update button to refresh.")

# Update button (WITH geocoding, slow)
if st.button("🔄 Update & Geocode (slow)"):
    st.warning("This may be **slow** due to geocoding. Keep this tab open; progress is shown below.")
    succ, fail, tout, errs = discover_and_ingest(
        geocode_enabled=True,
        debug_mode=debug_mode,
        limit=(None if limit == 0 else int(limit)),
    )
    st.success(f"Update complete. Success: {succ}, Failures: {fail}, Timeouts: {tout}.")
    st.session_state["last_errors"] = errs

st.divider()

# =========================
# Failure diagnostics UI
# =========================
st.subheader("Failure diagnostics")

errors = st.session_state.get("last_errors", [])
if not errors:
    st.caption("No failures recorded in this session.")
else:
    # Compact summary table
    df_err = pd.DataFrame([{
        "timestamp": e.get("timestamp"),
        "council": e.get("council"),
        "stage": e.get("stage"),
        "is_custom_fetcher": e.get("is_custom_fetcher"),
        "http_status": e.get("http_status"),
        "content_type": e.get("content_type"),
        "error_type": e.get("error_type"),
        "error_message": (e.get("error_message") or "")[:200],
    } for e in errors])

    st.write(f"Failures recorded: **{len(errors)}**")
    st.dataframe(df_err, use_container_width=True, height=min(400, 100 + 24 * len(df_err)))

    with st.expander("View full details per failure"):
        for i, e in enumerate(errors, 1):
            st.markdown(f"**{i}. {e.get('council')}** — stage: `{e.get('stage')}`, custom_fetcher: `{e.get('is_custom_fetcher')}`")
            meta_cols = st.columns(3)
            with meta_cols[0]:
                st.caption(f"HTTP: {e.get('http_status')} / {e.get('content_type')}")
            with meta_cols[1]:
                st.caption(f"Resolved URL: {e.get('resolved_url') or '—'}")
            with meta_cols[2]:
                st.caption(f"Len: {e.get('content_length') or '—'}")
            st.code(f"{e.get('error_type')}: {e.get('error_message')}")
            if e.get("snippet"):
                st.text_area("Response snippet", value=e["snippet"], height=120)
            if e.get("traceback"):
                st.text_area("Traceback", value=e["traceback"], height=160)
            st.markdown("---")

    # Save + download buttons
    json_path, csv_path = save_error_report(errors)
    if json_path and csv_path:
        with open(json_path, "rb") as f:
            st.download_button("Download error report (JSON)", f, file_name=os.path.basename(json_path), mime="application/json")
        with open(csv_path, "rb") as f:
            st.download_button("Download error report (CSV)", f, file_name=os.path.basename(csv_path), mime="text/csv")

st.divider()

# =========================
# Data explorer
# =========================
st.subheader("Explore data")

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

df = load_existing_dataframe(
    selected_council=None if selected_council == "All" else selected_council,
    date_from=str(date_from) if date_from else None,
    date_to=str(date_to) if date_to else None,
)

if df.empty:
    st.warning("No data available yet for the selected filters.")
else:
    st.write(f"Showing {min(len(df), preview_rows)} of {len(df)} rows")
    st.dataframe(df.head(preview_rows), use_container_width=True)

    with st.expander("Summary"):
        total_amount = df["amount_gbp"].fillna(0).sum() if "amount_gbp" in df.columns else 0
        st.write(f"**Payments:** {len(df):,} | **Total amount (shown rows)**: £{total_amount:,.2f}")

# =========================
# Anomaly detection
# =========================
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

# =========================
# Export current view
# =========================
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

st.caption("Tip: Use the **Update & Geocode (slow)** button to refresh data and add coordinates.")
