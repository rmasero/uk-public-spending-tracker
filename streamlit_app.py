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
from pattern_detection import detect_anomalies

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
# Fetcher loader (optional custom council parsers)
# -------------------
def _load_predefined_councils() -> List[Tuple[str, str, Callable]]:
    fetcher_dir = "council_fetchers"
    if not os.path.isdir(fetcher_dir):
        return []
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
# Refresh logic with progress + 3s timeout + one retry
# -------------------
def _call_with_timeout(fn: Callable, timeout_s: float = 3.0):
    start = time.time()
    try:
        out = fn()
    except Exception:
        return None, False, 0.0
    elapsed = time.time() - start
    if elapsed > timeout_s:
        return None, True, elapsed
    return out, False, elapsed

def refresh_all_data(do_geocode: bool = False, progress=None):
    inserted_total = skipped_total = 0
    failed = []

    # 1) Predefined councils first
    items = _load_predefined_councils()
    n = len(items)
    for i, (name, csv_url, fetch_fn) in enumerate(items, start=1):
        pct = int(5 + (i / max(1, n)) * 35)
        if progress:
            progress.progress(pct, text=f"[{i}/{n}] Fetching (custom): {name}")
        try:
            if callable(fetch_fn):
                records, timed_out, elapsed = _call_with_timeout(fetch_fn, timeout_s=3.0)
            elif csv_url:
                records, timed_out, elapsed = _call_with_timeout(lambda: fetch_new_council_csv(csv_url, name, timeout=3), timeout_s=3.0)
            else:
                records, timed_out, elapsed = ([], False, 0.0)

            if records is None or timed_out:
                failed.append((name, csv_url, fetch_fn))
                continue

            ins, skip = insert_records(records, do_geocode=do_geocode)
            inserted_total += ins
            skipped_total += skip
        except Exception:
            failed.append((name, csv_url, fetch_fn))

    # 2) Discovery from data.gov.uk (no cache)
    if progress:
        progress.progress(42, text="Discovering councils and CSVs from data.gov.uk …")
    try:
        discovered = discover_new_councils()
    except Exception as e:
        discovered = []
        if progress:
            progress.progress(45, text=f"Discovery error: {e}")

    m = len(discovered)
    for j, (name, url) in enumerate(discovered, start=1):
        pct = int(45 + (j / max(1, m)) * 45)
        if progress and (j % 10 == 0 or j == m):
            progress.progress(pct, text=f"[{j}/{m}] Importing: {name}")
        try:
            records, timed_out, elapsed = _call_with_timeout(lambda: fetch_new_council_csv(url, name, timeout=3), timeout_s=3.0)
            if records is None or timed_out:
                failed.append((name, url, None))
                continue
            ins, skip = insert_records(records, do_geocode=False)  # no geocode for speed
            inserted_total += ins
            skipped_total += skip
        except Exception:
            failed.append((name, url, None))

    # 3) Retry once
    for k, (name, url, fetch_fn) in enumerate(failed, start=1):
        if progress:
            progress.progress(95, text=f"Retrying ({k}/{len(failed)}): {name}")
        try:
            if callable(fetch_fn):
                records = fetch_fn()
            elif url:
                records = fetch_new_council_csv(url, name, timeout=6)  # slightly longer
            else:
                continue
            ins, skip = insert_records(records, do_geocode=do_geocode)
            inserted_total += ins
            skipped_total += skip
        except Exception:
            pass

    if progress:
        progress.progress(100, text="All done!")
    return inserted_total, skipped_total

# -------------------
# App start
# -------------------
with st.spinner("Setting up database…"):
    create_tables()

st.sidebar.header("Data controls")
st.sidebar.info("Automatic refresh runs on startup.\n\nUse the button to refresh with geocoding (slower).")

geocode_refresh = st.sidebar.button("Refresh with geocoding (slow)")

if "data_loaded" not in st.session_state:
    progress = st.sidebar.progress(0, text="Loading councils…")
    ins, skip = refresh_all_data(do_geocode=False, progress=progress)
    st.success(f"Startup load complete. Inserted {ins:,} new rows; skipped {skip:,}.")
    st.session_state.data_loaded = True
elif geocode_refresh:
    st.warning("Geocoding suppliers will be **slow** (free Nominatim). Running now…")
    progress = st.sidebar.progress(0, text="Refreshing with geocoding…")
    ins, skip = refresh_all_data(do_geocode=True, progress=progress)
    st.success(f"Geocoded refresh complete. Inserted {ins:,} new rows; skipped {skip:,}.")

# -------------------
# Filters
# -------------------
st.sidebar.subheader("Filters")
councils = _get_councils()
if not councils:
    st.info("No data yet. Try refreshing again.")
    st.stop()

sel_council = st.sidebar.selectbox("Council", councils, index=0)
start_date = st.sidebar.date_input("Start date", value=date(2023, 1, 1))
end_date = st.sidebar.date_input("End date", value=date.today())
supplier_query = st.sidebar.text_input("Supplier contains", "")

params = [sel_council, start_date.isoformat(), end_date.isoformat()]
sql = """
SELECT * FROM payments
WHERE council = ?
  AND (payment_date IS NULL OR payment_date BETWEEN ? AND ?)
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

# -------------------
# Anomalies / Alerts
# -------------------
with st.expander("Anomalies / Alerts", expanded=False):
    large, frequent, dup_inv, no_inv = detect_anomalies(sel_council)

    st.subheader("Large payments (> £100k)")
    if large:
        st.dataframe(pd.DataFrame(large, columns=["id","council","supplier","amount_gbp","payment_date"]), use_container_width=True)
    else:
        st.caption("None found.")

    st.subheader("Frequent monthly payments (>5)")
    if frequent:
        st.dataframe(pd.DataFrame(frequent, columns=["council","supplier","ym","cnt","total"]), use_container_width=True)
    else:
        st.caption("None found.")

    st.subheader("Duplicate invoice references")
    if dup_inv:
        st.dataframe(pd.DataFrame(dup_inv, columns=["invoice_ref","cnt","total"]), use_container_width=True)
    else:
        st.caption("None found.")

    st.subheader("Payments without invoice reference")
    if no_inv:
        st.dataframe(pd.DataFrame(no_inv, columns=["id","supplier","amount_gbp","payment_date","description"]), use_container_width=True)
    else:
        st.caption("None found.")

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
