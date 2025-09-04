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
    query = "SELECT
