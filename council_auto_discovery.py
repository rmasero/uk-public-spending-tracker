# council_auto_discovery.py (updated)

import requests
import pandas as pd
from io import BytesIO

# Optional: place for special council fetchers if needed
FETCHERS = {}


def discover_new_councils():
    """
    Placeholder: should return a list of (council_name, url) pairs.
    In practice this might scrape a registry or load from a config.
    """
    # Example placeholder — in real app this is already filled dynamically
    return []


def fetch_new_council_csv(url, council_name):
    """
    Fetch a council's spending CSV.
    - Ensures valid HTTP response.
    - Skips HTML/invalid responses gracefully.
    - Returns a list of dicts for DB insertion.
    """

    if council_name in FETCHERS:
        return FETCHERS[council_name]()

    try:
        r = requests.get(url, timeout=10)

        # HTTP check
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code} when fetching {url}")

        # Content type check
        content_type = r.headers.get("Content-Type", "").lower()
        if "csv" not in content_type and "excel" not in content_type:
            # Not a CSV — likely HTML or error page
            snippet = r.text[:500]
            raise ValueError(
                f"Unexpected content type ({content_type}). "
                f"First 500 chars:\n{snippet}"
            )

        # Parse as CSV
        df = pd.read_csv(BytesIO(r.content))

    except Exception as e:
        # Raise with council name for
