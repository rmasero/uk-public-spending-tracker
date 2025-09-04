# council_auto_discovery.py

import requests
import streamlit as st

# Broader search query terms to capture all spending datasets
QUERY_TERMS = [
    "\"Council Spending\"",
    "\"Local Authority Spend\"",
    "\"Payments to suppliers\"",
    "\"Spend over\"",
    "\"Spend over £500\"",
]

Q = " OR ".join(QUERY_TERMS)

API_BASE = "https://data.gov.uk/api/3/action/package_search"


def _extract_url(res):
    """Return the best URL for a resource (prefer download_url)."""
    return res.get("download_url") or res.get("url")


def discover_new_councils(rows_per_page=1000, max_pages=10):
    """
    Discover new councils and their spending CSVs from data.gov.uk.
    Returns a list of (council_name, csv_url).
    """
    discovered = []
    seen_urls = set()

    st.info("Starting council discovery from data.gov.uk…")

    for page in range(max_pages):
        start = page * rows_per_page
        params = {
            "q": Q,
            "start": start,
            "rows": rows_per_page,
        }

        try:
            resp = requests.get(API_BASE, params=params, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            st.warning(f"Failed to fetch page {page+1}: {e}")
            continue

        data = resp.json()
        results = data.get("result", {}).get("results", [])
        if not results:
            st.write(f"No more datasets found after {page} pages.")
            break

        st.write(f"Page {page+1}: {len(results)} datasets fetched")

        for pkg in results:
            org = pkg.get("organization", {})
            council_name = org.get("title") or org.get("name")
            if not council_name:
                continue

            for res in pkg.get("resources", []):
                fmt = str(res.get("format", "")).lower()
                url = _extract_url(res)
                if fmt == "csv" and url and url not in seen_urls:
                    discovered.append((council_name, url))
                    seen_urls.add(url)

        # Stop if we’ve exhausted total results
        total = data.get("result", {}).get("count", 0)
        if start + rows_per_page >= total:
            st.write("All available datasets scanned.")
            break

    st.success(f"Discovery complete: {len(discovered)} council CSVs found")
    return discovered


if __name__ == "__main__":
    # For debugging outside Streamlit
    councils = discover_new_councils()
    print(f"Discovered {len(councils)} councils")
    for c, u in councils[:10]:
        print(c, "->", u)
