"""
Builds and caches a catalog of UK councils and their spending CSV URLs
by querying the public data.gov.uk CKAN API (free, no key/rate limit).

The catalog structure:
{
  "<council_name>": {
      "datasets": [
          {
              "dataset_title": "...",
              "publisher": "...",
              "resources": [
                  {"name": "...", "url": "...", "format": "CSV", "last_modified": "..."}
              ]
          },
          ...
      ],
      "csv_urls": ["https://...", "https://...", ...]  # flattened unique list
  },
  ...
}

We prioritize datasets likely to be spending transparency:
- query terms include: "payments to suppliers", "spend over 500", "expenditure over 500",
  "over £500", "transparency spending", etc.
- we only keep resources with format CSV.

Caching:
- JSON cache written to ./.cache/councils_catalog.json (safe for Git to commit).
- Use load_catalog() to read if present; build_catalog() to refresh from the API.

Free resources only.
"""
from __future__ import annotations
import os
import json
import time
from typing import Dict, Any, List

import requests

CKAN_BASE = "https://data.gov.uk/api/3/action/package_search"
CACHE_DIR = os.path.join(".", ".cache")
CACHE_PATH = os.path.join(CACHE_DIR, "councils_catalog.json")

# Broad query that captures common dataset titles used by councils
QUERY_TERMS = [
    '"payments to suppliers"',
    '"spend over 500"',
    '"spending over 500"',
    '"expenditure over 500"',
    '"transparency spending"',
    '"supplier payments"',
    '"payments over 250"',   # some district councils publish over £250
    '"payments over 500"',
]
Q = " OR ".join(QUERY_TERMS)

def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)

def _is_csv(res: Dict[str, Any]) -> bool:
    fmt = str(res.get("format", "")).strip().lower()
    return fmt == "csv"

def _publisher_name(pkg: Dict[str, Any]) -> str:
    org = pkg.get("organization") or {}
    name = org.get("title") or org.get("name") or ""
    return str(name).strip()

def _council_name(pkg: Dict[str, Any]) -> str:
    # Prefer publisher title (council) over dataset title for grouping
    pub = _publisher_name(pkg)
    if pub:
        return pub
    # fallback: take the start of dataset title (not ideal, but rare)
    title = str(pkg.get("title") or "").strip()
    return title.split("-")[0].strip() if title else "Unknown publisher"

def _unique(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _page_search(q: str, rows: int = 1000, start: int = 0, timeout: int = 20) -> Dict[str, Any]:
    params = {"q": q, "rows": rows, "start": start}
    r = requests.get(CKAN_BASE, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def build_catalog(max_pages: int = 10, rows_per_page: int = 1000, sleep_between: float = 0.3) -> Dict[str, Any]:
    """
    Query CKAN in pages and aggregate all CSV resources that look like council spending.
    """
    catalog: Dict[str, Any] = {}
    total = None
    start = 0

    for page in range(max_pages):
        data = _page_search(Q, rows=rows_per_page, start=start)
        result = data.get("result", {})
        if total is None:
            total = int(result.get("count", 0))
        packages = result.get("results", []) or []
        if not packages:
            break

        for pkg in packages:
            council = _council_name(pkg)
            dataset_title = str(pkg.get("title") or "").strip()
            publisher = _publisher_name(pkg)

            # Filter CSV resources only
            resources = []
            for res in pkg.get("resources", []) or []:
                if not _is_csv(res):
                    continue
                url = res.get("url") or res.get("download_url")
                if not url:
                    continue
                resources.append({
                    "name": res.get("name") or res.get("description") or "CSV",
                    "url": url,
                    "format": "CSV",
                    "last_modified": res.get("last_modified") or res.get("created") or "",
                })
            if not resources:
                continue

            entry = catalog.setdefault(council, {"datasets": [], "csv_urls": []})
            entry["datasets"].append({
                "dataset_title": dataset_title,
                "publisher": publisher,
                "resources": resources,
            })
            entry["csv_urls"].extend([r["url"] for r in resources])

        start += rows_per_page
        # finished all results?
        if start >= total:
            break
        time.sleep(sleep_between)

    # de-duplicate flattened URLs
    for c in catalog.values():
        c["csv_urls"] = _unique(c["csv_urls"])

    _ensure_cache_dir()
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
    return catalog

def load_catalog() -> Dict[str, Any]:
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

if __name__ == "__main__":
    print("Building council spending catalog from data.gov.uk …")
    catalog = build_catalog()
    print(f"Found {len(catalog)} councils with CSV resources. Cached at {CACHE_PATH}.")
