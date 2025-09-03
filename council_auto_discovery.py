import requests
import pandas as pd
from io import BytesIO
import sys
import os
from requests.exceptions import RequestException, Timeout, ConnectTimeout
import time

# Add the current directory to Python path to find council_fetchers
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from council_fetchers import FETCHERS
except ImportError:
    # Fallback if council_fetchers can't be imported
    FETCHERS = {}

def fetch_new_council_csv(url, council_name, timeout=10):
    """
    Fetch council CSV data with timeout and error handling
    """
    if council_name in FETCHERS:
        try:
            return FETCHERS[council_name]()
        except Exception as e:
            print(f"Error fetching {council_name} with custom fetcher: {e}")
            return []
    
    try:
        print(f"Fetching data for {council_name} from {url}")
        
        # Add timeout and retry logic
        session = requests.Session()
        session.mount('http://', requests.adapters.HTTPAdapter(max_retries=2))
        session.mount('https://', requests.adapters.HTTPAdapter(max_retries=2))
        
        r = session.get(url, timeout=timeout)
        r.raise_for_status()  # Raise an exception for bad status codes
        
        df = pd.read_csv(BytesIO(r.content))
        
        payments = []
        for _, row in df.iterrows():
            payments.append({
                "council": council_name,
                "payment_date": row.get("PaymentDate") or row.get("Date"),
                "supplier": row.get("Supplier") or row.get("Supplier Name"),
                "description": row.get("Description") or row.get("Purpose"),
                "category": row.get("Department") or row.get("ServiceArea",""),
                "amount_gbp": row.get("Amount") or row.get("AmountPaid"),
                "invoice_ref": row.get("InvoiceRef") or row.get("InvoiceNumber","")
            })
        
        print(f"Successfully fetched {len(payments)} records for {council_name}")
        return payments
        
    except (ConnectTimeout, Timeout) as e:
        print(f"Timeout error fetching {council_name}: {e}")
        return []
    except RequestException as e:
        print(f"Request error fetching {council_name}: {e}")
        return []
    except pd.errors.EmptyDataError:
        print(f"Empty or invalid CSV data for {council_name}")
        return []
    except Exception as e:
        print(f"Unexpected error fetching {council_name}: {e}")
        return []

def discover_new_councils(data_gov_api_url="https://data.gov.uk/api/3/action/package_search?q=spending+csv", timeout=15):
    """
    Discover new councils with timeout handling
    """
    try:
        print("Discovering new councils...")
        r = requests.get(data_gov_api_url, timeout=timeout)
        r.raise_for_status()
        
        results = r.json().get("result", {}).get("results", [])
        discovered = []
        
        for res in results:
            name = res.get("title")
            if not name:
                continue
                
            resources = res.get("resources", [])
            csv_url = None
            
            for resrc in resources:
                if resrc.get("format","").lower() == "csv":
                    csv_url = resrc.get("url")
                    break
                    
            if csv_url:
                # Basic URL validation
                if csv_url.startswith(('http://', 'https://')):
                    discovered.append((name, csv_url))
                    
        print(f"Discovered {len(discovered)} potential councils")
        return discovered
        
    except (ConnectTimeout, Timeout) as e:
        print(f"Timeout error discovering councils: {e}")
        return []
    except RequestException as e:
        print(f"Request error discovering councils: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error discovering councils: {e}")
        return []
