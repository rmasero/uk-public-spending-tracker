import os
import json
import time
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="uk_public_spending_tracker_free")
CACHE_PATH = os.path.join(os.path.dirname(__file__), ".geocode_cache.json")

def _load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_cache(cache):
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f)
    except Exception:
        pass

_cache = _load_cache()

def geocode_address(address, rate_limit_seconds: float = 1.0):
    if not address:
        return None, None
    addr = str(address).strip()
    if addr in _cache:
        lat, lon = _cache[addr]
        return lat, lon
    try:
        location = geolocator.geocode(addr, timeout=10)
        time.sleep(rate_limit_seconds)  # respect free tier
        if location:
            lat, lon = float(location.latitude), float(location.longitude)
            _cache[addr] = [lat, lon]
            _save_cache(_cache)
            return lat, lon
    except Exception:
        return None, None
    return None, None
