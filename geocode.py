import time

try:
    from geopy.geocoders import Nominatim
    _GEOPY = True
except Exception:
    _GEOPY = False

_geolocator = Nominatim(user_agent="uk_public_spending_tracker") if _GEOPY else None
_cache = {}

def geocode_address(address: str):
    """
    Return (lat, lon) or (None, None). Uses free Nominatim with a small delay.
    Safe if geopy isn't installed.
    """
    if not address:
        return None, None
    if address in _cache:
        return _cache[address]
    if not _GEOPY or _geolocator is None:
        return None, None
    try:
        loc = _geolocator.geocode(address, timeout=8)
        time.sleep(1.0)  # be nice to the free service
        if loc:
            _cache[address] = (loc.latitude, loc.longitude)
            return _cache[address]
    except Exception:
        pass
    return None, None
