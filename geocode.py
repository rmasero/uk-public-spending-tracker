import time
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="uk_public_spending_tracker")
cache = {}

def geocode_address(address):
    if not address:
        return None, None
    if address in cache:
        return cache[address]
    try:
        location = geolocator.geocode(address)
        time.sleep(1)  # Rate limit
        if location:
            lat, lon = location.latitude, location.longitude
            cache[address] = (lat, lon)
            return lat, lon
    except:
        return None, None
    return None, None
