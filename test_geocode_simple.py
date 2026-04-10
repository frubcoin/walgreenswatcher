"""Test geocoding directly without database."""
import requests

NOMINATIM_GEOCODE_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "WalgreensWatcher/1.0 (contact@example.com)"

# Test address
address = "6015 E. BROWN ROAD"
city = "MESA"
state = "AZ"
zipcode = "85205"

query_parts = [address, city, state, zipcode]
query = ", ".join(query_parts)

print(f"Query: {query}")
print(f"URL: {NOMINATIM_GEOCODE_URL}")

try:
    response = requests.get(
        NOMINATIM_GEOCODE_URL,
        params={
            "q": query,
            "format": "json",
            "limit": "1",
            "countrycodes": "us",
        },
        headers={
            "User-Agent": NOMINATIM_USER_AGENT,
            "Accept": "application/json",
        },
        timeout=10,
    )
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text[:500]}")
    
    if response.status_code == 200:
        data = response.json()
        if data and isinstance(data, list) and len(data) > 0:
            result = data[0]
            lat = float(result.get("lat"))
            lng = float(result.get("lon"))
            print(f"SUCCESS: lat={lat}, lng={lng}")
        else:
            print("No results found")
    else:
        print(f"Error: {response.status_code}")
except Exception as e:
    print(f"Exception: {type(e).__name__}: {e}")
