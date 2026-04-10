import requests
from urllib.parse import quote

LOCATIONS_BASE = "https://www.acehardware.com/api/commerce/storefront/locationUsageTypes/SP/locations"
SKU = "9125200"
RADIUS_METERS = 48280.3  # 30 miles

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.acehardware.com/departments/home-and-decor/novelty-items/toys-and-games/9125200",
    "Origin": "https://www.acehardware.com",
    "X-Vol-Catalog": "1",
    "X-Vol-Currency": "USD",
    "X-Vol-Locale": "en-US",
    "X-Vol-Master-Catalog": "1",
    "X-Vol-Site": "37138",
    "X-Vol-Tenant": "24645",
}

def zip_to_lat_lng(zip_code: str):
    url = f"https://api.zippopotam.us/us/{zip_code.strip()}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    place = data["places"][0]
    return float(place["latitude"]), float(place["longitude"])

def get_nearby_stores(lat: float, lng: float, cookie: str = ""):
    filter_value = f"geo near({lat},{lng},{RADIUS_METERS})"
    url = f"{LOCATIONS_BASE}?filter={quote(filter_value)}"

    headers = HEADERS.copy()
    if cookie:
        headers["Cookie"] = cookie.replace("\n", "").strip()

    print(f"→ Hitting Ace API: {url}")
    r = requests.get(url, headers=headers, timeout=20)

    print(f"→ HTTP {r.status_code}")
    print(f"→ Content-Type: {r.headers.get('content-type', 'unknown')}")
    print(f"→ Content-Encoding: {r.headers.get('content-encoding', 'none')}")

    preview = r.text[:300].replace("\n", " ")
    print(f"→ Response preview: {preview}")

    r.raise_for_status()
    return r.json()

def check_stock_placeholder(store_code: str, sku: str = SKU):
    print(f"   [TODO] Stock check for {sku} at store {store_code}")
    return {"status": "placeholder", "inStock": "unknown", "quantity": "N/A"}

if __name__ == "__main__":
    zip_code = input("Enter zip code (e.g. 85208): ").strip() or "85208"
    cookie_value = input("Paste Cookie header if needed, or press Enter to skip: ").strip()

    try:
        lat, lng = zip_to_lat_lng(zip_code)
        print(f"✅ Zip {zip_code} → {lat:.7f}, {lng:.7f}")

        data = get_nearby_stores(lat, lng, cookie_value)
        stores = data.get("items", [])

        print(f"\n✅ Found {len(stores)} nearby Ace stores (showing first 10):\n")

        for i, store in enumerate(stores[:10], 1):
            code = store.get("code", "N/A")
            name = store.get("name", "N/A")
            addr = store.get("address", {})
            addr_str = (
                f"{addr.get('address1', '')} "
                f"{addr.get('cityOrTown', '')} "
                f"{addr.get('stateOrProvince', '')} "
                f"{addr.get('postalOrZipCode', '')}"
            ).strip()
            phone = store.get("phone", "N/A")

            print(f"{i:2d}. 📍 {code} | {name}")
            print(f"     {addr_str}")
            print(f"     📞 {phone}")

            stock = check_stock_placeholder(code)
            print(f"     Stock for {SKU}: {stock}\n")

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "unknown"
        reason = e.response.reason if e.response is not None else ""
        print(f"❌ HTTP Error: {status} {reason}")
    except Exception as e:
        print(f"❌ Error: {e}")