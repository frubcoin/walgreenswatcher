"""Test CVS geocoding directly."""
import sys
sys.path.insert(0, 'backend')

from cvs_scraper import CvsStockChecker
from database import StockDatabase

# Create checker with database
db = StockDatabase()
checker = CvsStockChecker(db=db)

# Test geocoding a known CVS store address
print("Testing geocoding...")
lat, lng = checker._geocode_address(
    "6015 E. BROWN ROAD",
    "MESA",
    "AZ",
    "85205",
    "4795"
)
print(f"Result: lat={lat}, lng={lng}")

if lat is None or lng is None:
    print("FAILED: Geocoding returned None")
else:
    print(f"SUCCESS: Coordinates are {lat}, {lng}")
    # Verify it's in the database
    cached = db.get_cvs_store_location("4795")
    print(f"Cached in DB: {cached}")
