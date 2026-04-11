import os
import sys

# Ensure backend modules can be imported
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/backend'))

from ace_product_resolver import AceProductResolver
from ace_scraper import AceStockChecker

PRODUCT_URL = "https://www.acehardware.com/departments/home-and-decor/novelty-items/toys-and-games/9125200"
EXPECTED_NAME = "Pokemon Perfect Order Sleeved Booster Trading Cards"

import logging

# Configure logging to see the debug output from the checker
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # --- Step 1: Verify the resolver fetches the correct product name ---
    print("=== Testing AceProductResolver ===")
    resolved = AceProductResolver.resolve_product_link(PRODUCT_URL)
    print(f"  product_id  : {resolved['product_id']}")
    print(f"  name        : {resolved['name']}")
    print(f"  image_url   : {resolved['image_url']}")
    print(f"  canonical   : {resolved['canonical_url']}")

    name_ok = EXPECTED_NAME.lower() in resolved["name"].lower()
    print(f"  name check  : {'PASS' if name_ok else 'FAIL'} (expected: {EXPECTED_NAME!r})")

    # --- Step 2: Check inventory availability ---
    print("\n=== Testing AceStockChecker ===")
    checker = AceStockChecker()
    product = {
        "article_id": resolved["product_id"],
        "name": resolved["name"],
        "source_url": resolved["canonical_url"],
    }

    # 85282 is near the user's test locations in Tempe
    result = checker.check_product_availability(product, zip_code="85282")

    print("FINISHED CHECK!")
    print(f"Products tracked: {result}")
