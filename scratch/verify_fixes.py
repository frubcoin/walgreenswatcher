import sys
import os
import sqlite3
import json
from datetime import datetime

# Add backend to path
backend_path = os.path.abspath("backend")
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

from database import StockDatabase
from ace import AceBrowserClient
from ace_product_resolver import AceProductResolver

def test_database_schema():
    print("--- Testing Database Schema ---")
    db = StockDatabase()
    with db._connect() as conn:
        # Check if sort_order exists
        columns = [row["name"] for row in conn.execute("PRAGMA table_info(tracked_products)").fetchall()]
        print(f"Columns in tracked_products: {columns}")
        if "sort_order" in columns:
            print("SUCCESS: sort_order column found.")
        else:
            print("FAILURE: sort_order column MISSING.")
        
        # Test inserting a product with sort_order
        try:
            db.add_tracked_product(
                user_id=1,
                article_id="TEST_ART",
                retailer="ace",
                name="Test Product",
                planogram="TEST_PLANO",
                image_url="http://example.com/img.png",
                source_url="https://www.acehardware.com/p/12345",
                product_id="12345"
            )
            print("SUCCESS: add_tracked_product worked.")
        except Exception as e:
            print(f"FAILURE: add_tracked_product failed: {e}")

def test_ace_resolver():
    print("\n--- Testing Ace Instant Resolver ---")
    test_url = "https://www.acehardware.com/departments/tools/power-tool-accessories/drill-bits/2361137"
    try:
        # We might not have internet or proxies might fail in this environment, 
        # but let's see if the logic at least executes or if we get a real response.
        metadata = AceProductResolver.resolve_product_link(test_url)
        print(f"Resolved Metadata: {json.dumps(metadata, indent=2)}")
        if metadata.get("name") and metadata["name"] != "Ace Hardware Product":
            print("SUCCESS: Ace resolver returned a real name.")
        else:
            print("WARNING: Ace resolver returned fallback name (possibly network/API issue in this env).")
    except Exception as e:
        print(f"FAILURE: Ace resolver failed: {e}")

if __name__ == "__main__":
    test_database_schema()
    test_ace_resolver()
