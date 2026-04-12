import sys
import os
from unittest.mock import MagicMock

# Create a mock for scrapling before importing any application code
sys.modules['scrapling'] = MagicMock()
sys.modules['scrapling.engines'] = MagicMock()
sys.modules['scrapling.engines._browsers'] = MagicMock()
sys.modules['scrapling.engines._browsers._stealth'] = MagicMock()

# Add backend to path
backend_path = os.path.abspath("backend")
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

from database import StockDatabase

def test_add_product():
    db = StockDatabase()
    try:
        # We don't need real data, just to see if it crashes on method calls
        success = db.add_tracked_product(
            user_id=1,
            article_id="VERIFY_FIX",
            retailer="ace",
            name="Verify Fix Product",
            planogram="VF123",
            source_url="https://www.acehardware.com/p/123"
        )
        if success:
            print("SUCCESS: add_tracked_product executed without AttributeError.")
        else:
            print("INFO: add_tracked_product returned False (possibly IntegrityError), but didn't crash.")
    except AttributeError as e:
        print(f"FAILURE: AttributeError still present: {e}")
    except Exception as e:
        print(f"INFO: Other error occurred (expected if DB not fully setup): {e}")

if __name__ == "__main__":
    test_add_product()
