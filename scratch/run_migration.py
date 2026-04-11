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

# Now import the database
try:
    from database import StockDatabase
    print("Successfully imported StockDatabase (with mocks).")
    
    # Instantiate to trigger migrations
    db = StockDatabase()
    print("StockDatabase instantiated. Migrations should have run.")
    
    # Verify via the same logic as verify_db.py
    with db._connect() as conn:
        conn.row_factory = MagicMock if sys.modules.get('sqlite3') else None # wait no
        # just use standard pragma
        cursor = conn.execute("PRAGMA table_info(tracked_products)")
        columns = [row[1] for row in cursor.fetchall()] # pragma columns: id, name, type, ...
        print(f"Columns in tracked_products: {columns}")
        
        if "sort_order" in columns:
            print("SUCCESS: 'sort_order' column now exists.")
        else:
            print("FAILURE: 'sort_order' column still missing.")
            
except Exception as e:
    print(f"Error during migration trigger: {e}")
    import traceback
    traceback.print_exc()
