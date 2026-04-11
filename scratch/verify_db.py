import sqlite3
import os

db_path = os.path.join("data", "watcher.sqlite3")

def verify():
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check tracked_products schema
        cursor.execute("PRAGMA table_info(tracked_products)")
        columns = [row["name"] for row in cursor.fetchall()]
        print(f"Columns in tracked_products: {columns}")
        
        if "sort_order" in columns:
            print("SUCCESS: 'sort_order' column exists in 'tracked_products'.")
        else:
            print("FAILURE: 'sort_order' column is MISSING in 'tracked_products'.")
            
        conn.close()
    except Exception as e:
        print(f"Error during verification: {e}")

if __name__ == "__main__":
    verify()
