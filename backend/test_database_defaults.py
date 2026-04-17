import os
import sys
import tempfile
import unittest


BACKEND_DIR = os.path.dirname(__file__)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from database import StockDatabase


class DatabaseDefaultsTests(unittest.TestCase):
    def test_new_users_default_ping_on_change_only_to_true(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.sqlite3")
            db = StockDatabase(file_path=db_path)

            user = db.upsert_user_from_google(
                google_sub="sub-123",
                email="user@example.com",
                name="Example User",
            )

            settings = db.get_user_settings(int(user["id"]))
            self.assertTrue(settings["discord_ping_on_change_only"])


if __name__ == "__main__":
    unittest.main()
