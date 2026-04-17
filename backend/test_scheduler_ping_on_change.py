import os
import sys
import unittest


BACKEND_DIR = os.path.dirname(__file__)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from scheduler import StockCheckScheduler


def _product_with_stores(*stores):
    return {
        "stores": [dict(store) for store in stores],
    }


class SchedulerPingOnChangeTests(unittest.TestCase):
    def setUp(self):
        self.scheduler = StockCheckScheduler.__new__(StockCheckScheduler)
        self.scheduler.last_notified_products = {}
        self.scheduler.tracked_products = {}
        self.scheduler.max_notification_distance_miles = 25

    def test_products_info_changed_uses_previous_scan_snapshot(self):
        previous_products = {}
        current_products = {
            "walgreens:123": _product_with_stores(
                {"store_id": "1001", "inventory_count": 2, "distance": 4.0}
            )
        }

        self.assertTrue(
            self.scheduler._products_info_changed(
                current_products,
                previous_products=previous_products,
            )
        )

    def test_products_info_changed_returns_false_when_snapshot_matches(self):
        previous_products = {
            "walgreens:123": _product_with_stores(
                {"store_id": "1001", "inventory_count": 2, "distance": 4.0}
            )
        }
        current_products = {
            "walgreens:123": _product_with_stores(
                {"store_id": "1001", "inventory_count": 2, "distance": 4.0}
            )
        }

        self.assertFalse(
            self.scheduler._products_info_changed(
                current_products,
                previous_products=previous_products,
            )
        )

    def test_compute_product_changes_marks_new_up_and_down_vs_previous_scan(self):
        previous_products = {
            "walgreens:123": _product_with_stores(
                {"store_id": "1001", "inventory_count": 2, "distance": 4.0},
                {"store_id": "1002", "inventory_count": 5, "distance": 6.0},
                {"store_id": "1003", "inventory_count": 1, "distance": 7.0},
            )
        }
        current_products = {
            "walgreens:123": _product_with_stores(
                {"store_id": "1001", "inventory_count": 4, "distance": 4.0},
                {"store_id": "1002", "inventory_count": 3, "distance": 6.0},
                {"store_id": "1004", "inventory_count": 1, "distance": 5.0},
            )
        }

        changes = self.scheduler._compute_product_changes(
            current_products,
            previous_products=previous_products,
        )

        self.assertEqual(
            changes,
            {
                "walgreens:123": {
                    "1001": "up",
                    "1002": "down",
                    "1004": "new",
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
