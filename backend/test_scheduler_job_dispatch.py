import os
import sys
import threading
import unittest


BACKEND_DIR = os.path.dirname(__file__)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from scheduler import StockCheckScheduler


class SchedulerJobDispatchTests(unittest.TestCase):
    def test_run_scheduled_check_uses_background_launcher(self):
        scheduler = StockCheckScheduler.__new__(StockCheckScheduler)
        scheduler.user_id = 7

        calls = []

        def fake_start_check_thread(*, reason):
            calls.append(reason)
            return True

        scheduler._start_check_thread = fake_start_check_thread
        scheduler._run_scheduled_check()

        self.assertEqual(calls, ["scheduled"])

    def test_check_stock_tolerates_generic_cvs_failure(self):
        scheduler = StockCheckScheduler.__new__(StockCheckScheduler)
        scheduler.user_id = 7
        scheduler.state_lock = threading.RLock()
        scheduler.check_in_progress = False
        scheduler.active_check_thread = None
        scheduler.current_phase = "idle"
        scheduler.progress_message = "Idle"
        scheduler.current_store = None
        scheduler.current_product = None
        scheduler.stores_checked = 0
        scheduler.total_stores = 0
        scheduler.current_product_index = 0
        scheduler.total_products = 0
        scheduler.stores_with_stock_current = 0
        scheduler.progress_completed_units = 0.0
        scheduler.progress_total_units = 0.0
        scheduler.current_zipcode = "85001"
        scheduler.max_notification_distance_miles = 25
        scheduler.last_products_with_stock = {}
        scheduler.last_total_stores_checked = 0
        scheduler.last_notified_products = {}
        scheduler.tracked_products = {
            "cvs:444357": {
                "article_id": "444357",
                "retailer": "cvs",
                "name": "Poke Ball Tin",
                "image_url": "",
                "source_url": "https://www.cvs.com/example",
                "exclude_from_discord": False,
            }
        }
        scheduler.notifier = type(
            "NotifierStub",
            (),
            {
                "notify_stock_found": staticmethod(lambda *args, **kwargs: None),
                "notify_error": staticmethod(lambda *args, **kwargs: None),
            },
        )()
        scheduler.db = type(
            "DbStub",
            (),
            {
                "add_check_result": staticmethod(lambda *args, **kwargs: None),
                "update_user_settings": staticmethod(lambda *args, **kwargs: None),
                "update_product_image": staticmethod(lambda *args, **kwargs: None),
            },
        )()
        scheduler.walgreens_checker = type("WalgreensStub", (), {"custom_product_names": {}})()
        scheduler.fivebelow_checker = type("FiveBelowStub", (), {})()
        scheduler.ace_checker = type("AceStub", (), {})()
        scheduler.aldi_checker = type("AldiStub", (), {})()
        scheduler.cvs_checker = type(
            "CvsStub",
            (),
            {
                "check_product_availability": staticmethod(
                    lambda *args, **kwargs: (_ for _ in ()).throw(
                        ValueError("CVS Playwright node-script flow failed: net::ERR_TIMED_OUT")
                    )
                )
            },
        )()

        scheduler.refresh_from_db = lambda: None
        scheduler._reset_progress = lambda: None
        scheduler._set_progress = lambda **kwargs: None
        scheduler._product_specs = lambda: [
            {
                "article_id": "444357",
                "retailer": "cvs",
                "name": "Poke Ball Tin",
                "key": "cvs:444357",
                "image_url": "",
                "source_url": "https://www.cvs.com/example",
            }
        ]
        scheduler._extract_products_with_stock = lambda check_results, tracked_products: {}
        scheduler._prepare_products_for_discord = lambda products: {}
        scheduler._products_info_changed = lambda *args, **kwargs: False
        scheduler._compute_product_changes = lambda *args, **kwargs: {}

        scheduler._check_stock()

        self.assertFalse(scheduler.check_in_progress)

    def test_check_stock_tolerates_walgreens_401_product_failure(self):
        scheduler = StockCheckScheduler.__new__(StockCheckScheduler)
        scheduler.user_id = 7
        scheduler.state_lock = threading.RLock()
        scheduler.check_in_progress = False
        scheduler.active_check_thread = None
        scheduler.current_phase = "idle"
        scheduler.progress_message = "Idle"
        scheduler.current_store = None
        scheduler.current_product = None
        scheduler.stores_checked = 0
        scheduler.total_stores = 0
        scheduler.current_product_index = 0
        scheduler.total_products = 0
        scheduler.stores_with_stock_current = 0
        scheduler.progress_completed_units = 0.0
        scheduler.progress_total_units = 0.0
        scheduler.current_zipcode = "85001"
        scheduler.max_notification_distance_miles = 25
        scheduler.last_products_with_stock = {}
        scheduler.last_total_stores_checked = 0
        scheduler.last_notified_products = {}
        scheduler.tracked_products = {
            "walgreens:29700302": {
                "article_id": "29700302",
                "retailer": "walgreens",
                "name": "Pokemon Mini Tin",
                "image_url": "",
                "source_url": "https://www.walgreens.com/example",
                "exclude_from_discord": False,
            }
        }

        notifications = {"errors": 0}

        def record_error(*args, **kwargs):
            notifications["errors"] += 1

        scheduler.notifier = type(
            "NotifierStub",
            (),
            {
                "notify_stock_found": staticmethod(lambda *args, **kwargs: None),
                "notify_error": staticmethod(record_error),
            },
        )()

        saved_results = []
        scheduler.db = type(
            "DbStub",
            (),
            {
                "add_check_result": staticmethod(lambda *args, **kwargs: saved_results.append(args)),
                "update_user_settings": staticmethod(lambda *args, **kwargs: None),
                "update_product_image": staticmethod(lambda *args, **kwargs: None),
            },
        )()
        scheduler.walgreens_checker = type(
            "WalgreensStub",
            (),
            {
                "custom_product_names": {},
                "_fetch_stores_near_zip": staticmethod(
                    lambda *args, **kwargs: [{"storeNumber": "1", "name": "Store 1"}]
                ),
                "check_product_availability": staticmethod(
                    lambda *args, **kwargs: (_ for _ in ()).throw(
                        ValueError(
                            "401 Client Error: Unauthorized for url: https://www.walgreens.com/store/aldi/products/29700302"
                        )
                    )
                ),
            },
        )()
        scheduler.cvs_checker = type("CvsStub", (), {})()
        scheduler.fivebelow_checker = type("FiveBelowStub", (), {})()
        scheduler.ace_checker = type("AceStub", (), {})()
        scheduler.aldi_checker = type("AldiStub", (), {})()

        scheduler.refresh_from_db = lambda: None
        scheduler._reset_progress = lambda: None
        scheduler._set_progress = lambda **kwargs: [
            setattr(scheduler, key, value) for key, value in kwargs.items()
        ]
        scheduler._product_specs = lambda: [
            {
                "article_id": "29700302",
                "retailer": "walgreens",
                "name": "Pokemon Mini Tin",
                "key": "walgreens:29700302",
                "image_url": "",
                "source_url": "https://www.walgreens.com/example",
                "planogram": "29700302",
            }
        ]
        scheduler._extract_products_with_stock = lambda check_results, tracked_products: {}
        scheduler._prepare_products_for_discord = lambda products: {}
        scheduler._products_info_changed = lambda *args, **kwargs: False
        scheduler._compute_product_changes = lambda *args, **kwargs: {}

        scheduler._check_stock()

        self.assertFalse(scheduler.check_in_progress)
        self.assertEqual(notifications["errors"], 0)
        self.assertEqual(scheduler.current_phase, "complete")
        self.assertEqual(len(saved_results), 1)


if __name__ == "__main__":
    unittest.main()
