import os
import sys
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


if __name__ == "__main__":
    unittest.main()
