import os
import sys
import unittest


BACKEND_DIR = os.path.dirname(__file__)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app import app


class CsrfRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.origin = "http://localhost:5000"

    def test_csrf_failure_returns_fresh_token_that_reaches_route_logic_on_retry(self):
        session_response = self.client.get(
            "/api/admin/session",
            headers={"Origin": self.origin},
        )
        self.assertEqual(session_response.status_code, 200)
        self.assertTrue(session_response.get_json().get("csrf_token"))

        failed_response = self.client.post(
            "/api/admin/login",
            json={"password": "example"},
            headers={
                "Origin": self.origin,
                "X-CSRF-Token": "stale-token",
            },
        )
        self.assertEqual(failed_response.status_code, 403)
        failed_payload = failed_response.get_json() or {}
        self.assertEqual(failed_payload.get("error"), "CSRF validation failed")
        self.assertTrue(failed_payload.get("csrf_token"))

        retried_response = self.client.post(
            "/api/admin/login",
            json={"password": "example"},
            headers={
                "Origin": self.origin,
                "X-CSRF-Token": failed_payload["csrf_token"],
            },
        )
        retried_payload = retried_response.get_json() or {}
        self.assertNotEqual(retried_response.status_code, 403)
        self.assertNotEqual(retried_payload.get("error"), "CSRF validation failed")


if __name__ == "__main__":
    unittest.main()
