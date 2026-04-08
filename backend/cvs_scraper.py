"""CVS stock checker using the retail inventory endpoint seen on PDP pages."""

from __future__ import annotations

import logging
import random
import re
import secrets
import time
from typing import Any, Dict, Iterable, List

import requests
try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - optional runtime dependency
    curl_requests = None

from config import TARGET_ZIP_CODE, USER_AGENTS

PROGRESS_UI_YIELD_SECONDS = 0.02

logger = logging.getLogger(__name__)
CURL_IMPERSONATION_TARGET = "chrome"


class CvsStockChecker:
    """Check CVS local inventory using the PDP inventory service."""

    INVENTORY_URLS = (
        "https://www.cvs.com/RETAGPV3/Inventory/V1/getStoreDetailsAndInventory",
        "https://www.cvs.com/REIAGPV3/Inventory/V1/getStoreDetailsAndInventory",
    )

    def __init__(self) -> None:
        self.progress_callback = None
        self.current_zip_code = TARGET_ZIP_CODE

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        if self.progress_callback:
            self.progress_callback(progress_info)

    @staticmethod
    def _new_session() -> requests.Session:
        if curl_requests is not None:
            return curl_requests.Session(impersonate=CURL_IMPERSONATION_TARGET)
        return requests.Session()

    @staticmethod
    def _request_headers(*, referer: str, api_key: str = "") -> Dict[str, str]:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://www.cvs.com",
            "Referer": referer,
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "X-Requested-With": "XMLHttpRequest",
            "devicetype": "desktop",
        }
        if api_key:
            headers["x-api-key"] = api_key
        return headers

    @staticmethod
    def _extract_api_key(html: str) -> str:
        patterns = (
            r'"x-api-key"\s*:\s*"([^"]+)"',
            r'"apiKey"\s*:\s*"([^"]+)"',
            r"['\"]x-api-key['\"]\s*,\s*['\"]([^'\"]+)['\"]",
        )
        for pattern in patterns:
            match = re.search(pattern, html or "", re.IGNORECASE)
            if match:
                return str(match.group(1) or "").strip()
        return ""

    def _bootstrap_session(self, session: requests.Session, product: Dict[str, Any]) -> tuple[str, str]:
        referer = str(product.get("source_url", "")).strip() or "https://www.cvs.com/"
        api_key = ""
        for url in ("https://www.cvs.com/", referer):
            try:
                response = session.get(
                    url,
                    headers=self._request_headers(referer=referer),
                    timeout=30,
                )
                if response.status_code >= 400:
                    logger.warning("CVS bootstrap blocked for %s with HTTP %s", url, response.status_code)
                    continue
                if not api_key and "html" in response.headers.get("content-type", "").lower():
                    api_key = self._extract_api_key(response.text)
                    if api_key:
                        logger.info("CVS bootstrap extracted API key from %s", url)
            except Exception as exc:
                logger.warning("CVS bootstrap request failed for %s: %s", url, exc)
                continue
        return referer, api_key

    @staticmethod
    def _request_body_header(api_key: str, device_token: str) -> Dict[str, Any]:
        return {
            "apiKey": api_key,
            "channelName": "WEB",
            "deviceToken": device_token,
            "deviceType": "DESKTOP",
            "responseFormat": "JSON",
            "securityType": "apiKey",
            "source": "CVS_WEB",
            "appName": "CVS_WEB",
            "lineOfBusiness": "RETAIL",
            "type": "rdp",
        }

    @classmethod
    def _payload_candidates(cls, product_id: str, zip_code: str, api_key: str) -> Iterable[Dict[str, Any]]:
        device_token = secrets.token_hex(8)
        request_header = cls._request_body_header(api_key, device_token) if api_key else {}

        if request_header:
            yield {
                "getStoreDetailsAndInventoryRequest": {
                    "header": request_header,
                    "productId": product_id,
                    "geolatitude": "",
                    "geolongitude": "",
                    "addressLine": zip_code,
                }
            }
            yield {
                "getStoreDetailsAndInventoryRequest": {
                    "header": request_header,
                    "productId": product_id,
                    "addressLine": zip_code,
                }
            }

        base_variants = [
            {"productId": product_id, "zipCode": zip_code},
            {"productId": product_id, "addressLine": zip_code},
            {"productId": product_id, "zip": zip_code},
            {"prodId": product_id, "zipCode": zip_code},
            {"itemId": product_id, "zipCode": zip_code},
            {"itemid": product_id, "zipCode": zip_code},
            {"productid": product_id, "zipCode": zip_code},
        ]
        for base in base_variants:
            yield dict(base)
            yield {**base, "qty": 1}
            yield {**base, "quantity": 1}
            yield {"request": dict(base)}

    @staticmethod
    def _looks_like_inventory_response(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        response = payload.get("response")
        if not isinstance(response, dict):
            return False
        locations = payload.get("atgResponse")
        if not isinstance(locations, list):
            locations = response.get("atgResponse")
        return isinstance(locations, list)

    def _fetch_inventory_payload(self, product: Dict[str, Any], zip_code: str) -> Dict[str, Any]:
        product_id = str(
            product.get("product_id")
            or product.get("article_id")
            or product.get("id")
            or ""
        ).strip()
        if not product_id:
            raise ValueError("CVS products require a numeric product id")

        session = self._new_session()
        referer, api_key = self._bootstrap_session(session, product)
        failures: List[str] = []

        try:
            for inventory_url in self.INVENTORY_URLS:
                for payload in self._payload_candidates(product_id, zip_code, api_key):
                    try:
                        response = session.post(
                            inventory_url,
                            json=payload,
                            headers=self._request_headers(referer=referer, api_key=api_key),
                            timeout=30,
                        )
                    except requests.RequestException as exc:
                        failures.append(f"{inventory_url} {type(exc).__name__}: {exc}")
                        logger.warning("CVS inventory request exception for %s: %s", inventory_url, exc)
                        continue

                    try:
                        data = response.json()
                    except ValueError:
                        snippet = response.text[:200].replace("\n", " ")
                        failures.append(f"{inventory_url} HTTP {response.status_code}: {snippet}")
                        logger.warning(
                            "CVS inventory non-JSON response from %s: HTTP %s %s",
                            inventory_url,
                            response.status_code,
                            snippet,
                        )
                        continue

                    if self._looks_like_inventory_response(data):
                        logger.info("CVS inventory response accepted from %s", inventory_url)
                        return data

                    header = ((data.get("response") or {}).get("header") or {}) if isinstance(data, dict) else {}
                    failures.append(
                        f"{inventory_url} HTTP {response.status_code}: "
                        f"{header.get('statusCode') or 'unknown'} {header.get('statusDesc') or 'unexpected payload'}"
                    )
                    logger.warning(
                        "CVS inventory unexpected payload from %s: HTTP %s statusCode=%s statusDesc=%s",
                        inventory_url,
                        response.status_code,
                        header.get("statusCode") or "unknown",
                        header.get("statusDesc") or "unexpected payload",
                    )
        finally:
            session.close()

        failure_detail = "; ".join(failures[:4]).strip()
        if failure_detail:
            raise ValueError(f"CVS inventory API did not accept the request shape: {failure_detail}")
        raise ValueError("CVS inventory API did not return store inventory")

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _candidate_quantities(cls, store: Dict[str, Any]) -> List[int]:
        quantities = [
            cls._safe_int(store.get("Qty")),
            cls._safe_int((store.get("bopis") or {}).get("Qty")),
            cls._safe_int((store.get("bopus") or {}).get("Qty")),
            cls._safe_int((store.get("ShopMyStore") or {}).get("Qty")),
            cls._safe_int((store.get("SDD") or {}).get("Qty")),
        ]
        return [qty for qty in quantities if qty > 0]

    @classmethod
    def _inventory_count(cls, store: Dict[str, Any]) -> int:
        positive = cls._candidate_quantities(store)
        if positive:
            return max(positive)
        return max(
            cls._safe_int(store.get("Qty")),
            cls._safe_int((store.get("bopis") or {}).get("Qty")),
            cls._safe_int((store.get("bopus") or {}).get("Qty")),
            cls._safe_int((store.get("ShopMyStore") or {}).get("Qty")),
            cls._safe_int((store.get("SDD") or {}).get("Qty")),
        )

    @classmethod
    def _inventory_in_stock(cls, store: Dict[str, Any]) -> bool:
        if cls._inventory_count(store) > 0:
            return True
        status_fields = (
            store.get("bopStatus"),
            store.get("bohStatus"),
            store.get("status"),
            (store.get("bopis") or {}).get("status"),
            (store.get("bopus") or {}).get("status"),
            (store.get("ShopMyStore") or {}).get("status"),
            (store.get("SDD") or {}).get("status"),
        )
        normalized_statuses = {
            str(value or "").strip().lower()
            for value in status_fields
            if str(value or "").strip()
        }
        return any("in stock" in status for status in normalized_statuses)

    @staticmethod
    def _normalize_address(store: Dict[str, Any]) -> str:
        street = str(store.get("storeAddress", "")).strip()
        city = str(store.get("City", "")).strip()
        state = str(store.get("State", "")).strip()
        zipcode = str(store.get("Zipcode", "")).strip()
        city_state_zip = " ".join(part for part in (state, zipcode) if part)
        location = ", ".join(part for part in (city, city_state_zip) if part)
        return ", ".join(part for part in (street, location) if part) or "Address unavailable"

    @staticmethod
    def _normalize_distance(value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _store_detail(self, store: Dict[str, Any]) -> Dict[str, Any]:
        store_id = str(
            store.get("storeId")
            or (store.get("bopis") or {}).get("storeId")
            or (store.get("bopus") or {}).get("storeId")
            or (store.get("ShopMyStore") or {}).get("storeId")
            or (store.get("SDD") or {}).get("storeId")
            or ""
        ).strip()
        return {
            "store_id": store_id,
            "name": f"CVS #{store_id}" if store_id else "CVS",
            "address": self._normalize_address(store),
            "distance": self._normalize_distance(store.get("dt")),
            "inventory_count": self._inventory_count(store),
            "latitude": None,
            "longitude": None,
        }

    def check_product_availability(
        self,
        product: Dict[str, Any],
        zip_code: str,
        product_index: int = 1,
        product_total: int = 1,
    ) -> Dict[str, Any]:
        product_name = str(product.get("name") or product.get("article_id") or "CVS product").strip()
        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)
        self.current_zip_code = zip_code

        self._emit_progress(
            {
                "phase": "fetching_inventory",
                "message": f"Requesting CVS local inventory for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "CVS inventory API",
                "stores_processed": 0,
                "total_stores": 0,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units),
                "total_units": float(total_units),
            }
        )

        payload = self._fetch_inventory_payload(product, zip_code)
        response = payload.get("response") or {}
        header = response.get("header") or {}
        if str(header.get("statusCode") or "").strip() not in {"", "0000"}:
            raise ValueError(
                f"CVS inventory API returned {header.get('statusCode')}: {header.get('statusDesc') or 'Unknown error'}"
            )

        locations = payload.get("atgResponse") or response.get("atgResponse") or []
        total_stores = len(locations)
        availability: Dict[str, bool] = {}
        store_details: Dict[str, Dict[str, Any]] = {}
        location_ids: List[str] = []

        for index, store in enumerate(locations, start=1):
            detail = self._store_detail(store)
            store_id = detail["store_id"]
            if not store_id:
                continue

            location_ids.append(store_id)
            in_stock = self._inventory_in_stock(store)
            availability[store_id] = in_stock
            if in_stock:
                store_details[store_id] = detail

            self._emit_progress(
                {
                    "phase": "processing_results",
                    "message": f"Reviewed {index} of {max(total_stores, 1)} CVS stores for {product_name}",
                    "product": product_name,
                    "product_index": product_index,
                    "product_total": product_total,
                    "store_id": store_id,
                    "store_name": detail["name"],
                    "stores_processed": index,
                    "total_stores": total_stores,
                    "stores_with_stock_current": len(store_details),
                    "completed_units": float(base_units + 1 + (index / max(total_stores, 1))),
                    "total_units": float(total_units),
                }
            )
            if self.progress_callback and total_stores > 1:
                time.sleep(PROGRESS_UI_YIELD_SECONDS)

        self._emit_progress(
            {
                "phase": "product_complete",
                "message": f"Finished {product_name}: {len(store_details)} CVS stores in stock",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Product scan complete",
                "stores_processed": total_stores,
                "total_stores": total_stores,
                "stores_with_stock_current": len(store_details),
                "completed_units": float(base_units + 2),
                "total_units": float(total_units),
            }
        )

        return {
            "availability": availability,
            "stores": store_details,
            "location_ids": location_ids,
        }
