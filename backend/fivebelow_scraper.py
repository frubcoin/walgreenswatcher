"""Five Below stock checker using public store search and inventory APIs."""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Dict, List, Optional

import requests

from config import STORE_LOCATOR_CACHE_TTL_SECONDS, TARGET_ZIP_CODE, USER_AGENTS

PROGRESS_UI_YIELD_SECONDS = 0.02
FIVEBELOW_LOCATOR_API_URL = "https://prod-cdn.us.yextapis.com/v2/accounts/me/search/vertical/query"
FIVEBELOW_INVENTORY_URL = "https://www.fivebelow.com/frontastic/action/inventory/getInventoryBySkus"
FIVEBELOW_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
FIVEBELOW_LOCATOR_API_KEY = "ea3012153391597145ac258fb4e06aef"
FIVEBELOW_LOCATOR_EXPERIENCE_KEY = "locator-1"
FIVEBELOW_LOCATOR_VERSION = "20220511"
FIVEBELOW_LOCATOR_VERTICAL_KEY = "locations"
FIVEBELOW_STORE_LIMIT = 50

logger = logging.getLogger(__name__)
_shared_zip_geocode_cache: Dict[str, Dict[str, Any]] = {}
_shared_zip_geocode_cache_lock = threading.RLock()
_shared_store_locator_cache: Dict[str, Dict[str, Any]] = {}
_shared_store_locator_cache_lock = threading.RLock()


class FiveBelowStockChecker:
    """Check Five Below local pickup inventory using store and SKU APIs."""

    def __init__(self) -> None:
        self.progress_callback = None
        self.current_zip_code = TARGET_ZIP_CODE

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        if self.progress_callback:
            self.progress_callback(progress_info)

    @staticmethod
    def _cache_expiry_time() -> float:
        return time.monotonic() + float(STORE_LOCATOR_CACHE_TTL_SECONDS)

    @staticmethod
    def _normalize_distance_miles(distance_meters: Any) -> float | None:
        try:
            return round(float(distance_meters) / 1609.344, 2) if distance_meters is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _format_address(address: Dict[str, Any]) -> str:
        if not isinstance(address, dict):
            return "Address unavailable"

        street = ", ".join(
            part
            for part in (
                str(address.get("line1", "")).strip(),
                str(address.get("line2", "")).strip(),
            )
            if part
        )
        city_state_zip = " ".join(
            part
            for part in (
                str(address.get("city", "")).strip(),
                str(address.get("region", "")).strip(),
                str(address.get("postalCode", "")).strip(),
            )
            if part
        )
        return ", ".join(part for part in (street, city_state_zip) if part) or "Address unavailable"

    @staticmethod
    def _geocode_headers() -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "User-Agent": "retail-stock-watcher/1.0 (zip geocoding for Five Below store lookup)",
        }

    @staticmethod
    def _request_headers(*, accept: str, referer: str = "https://www.fivebelow.com/") -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": accept,
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": referer,
            "Origin": "https://www.fivebelow.com",
        }

    def _get_cached_geocode_entry(self, zip_code: str) -> Optional[Dict[str, Any]]:
        if STORE_LOCATOR_CACHE_TTL_SECONDS <= 0:
            return None

        with _shared_zip_geocode_cache_lock:
            entry = _shared_zip_geocode_cache.get(zip_code)
            if not entry:
                return None
            if float(entry.get("expires_at", 0.0)) <= time.monotonic():
                _shared_zip_geocode_cache.pop(zip_code, None)
                return None
            return dict(entry)

    def _store_cached_geocode_entry(self, zip_code: str, location: Dict[str, Any]) -> None:
        if STORE_LOCATOR_CACHE_TTL_SECONDS <= 0:
            return

        with _shared_zip_geocode_cache_lock:
            _shared_zip_geocode_cache[zip_code] = {
                "expires_at": self._cache_expiry_time(),
                "location": dict(location),
            }

    def _get_cached_store_locator_entry(self, zip_code: str) -> Optional[Dict[str, Any]]:
        if STORE_LOCATOR_CACHE_TTL_SECONDS <= 0:
            return None

        with _shared_store_locator_cache_lock:
            entry = _shared_store_locator_cache.get(zip_code)
            if not entry:
                return None
            if float(entry.get("expires_at", 0.0)) <= time.monotonic():
                _shared_store_locator_cache.pop(zip_code, None)
                return None
            return dict(entry)

    def _store_cached_store_locator_entry(
        self,
        zip_code: str,
        *,
        location: Dict[str, Any],
        stores: List[Dict[str, Any]],
    ) -> None:
        if STORE_LOCATOR_CACHE_TTL_SECONDS <= 0:
            return

        with _shared_store_locator_cache_lock:
            _shared_store_locator_cache[zip_code] = {
                "expires_at": self._cache_expiry_time(),
                "location": dict(location),
                "stores": list(stores),
            }

    def _geocode_zip_code(self, zip_code: str) -> Dict[str, Any]:
        cached = self._get_cached_geocode_entry(zip_code)
        if cached and cached.get("location"):
            return dict(cached["location"])

        response = requests.get(
            FIVEBELOW_NOMINATIM_URL,
            params={
                "format": "jsonv2",
                "countrycodes": "us",
                "limit": "1",
                "postalcode": str(zip_code or "").strip(),
            },
            headers=self._geocode_headers(),
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            raise ValueError(f"Could not geocode ZIP code {zip_code} for Five Below store lookup")

        item = payload[0] or {}
        location = {
            "lat": self._safe_float(item.get("lat")),
            "lng": self._safe_float(item.get("lon")),
        }
        if location["lat"] is None or location["lng"] is None:
            raise ValueError(f"Could not geocode ZIP code {zip_code} for Five Below store lookup")

        self._store_cached_geocode_entry(zip_code, location)
        return location

    def _fetch_stores_near_zip_remote(self, zip_code: str) -> List[Dict[str, Any]]:
        location = self._geocode_zip_code(zip_code)
        response = requests.get(
            FIVEBELOW_LOCATOR_API_URL,
            params={
                "api_key": FIVEBELOW_LOCATOR_API_KEY,
                "experienceKey": FIVEBELOW_LOCATOR_EXPERIENCE_KEY,
                "v": FIVEBELOW_LOCATOR_VERSION,
                "locale": "en",
                "verticalKey": FIVEBELOW_LOCATOR_VERTICAL_KEY,
                "limit": str(FIVEBELOW_STORE_LIMIT),
                "location": f"{location['lat']},{location['lng']}",
                "input": "",
            },
            headers=self._request_headers(accept="application/json"),
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        errors = ((payload.get("meta") or {}).get("errors") or [])
        if errors:
            first_error = errors[0] or {}
            raise ValueError(
                f"Five Below store search failed: {first_error.get('message') or 'Unknown error'}"
            )

        api_response = payload.get("response") or {}
        results = (
            api_response.get("results")
            or (api_response.get("allResultsForVertical") or {}).get("results")
            or []
        )
        stores: List[Dict[str, Any]] = []
        for item in results:
            store_data = item.get("data") or {}
            store_id = str(store_data.get("id") or "").strip()
            if not store_id:
                continue

            coordinates = store_data.get("yextDisplayCoordinate") or {}
            stores.append(
                {
                    "store_id": store_id,
                    "name": f"Five Below #{store_id}",
                    "address": self._format_address(store_data.get("address") or {}),
                    "distance": self._normalize_distance_miles(item.get("distance")),
                    "latitude": self._safe_float(coordinates.get("latitude")),
                    "longitude": self._safe_float(coordinates.get("longitude")),
                }
            )

        if not stores:
            raise ValueError(f"No Five Below stores found near ZIP {zip_code}")

        self._store_cached_store_locator_entry(zip_code, location=location, stores=stores)
        logger.info("Found %s Five Below stores near %s", len(stores), zip_code)
        return stores

    def _fetch_stores_near_zip(self, zip_code: str) -> List[Dict[str, Any]]:
        cached = self._get_cached_store_locator_entry(zip_code)
        if cached and cached.get("stores"):
            logger.info(
                "Using cached Five Below store list for ZIP %s (%s stores)",
                zip_code,
                len(cached["stores"]),
            )
            return list(cached["stores"])
        return self._fetch_stores_near_zip_remote(zip_code)

    def _fetch_inventory_payload(self, product: Dict[str, Any], store_ids: List[str]) -> Dict[str, Any]:
        sku = str(product.get("article_id") or product.get("product_id") or "").strip()
        if not sku:
            raise ValueError("Five Below products require a SKU")
        if not store_ids:
            raise ValueError("No Five Below store keys were available for the request")

        referer = str(product.get("source_url", "")).strip() or "https://www.fivebelow.com/"
        response = requests.post(
            FIVEBELOW_INVENTORY_URL,
            json={"skus": [sku], "storeKeys": store_ids},
            headers={
                **self._request_headers(accept="application/json", referer=referer),
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            snippet = response.text[:300].replace("\n", " ")
            raise ValueError(f"Five Below returned non-JSON inventory content: {snippet}") from exc

        errors = payload.get("errors") or []
        if errors:
            raise ValueError(f"Five Below inventory API returned errors: {errors}")
        return payload

    def check_product_availability(
        self,
        product: Dict[str, Any],
        stores: List[Dict[str, Any]],
        product_index: int = 1,
        product_total: int = 1,
    ) -> Dict[str, Any]:
        product_name = str(product.get("name") or product.get("article_id") or "Five Below product").strip()
        total_stores = len(stores)
        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)
        self.current_zip_code = str(self.current_zip_code or TARGET_ZIP_CODE)

        self._emit_progress(
            {
                "phase": "fetching_inventory",
                "message": f"Requesting Five Below pickup inventory for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Five Below inventory API",
                "stores_processed": 0,
                "total_stores": total_stores,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units),
                "total_units": float(total_units),
            }
        )

        store_lookup = {
            str(store.get("store_id") or "").strip(): dict(store)
            for store in stores
            if str(store.get("store_id") or "").strip()
        }
        store_ids = list(store_lookup.keys())
        availability = {store_id: False for store_id in store_ids}
        store_details: Dict[str, Dict[str, Any]] = {}
        payload = self._fetch_inventory_payload(product, store_ids)

        sku = str(product.get("article_id") or product.get("product_id") or "").strip()
        for item in (((payload.get("data") or {}).get(sku)) or []):
            store_id = str(item.get("channelKey") or "").strip()
            if not store_id:
                continue
            quantity = self._safe_int(item.get("availableQuantity"))
            in_stock = bool(item.get("isOnStock")) or quantity > 0
            availability[store_id] = in_stock
            if in_stock and store_id in store_lookup:
                store_details[store_id] = {
                    **store_lookup[store_id],
                    "inventory_count": quantity,
                    "pickup_available": True,
                }

        for index, store in enumerate(stores, start=1):
            store_id = str(store.get("store_id") or "").strip()
            if not store_id:
                continue
            self._emit_progress(
                {
                    "phase": "processing_results",
                    "message": f"Reviewed {index} of {max(total_stores, 1)} Five Below stores for {product_name}",
                    "product": product_name,
                    "product_index": product_index,
                    "product_total": product_total,
                    "store_id": store_id,
                    "store_name": store.get("name") or f"Five Below #{store_id}",
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
                "message": f"Finished {product_name}: {len(store_details)} Five Below stores in stock",
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
            "location_ids": store_ids,
        }
