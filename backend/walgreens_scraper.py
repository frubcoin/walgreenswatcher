"""Walgreens stock checker using pickup inventory endpoints."""

from __future__ import annotations

import logging
import random
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import (
    DEFAULT_TRACKED_PRODUCTS,
    SEARCH_RADIUS_MILES,
    STORE_LOCATOR_CACHE_TTL_SECONDS,
    TARGET_ZIP_CODE,
    USER_AGENTS,
    WALGREENS_PRODUCT_NAMES,
)
from rate_limiter import rate_limited

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROGRESS_UI_YIELD_SECONDS = 0.02
_shared_store_locator_cache: Dict[str, Dict[str, Any]] = {}
_shared_store_locator_cache_lock = threading.RLock()


class WalgreensStockChecker:
    """Check Walgreens pickup stock availability using live inventory endpoints."""

    def __init__(self) -> None:
        self.progress_callback = None
        self.custom_product_names: Dict[str, str] = {}
        self.current_zip_code = TARGET_ZIP_CODE
        self.location_cache: Dict[str, Dict[str, str]] = {}

    @staticmethod
    def _cache_expiry_time() -> float:
        return time.monotonic() + float(STORE_LOCATOR_CACHE_TTL_SECONDS)

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

    def _remember_location_context(self, zip_code: str, filter_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        lat = filter_data.get("lat")
        lng = filter_data.get("lng")
        if not lat or not lng:
            return None

        context = {"lat": str(lat), "lng": str(lng)}
        self.location_cache[zip_code] = context
        return context

    def _store_cached_store_locator_entry(
        self,
        zip_code: str,
        *,
        location: Optional[Dict[str, str]] = None,
        stores: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if STORE_LOCATOR_CACHE_TTL_SECONDS <= 0:
            return

        with _shared_store_locator_cache_lock:
            existing = _shared_store_locator_cache.get(zip_code, {})
            entry = {
                "expires_at": self._cache_expiry_time(),
                "location": location or existing.get("location"),
                "stores": stores if stores is not None else existing.get("stores"),
            }
            _shared_store_locator_cache[zip_code] = entry

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        """Send a progress update to the scheduler/UI when available."""
        if self.progress_callback:
            self.progress_callback(progress_info)

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send a JSON POST with lightweight headers that Walgreens accepts."""
        response = requests.post(
            url,
            json=payload,
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
            },
            timeout=20,
        )
        response.raise_for_status()
        try:
            return response.json()
        except ValueError as exc:
            snippet = response.text[:300].replace("\n", " ")
            raise ValueError(f"Walgreens returned non-JSON content: {snippet}") from exc

    @staticmethod
    def _format_store(store: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a Walgreens store object into the app's store format."""
        store_info = store.get("store", {})
        return {
            "storeNumber": str(store.get("storeNumber") or store_info.get("storeNumber") or ""),
            "name": store_info.get("name", f'Store {store.get("storeNumber", "Unknown")}'),
            "address": store_info.get("address", {}),
            "phone": store_info.get("phone", {}),
            "latitude": store.get("latitude"),
            "longitude": store.get("longitude"),
            "store": store_info,
            "distance": store.get("distance"),
        }

    @staticmethod
    def _inventory_count(store: Dict[str, Any], article_id: str) -> int:
        """Return the reported pickup inventory count for an article."""
        for item in store.get("inventory", []):
            if str(item.get("articleId")) != str(article_id):
                continue
            try:
                return int(item.get("inventoryCount", 0) or 0)
            except (TypeError, ValueError):
                return 0
        return 0

    @staticmethod
    def _inventory_in_stock(store: Dict[str, Any], article_id: str) -> bool:
        """Return True when the inventory block reports positive pickup stock."""
        for item in store.get("inventory", []):
            if str(item.get("articleId")) != str(article_id):
                continue
            if item.get("status") != "In Stock":
                continue
            return WalgreensStockChecker._inventory_count(store, article_id) > 0
        return False

    @staticmethod
    def _format_address(address: Dict[str, Any]) -> str:
        """Format a Walgreens address object for notifications and UI."""
        if not isinstance(address, dict):
            return "Address unavailable"

        parts = [
            str(address.get("street", "")).strip(),
            str(address.get("city", "")).strip(),
            str(address.get("state", "")).strip(),
            str(address.get("zip", "")).strip(),
        ]
        if not parts[0]:
            return "Address unavailable"

        city_state_zip = " ".join(part for part in parts[2:] if part)
        location = ", ".join(part for part in [parts[1], city_state_zip] if part)
        return ", ".join(part for part in [parts[0], location] if part)

    def _store_stock_detail(self, store: Dict[str, Any], article_id: str) -> Dict[str, Any]:
        """Build a normalized store detail object for stock reporting."""
        store_info = store.get("store", {})
        address = store_info.get("address", {})
        store_id = str(store.get("storeNumber") or store_info.get("storeNumber") or "")
        distance = store.get("distance")
        latitude = store.get("latitude")
        longitude = store.get("longitude")

        try:
            latitude = float(latitude) if latitude is not None else None
        except (TypeError, ValueError):
            latitude = None

        try:
            longitude = float(longitude) if longitude is not None else None
        except (TypeError, ValueError):
            longitude = None

        return {
            "store_id": store_id,
            "name": store_info.get("name", f"Store {store_id}"),
            "address": self._format_address(address),
            "distance": float(distance) if distance is not None else None,
            "inventory_count": self._inventory_count(store, article_id),
            "latitude": latitude,
            "longitude": longitude,
        }

    def _product_name(self, product: Dict[str, Any]) -> str:
        article_id = str(product["article_id"])
        return (
            product.get("name")
            or self.custom_product_names.get(article_id)
            or WALGREENS_PRODUCT_NAMES.get(article_id, article_id)
        )

    def _fetch_location_context(self, zip_code: str) -> Dict[str, str]:
        """Fetch Walgreens' preferred lat/lng for a ZIP code."""
        if zip_code in self.location_cache:
            return self.location_cache[zip_code]

        cached_entry = self._get_cached_store_locator_entry(zip_code)
        if cached_entry and cached_entry.get("location"):
            context = {
                "lat": str(cached_entry["location"]["lat"]),
                "lng": str(cached_entry["location"]["lng"]),
            }
            self.location_cache[zip_code] = context
            logger.info("Using cached Walgreens location context for ZIP %s", zip_code)
            return context

        url = "https://www.walgreens.com/locator/v1/stores/search"
        payload = {
            "r": str(SEARCH_RADIUS_MILES),
            "requestType": "locator",
            "s": "1",
            "p": "1",
            "zip": zip_code,
            "inStockOnly": "false",
            "articles": [],
        }

        data = self._post_json(url, payload)
        filter_data = data.get("filter", {})
        context = self._remember_location_context(zip_code, filter_data)
        if not context:
            raise ValueError(f"Walgreens did not return lat/lng for ZIP {zip_code}")

        self._store_cached_store_locator_entry(zip_code, location=context)
        return context

    @rate_limited
    def _fetch_stores_near_zip_remote(self, zip_code: str) -> List[Dict[str, Any]]:
        """Fetch nearby stores for a ZIP code from Walgreens."""
        logger.info("Fetching stores near ZIP %s...", zip_code)

        url = "https://www.walgreens.com/locator/v1/stores/search"
        payload = {
            "r": str(SEARCH_RADIUS_MILES),
            "requestType": "locator",
            "s": "50",
            "p": "1",
            "zip": zip_code,
            "inStockOnly": "false",
            "articles": [],
        }

        data = self._post_json(url, payload)
        stores = data.get("results", [])
        context = self._remember_location_context(zip_code, data.get("filter", {}))

        if not stores:
            logger.warning("No stores found near ZIP %s", zip_code)
            return []

        formatted_stores = [self._format_store(store) for store in stores]
        self._store_cached_store_locator_entry(
            zip_code,
            location=context,
            stores=formatted_stores,
        )
        logger.info("Found %s stores near %s", len(formatted_stores), zip_code)
        return formatted_stores

    def _fetch_stores_near_zip(self, zip_code: str) -> List[Dict[str, Any]]:
        """Fetch nearby stores for a ZIP code, reusing a short shared cache when possible."""
        cached_entry = self._get_cached_store_locator_entry(zip_code)
        cached_stores = cached_entry.get("stores") if cached_entry else None
        if cached_stores:
            cached_location = cached_entry.get("location") or {}
            if cached_location.get("lat") and cached_location.get("lng"):
                self.location_cache[zip_code] = {
                    "lat": str(cached_location["lat"]),
                    "lng": str(cached_location["lng"]),
                }
            logger.info(
                "Using cached Walgreens store list for ZIP %s (%s stores)",
                zip_code,
                len(cached_stores),
            )
            return list(cached_stores)

        return self._fetch_stores_near_zip_remote(zip_code)

    @rate_limited
    def _fetch_stores_with_inventory(
        self, zip_code: str, product: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Fetch in-stock pickup stores for a product."""
        article_id = str(product["article_id"])
        planogram = str(product["planogram"])
        product_name = self._product_name(product)
        location = self._fetch_location_context(zip_code)

        url = "https://www.walgreens.com/locator/v1/search/stores/inventory/radius?requestor=COS"
        payload = {
            "requestType": "filterInStockStores",
            "p": "1",
            "s": "50",
            "r": str(SEARCH_RADIUS_MILES),
            "excludeEmergencyClosed": True,
            "articles": [
                {
                    "planogram": planogram,
                    "articleId": article_id,
                    "qty": 1,
                    "isSelectedItem": "true",
                    "opstudyId": None,
                }
            ],
            "bySource": "Web_BAS_PDP_COS",
            "inStockOnly": True,
            "zip": zip_code,
            "q": zip_code,
            "lat": location["lat"],
            "lng": location["lng"],
        }

        data = self._post_json(url, payload)

        if data.get("type") == "ERROR":
            raise ValueError(data.get("message", "Walgreens inventory API returned an error"))

        stores = data.get("results", [])
        logger.info("Found %s in-stock stores for %s near %s", len(stores), product_name, zip_code)
        return stores

    def check_product_at_store(self, product: Dict[str, Any], store_id: str) -> bool:
        """Check whether a product is in stock at a specific store."""
        try:
            stores_data = self._fetch_stores_with_inventory(self.current_zip_code, product)
            for store in stores_data:
                if str(store.get("storeNumber")) != str(store_id):
                    continue
                return self._inventory_in_stock(store, str(product["article_id"]))
            return False
        except Exception as exc:
            logger.debug(
                "Error checking stock for product %s at store %s: %s",
                product.get("article_id"),
                store_id,
                exc,
            )
            return False

    def check_product_availability(
        self,
        product: Dict[str, Any],
        stores: List[Dict[str, Any]],
        product_index: int = 1,
        product_total: int = 1,
    ) -> Dict[str, Any]:
        """Check if a product is available at each store using pickup inventory."""
        availability: Dict[str, bool] = {}
        store_details: Dict[str, Dict[str, Any]] = {}
        product_name = self._product_name(product)
        article_id = str(product["article_id"])
        total_stores = len(stores)
        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)

        logger.info("Checking %s availability near %s...", product_name, self.current_zip_code)
        self._emit_progress(
            {
                "phase": "fetching_inventory",
                "message": f"Requesting pickup inventory for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Walgreens inventory API",
                "stores_processed": 0,
                "total_stores": total_stores,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units),
                "total_units": float(total_units),
            }
        )
        in_stock_stores = self._fetch_stores_with_inventory(self.current_zip_code, product)

        for store in stores:
            store_id = store.get("storeNumber")
            if store_id:
                availability[str(store_id)] = False

        self._emit_progress(
            {
                "phase": "processing_results",
                "message": f"Processing store results for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Matching stores against Walgreens results",
                "stores_processed": 0,
                "total_stores": total_stores,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units + 1),
                "total_units": float(total_units),
            }
        )

        for store in in_stock_stores:
            store_id = str(store.get("storeNumber"))
            if self._inventory_in_stock(store, article_id):
                availability[store_id] = True
                store_details[store_id] = self._store_stock_detail(store, article_id)
                store_name = store.get("store", {}).get("name", f"Store {store_id}")
                logger.info("  STOCK: %s (#%s)", store_name, store_id)

        for index, store in enumerate(stores, start=1):
            store_id = store.get("storeNumber")
            if not store_id:
                continue
            store_name = store.get("name", f"Store {store_id}")
            self._emit_progress(
                {
                    "phase": "processing_results",
                    "message": f"Reviewed {index} of {total_stores} stores for {product_name}",
                    "product": product_name,
                    "product_index": product_index,
                    "product_total": product_total,
                    "store_id": store_id,
                    "store_name": store_name,
                    "stores_processed": index,
                    "total_stores": total_stores,
                    "stores_with_stock_current": len(store_details),
                    "completed_units": float(base_units + 1 + (index / max(total_stores, 1))),
                    "total_units": float(total_units),
                }
            )
            if self.progress_callback and total_stores > 1:
                time.sleep(PROGRESS_UI_YIELD_SECONDS)

        in_stock_count = sum(1 for in_stock in availability.values() if in_stock)
        self._emit_progress(
            {
                "phase": "product_complete",
                "message": f"Finished {product_name}: {in_stock_count} stores in stock",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Product scan complete",
                "stores_processed": total_stores,
                "total_stores": total_stores,
                "stores_with_stock_current": in_stock_count,
                "completed_units": float(base_units + 2),
                "total_units": float(total_units),
            }
        )
        logger.info("Found %s stores with %s in stock", in_stock_count, product_name)
        return {"availability": availability, "stores": store_details}

    def check_products_at_stores(
        self, products: List[Dict[str, Any]], stores: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Check specific products at all stores."""
        results: Dict[str, Dict[str, Any]] = {}
        product_count = len(products)

        for index, product in enumerate(products, start=1):
            article_id = str(product["article_id"])
            product_display_name = self._product_name(product)

            logger.info("\n[%s/%s] %s", index, product_count, product_display_name)
            logger.info("-" * 50)

            product_result = self.check_product_availability(
                product,
                stores,
                product_index=index,
                product_total=product_count,
            )
            results[article_id] = {
                "name": product_display_name,
                "image_url": product.get("image_url", ""),
                "source_url": product.get("source_url", ""),
                "availability": product_result["availability"],
                "stores": product_result["stores"],
            }

        return results

    def get_stores_with_stock(self, check_results: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Extract only stores that have items in stock."""
        stores_with_stock: Dict[str, Dict[str, Any]] = {}

        for product_id, product_data in check_results.items():
            availability = product_data["availability"]
            in_stock_stores = [store_id for store_id, in_stock in availability.items() if in_stock]
            if not in_stock_stores:
                continue
            stores_with_stock[product_id] = {
                "product_name": product_data["name"],
                "image_url": product_data.get("image_url", ""),
                "source_url": product_data.get("source_url", ""),
                "store_ids": in_stock_stores,
                "count": len(in_stock_stores),
                "total_inventory": sum(
                    store.get("inventory_count", 0)
                    for store in product_data.get("stores", {}).values()
                ),
                "stores": sorted(
                    product_data.get("stores", {}).values(),
                    key=lambda store: (
                        store.get("distance") is None,
                        store.get("distance") if store.get("distance") is not None else float("inf"),
                        -store.get("inventory_count", 0),
                    ),
                ),
            }

        return stores_with_stock

    def check_stock(self, zip_code: str = TARGET_ZIP_CODE) -> Tuple[bool, Dict[str, Any]]:
        """Run a one-shot stock check using the configured product names map."""
        try:
            self.current_zip_code = zip_code

            logger.info("=" * 60)
            logger.info("WALGREENS STOCK CHECK")
            logger.info("Time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("Location: ZIP %s (%s mile radius)", zip_code, SEARCH_RADIUS_MILES)
            logger.info("=" * 60)

            stores = self._fetch_stores_near_zip(zip_code)
            if not stores:
                logger.error("No stores found")
                return False, {"error": "No stores found for zip code"}

            logger.info("\nStores to check: %s", len(stores))
            logger.info("\n" + "=" * 60)
            logger.info("CHECKING PRODUCTS...")
            logger.info("=" * 60)

            products = [
                {
                    "article_id": article_id,
                    "name": product["name"],
                    "planogram": product["planogram"],
                }
                for article_id, product in DEFAULT_TRACKED_PRODUCTS.items()
            ]
            results = self.check_products_at_stores(products, stores)
            stores_with_stock = self.get_stores_with_stock(results)

            logger.info("\n" + "=" * 60)
            logger.info("CHECK COMPLETE")
            logger.info("=" * 60)
            logger.info("Stores checked: %s", len(stores))
            logger.info("Products checked: %s", len(products))
            if stores_with_stock:
                logger.info("STOCK FOUND: %s product(s)", len(stores_with_stock))
                for product_id, info in stores_with_stock.items():
                    logger.info("  - %s: %s stores", info["product_name"], info["count"])
            else:
                logger.info("No stock found")
            logger.info("=" * 60 + "\n")

            return True, {
                "timestamp": datetime.now().isoformat(),
                "total_stores_checked": len(stores),
                "products_with_stock": stores_with_stock,
                "all_results": results,
            }
        except Exception as exc:
            logger.error("Stock check failed: %s", exc)
            logger.info("=" * 60 + "\n")
            return False, {"error": str(exc), "type": type(exc).__name__}
