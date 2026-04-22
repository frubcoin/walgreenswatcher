"""ALDI stock checker using the Instacart-backed storefront API."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

import requests

from aldi import AldiGraphqlClient
from config import SEARCH_RADIUS_MILES, STORE_LOCATOR_CACHE_TTL_SECONDS, TARGET_ZIP_CODE

PROGRESS_UI_YIELD_SECONDS = 0.02
ALDI_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

logger = logging.getLogger(__name__)
_shared_zip_geocode_cache: Dict[str, Dict[str, Any]] = {}
_shared_zip_geocode_cache_lock = threading.RLock()


class AldiStockChecker:
    """Check ALDI local availability through its Instacart storefront."""

    def __init__(self) -> None:
        self.progress_callback = None
        self.current_zip_code = TARGET_ZIP_CODE
        self.search_radius_miles = int(SEARCH_RADIUS_MILES or 20)

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        if self.progress_callback:
            self.progress_callback(progress_info)

    @staticmethod
    def _cache_expiry_time() -> float:
        return time.monotonic() + float(STORE_LOCATOR_CACHE_TTL_SECONDS)

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

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

    def _geocode_zip_code(self, zip_code: str) -> Dict[str, float]:
        cached = self._get_cached_geocode_entry(zip_code)
        if cached and cached.get("location"):
            return dict(cached["location"])

        response = requests.get(
            ALDI_NOMINATIM_URL,
            params={
                "format": "jsonv2",
                "countrycodes": "us",
                "limit": "1",
                "postalcode": str(zip_code or "").strip(),
            },
            headers={
                "Accept": "application/json",
                "User-Agent": "retail-stock-watcher/1.0 (zip geocoding for ALDI store lookup)",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            raise ValueError(f"Could not geocode ZIP code {zip_code} for ALDI store lookup")

        item = payload[0] or {}
        location = {
            "lat": self._safe_float(item.get("lat")),
            "lng": self._safe_float(item.get("lon")),
        }
        if location["lat"] is None or location["lng"] is None:
            raise ValueError(f"Could not geocode ZIP code {zip_code} for ALDI store lookup")

        self._store_cached_geocode_entry(zip_code, location)
        return location

    @staticmethod
    def _availability_text(item: Dict[str, Any]) -> str:
        availability = item.get("availability") or {}
        view_section = availability.get("viewSection") or {}
        label = str(view_section.get("stockLevelLabelString") or "").strip()
        if label:
            return label
        stock_level = str(availability.get("stockLevel") or "").strip()
        is_available = bool(availability.get("available"))
        
        if stock_level.lower() in {"instock", "in_stock"}:
            return "In stock (Quantity unknown)"
        elif stock_level.lower() in {"lowstock", "low_stock"}:
            return "Low stock (Quantity unknown)"
        elif stock_level:
            return stock_level
            
        return "In stock (Quantity unknown)" if is_available else "Unavailable"

    @classmethod
    def _is_available(cls, item: Dict[str, Any]) -> bool:
        availability = item.get("availability") or {}
        stock_level = str(availability.get("stockLevel") or "").strip().lower()
        return bool(availability.get("available")) or stock_level in {"in_stock", "instock", "lowstock", "low_stock"}

    def check_product_availability(
        self,
        product: Dict[str, Any],
        zip_code: str = "",
        product_index: int = 1,
        product_total: int = 1,
    ) -> Dict[str, Any]:
        active_zip = str(zip_code or self.current_zip_code or TARGET_ZIP_CODE).strip()
        source_url = str(product.get("source_url") or "").strip()
        product_id = str(product.get("article_id") or product.get("product_id") or "").strip()
        product_name = str(product.get("name") or product_id or "ALDI product").strip()
        if not source_url:
            raise ValueError("ALDI products require a source URL")
        if not product_id:
            product_id = AldiGraphqlClient.extract_product_id(source_url)
        if not product_id:
            raise ValueError("ALDI products require a product id")

        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)
        self.current_zip_code = active_zip

        self._emit_progress(
            {
                "phase": "locating_stores",
                "message": f"Finding ALDI stores near {active_zip}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "ALDI location services",
                "stores_processed": 0,
                "total_stores": 0,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units),
                "total_units": float(total_units),
            }
        )

        product_html = AldiGraphqlClient.fetch_product_page(source_url)
        metadata = AldiGraphqlClient.extract_product_metadata(source_url, html=product_html)
        token = str(metadata.get("auth_token") or "").strip()
        if not token:
            raise ValueError("ALDI did not return a guest auth token for the product page")
        operation_hashes = AldiGraphqlClient.operation_hashes(product_html)
        location = self._geocode_zip_code(active_zip)
        stores = AldiGraphqlClient.fetch_stores(
            postal_code=active_zip,
            latitude=float(location["lat"]),
            longitude=float(location["lng"]),
            token=token,
            operation_hashes=operation_hashes,
            referer=source_url,
        )

        # Pre-filter stores by configured search radius before querying inventory.
        # Stores whose distance is None (API returned no coordinates) are included
        # as a safe fallback — ALDI's storefront already scopes results geographically.
        try:
            radius = float(self.search_radius_miles)
        except (TypeError, ValueError):
            radius = None

        if radius is not None:
            stores = [
                store for store in stores
                if store.get("distance") is None or store["distance"] <= radius
            ]

        total_stores = len(stores)
        availability = {str(store.get("store_id") or "").strip(): False for store in stores}
        store_details: Dict[str, Dict[str, Any]] = {}

        self._emit_progress(
            {
                "phase": "fetching_inventory",
                "message": f"Requesting ALDI availability for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "ALDI storefront API",
                "stores_processed": 0,
                "total_stores": total_stores,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units + 1),
                "total_units": float(total_units),
            }
        )

        for index, store in enumerate(stores, start=1):
            store_id = str(store.get("store_id") or "").strip()
            if not store_id:
                continue
            item = AldiGraphqlClient.fetch_item(
                product_id=product_id,
                store=store,
                postal_code=active_zip,
                operation_hashes=operation_hashes,
                referer=source_url,
            )
            in_stock = self._is_available(item)
            availability[store_id] = in_stock
            availability_text = self._availability_text(item)
            if in_stock:
                store_details[store_id] = {
                    **store,
                    "inventory_count": 1,
                    "inventory_count_known": False,
                    "availability_mode": "fulfillment",
                    "availability_text": availability_text,
                    "pickup_available": store.get("service_type") == "pickup",
                    "delivery_available": store.get("service_type") == "delivery",
                    "retailer": "aldi",
                }

            self._emit_progress(
                {
                    "phase": "processing_results",
                    "message": f"Reviewed {index} of {max(total_stores, 1)} ALDI stores for {product_name}",
                    "product": product_name,
                    "product_index": product_index,
                    "product_total": product_total,
                    "store_id": store_id,
                    "store_name": store.get("name") or f"ALDI #{store_id}",
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
                "message": f"Finished {product_name}: {len(store_details)} ALDI stores in stock",
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

        result: Dict[str, Any] = {
            "availability": availability,
            "stores": store_details,
            "location_ids": [str(store.get("store_id") or "").strip() for store in stores if store.get("store_id")],
        }
        image_url = str(metadata.get("image_url") or "").strip()
        if image_url:
            result["_extracted_image_url"] = image_url
        return result
