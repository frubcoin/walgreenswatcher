"""Best Buy stock checker using store availability responses."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, List

import requests

from config import TARGET_ZIP_CODE, USER_AGENTS

PROGRESS_UI_YIELD_SECONDS = 0.02

logger = logging.getLogger(__name__)


class BestBuyStockChecker:
    """Check Best Buy local pickup availability using a beta store-availability endpoint."""

    STORE_AVAILABILITY_URL = "https://www.bestbuy.com/productfulfillment/c/api/2.0/storeAvailability"

    def __init__(self) -> None:
        self.progress_callback = None
        self.current_zip_code = TARGET_ZIP_CODE

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        if self.progress_callback:
            self.progress_callback(progress_info)

    @staticmethod
    def _normalize_store_address(location: Dict[str, Any]) -> str:
        parts = [
            str(location.get("address", "")).strip(),
            str(location.get("address2", "")).strip(),
            str(location.get("city", "")).strip(),
            str(location.get("state", "")).strip(),
            str(location.get("zipCode", "")).strip(),
        ]
        street = ", ".join(part for part in parts[:2] if part)
        city_state_zip = " ".join(part for part in parts[3:] if part)
        return ", ".join(part for part in [street, ", ".join(part for part in [parts[2], city_state_zip] if part)] if part)

    @staticmethod
    def _normalize_location(location: Dict[str, Any]) -> Dict[str, Any]:
        latitude = location.get("latitude")
        longitude = location.get("longitude")
        distance = location.get("distance")
        try:
            latitude = float(latitude) if latitude is not None else None
        except (TypeError, ValueError):
            latitude = None
        try:
            longitude = float(longitude) if longitude is not None else None
        except (TypeError, ValueError):
            longitude = None
        try:
            distance = float(distance) if distance is not None else None
        except (TypeError, ValueError):
            distance = None

        location_id = str(location.get("id") or "").strip()
        return {
            "store_id": location_id,
            "name": str(location.get("name") or f"Best Buy {location_id}").strip(),
            "address": BestBuyStockChecker._normalize_store_address(location),
            "distance": distance,
            "latitude": latitude,
            "longitude": longitude,
        }

    def _session_headers(self, *, referer: str) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://www.bestbuy.com",
            "Referer": referer,
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }

    def _bootstrap_session(self, session: requests.Session, product: Dict[str, Any]) -> str:
        referer = str(product.get("source_url", "")).strip() or "https://www.bestbuy.com/"
        bootstrap_targets = [
            "https://www.bestbuy.com/",
            referer,
        ]
        for url in bootstrap_targets:
            try:
                session.get(
                    url,
                    headers=self._session_headers(referer=referer),
                    timeout=30,
                )
            except requests.RequestException:
                continue
        return referer

    def _build_payload(self, sku: str, zip_code: str) -> Dict[str, Any]:
        return {
            "buttonState": [
                {
                    "skuId": sku,
                    "condition": None,
                }
            ],
            "consolidatedButtonState": None,
            "ispu": {
                "searchArea": zip_code,
                "items": [
                    {
                        "sku": sku,
                    }
                ],
            },
        }

    def _fetch_store_availability(self, product: Dict[str, Any], zip_code: str) -> Dict[str, Any]:
        sku = str(product.get("article_id") or "").strip()
        if not sku.isdigit():
            raise ValueError("Best Buy products require a numeric SKU")

        session = requests.Session()
        referer = self._bootstrap_session(session, product)
        response = session.post(
            self.STORE_AVAILABILITY_URL,
            json=self._build_payload(sku, zip_code),
            headers=self._session_headers(referer=referer),
            timeout=30,
        )
        response.raise_for_status()
        try:
            return response.json()
        except ValueError as exc:
            snippet = response.text[:300].replace("\n", " ")
            raise ValueError(f"Best Buy returned non-JSON content: {snippet}") from exc

    def check_product_availability(
        self,
        product: Dict[str, Any],
        zip_code: str,
        product_index: int = 1,
        product_total: int = 1,
    ) -> Dict[str, Any]:
        product_name = str(product.get("name") or product.get("article_id") or "Best Buy product").strip()
        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)
        self.current_zip_code = zip_code

        self._emit_progress(
            {
                "phase": "fetching_inventory",
                "message": f"Requesting Best Buy pickup inventory for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Best Buy fulfillment API",
                "stores_processed": 0,
                "total_stores": 0,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units),
                "total_units": float(total_units),
            }
        )

        payload = self._fetch_store_availability(product, zip_code)
        ispu = payload.get("ispu") or {}
        location_map = {
            str(location.get("id") or "").strip(): self._normalize_location(location)
            for location in (ispu.get("locations") or [])
            if str(location.get("id") or "").strip()
        }

        item = None
        sku = str(product.get("article_id") or "").strip()
        for candidate in (ispu.get("items") or []):
            if str(candidate.get("sku") or "").strip() == sku:
                item = candidate
                break
        if item is None:
            item = (ispu.get("items") or [{}])[0]

        item_locations = item.get("locations") or []
        total_stores = len(location_map) or len(item_locations)
        availability: Dict[str, bool] = {}
        store_details: Dict[str, Dict[str, Any]] = {}

        for index, location in enumerate(item_locations, start=1):
            location_id = str(location.get("locationId") or "").strip()
            if not location_id:
                continue

            available_quantity = 0
            availability_info = location.get("availability") or {}
            try:
                available_quantity = int(availability_info.get("availablePickupQuantity", 0) or 0)
            except (TypeError, ValueError):
                available_quantity = 0

            in_stock = available_quantity > 0
            availability[location_id] = in_stock

            if in_stock:
                detail = dict(location_map.get(location_id) or {"store_id": location_id, "name": f"Best Buy {location_id}", "address": "Address unavailable", "distance": None, "latitude": None, "longitude": None})
                detail["inventory_count"] = available_quantity
                store_details[location_id] = detail

            self._emit_progress(
                {
                    "phase": "processing_results",
                    "message": f"Reviewed {index} of {max(len(item_locations), 1)} Best Buy locations for {product_name}",
                    "product": product_name,
                    "product_index": product_index,
                    "product_total": product_total,
                    "store_id": location_id,
                    "store_name": (location_map.get(location_id) or {}).get("name", f"Best Buy {location_id}"),
                    "stores_processed": index,
                    "total_stores": total_stores,
                    "stores_with_stock_current": len(store_details),
                    "completed_units": float(base_units + 1 + (index / max(len(item_locations), 1))),
                    "total_units": float(total_units),
                }
            )
            if self.progress_callback and len(item_locations) > 1:
                time.sleep(PROGRESS_UI_YIELD_SECONDS)

        self._emit_progress(
            {
                "phase": "product_complete",
                "message": f"Finished {product_name}: {len(store_details)} Best Buy locations in stock",
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
            "location_ids": list(location_map.keys()),
        }
