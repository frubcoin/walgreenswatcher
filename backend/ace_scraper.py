"""Ace Hardware stock checker using Scrapling browser sessions."""

from __future__ import annotations

import logging
import math
import time
from typing import Any, Dict, List

from ace import AceBrowserClient, AceBrowserError
from config import SEARCH_RADIUS_MILES, TARGET_ZIP_CODE

PROGRESS_UI_YIELD_SECONDS = 0.02


def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    try:
        r = 3959.0
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.asin(math.sqrt(a))
        return round(r * c, 2)
    except Exception:
        return 999.0

logger = logging.getLogger(__name__)


class AceStockChecker:
    """Check Ace local inventory by driving the PDP nearby-store tray."""

    def __init__(self) -> None:
        self.progress_callback = None
        self.current_zip_code = TARGET_ZIP_CODE
        self.search_radius_miles = int(SEARCH_RADIUS_MILES or 20)

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        if self.progress_callback:
            self.progress_callback(progress_info)

    def check_product_availability(
        self,
        product: Dict[str, Any],
        zip_code: str = "",
        product_index: int = 1,
        product_total: int = 1,
    ) -> Dict[str, Any]:
        product_name = str(product.get("name") or product.get("article_id") or "Ace product").strip()
        active_zip = str(zip_code or self.current_zip_code or TARGET_ZIP_CODE).strip()
        source_url = str(product.get("source_url") or "").strip()
        if not source_url:
            raise AceBrowserError(f"Ace product {product_name} is missing a source URL")

        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)

        self._emit_progress(
            {
                "phase": "fetching_inventory",
                "message": f"Requesting Ace local inventory for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Ace browser inventory flow",
                "stores_processed": 0,
                "total_stores": 0,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units),
                "total_units": float(total_units),
            }
        )

        context = AceBrowserClient.fetch_product_context(source_url, zip_code=active_zip)
        product_metadata = dict(context.get("product") or {})
        store_candidates = list(context.get("store_candidates") or [])
        store_lookup = AceBrowserClient.build_store_lookup(store_candidates)
        in_stock_stores = dict(context.get("stores") or {})

        target_location = AceBrowserClient.geocode_zip(active_zip)
        target_lat = target_location.get("lat")
        target_lng = target_location.get("lng")

        all_store_ids = list(store_lookup.keys())
        availability = {store_id: False for store_id in all_store_ids}
        store_details: Dict[str, Dict[str, Any]] = {}
        for store_id, store in in_stock_stores.items():
            normalized_store_id = str(store_id or "").strip()
            if not normalized_store_id:
                continue
            
            # Merge the in-stock store data with the normalized store details from the lookup
            # This ensures top-level latitude and longitude are present for the map.
            lookup_detail = store_lookup.get(normalized_store_id) or {}
            
            store_lat = lookup_detail.get("latitude")
            store_lng = lookup_detail.get("longitude")
            distance = 999.0
            if target_lat is not None and target_lng is not None and store_lat is not None and store_lng is not None:
                distance = _haversine_distance(target_lat, target_lng, store_lat, store_lng)
            
            availability[normalized_store_id] = True
            store_details[normalized_store_id] = {
                **lookup_detail,
                "distance": distance,
                "inventory_count": store.get("inventory_count", 1),
                "inventory_count_known": bool(store.get("inventory_count_known", False)),
                "availability_mode": str(store.get("availability_mode") or "fulfillment").strip() or "fulfillment",
                "pickup_available": store.get("pickup_available", False),
                "delivery_available": store.get("delivery_available", False),
                "supports_inventory": bool(store.get("supports_inventory", False)),
                "fulfillment_types": list(store.get("fulfillment_types") or []),
                "availability_text": store.get("availability_text", "In Stock"),
            }

        total_stores = len(all_store_ids)
        self._emit_progress(
            {
                "phase": "processing_results",
                "message": f"Reviewed {total_stores} Ace stores for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Ace store tray results",
                "stores_processed": total_stores,
                "total_stores": total_stores,
                "stores_with_stock_current": len(store_details),
                "completed_units": float(base_units + 1),
                "total_units": float(total_units),
            }
        )
        if self.progress_callback and total_stores > 1:
            time.sleep(PROGRESS_UI_YIELD_SECONDS)

        self._emit_progress(
            {
                "phase": "product_complete",
                "message": f"Finished {product_name}: {len(store_details)} Ace stores in stock",
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

        result = {
            "availability": availability,
            "stores": store_details,
            "location_ids": all_store_ids,
        }
        extracted_image_url = str(product_metadata.get("image_url") or "").strip()
        if extracted_image_url:
            result["_extracted_image_url"] = extracted_image_url
        extracted_canonical_url = str(product_metadata.get("canonical_url") or "").strip()
        if extracted_canonical_url:
            result["_canonical_url"] = extracted_canonical_url
        return result
