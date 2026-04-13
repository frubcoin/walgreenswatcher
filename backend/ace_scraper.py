"""Ace Hardware stock checker using Scrapling browser sessions."""

from __future__ import annotations

import logging
import math
import time
from typing import Any, Dict, List, Optional

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

    def _emit_step_progress(
        self,
        *,
        product_name: str,
        product_index: int,
        product_total: int,
        phase: str,
        message: str,
        store_name: str,
        step_progress: float = 0.0,
        stores_processed: int = 0,
        total_stores: int = 0,
        stores_with_stock_current: int = 0,
    ) -> None:
        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)
        bounded_step_progress = max(0.0, min(float(step_progress or 0.0), 0.95))
        self._emit_progress(
            {
                "phase": phase,
                "message": message,
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": store_name,
                "stores_processed": stores_processed,
                "total_stores": total_stores,
                "stores_with_stock_current": stores_with_stock_current,
                "completed_units": float(base_units + bounded_step_progress),
                "total_units": float(total_units),
            }
        )

    def _make_detail_progress_callback(
        self,
        *,
        product_name: str,
        product_index: int,
        product_total: int,
    ):
        def callback(progress_info: Dict[str, Any]) -> None:
            self._emit_step_progress(
                product_name=product_name,
                product_index=product_index,
                product_total=product_total,
                phase=str(progress_info.get("phase") or "fetching_inventory"),
                message=str(progress_info.get("message") or f"Checking Ace inventory for {product_name}"),
                store_name=str(progress_info.get("store_name") or "Ace inventory"),
                step_progress=float(progress_info.get("step_progress") or 0.0),
                stores_processed=int(progress_info.get("stores_processed") or 0),
                total_stores=int(progress_info.get("total_stores") or 0),
                stores_with_stock_current=int(progress_info.get("stores_with_stock_current") or 0),
            )

        return callback

    @staticmethod
    def _empty_result() -> Dict[str, Any]:
        return {
            "availability": {},
            "stores": {},
            "location_ids": [],
        }

    @staticmethod
    def _result_image_fields(context: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        product_metadata = dict(context.get("product") or {})
        extracted_image_url = str(product_metadata.get("image_url") or "").strip()
        if extracted_image_url:
            result["_extracted_image_url"] = extracted_image_url
        extracted_canonical_url = str(product_metadata.get("canonical_url") or "").strip()
        if extracted_canonical_url:
            result["_canonical_url"] = extracted_canonical_url
        return result

    def _build_result_from_context(
        self,
        context: Dict[str, Any],
        *,
        active_zip: str,
        product_name: str,
        product_index: int,
        product_total: int,
    ) -> Dict[str, Any]:
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
        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)

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

        return self._result_image_fields(
            context,
            {
                "availability": availability,
                "stores": store_details,
                "location_ids": all_store_ids,
            },
        )

    def _emit_product_start(
        self,
        *,
        product_name: str,
        product_index: int,
        product_total: int,
    ) -> None:
        self._emit_step_progress(
            product_name=product_name,
            product_index=product_index,
            product_total=product_total,
            phase="ace_preparing",
            message=f"Preparing Ace scan for {product_name}",
            store_name="Ace scan queued",
            step_progress=0.0,
        )

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

        self._emit_product_start(
            product_name=product_name,
            product_index=product_index,
            product_total=product_total,
        )
        detail_progress = self._make_detail_progress_callback(
            product_name=product_name,
            product_index=product_index,
            product_total=product_total,
        )

        context = None
        try:
            context = AceBrowserClient._try_direct_api(
                source_url,
                zip_code=active_zip,
                product_hints=product,
                progress_callback=detail_progress,
            )
        except Exception as e:
            logger.warning("Ace direct-API fast-path failed for %s, falling back to browser: %s", product_name, e)

        if not context:
            self._emit_step_progress(
                product_name=product_name,
                product_index=product_index,
                product_total=product_total,
                phase="ace_browser_queue",
                message=f"Direct Ace API was inconclusive for {product_name}. Switching to browser fallback...",
                store_name="Ace browser fallback queued",
                step_progress=0.6,
            )
            logger.info("Ace direct-API failed or returned no stock, attempting browser fallback for %s", product_name)
            context = AceBrowserClient.fetch_product_context(
                source_url,
                zip_code=active_zip,
                product_hints=product,
                progress_callback=detail_progress,
            )
        return self._build_result_from_context(
            context,
            active_zip=active_zip,
            product_name=product_name,
            product_index=product_index,
            product_total=product_total,
        )

    def check_products_availability(
        self,
        products: List[Dict[str, Any]],
        zip_code: str = "",
        *,
        product_positions: Optional[List[int]] = None,
        product_total: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        active_zip = str(zip_code or self.current_zip_code or TARGET_ZIP_CODE).strip()
        resolved_total = int(product_total or len(products) or 1)
        results: List[Dict[str, Any]] = [self._empty_result() for _ in products]
        pending_browser_products: List[Dict[str, Any]] = []

        for offset, product in enumerate(products):
            product_name = str(product.get("name") or product.get("article_id") or "Ace product").strip()
            source_url = str(product.get("source_url") or "").strip()
            product_index = (
                int(product_positions[offset])
                if product_positions and offset < len(product_positions)
                else (offset + 1)
            )

            if not source_url:
                logger.warning("Ace product %s is missing a source URL", product_name)
                continue

            self._emit_product_start(
                product_name=product_name,
                product_index=product_index,
                product_total=resolved_total,
            )
            detail_progress = self._make_detail_progress_callback(
                product_name=product_name,
                product_index=product_index,
                product_total=resolved_total,
            )

            context = None
            try:
                context = AceBrowserClient._try_direct_api(
                    source_url,
                    zip_code=active_zip,
                    product_hints=product,
                    progress_callback=detail_progress,
                )
            except Exception as exc:
                logger.warning("Ace direct-API fast-path failed for %s, falling back to browser: %s", product_name, exc)

            if context:
                results[offset] = self._build_result_from_context(
                    context,
                    active_zip=active_zip,
                    product_name=product_name,
                    product_index=product_index,
                    product_total=resolved_total,
                )
                continue

            logger.info("Ace direct-API failed or returned no stock, queueing shared browser fallback for %s", product_name)
            self._emit_step_progress(
                product_name=product_name,
                product_index=product_index,
                product_total=resolved_total,
                phase="ace_browser_queue",
                message=f"Direct Ace API was inconclusive for {product_name}. Queueing shared browser fallback...",
                store_name="Ace browser fallback queued",
                step_progress=0.6,
            )
            pending_browser_products.append(
                {
                    "offset": offset,
                    "product": product,
                    "product_name": product_name,
                    "product_index": product_index,
                    "progress_callback": detail_progress,
                }
            )

        if not pending_browser_products:
            return results

        self._emit_progress(
            {
                "phase": "ace_browser_session",
                "message": f"Reusing one Ace browser session for {len(pending_browser_products)} product(s)",
                "product": pending_browser_products[0]["product_name"],
                "product_index": pending_browser_products[0]["product_index"],
                "product_total": resolved_total,
                "store_name": "Ace shared browser session",
                "stores_processed": 0,
                "total_stores": 0,
                "stores_with_stock_current": 0,
                "completed_units": float(1 + ((pending_browser_products[0]['product_index'] - 1) * 2) + 0.64),
                "total_units": float(max(2, (resolved_total * 2) + 2)),
            }
        )

        browser_contexts, browser_errors = AceBrowserClient.fetch_product_contexts(
            [entry["product"] for entry in pending_browser_products],
            zip_code=active_zip,
            progress_callbacks=[entry["progress_callback"] for entry in pending_browser_products],
        )

        for batch_index, pending_entry in enumerate(pending_browser_products):
            context = browser_contexts[batch_index] if batch_index < len(browser_contexts) else None
            if context:
                results[pending_entry["offset"]] = self._build_result_from_context(
                    context,
                    active_zip=active_zip,
                    product_name=pending_entry["product_name"],
                    product_index=pending_entry["product_index"],
                    product_total=resolved_total,
                )
                continue

            error = browser_errors.get(batch_index)
            logger.warning(
                "Ace inventory blocked or failed for product %s in shared session: %s",
                pending_entry["product_name"],
                error,
            )

        return results
