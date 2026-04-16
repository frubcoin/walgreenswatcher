"""Per-user scheduler management for the hosted local pick-up monitor."""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler

from ace import AceBrowserClient, AceBrowserError
from ace_scraper import AceStockChecker
from config import (
    DEFAULT_CHECK_INTERVAL_MINUTES,
    DEFAULT_TRACKED_PRODUCTS,
    SEARCH_RADIUS_MILES,
    TARGET_ZIP_CODE,
)

from cvs_scraper import CvsBlockedError, CvsDisabledError, CvsStockChecker
from database import StockDatabase
from discord_notifier import DiscordNotifier
from fivebelow_scraper import FiveBelowStockChecker
from walgreens_scraper import WalgreensStockChecker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_POKEMON_BACKGROUND_ENABLED = False
DEFAULT_POKEMON_BACKGROUND_THEME = "gyra"
DEFAULT_POKEMON_BACKGROUND_TILE_SIZE = 645
MIN_POKEMON_BACKGROUND_TILE_SIZE = 200
MAX_POKEMON_BACKGROUND_TILE_SIZE = 1200
MIN_NOTIFICATION_DISTANCE_MILES = 1
MAX_NOTIFICATION_DISTANCE_MILES = 50
ALLOWED_POKEMON_BACKGROUND_THEMES = {
    "ancient",
    "bee",
    "bulba",
    "charmander",
    "eevee",
    "graveler",
    "gx5YNZ3",
    "gyra",
    "karp",
    "pattern1",
    "pika",
    "poli",
    "ponyta",
    "puff",
    "rat",
    "slowpoke",
    "slowpoke2",
    "squirtle",
    "vulpix",
    "western",
}


class StockCheckScheduler:
    """One scheduler and one live progress stream for one user."""

    def __init__(self, user_id: int, db: StockDatabase):
        self.user_id = int(user_id)
        self.db = db
        self.scheduler: Optional[BackgroundScheduler] = None
        self.state_lock = threading.RLock()

        self.walgreens_checker = WalgreensStockChecker()
        self.cvs_checker = CvsStockChecker()
        self.fivebelow_checker = FiveBelowStockChecker()
        self.ace_checker = AceStockChecker()

        self.notifier = DiscordNotifier([])
        self.is_running = False
        self.last_check_time: Optional[datetime] = None
        self.last_products_with_stock: Dict[str, Dict[str, Any]] = {}
        self.last_notified_products: Dict[str, Dict[str, Any]] = {}

        self.current_zipcode = TARGET_ZIP_CODE
        self.check_interval_minutes = DEFAULT_CHECK_INTERVAL_MINUTES
        self.max_notification_distance_miles = int(SEARCH_RADIUS_MILES)
        self.discord_destinations: List[Dict[str, str]] = []
        self.pokemon_background_enabled = DEFAULT_POKEMON_BACKGROUND_ENABLED
        self.pokemon_background_theme = DEFAULT_POKEMON_BACKGROUND_THEME
        self.pokemon_background_tile_size = DEFAULT_POKEMON_BACKGROUND_TILE_SIZE
        self.map_provider = "google"
        self.discord_ping_on_change_only = False
        self.tracked_products: Dict[str, Dict[str, str]] = {}

        self.check_in_progress = False
        self.current_store = None
        self.current_product = None
        self.stores_checked = 0
        self.total_stores = 0
        self.current_phase = "idle"
        self.progress_message = "Idle"
        self.current_product_index = 0
        self.total_products = 0
        self.stores_with_stock_current = 0
        self.progress_completed_units = 0.0
        self.progress_total_units = 0.0
        self.last_total_stores_checked = 0

        self.refresh_from_db()
        self._load_last_check_snapshot()

    @staticmethod
    def _tracked_product_key(article_id: Any, retailer: Any) -> str:
        normalized_retailer = str(retailer or "walgreens").strip().lower() or "walgreens"
        normalized_article_id = str(article_id or "").strip()
        return f"{normalized_retailer}:{normalized_article_id}"

    def _load_last_check_snapshot(self) -> None:
        last_check = self.db.get_last_check(self.user_id)
        if not last_check:
            return

        timestamp = last_check.get("timestamp")
        if timestamp:
            try:
                self.last_check_time = datetime.fromisoformat(timestamp)
            except ValueError:
                self.last_check_time = None
        self.last_products_with_stock = dict(last_check.get("products_found") or {})
        check_result = last_check.get("check_result") or {}
        self.last_total_stores_checked = int(
            check_result.get("total_stores_checked")
            or last_check.get("total_stores_checked")
            or 0
        )

    def refresh_from_db(self) -> None:
        settings = self.db.get_user_settings(self.user_id)
        admin_settings = self.db.get_admin_settings()
        products = self.db.list_tracked_products(self.user_id)

        CvsStockChecker.set_proxy_urls_override(admin_settings.get("cvs_proxy_urls"))
        AceBrowserClient.set_proxy_urls_override(admin_settings.get("cvs_proxy_urls"))

        if not products:
            for article_id, product in DEFAULT_TRACKED_PRODUCTS.items():
                self.db.add_tracked_product(
                    self.user_id,
                    article_id,
                    product.get("retailer", "walgreens"),
                    product["name"],
                    product["planogram"],
                    image_url=product.get("image_url", ""),
                    source_url=product.get("source_url", ""),
                    product_id=product.get("product_id", ""),
                )
            products = self.db.list_tracked_products(self.user_id)

        self.current_zipcode = settings["current_zipcode"]
        self.check_interval_minutes = settings["check_interval_minutes"]
        self.max_notification_distance_miles = settings["max_notification_distance_miles"]
        self.discord_destinations = list(settings["discord_destinations"])
        self.pokemon_background_enabled = settings["pokemon_background_enabled"]
        self.pokemon_background_theme = settings["pokemon_background_theme"]
        self.pokemon_background_tile_size = settings["pokemon_background_tile_size"]
        self.map_provider = settings.get("map_provider", "google")
        self.discord_ping_on_change_only = settings.get("discord_ping_on_change_only", False)
        self.notifier = DiscordNotifier(self.discord_destinations or None, map_provider=self.map_provider)
        self.tracked_products = {
            self._tracked_product_key(product["id"], product.get("retailer", "walgreens")): {
                "key": product.get("key")
                or self._tracked_product_key(product["id"], product.get("retailer", "walgreens")),
                "article_id": product["id"],
                "retailer": product.get("retailer", "walgreens"),
                "name": product["name"],
                "planogram": product["planogram"],
                "image_url": product.get("image_url", ""),
                "source_url": product.get("source_url", ""),
                "product_id": product.get("product_id", ""),
                "exclude_from_discord": product.get("exclude_from_discord", False),
            }
            for product in products
        }
        self.walgreens_checker.current_zip_code = self.current_zipcode
        self.walgreens_checker.search_radius_miles = self.max_notification_distance_miles
        self.cvs_checker.current_zip_code = self.current_zipcode
        self.cvs_checker.search_radius_miles = self.max_notification_distance_miles
        self.fivebelow_checker.current_zip_code = self.current_zipcode
        self.ace_checker.current_zip_code = self.current_zipcode
        self.ace_checker.search_radius_miles = self.max_notification_distance_miles
        self._load_last_check_snapshot()

    def get_last_check_snapshot(self) -> Optional[Dict[str, Any]]:
        if not self.last_check_time:
            return None

        timestamp = self.last_check_time.isoformat()
        products_found = dict(self.last_products_with_stock or {})
        total_stores_checked = int(self.last_total_stores_checked or 0)
        return {
            "timestamp": timestamp,
            "has_stock": bool(products_found),
            "products_found": products_found,
            "check_result": {
                "timestamp": timestamp,
                "total_stores_checked": total_stores_checked,
            },
        }

    @staticmethod
    def _validate_interval_minutes(interval_minutes: Any) -> int:
        try:
            value = int(interval_minutes)
        except (TypeError, ValueError) as exc:
            raise ValueError("Schedule interval must be a whole number of minutes") from exc

        if value < 30:
            raise ValueError("Schedule interval must be at least 30 minutes")
        if value > 1440:
            raise ValueError("Schedule interval must be 1440 minutes or less")
        return value

    @staticmethod
    def _validate_boolean_setting(value: Any, label: str) -> bool:
        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return bool(value)

        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False

        raise ValueError(f"{label} must be true or false")

    @staticmethod
    def _validate_pokemon_background_theme(theme: Any) -> str:
        normalized = str(theme or "").strip()
        if normalized in ALLOWED_POKEMON_BACKGROUND_THEMES:
            return normalized
        raise ValueError("Pokemon background theme is not supported")

    @staticmethod
    def _validate_pokemon_background_tile_size(tile_size: Any) -> int:
        try:
            value = int(tile_size)
        except (TypeError, ValueError) as exc:
            raise ValueError("Pokemon background tile size must be a whole number") from exc

        if value < MIN_POKEMON_BACKGROUND_TILE_SIZE:
            raise ValueError(
                f"Pokemon background tile size must be at least {MIN_POKEMON_BACKGROUND_TILE_SIZE}px"
            )
        if value > MAX_POKEMON_BACKGROUND_TILE_SIZE:
            raise ValueError(
                f"Pokemon background tile size must be {MAX_POKEMON_BACKGROUND_TILE_SIZE}px or less"
            )
        return value

    @staticmethod
    def _validate_notification_distance_miles(distance_miles: Any) -> int:
        try:
            value = int(distance_miles)
        except (TypeError, ValueError) as exc:
            raise ValueError("Max notification range must be a whole number of miles") from exc

        if value < MIN_NOTIFICATION_DISTANCE_MILES:
            raise ValueError(
                f"Max notification range must be at least {MIN_NOTIFICATION_DISTANCE_MILES} mile"
            )
        if value > MAX_NOTIFICATION_DISTANCE_MILES:
            raise ValueError(
                f"Max notification range must be {MAX_NOTIFICATION_DISTANCE_MILES} miles or less"
            )
        return value

    def _update_setting(self, **updates: Any) -> None:
        self.db.update_user_settings(self.user_id, updates)
        self.refresh_from_db()

    def add_product(
        self,
        article_id: str,
        retailer: str,
        product_name: str,
        planogram: str,
        image_url: str = "",
        source_url: str = "",
        product_id: str = "",
    ) -> bool:
        success = self.db.add_tracked_product(
            self.user_id,
            article_id.strip(),
            retailer.strip() or "walgreens",
            product_name.strip(),
            planogram.strip(),
            image_url=image_url.strip(),
            source_url=source_url.strip(),
            product_id=product_id.strip(),
        )
        if success:
            self.refresh_from_db()
        return success

    def remove_product(self, product_id: str, retailer: str = "") -> bool:
        success = self.db.remove_tracked_product(self.user_id, product_id, retailer.strip())
        if success:
            self.refresh_from_db()
        return success

    def update_product_name(self, product_id: str, product_name: str, retailer: str = "") -> bool:
        product_name = str(product_name).strip()
        if not product_name:
            raise ValueError("Product name cannot be empty")

        success = self.db.update_tracked_product_name(
            self.user_id,
            product_id,
            product_name,
            retailer.strip(),
        )
        if success:
            self.refresh_from_db()
        return success

    def set_product_discord_exclusion(self, product_id: str, exclude: bool, retailer: str = "") -> bool:
        success = self.db.update_product_discord_exclusion(
            self.user_id,
            product_id,
            exclude,
            retailer.strip(),
        )
        if success:
            self.refresh_from_db()
        return success

    def set_zipcode(self, zipcode: str) -> None:
        normalized_zipcode = str(zipcode).strip() or TARGET_ZIP_CODE
        self._update_setting(current_zipcode=normalized_zipcode)

    def set_check_interval_minutes(self, interval_minutes: Any) -> int:
        validated = self._validate_interval_minutes(interval_minutes)
        self._update_setting(check_interval_minutes=validated)

        if self.is_running and self.scheduler is not None:
            self.scheduler.reschedule_job(
                self._job_id,
                trigger="interval",
                minutes=self.check_interval_minutes,
            )
        return self.check_interval_minutes

    def set_max_notification_distance_miles(self, distance_miles: Any) -> int:
        validated = self._validate_notification_distance_miles(distance_miles)
        self._update_setting(max_notification_distance_miles=validated)
        return self.max_notification_distance_miles

    def set_discord_destinations(self, destinations: Optional[List[Dict[str, Any]]]) -> List[Dict[str, str]]:
        normalized = DiscordNotifier._normalize_destinations(destinations)
        self._update_setting(discord_destinations=normalized)
        return list(self.discord_destinations)

    def set_pokemon_background_enabled(self, enabled: Any) -> bool:
        value = self._validate_boolean_setting(enabled, "Pokemon background setting")
        self._update_setting(pokemon_background_enabled=value)
        return self.pokemon_background_enabled

    def set_pokemon_background_theme(self, theme: Any) -> str:
        value = self._validate_pokemon_background_theme(theme)
        self._update_setting(pokemon_background_theme=value)
        return self.pokemon_background_theme

    def set_pokemon_background_tile_size(self, tile_size: Any) -> int:
        value = self._validate_pokemon_background_tile_size(tile_size)
        self._update_setting(pokemon_background_tile_size=value)
        return self.pokemon_background_tile_size

    def set_map_provider(self, provider: Any) -> str:
        normalized = str(provider or "google").strip().lower()
        if normalized not in ("google", "apple"):
            raise ValueError("Map provider must be 'google' or 'apple'")
        self._update_setting(map_provider=normalized)
        self.map_provider = normalized
        self.notifier = DiscordNotifier(self.discord_destinations or None, map_provider=normalized)
        return self.map_provider

    def set_discord_ping_on_change_only(self, value: Any) -> bool:
        normalized = bool(value)
        self._update_setting(discord_ping_on_change_only=normalized)
        self.discord_ping_on_change_only = normalized
        return self.discord_ping_on_change_only

    def _product_specs(self) -> List[Dict[str, str]]:
        return [
            {
                "key": key,
                "article_id": product["article_id"],
                "retailer": product.get("retailer", "walgreens"),
                "name": product["name"],
                "planogram": product["planogram"],
                "image_url": product.get("image_url", ""),
                "source_url": product.get("source_url", ""),
                "product_id": product.get("product_id", ""),
            }
            for key, product in self.tracked_products.items()
        ]

    def _reset_progress(self) -> None:
        self.current_store = None
        self.current_product = None
        self.stores_checked = 0
        self.total_stores = 0
        self.current_phase = "idle"
        self.progress_message = "Idle"
        self.current_product_index = 0
        self.total_products = 0
        self.stores_with_stock_current = 0
        self.progress_completed_units = 0.0
        self.progress_total_units = 0.0

    def _set_progress(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    @property
    def _job_id(self) -> str:
        return f"stock_check_user_{self.user_id}"

    def _extract_products_with_stock(
        self, check_results: Dict[str, Dict[str, Any]], tracked_products: Dict[str, Dict[str, str]]
    ) -> Dict[str, Dict[str, Any]]:
        products_with_stock: Dict[str, Dict[str, Any]] = {}

        for product_id, product_data in check_results.items():
            # product_id is already the key (retailer:article_id), use it directly
            availability = dict(product_data.get("availability") or {})
            in_stock_stores = [store_id for store_id, in_stock in availability.items() if in_stock]
            if not in_stock_stores:
                continue

            stores = sorted(
                (product_data.get("stores") or {}).values(),
                key=lambda store: (
                    store.get("distance") is None,
                    store.get("distance") if store.get("distance") is not None else float("inf"),
                    -store.get("inventory_count", 0),
                ),
            )

            products_with_stock[product_id] = {
                "retailer": product_data.get("retailer", "walgreens"),
                "product_name": product_data.get("name") or product_id,
                "image_url": product_data.get("image_url", ""),
                "source_url": product_data.get("source_url", ""),
                "store_ids": in_stock_stores,
                "count": len(in_stock_stores),
                "total_inventory": sum(store.get("inventory_count", 0) for store in stores),
                "stores": stores,
            }

        return products_with_stock

    def _filter_products_for_discord(
        self,
        products_with_stock: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        filtered_products: Dict[str, Dict[str, Any]] = {}

        for product_id, product_data in (products_with_stock or {}).items():
            stores = [
                dict(store)
                for store in (product_data.get("stores") or [])
                if self._store_within_notification_range(store)
            ]
            if not stores:
                continue

            filtered_products[product_id] = {
                **product_data,
                "store_ids": [
                    str(store.get("store_id") or "").strip()
                    for store in stores
                    if str(store.get("store_id") or "").strip()
                ],
                "count": len(stores),
                "total_inventory": sum(int(store.get("inventory_count", 0) or 0) for store in stores),
                "stores": stores,
            }

        return filtered_products

    def _products_info_changed(
        self,
        current_products: Dict[str, Dict[str, Any]],
    ) -> bool:
        """Check if product information has changed compared to last notification.

        Returns True if:
        - No previous notification recorded
        - Different set of products in stock
        - Different stores for any product
        - Different inventory counts for any store
        """
        if not self.last_notified_products:
            return True

        current_keys = set(current_products.keys())
        last_keys = set(self.last_notified_products.keys())

        # Check if product set changed
        if current_keys != last_keys:
            return True

        # Check each product's stores and inventory
        for product_id, current_data in current_products.items():
            last_data = self.last_notified_products.get(product_id, {})
            current_stores = current_data.get("stores", [])
            last_stores = last_data.get("stores", [])

            # Build store lookup by ID for comparison
            current_by_store: Dict[str, Dict] = {}
            for store in current_stores:
                store_id = str(store.get("store_id") or "").strip()
                if store_id:
                    current_by_store[store_id] = store

            last_by_store: Dict[str, Dict] = {}
            for store in last_stores:
                store_id = str(store.get("store_id") or "").strip()
                if store_id:
                    last_by_store[store_id] = store

            # Check if store set changed
            if set(current_by_store.keys()) != set(last_by_store.keys()):
                return True

            # Check inventory counts for each store
            for store_id, current_store in current_by_store.items():
                last_store = last_by_store.get(store_id, {})
                current_count = int(current_store.get("inventory_count", 0) or 0)
                last_count = int(last_store.get("inventory_count", 0) or 0)
                if current_count != last_count:
                    return True

        return False

    @staticmethod
    def _store_distance_miles(store: Dict[str, Any]) -> Optional[float]:
        try:
            value = store.get("distance")
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _store_within_notification_range(self, store: Dict[str, Any]) -> bool:
        distance = self._store_distance_miles(store)
        if distance is None:
            return False
        return distance <= float(self.max_notification_distance_miles)

    def _filter_product_result_by_notification_range(
        self,
        product_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        stores = dict(product_result.get("stores") or {})
        availability = dict(product_result.get("availability") or {})
        location_ids = [str(store_id) for store_id in (product_result.get("location_ids") or []) if store_id]

        allowed_store_ids = {
            store_id
            for store_id, store in stores.items()
            if self._store_within_notification_range(store)
        }

        filtered_availability = {
            store_id: in_stock
            for store_id, in_stock in availability.items()
            if store_id in allowed_store_ids
        }
        filtered_stores = {
            store_id: store
            for store_id, store in stores.items()
            if store_id in allowed_store_ids
        }
        filtered_location_ids = [
            store_id for store_id in location_ids if store_id in allowed_store_ids
        ]

        return {
            **product_result,
            "availability": filtered_availability,
            "stores": filtered_stores,
            "location_ids": filtered_location_ids,
        }

    def _check_stock(self) -> None:
        try:
            with self.state_lock:
                if self.check_in_progress:
                    logger.info("User %s requested a check while one is already running", self.user_id)
                    return
                self.check_in_progress = True
                self._reset_progress()

            self.refresh_from_db()

            product_specs = self._product_specs()
            progress_total_units = float(max(2, (len(product_specs) * 2) + 2))
            self._set_progress(
                current_phase="starting",
                progress_message="Preparing stock check...",
                total_products=len(product_specs),
                progress_total_units=progress_total_units,
            )

            if not product_specs:
                self._set_progress(
                    current_phase="complete",
                    progress_message="No tracked products configured",
                    progress_completed_units=progress_total_units,
                )
                return

            last_logged = {"phase": None, "product": None, "stores_processed": -1}

            def update_progress(progress_info: Dict[str, Any]) -> None:
                current_phase = progress_info.get("phase", self.current_phase)
                current_product = progress_info.get("product", self.current_product or "")
                stores_processed = progress_info.get("stores_processed", self.stores_checked or 0)
                total_stores = progress_info.get("total_stores", self.total_stores or 0)

                self._set_progress(
                    current_phase=current_phase,
                    progress_message=progress_info.get("message", self.progress_message),
                    current_store=progress_info.get("store_name", self.current_store),
                    current_product=current_product,
                    current_product_index=progress_info.get("product_index", self.current_product_index),
                    total_products=progress_info.get("product_total", self.total_products),
                    stores_checked=stores_processed,
                    total_stores=total_stores,
                    stores_with_stock_current=progress_info.get(
                        "stores_with_stock_current",
                        self.stores_with_stock_current,
                    ),
                    progress_completed_units=progress_info.get(
                        "completed_units",
                        self.progress_completed_units,
                    ),
                    progress_total_units=progress_info.get(
                        "total_units",
                        self.progress_total_units,
                    ),
                )

                should_log = (
                    current_phase != last_logged["phase"]
                    or current_product != last_logged["product"]
                    or stores_processed in {0, total_stores}
                    or (
                        total_stores
                        and stores_processed != last_logged["stores_processed"]
                        and stores_processed % 10 == 0
                    )
                )
                if should_log:
                    logger.info(
                        "[user=%s][%s] %s | %s | %s/%s stores processed",
                        self.user_id,
                        current_phase,
                        current_product or "No product",
                        self.progress_message,
                        stores_processed,
                        total_stores,
                    )
                    last_logged["phase"] = current_phase
                    last_logged["product"] = current_product
                    last_logged["stores_processed"] = stores_processed

            self.walgreens_checker.progress_callback = update_progress
            self.walgreens_checker.current_zip_code = self.current_zipcode
            self.walgreens_checker.search_radius_miles = self.max_notification_distance_miles
            self.cvs_checker.progress_callback = update_progress
            self.cvs_checker.current_zip_code = self.current_zipcode
            self.cvs_checker.search_radius_miles = self.max_notification_distance_miles
            self.fivebelow_checker.progress_callback = update_progress
            self.fivebelow_checker.current_zip_code = self.current_zipcode
            self.ace_checker.progress_callback = update_progress
            self.ace_checker.current_zip_code = self.current_zipcode
            self.ace_checker.search_radius_miles = self.max_notification_distance_miles

            walgreens_products = [
                product for product in product_specs if product.get("retailer", "walgreens") == "walgreens"
            ]
            fivebelow_products = [
                product for product in product_specs if product.get("retailer", "walgreens") == "fivebelow"
            ]
            ace_products = [
                product for product in product_specs if product.get("retailer", "walgreens") == "ace"
            ]
            walgreens_stores: List[Dict[str, Any]] = []
            fivebelow_stores: List[Dict[str, Any]] = []
            scanned_store_keys = set()

            if walgreens_products:
                self._set_progress(
                    current_phase="locating_stores",
                    progress_message=f"Finding Walgreens stores near {self.current_zipcode}...",
                    current_store="Store locator",
                    progress_completed_units=0.0,
                    progress_total_units=progress_total_units,
                )
                walgreens_stores = self.walgreens_checker._fetch_stores_near_zip(self.current_zipcode)
                if not walgreens_stores:
                    self._set_progress(
                        current_phase="error",
                        progress_message=f"No Walgreens stores found near {self.current_zipcode}",
                        current_store="Store locator",
                        progress_completed_units=progress_total_units,
                    )
                    return

                scanned_store_keys.update(
                    f"walgreens:{store.get('storeNumber')}"
                    for store in walgreens_stores
                    if store.get("storeNumber")
                )
                self.total_stores = len(walgreens_stores)
                self._set_progress(
                    current_phase="stores_loaded",
                    progress_message=f"Found {len(walgreens_stores)} Walgreens stores near {self.current_zipcode}",
                    current_store="Store list ready",
                    progress_completed_units=1.0,
                    progress_total_units=progress_total_units,
                )

            if fivebelow_products:
                self._set_progress(
                    current_phase="locating_stores",
                    progress_message=f"Finding Five Below stores near {self.current_zipcode}...",
                    current_store="Store locator",
                    progress_completed_units=1.0,
                    progress_total_units=progress_total_units,
                )
                fivebelow_stores = self.fivebelow_checker._fetch_stores_near_zip(self.current_zipcode)
                if not fivebelow_stores:
                    self._set_progress(
                        current_phase="error",
                        progress_message=f"No Five Below stores found near {self.current_zipcode}",
                        current_store="Store locator",
                        progress_completed_units=progress_total_units,
                    )
                    return

                scanned_store_keys.update(
                    f"fivebelow:{store.get('store_id')}"
                    for store in fivebelow_stores
                    if store.get("store_id")
                )
                self.total_stores = max(self.total_stores, len(fivebelow_stores))
                self._set_progress(
                    current_phase="stores_loaded",
                    progress_message=f"Found {len(fivebelow_stores)} Five Below stores near {self.current_zipcode}",
                    current_store="Store list ready",
                    progress_completed_units=1.0,
                    progress_total_units=progress_total_units,
                )

            if ace_products:
                self._set_progress(
                    current_phase="stores_loaded",
                    progress_message=f"Ace checks will use the live nearby-store tray near {self.current_zipcode}",
                    current_store="Ace browser flow ready",
                    progress_completed_units=1.0,
                    progress_total_units=progress_total_units,
                )

            self.walgreens_checker.custom_product_names = {
                product["article_id"]: product["name"] for product in walgreens_products
            }

            check_results: Dict[str, Dict[str, Any]] = {}
            for index, product in enumerate(product_specs, start=1):
                retailer = product.get("retailer", "walgreens")
                article_id = str(product["article_id"])
                product_key = str(product.get("key") or self._tracked_product_key(article_id, retailer))
                product_display_name = product.get("name") or article_id

                logger.info("\n[%s/%s][%s] %s", index, len(product_specs), retailer, product_display_name)
                logger.info("-" * 50)

                if retailer == "walgreens":
                    product_result = self.walgreens_checker.check_product_availability(
                        product,
                        walgreens_stores,
                        product_index=index,
                        product_total=len(product_specs),
                    )
                elif retailer == "cvs":
                    try:
                        product_result = self.cvs_checker.check_product_availability(
                            product,
                            self.current_zipcode,
                            product_index=index,
                            product_total=len(product_specs),
                        )
                        # Update product image if extracted from browser
                        extracted_image = product_result.get("_extracted_image_url", "")
                        if extracted_image and product.get("article_id"):
                            try:
                                self.db.update_product_image(
                                    self.user_id,
                                    product["article_id"],
                                    image_url=extracted_image,
                                    retailer="cvs",
                                )
                            except Exception as img_exc:
                                logger.warning(
                                    "Failed to update product image for %s: %s",
                                    product_display_name,
                                    img_exc,
                                )
                    except CvsDisabledError as exc:
                        logger.info(
                            "CVS inventory skipped for user %s product %s: %s",
                            self.user_id,
                            product_display_name,
                            exc,
                        )
                        product_result = {
                            "availability": {},
                            "stores": {},
                            "location_ids": [],
                        }
                    except CvsBlockedError as exc:
                        logger.warning(
                            "CVS inventory blocked for user %s product %s: %s",
                            self.user_id,
                            product_display_name,
                            exc,
                        )
                        product_result = {
                            "availability": {},
                            "stores": {},
                            "location_ids": [],
                        }
                elif retailer == "fivebelow":
                    product_result = self.fivebelow_checker.check_product_availability(
                        product,
                        fivebelow_stores,
                        product_index=index,
                        product_total=len(product_specs),
                    )
                elif retailer == "ace":
                    # Ace products are processed in batch after the loop to reuse browser session
                    continue
                else:
                    raise ValueError(f"Unsupported retailer: {retailer}")

                scanned_store_keys.update(
                    f"{retailer}:{store_id}"
                    for store_id in (product_result.get("location_ids") or [])
                    if store_id
                )
                self.total_stores = max(self.total_stores, len(product_result.get("location_ids") or []))

                check_results[product_key] = {
                    "id": article_id,
                    "key": product_key,
                    "retailer": retailer,
                    "name": product_display_name,
                    "image_url": product.get("image_url", ""),
                    "source_url": product.get("source_url", ""),
                    "availability": product_result["availability"],
                    "stores": product_result["stores"],
                }

            # Process Ace products in batch with shared browser session for performance
            if ace_products:
                try:
                    ace_positions = [
                        index for index, product in enumerate(product_specs, start=1)
                        if product.get("retailer", "walgreens") == "ace"
                    ]
                    ace_results = self.ace_checker.check_products_availability(
                        ace_products,
                        self.current_zipcode,
                        product_positions=ace_positions,
                        product_total=len(product_specs),
                    )
                    for ace_index, ace_product in enumerate(ace_products):
                        article_id = str(ace_product["article_id"])
                        product_key = str(ace_product.get("key") or self._tracked_product_key(article_id, "ace"))
                        product_display_name = ace_product.get("name") or article_id
                        product_result = ace_results[ace_index] if ace_index < len(ace_results) else {"availability": {}, "stores": {}, "location_ids": []}

                        # Update product image if extracted from browser
                        extracted_image = product_result.get("_extracted_image_url", "")
                        if extracted_image and ace_product.get("article_id"):
                            try:
                                self.db.update_product_image(
                                    self.user_id,
                                    ace_product["article_id"],
                                    image_url=extracted_image,
                                    retailer="ace",
                                )
                            except Exception as img_exc:
                                logger.warning(
                                    "Failed to update Ace product image for %s: %s",
                                    product_display_name,
                                    img_exc,
                                )

                        scanned_store_keys.update(
                            f"ace:{store_id}"
                            for store_id in (product_result.get("location_ids") or [])
                            if store_id
                        )
                        self.total_stores = max(self.total_stores, len(product_result.get("location_ids") or []))

                        check_results[product_key] = {
                            "id": article_id,
                            "key": product_key,
                            "retailer": "ace",
                            "name": product_display_name,
                            "image_url": ace_product.get("image_url", ""),
                            "source_url": ace_product.get("source_url", ""),
                            "availability": product_result["availability"],
                            "stores": product_result["stores"],
                        }
                except AceBrowserError as exc:
                    logger.warning(
                        "Ace inventory blocked or failed for user %s: %s",
                        self.user_id,
                        exc,
                    )
                    # Add empty results for all Ace products on total failure
                    for ace_product in ace_products:
                        article_id = str(ace_product["article_id"])
                        product_key = str(ace_product.get("key") or self._tracked_product_key(article_id, "ace"))
                        check_results[product_key] = {
                            "id": article_id,
                            "key": product_key,
                            "retailer": "ace",
                            "name": ace_product.get("name") or article_id,
                            "image_url": ace_product.get("image_url", ""),
                            "source_url": ace_product.get("source_url", ""),
                            "availability": {},
                            "stores": {},
                        }

            products_with_stock = self._extract_products_with_stock(check_results, self.tracked_products)

            timestamp = datetime.utcnow().isoformat()
            self._set_progress(
                current_phase="finalizing",
                progress_message="Saving check results...",
                current_store="Finalizing results",
                stores_checked=self.total_stores,
                stores_with_stock_current=0,
                progress_completed_units=max(progress_total_units - 1, 0),
                progress_total_units=progress_total_units,
            )
            self.db.add_check_result(
                self.user_id,
                total_stores_checked=len(scanned_store_keys),
                products_with_stock=products_with_stock,
                timestamp=timestamp,
            )

            self.last_check_time = datetime.fromisoformat(timestamp)
            self.last_products_with_stock = products_with_stock
            self.last_total_stores_checked = len(scanned_store_keys)

            if products_with_stock:
                # Filter out products excluded from Discord notifications
                discord_products = {
                    k: v for k, v in products_with_stock.items()
                    if not self.tracked_products.get(k, {}).get("exclude_from_discord", False)
                }

                discord_products = self._filter_products_for_discord(discord_products)

                if discord_products:
                    # Determine if we should ping based on settings and changes
                    products_changed = self._products_info_changed(discord_products)
                    should_mention = (
                        not self.discord_ping_on_change_only
                    ) or products_changed

                    self._set_progress(
                        current_phase="notifying",
                        progress_message=f"Sending Discord alerts for {len(discord_products)} product(s)...",
                        current_store="Discord notifications",
                        progress_completed_units=max(progress_total_units - 0.5, 0),
                        progress_total_units=progress_total_units,
                    )
                    self.notifier.notify_stock_found(
                        discord_products, self.current_zipcode, mention_roles=should_mention
                    )

                    # Track products that triggered a mention for future comparison
                    if should_mention:
                        self.last_notified_products = dict(discord_products)

            self._set_progress(
                current_phase="complete",
                progress_message="Stock check complete",
                current_store="Done",
                progress_completed_units=progress_total_units,
                progress_total_units=progress_total_units,
            )
        except Exception as exc:
            logger.error("Error during stock check for user %s: %s", self.user_id, exc, exc_info=True)
            self._set_progress(
                current_phase="error",
                progress_message=str(exc),
                current_store="Error",
                progress_completed_units=self.progress_total_units or 0.0,
            )
            self.notifier.notify_error(str(exc))
        finally:
            self.check_in_progress = False

    def start(self, *, run_immediately: bool = True) -> bool:
        with self.state_lock:
            if self.is_running:
                return True

            self.refresh_from_db()
            self.scheduler = BackgroundScheduler()
            self.scheduler.add_job(
                self._check_stock,
                "interval",
                minutes=self.check_interval_minutes,
                id=self._job_id,
                name=f"Retail Stock Check (user {self.user_id})",
                replace_existing=True,
            )
            self.scheduler.start()
            self.is_running = True
            self.db.update_user_settings(self.user_id, {"scheduler_enabled": True})

            if run_immediately:
                thread = threading.Thread(target=self._check_stock, daemon=True)
                thread.start()
            return True

    def stop(self) -> bool:
        with self.state_lock:
            if not self.is_running:
                return True

            if self.scheduler is not None:
                self.scheduler.shutdown(wait=False)
                self.scheduler = None
            self.is_running = False
            self.db.update_user_settings(self.user_id, {"scheduler_enabled": False})
            return True

    def manual_check(self) -> Dict[str, Any]:
        if self.check_in_progress:
            return {"success": False, "error": "Check already in progress"}

        thread = threading.Thread(target=self._check_stock, daemon=True)
        thread.start()
        return {"success": True, "message": "Check started in background"}

    def get_progress(self) -> Dict[str, Any]:
        if not self.check_in_progress:
            return {
                "in_progress": False,
                "phase": self.current_phase,
                "message": self.progress_message,
                "current_store": None,
                "current_product": None,
                "current_product_index": self.current_product_index,
                "total_products": self.total_products,
                "stores_checked": 0,
                "stores_processed": 0,
                "total_stores": 0,
                "stores_with_stock_current": self.stores_with_stock_current,
                "completed_units": self.progress_completed_units,
                "total_units": self.progress_total_units,
                "progress_percent": 0,
            }

        progress_percent = 0
        if self.progress_total_units > 0:
            progress_percent = round(
                min(100.0, (self.progress_completed_units / self.progress_total_units) * 100),
                1,
            )

        return {
            "in_progress": True,
            "phase": self.current_phase,
            "message": self.progress_message,
            "current_store": self.current_store,
            "current_product": self.current_product,
            "current_product_index": self.current_product_index,
            "total_products": self.total_products,
            "stores_checked": self.stores_checked,
            "stores_processed": self.stores_checked,
            "total_stores": self.total_stores,
            "stores_with_stock_current": self.stores_with_stock_current,
            "completed_units": self.progress_completed_units,
            "total_units": self.progress_total_units,
            "progress_percent": progress_percent,
        }

    def get_status(self) -> Dict[str, Any]:
        products_list = [
            {
                "key": product.get("key") or key,
                "id": product["article_id"],
                "retailer": product.get("retailer", "walgreens"),
                "name": product["name"],
                "planogram": product["planogram"],
                "image_url": product.get("image_url", ""),
                "source_url": product.get("source_url", ""),
                "product_id": product.get("product_id", ""),
                "exclude_from_discord": product.get("exclude_from_discord", False),
            }
            for key, product in self.tracked_products.items()
        ]
        next_run_time = None
        if self.scheduler is not None:
            job = self.scheduler.get_job(self._job_id)
            if job is not None and getattr(job, "next_run_time", None) is not None:
                next_run_time = job.next_run_time.isoformat()

        return {
            "is_running": self.is_running,
            "last_check": self.last_check_time.isoformat() if self.last_check_time else None,
            "next_run_time": next_run_time,
            "last_products_found": self.last_products_with_stock,
            "last_check_result": self.get_last_check_snapshot(),
            "check_interval_minutes": self.check_interval_minutes,
            "discord_configured": self.notifier.is_configured,
            "discord_webhook_count": len(self.notifier.webhook_urls),
            "discord_destinations": self.discord_destinations,
            "current_zipcode": self.current_zipcode,
            "max_notification_distance_miles": self.max_notification_distance_miles,
            "pokemon_background_enabled": self.pokemon_background_enabled,
            "pokemon_background_theme": self.pokemon_background_theme,
            "pokemon_background_tile_size": self.pokemon_background_tile_size,
            "map_provider": self.map_provider,
            "discord_ping_on_change_only": self.discord_ping_on_change_only,
            "tracked_products": products_list,
        }


class SchedulerManager:
    """Cache and resume per-user schedulers."""

    def __init__(self, db: StockDatabase):
        self.db = db
        self.schedulers: Dict[int, StockCheckScheduler] = {}
        self.lock = threading.RLock()

    def get_or_create(self, user_id: int) -> StockCheckScheduler:
        normalized_user_id = int(user_id)
        with self.lock:
            scheduler = self.schedulers.get(normalized_user_id)
            if scheduler is None:
                scheduler = StockCheckScheduler(normalized_user_id, self.db)
                self.schedulers[normalized_user_id] = scheduler
            else:
                scheduler.refresh_from_db()
            return scheduler

    def start_enabled_schedulers(self) -> None:
        for user in self.db.list_users_with_enabled_schedulers():
            scheduler = self.get_or_create(int(user["id"]))
            if not scheduler.is_running:
                scheduler.start(run_immediately=False)

    def refresh_all_from_db(self) -> None:
        with self.lock:
            for scheduler in self.schedulers.values():
                scheduler.refresh_from_db()
