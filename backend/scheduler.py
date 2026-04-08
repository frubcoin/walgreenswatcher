"""Per-user scheduler management for the hosted Walgreens watcher."""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler

from config import DEFAULT_CHECK_INTERVAL_MINUTES, DEFAULT_TRACKED_PRODUCTS, TARGET_ZIP_CODE

from cvs_scraper import CvsBlockedError, CvsDisabledError, CvsStockChecker
from database import StockDatabase
from discord_notifier import DiscordNotifier
from walgreens_scraper import WalgreensStockChecker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_POKEMON_BACKGROUND_ENABLED = False
DEFAULT_POKEMON_BACKGROUND_THEME = "gyra"
DEFAULT_POKEMON_BACKGROUND_TILE_SIZE = 645
MIN_POKEMON_BACKGROUND_TILE_SIZE = 200
MAX_POKEMON_BACKGROUND_TILE_SIZE = 1200
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

        self.notifier = DiscordNotifier([])
        self.is_running = False
        self.last_check_time: Optional[datetime] = None
        self.last_products_with_stock: Dict[str, Dict[str, Any]] = {}

        self.current_zipcode = TARGET_ZIP_CODE
        self.check_interval_minutes = DEFAULT_CHECK_INTERVAL_MINUTES
        self.discord_destinations: List[Dict[str, str]] = []
        self.pokemon_background_enabled = DEFAULT_POKEMON_BACKGROUND_ENABLED
        self.pokemon_background_theme = DEFAULT_POKEMON_BACKGROUND_THEME
        self.pokemon_background_tile_size = DEFAULT_POKEMON_BACKGROUND_TILE_SIZE
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

    def refresh_from_db(self) -> None:
        settings = self.db.get_user_settings(self.user_id)
        products = self.db.list_tracked_products(self.user_id)

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
        self.discord_destinations = list(settings["discord_destinations"])
        self.pokemon_background_enabled = settings["pokemon_background_enabled"]
        self.pokemon_background_theme = settings["pokemon_background_theme"]
        self.pokemon_background_tile_size = settings["pokemon_background_tile_size"]
        self.notifier = DiscordNotifier(self.discord_destinations or None)
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
            }
            for product in products
        }
        self.walgreens_checker.current_zip_code = self.current_zipcode
        self.cvs_checker.current_zip_code = self.current_zipcode


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

    @staticmethod
    def _extract_products_with_stock(check_results: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        products_with_stock: Dict[str, Dict[str, Any]] = {}

        for product_id, product_data in check_results.items():
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
            self.cvs_checker.progress_callback = update_progress
            self.cvs_checker.current_zip_code = self.current_zipcode


            walgreens_products = [
                product for product in product_specs if product.get("retailer", "walgreens") == "walgreens"
            ]
            walgreens_stores: List[Dict[str, Any]] = []
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

            products_with_stock = self._extract_products_with_stock(check_results)

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

            if products_with_stock:
                self._set_progress(
                    current_phase="notifying",
                    progress_message=f"Sending Discord alerts for {len(products_with_stock)} product(s)...",
                    current_store="Discord notifications",
                    progress_completed_units=max(progress_total_units - 0.5, 0),
                    progress_total_units=progress_total_units,
                )
                self.notifier.notify_stock_found(products_with_stock, self.current_zipcode)

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
        if self.is_running:
            return False

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
        if not self.is_running:
            return False

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
            }
            for key, product in self.tracked_products.items()
        ]

        return {
            "is_running": self.is_running,
            "last_check": self.last_check_time.isoformat() if self.last_check_time else None,
            "last_products_found": self.last_products_with_stock,
            "check_interval_minutes": self.check_interval_minutes,
            "discord_configured": self.notifier.is_configured,
            "discord_webhook_count": len(self.notifier.webhook_urls),
            "discord_destinations": self.discord_destinations,
            "current_zipcode": self.current_zipcode,
            "pokemon_background_enabled": self.pokemon_background_enabled,
            "pokemon_background_theme": self.pokemon_background_theme,
            "pokemon_background_tile_size": self.pokemon_background_tile_size,
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
