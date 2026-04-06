"""Scheduler for periodic stock checks."""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from apscheduler.schedulers.background import BackgroundScheduler

from config import (
    APP_SETTINGS_FILE,
    DEFAULT_CHECK_INTERVAL_MINUTES,
    DEFAULT_TRACKED_PRODUCTS,
    TARGET_ZIP_CODE,
)
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
    """Manages scheduled stock checks."""

    def __init__(self, webhook_url: str = ""):
        self.scheduler = BackgroundScheduler()
        self.checker = WalgreensStockChecker()
        self.db = StockDatabase()
        self.is_running = False
        self.last_check_time = None
        self.last_products_with_stock: Dict[str, Dict[str, Any]] = {}
        self.current_zipcode = TARGET_ZIP_CODE
        self.settings_file = APP_SETTINGS_FILE
        self.check_interval_minutes = DEFAULT_CHECK_INTERVAL_MINUTES
        self.discord_destinations: List[Dict[str, str]] = []
        self.pokemon_background_enabled = DEFAULT_POKEMON_BACKGROUND_ENABLED
        self.pokemon_background_theme = DEFAULT_POKEMON_BACKGROUND_THEME
        self.pokemon_background_tile_size = DEFAULT_POKEMON_BACKGROUND_TILE_SIZE

        self.checker.current_zip_code = self.current_zipcode

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

        self.products_data_file = os.path.join(
            os.path.dirname(__file__), "..", "data", "tracked_products.json"
        )
        os.makedirs(os.path.dirname(self.products_data_file), exist_ok=True)
        self._load_settings()
        self.checker.current_zip_code = self.current_zipcode
        initial_webhook_config = webhook_url if webhook_url else (self.discord_destinations or None)
        self.notifier = DiscordNotifier(initial_webhook_config)
        self.discord_destinations = list(self.notifier.destinations)
        self.tracked_products = self._load_products()

        if not self.tracked_products:
            self.tracked_products = self._normalize_products(DEFAULT_TRACKED_PRODUCTS)
            self._save_products()

    def _normalize_products(self, raw_products: Any) -> Dict[str, Dict[str, str]]:
        """Normalize product storage to article_id -> tracked metadata."""
        normalized: Dict[str, Dict[str, str]] = {}

        if not isinstance(raw_products, dict):
            return normalized

        for article_id, value in raw_products.items():
            article_id = str(article_id).strip()
            if not article_id:
                continue

            if isinstance(value, str):
                logger.warning(
                    "Skipping legacy product %s because it has no planogram metadata",
                    article_id,
                )
                continue

            if not isinstance(value, dict):
                logger.warning("Skipping product %s with unsupported format", article_id)
                continue

            name = str(value.get("name", "")).strip()
            planogram = str(value.get("planogram", "")).strip()
            if not name or not planogram:
                logger.warning(
                    "Skipping product %s because name or planogram is missing",
                    article_id,
                )
                continue

            normalized_product = {"name": name, "planogram": planogram}

            image_url = str(value.get("image_url", "")).strip()
            source_url = str(value.get("source_url", "")).strip()
            product_id = str(value.get("product_id", "")).strip()
            if image_url:
                normalized_product["image_url"] = image_url
            if source_url:
                normalized_product["source_url"] = source_url
            if product_id:
                normalized_product["product_id"] = product_id

            normalized[article_id] = normalized_product

        return normalized

    def _load_products(self) -> Dict[str, Dict[str, str]]:
        """Load tracked products from file."""
        try:
            if os.path.exists(self.products_data_file):
                with open(self.products_data_file, "r", encoding="utf-8") as handle:
                    return self._normalize_products(json.load(handle))
        except Exception as exc:
            logger.error("Error loading products: %s", exc)
        return {}

    def _load_settings(self) -> None:
        """Load persisted app settings."""
        try:
            if not os.path.exists(self.settings_file):
                return

            with open(self.settings_file, "r", encoding="utf-8") as handle:
                data = json.load(handle)

            interval_minutes = data.get("check_interval_minutes")
            if interval_minutes is not None:
                self.check_interval_minutes = self._validate_interval_minutes(interval_minutes)

            configured_zipcode = data.get("current_zipcode")
            if configured_zipcode is not None:
                self.current_zipcode = str(configured_zipcode).strip() or TARGET_ZIP_CODE

            destinations = data.get("discord_destinations")
            if isinstance(destinations, list):
                self.discord_destinations = [
                    destination
                    for destination in DiscordNotifier._normalize_destinations(destinations)
                ]

            background_enabled = data.get("pokemon_background_enabled")
            if background_enabled is not None:
                self.pokemon_background_enabled = self._validate_boolean_setting(
                    background_enabled,
                    "Pokemon background setting",
                )

            background_theme = data.get("pokemon_background_theme")
            if background_theme is not None:
                self.pokemon_background_theme = self._validate_pokemon_background_theme(
                    background_theme
                )

            background_tile_size = data.get("pokemon_background_tile_size")
            if background_tile_size is not None:
                self.pokemon_background_tile_size = self._validate_pokemon_background_tile_size(
                    background_tile_size
                )
        except Exception as exc:
            logger.error("Error loading app settings: %s", exc)

    def _save_products(self) -> None:
        """Save tracked products to file."""
        try:
            os.makedirs(os.path.dirname(self.products_data_file), exist_ok=True)
            with open(self.products_data_file, "w", encoding="utf-8") as handle:
                json.dump(self.tracked_products, handle, indent=2, ensure_ascii=False)
        except Exception as exc:
            logger.error("Error saving products: %s", exc)

    def _save_settings(self) -> None:
        """Persist app settings to disk."""
        try:
            os.makedirs(os.path.dirname(self.settings_file), exist_ok=True)
            with open(self.settings_file, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "check_interval_minutes": self.check_interval_minutes,
                        "current_zipcode": self.current_zipcode,
                        "discord_destinations": self.discord_destinations,
                        "pokemon_background_enabled": self.pokemon_background_enabled,
                        "pokemon_background_theme": self.pokemon_background_theme,
                        "pokemon_background_tile_size": self.pokemon_background_tile_size,
                    },
                    handle,
                    indent=2,
                    ensure_ascii=False,
                )
        except Exception as exc:
            logger.error("Error saving app settings: %s", exc)

    @staticmethod
    def _validate_interval_minutes(interval_minutes: Any) -> int:
        """Validate and normalize a scheduler interval in minutes."""
        try:
            value = int(interval_minutes)
        except (TypeError, ValueError) as exc:
            raise ValueError("Schedule interval must be a whole number of minutes") from exc

        if value < 1:
            raise ValueError("Schedule interval must be at least 1 minute")
        if value > 1440:
            raise ValueError("Schedule interval must be 1440 minutes or less")
        return value

    @staticmethod
    def _validate_boolean_setting(value: Any, label: str) -> bool:
        """Normalize a persisted boolean-like setting."""
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
        """Validate a Pokemon background theme key."""
        normalized = str(theme or "").strip()
        if normalized in ALLOWED_POKEMON_BACKGROUND_THEMES:
            return normalized
        raise ValueError("Pokemon background theme is not supported")

    @staticmethod
    def _validate_pokemon_background_tile_size(tile_size: Any) -> int:
        """Validate the Pokemon background tile size in pixels."""
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

    def add_product(
        self,
        article_id: str,
        product_name: str,
        planogram: str,
        image_url: str = "",
        source_url: str = "",
        product_id: str = "",
    ) -> bool:
        """Add a new product to track."""
        article_id = article_id.strip()
        product_name = product_name.strip()
        planogram = planogram.strip()
        image_url = image_url.strip()
        source_url = source_url.strip()
        product_id = product_id.strip()

        if article_id in self.tracked_products:
            logger.warning("Product %s already tracked", article_id)
            return False

        tracked_product = {"name": product_name, "planogram": planogram}
        if image_url:
            tracked_product["image_url"] = image_url
        if source_url:
            tracked_product["source_url"] = source_url
        if product_id:
            tracked_product["product_id"] = product_id

        self.tracked_products[article_id] = tracked_product
        self._save_products()
        logger.info("Added product: %s (%s / %s)", product_name, article_id, planogram)
        return True

    def remove_product(self, product_id: str) -> bool:
        """Remove a product from tracking."""
        if product_id not in self.tracked_products:
            logger.warning("Product %s not found", product_id)
            return False

        name = self.tracked_products.pop(product_id)["name"]
        self._save_products()
        logger.info("Removed product: %s (%s)", name, product_id)
        return True

    def update_product_name(self, product_id: str, product_name: str) -> bool:
        """Update the display name for a tracked product."""
        product_id = str(product_id).strip()
        product_name = str(product_name).strip()

        if not product_id or product_id not in self.tracked_products:
            logger.warning("Product %s not found for rename", product_id)
            return False

        if not product_name:
            raise ValueError("Product name cannot be empty")

        self.tracked_products[product_id]["name"] = product_name
        self._save_products()
        logger.info("Renamed product %s to %s", product_id, product_name)
        return True

    def set_zipcode(self, zipcode: str) -> None:
        """Update the search ZIP code."""
        normalized_zipcode = str(zipcode).strip() or TARGET_ZIP_CODE
        self.current_zipcode = normalized_zipcode
        self.checker.current_zip_code = normalized_zipcode
        self._save_settings()
        logger.info("ZIP code updated to %s", normalized_zipcode)

    def set_check_interval_minutes(self, interval_minutes: Any) -> int:
        """Update the recurring scheduler interval and reschedule if needed."""
        validated = self._validate_interval_minutes(interval_minutes)
        self.check_interval_minutes = validated
        self._save_settings()

        if self.is_running:
            self.scheduler.reschedule_job(
                "stock_check",
                trigger="interval",
                minutes=self.check_interval_minutes,
            )
            logger.info(
                "Scheduler interval updated while running: every %s minute(s)",
                self.check_interval_minutes,
            )
        else:
            logger.info(
                "Scheduler interval updated: every %s minute(s)",
                self.check_interval_minutes,
            )

        return self.check_interval_minutes

    def set_discord_destinations(
        self, destinations: Optional[Union[str, List[Union[str, Dict[str, Any]]]]]
    ) -> List[Dict[str, str]]:
        """Replace configured Discord webhook destinations and persist them."""
        self.notifier.set_webhook_urls(destinations)
        self.discord_destinations = list(self.notifier.destinations)
        self._save_settings()
        logger.info("Discord destinations updated: %s webhook(s)", len(self.discord_destinations))
        return self.discord_destinations

    def set_pokemon_background_enabled(self, enabled: Any) -> bool:
        """Update whether the themed background is enabled."""
        self.pokemon_background_enabled = self._validate_boolean_setting(
            enabled,
            "Pokemon background setting",
        )
        self._save_settings()
        return self.pokemon_background_enabled

    def set_pokemon_background_theme(self, theme: Any) -> str:
        """Update the selected Pokemon background theme."""
        self.pokemon_background_theme = self._validate_pokemon_background_theme(theme)
        self._save_settings()
        return self.pokemon_background_theme

    def set_pokemon_background_tile_size(self, tile_size: Any) -> int:
        """Update the selected Pokemon background tile size."""
        self.pokemon_background_tile_size = self._validate_pokemon_background_tile_size(tile_size)
        self._save_settings()
        return self.pokemon_background_tile_size

    def _product_specs(self) -> List[Dict[str, str]]:
        """Build normalized product specs for the scraper."""
        return [
            {
                "article_id": article_id,
                "name": product["name"],
                "planogram": product["planogram"],
                "image_url": product.get("image_url", ""),
                "source_url": product.get("source_url", ""),
                "product_id": product.get("product_id", ""),
            }
            for article_id, product in self.tracked_products.items()
        ]

    def _reset_progress(self) -> None:
        """Reset the live progress tracker to its idle state."""
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
        """Update one or more live progress fields."""
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _check_stock(self) -> None:
        """Internal method to check stock and notify."""
        try:
            self.check_in_progress = True
            self._reset_progress()

            logger.info("[%s] Running scheduled stock check...", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("Checking %s products...", len(self.tracked_products))

            product_specs = self._product_specs()
            progress_total_units = float(max(2, (len(product_specs) * 2) + 2))
            self._set_progress(
                current_phase="starting",
                progress_message="Preparing stock check...",
                total_products=len(product_specs),
                progress_total_units=progress_total_units,
            )

            if not product_specs:
                logger.warning("No tracked products configured")
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
                        "[%s] %s | %s | %s/%s stores processed",
                        current_phase,
                        current_product or "No product",
                        self.progress_message,
                        stores_processed,
                        total_stores,
                    )
                    last_logged["phase"] = current_phase
                    last_logged["product"] = current_product
                    last_logged["stores_processed"] = stores_processed

            self.checker.progress_callback = update_progress
            self.checker.current_zip_code = self.current_zipcode

            logger.info("Searching for stores near %s...", self.current_zipcode)
            self._set_progress(
                current_phase="locating_stores",
                progress_message=f"Finding Walgreens stores near {self.current_zipcode}...",
                current_store="Store locator",
                progress_completed_units=0.0,
                progress_total_units=progress_total_units,
            )
            stores = self.checker._fetch_stores_near_zip(self.current_zipcode)
            if not stores:
                logger.warning("No stores found near ZIP code")
                self._set_progress(
                    current_phase="complete",
                    progress_message=f"No Walgreens stores found near {self.current_zipcode}",
                    current_store="Store locator",
                    progress_completed_units=progress_total_units,
                )
                return

            self.total_stores = len(stores)
            self._set_progress(
                current_phase="stores_loaded",
                progress_message=f"Found {len(stores)} Walgreens stores near {self.current_zipcode}",
                current_store="Store list ready",
                progress_completed_units=1.0,
                progress_total_units=progress_total_units,
            )
            logger.info("Found %s stores. Starting checks...", self.total_stores)

            self.checker.custom_product_names = {
                product["article_id"]: product["name"] for product in product_specs
            }

            check_results = self.checker.check_products_at_stores(product_specs, stores)
            products_with_stock = self.checker.get_stores_with_stock(check_results)
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
                {
                    "total_stores_checked": len(stores),
                    "timestamp": datetime.now().isoformat(),
                },
                products_with_stock,
            )

            self.last_check_time = datetime.now()
            self.last_products_with_stock = products_with_stock

            if products_with_stock:
                logger.info("Stock found in %s product(s)", len(products_with_stock))
                self._set_progress(
                    current_phase="notifying",
                    progress_message=f"Sending Discord alerts for {len(products_with_stock)} product(s)...",
                    current_store="Discord notifications",
                    progress_completed_units=max(progress_total_units - 0.5, 0),
                    progress_total_units=progress_total_units,
                )
                self.notifier.notify_stock_found(products_with_stock, self.current_zipcode)
            else:
                logger.info("Check completed. No stock found.")

            self._set_progress(
                current_phase="complete",
                progress_message="Stock check complete",
                current_store="Done",
                progress_completed_units=progress_total_units,
                progress_total_units=progress_total_units,
            )
        except Exception as exc:
            logger.error("Error during scheduled check: %s", exc, exc_info=True)
            self._set_progress(
                current_phase="error",
                progress_message=str(exc),
                current_store="Error",
                progress_completed_units=self.progress_total_units or 0.0,
            )
            self.notifier.notify_error(str(exc))
        finally:
            self.check_in_progress = False

    def start(self) -> bool:
        """Start the scheduler."""
        try:
            if self.is_running:
                logger.warning("Scheduler is already running")
                return False

            self.scheduler.add_job(
                self._check_stock,
                "interval",
                minutes=self.check_interval_minutes,
                id="stock_check",
                name="Walgreens Stock Check",
                replace_existing=True,
            )

            self.scheduler.start()
            self.is_running = True
            logger.info(
                "Scheduler started. Will check every %s minute(s)",
                self.check_interval_minutes,
            )

            self._check_stock()
            return True
        except Exception as exc:
            logger.error("Error starting scheduler: %s", exc)
            return False

    def stop(self) -> bool:
        """Stop the scheduler."""
        try:
            if not self.is_running:
                logger.warning("Scheduler is not running")
                return False

            self.scheduler.shutdown()
            self.is_running = False
            logger.info("Scheduler stopped")
            return True
        except Exception as exc:
            logger.error("Error stopping scheduler: %s", exc)
            return False

    def manual_check(self) -> Dict[str, Any]:
        """Perform a manual stock check in a background thread."""
        if self.check_in_progress:
            return {"success": False, "error": "Check already in progress"}

        logger.info("Manual stock check requested - starting in background")
        thread = threading.Thread(target=self._check_stock, daemon=True)
        thread.start()
        return {"success": True, "message": "Check started in background"}

    def get_progress(self) -> Dict[str, Any]:
        """Get current check progress."""
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
        """Get scheduler status."""
        products_list = [
            {
                "id": article_id,
                "name": product["name"],
                "planogram": product["planogram"],
                "image_url": product.get("image_url", ""),
                "source_url": product.get("source_url", ""),
                "product_id": product.get("product_id", ""),
            }
            for article_id, product in self.tracked_products.items()
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
