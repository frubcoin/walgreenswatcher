"""Discord webhook notifications."""

from __future__ import annotations

import json
import logging
import os
from urllib.parse import quote, urlparse
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Union

import requests

from config import DISCORD_ROLE_ID, DISCORD_WEBHOOK_URL, FRONTEND_BASE_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Send notifications to Discord via webhook."""

    ALLOWED_WEBHOOK_HOSTS = {
        "discord.com",
        "www.discord.com",
        "canary.discord.com",
        "ptb.discord.com",
        "discordapp.com",
        "www.discordapp.com",
    }
    EMBED_TOTAL_LIMIT = 6000
    EMBED_DESCRIPTION_LIMIT = 4096
    EMBED_TITLE_LIMIT = 256
    EMBED_FIELD_VALUE_LIMIT = 1024
    MAX_EMBEDS_PER_MESSAGE = 10

    def __init__(self, webhook_url: Optional[Union[str, Sequence[str]]] = None, map_provider: str = "google"):
        self.destinations = self._normalize_destinations(webhook_url or DISCORD_WEBHOOK_URL)
        self.webhook_urls = [destination["url"] for destination in self.destinations]
        self.is_configured = bool(self.destinations)
        self.map_provider = map_provider if map_provider in ("google", "apple") else "google"
        self.brand_logo_filename = "frubgreens.webp"
        self.brand_logo_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", self.brand_logo_filename
        )

    @staticmethod
    def _mask_webhook_url(url: str) -> str:
        """Mask the sensitive token portion of a Discord webhook URL for logging."""
        normalized = str(url or "").strip()
        if not normalized:
            return ""
        try:
            parts = normalized.rsplit("/", 1)
            if len(parts) == 2 and len(parts[1]) > 8:
                return f"{parts[0]}/{'*' * (len(parts[1]) - 4)}{parts[1][-4:]}"
            return normalized
        except Exception:
            return normalized

    @staticmethod
    def _extract_role_id(value: Any) -> str:
        """Normalize a role mention or raw role id into Discord's numeric role id."""
        text = str(value or "").strip()
        if not text:
            return ""
        if text.startswith("<@&") and text.endswith(">"):
            text = text[3:-1]
        return "".join(ch for ch in text if ch.isdigit())

    @classmethod
    def _is_allowed_webhook_url(cls, value: Any) -> bool:
        """Allow only HTTPS Discord webhook endpoints."""
        normalized = str(value or "").strip()
        if not normalized:
            return False

        parsed = urlparse(normalized)
        host = str(parsed.netloc or "").lower()
        path = str(parsed.path or "")
        is_discord_host = parsed.scheme == "https" and host in cls.ALLOWED_WEBHOOK_HOSTS
        is_webhook_path = path.startswith("/api/webhooks/") or (
            path.startswith("/api/v") and "/webhooks/" in path
        )
        return bool(is_discord_host and is_webhook_path)

    @classmethod
    def _normalize_destinations(
        cls, webhook_value: Optional[Union[str, Sequence[Union[str, Dict[str, Any]]]]]
    ) -> List[Dict[str, str]]:
        """Normalize webhook configuration into a de-duplicated destination list."""
        if webhook_value is None:
            return []

        if isinstance(webhook_value, str):
            raw_values = webhook_value.replace(",", "\n").splitlines()
        else:
            raw_values = list(webhook_value)

        seen = set()
        normalized: List[Dict[str, str]] = []
        for value in raw_values:
            if isinstance(value, dict):
                url = str(value.get("url") or value.get("webhook_url") or "").strip()
                role_id = cls._extract_role_id(value.get("role_id") or value.get("role"))
            else:
                url = str(value).strip()
                role_id = ""

            if not url or url in seen:
                continue
            if not cls._is_allowed_webhook_url(url):
                logger.warning("Ignoring non-Discord webhook destination")
                continue
            seen.add(url)
            destination = {"url": url}
            if role_id:
                destination["role_id"] = role_id
            normalized.append(destination)
        return normalized

    def set_webhook_urls(self, webhook_value: Optional[Union[str, Sequence[Union[str, Dict[str, Any]]]]]) -> None:
        """Replace the configured webhook destinations."""
        self.destinations = self._normalize_destinations(webhook_value)
        self.webhook_urls = [destination["url"] for destination in self.destinations]
        self.is_configured = bool(self.destinations)

    def _brand_author(self) -> Dict[str, str]:
        """Build the standard branded embed author."""
        author = {"name": "Local Pick-up Monitor by frub"}
        if os.path.exists(self.brand_logo_path):
            author["icon_url"] = f"attachment://{self.brand_logo_filename}"
        return author

    def send_message(self, embed: Union[Dict, List[Dict]], content: str = "", mention_roles: bool = False) -> bool:
        """Send one or more embeds to Discord."""
        if not self.is_configured:
            logger.warning("Discord webhook URL not configured")
            return False

        try:
            embeds = embed if isinstance(embed, list) else [embed]
            payload_batches = self._chunk_embed_payloads(embeds)

            successes = 0
            failures = 0

            for destination in self.destinations:
                webhook_url = destination["url"]
                role_id = destination.get("role_id") or DISCORD_ROLE_ID

                for batch_index, embed_batch in enumerate(payload_batches, start=1):
                    payload_to_send = {"embeds": embed_batch}
                    destination_content = content

                    if mention_roles and batch_index == 1:
                        destination_content = f"<@&{role_id}>" if role_id else ""

                    if destination_content:
                        payload_to_send["content"] = destination_content

                    if os.path.exists(self.brand_logo_path):
                        with open(self.brand_logo_path, "rb") as logo_file:
                            response = requests.post(
                                webhook_url,
                                data={"payload_json": json.dumps(payload_to_send)},
                                files={
                                    "files[0]": (
                                        self.brand_logo_filename,
                                        logo_file,
                                        "image/webp",
                                    )
                                },
                                timeout=10,
                            )
                    else:
                        response = requests.post(webhook_url, json=payload_to_send, timeout=10)

                    if 200 <= response.status_code < 300:
                        successes += 1
                        continue

                    failures += 1
                    embed_lengths = [
                        self._embed_text_length(single_embed) for single_embed in payload_to_send.get("embeds", [])
                    ]
                    logger.error(
                        "Discord API returned status %s for %s (batch %s/%s, embed lengths=%s, total=%s): %s",
                        response.status_code,
                        self._mask_webhook_url(webhook_url),
                        batch_index,
                        len(payload_batches),
                        embed_lengths,
                        sum(embed_lengths),
                        response.text,
                    )

            if successes:
                logger.info(
                    "Discord notification sent successfully to %s webhook(s)%s",
                    successes,
                    f" with {failures} failure(s)" if failures else "",
                )
                return True

            return False
        except Exception as exc:
            logger.error("Failed to send Discord notification: %s", exc)
            return False

    @staticmethod
    def _distance_text(distance: object) -> str:
        """Format a store distance for display."""
        if isinstance(distance, (int, float)):
            return f"{distance:.2f} mi"
        return "Distance unavailable"

    def _directions_url(self, address: object) -> str:
        """Build a directions URL for a store address using the configured map provider."""
        normalized_address = str(address or "").strip()
        if not normalized_address:
            return ""
        if self.map_provider == "apple":
            return f"http://maps.apple.com/?daddr={quote(normalized_address)}"
        return f"https://www.google.com/maps/dir/?api=1&destination={quote(normalized_address)}"

    def _address_link(self, address: object, label: Optional[str] = None) -> str:
        """Render a Discord-friendly markdown link when an address is available."""
        normalized_address = str(address or "").strip()
        if not normalized_address:
            return "Address unavailable"

        directions_url = self._directions_url(normalized_address)
        safe_label = str(label or normalized_address).replace("[", "\\[").replace("]", "\\]")
        return f"[{safe_label}]({directions_url})" if directions_url else normalized_address

    def _store_line(self, store: Dict) -> str:
        """Format a single store entry for Discord."""
        address = store.get("address", "Address unavailable")
        inventory_count = store.get("inventory_count", 0)
        distance = store.get("distance")
        distance_text = self._distance_text(distance)
        change_indicator = store.get("change_indicator", "")
        
        inventory_known = store.get("inventory_count_known", True)
        if not inventory_known:
            availability_text = store.get("availability_text") or "In Stock"
            change_suffix = f" {change_indicator}" if change_indicator else ""
            return f"{self._address_link(address)}\n{availability_text}{change_suffix} | Distance: {distance_text}"

        change_suffix = f" {change_indicator}" if change_indicator else ""
        return f"{self._address_link(address)}\nQty: **{inventory_count}**{change_suffix} | Distance: {distance_text}"

    def _chunk_store_lines(self, stores: List[Dict], limit: int = 3000) -> List[str]:
        """Chunk formatted store lines so each embed fits Discord limits."""
        chunks: List[str] = []
        current_lines: List[str] = []
        current_len = 0

        for index, store in enumerate(stores, start=1):
            line = f"{index}. {self._store_line(store)}"
            projected = current_len + len(line) + (2 if current_lines else 0)
            if current_lines and projected > limit:
                chunks.append("\n\n".join(current_lines))
                current_lines = [line]
                current_len = len(line)
            else:
                current_lines.append(line)
                current_len = projected

        if current_lines:
            chunks.append("\n\n".join(current_lines))

        return chunks or ["No stores found"]

    @classmethod
    def _embed_text_length(cls, embed: Dict[str, Any]) -> int:
        """Estimate the Discord embed text budget usage."""
        total = len(str(embed.get("title", ""))) + len(str(embed.get("description", "")))

        author = embed.get("author") or {}
        total += len(str(author.get("name", "")))

        footer = embed.get("footer") or {}
        total += len(str(footer.get("text", "")))

        for field in embed.get("fields", []):
            total += len(str(field.get("name", "")))
            total += len(str(field.get("value", "")))

        return total

    @classmethod
    def _chunk_embed_payloads(cls, embeds: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Split embeds into individual messages so every page renders separately in Discord."""
        return [[embed] for embed in embeds]

    @classmethod
    def _store_chunk_limit(
        cls,
        base_embed: Dict[str, Any],
        description_prefix: str,
        configured_zip: str,
        store_count_hint: int,
    ) -> int:
        """Compute a safe per-page character budget for store detail lines."""
        page_digits = len(str(max(store_count_hint, 1)))
        footer_reserve = len(
            f"Page {'9' * page_digits} of {'9' * page_digits} | ZIP {configured_zip}"
        )

        description_room = cls.EMBED_DESCRIPTION_LIMIT - len(description_prefix)
        total_room = cls.EMBED_TOTAL_LIMIT - cls._embed_text_length(base_embed) - len(description_prefix) - footer_reserve
        return max(250, min(description_room, total_room))

    # Map retailer codes to display names and icon URLs
    RETAILER_INFO: Dict[str, Dict[str, str]] = {
        "walgreens": {"name": "Walgreens", "icon": "https://www.walgreens.com/favicon.ico"},
        "cvs": {"name": "CVS", "icon": f"{FRONTEND_BASE_URL}/icons/cvs.svg"},
        "fivebelow": {"name": "Five Below", "icon": f"{FRONTEND_BASE_URL}/icons/fivebelow.svg"},
        "ace": {"name": "Ace Hardware", "icon": "https://www.acehardware.com/favicon.ico"},
    }

    def _get_retailer_info(self, retailer_code: str) -> Dict[str, str]:
        """Get display name and icon for a retailer code."""
        return self.RETAILER_INFO.get(retailer_code.lower(), {"name": retailer_code.title(), "icon": ""})

    def _build_stock_embeds(self, products_with_stock: Dict, configured_zip: str, product_changes: Optional[Dict[str, Dict[str, str]]] = None) -> List[Dict[str, Any]]:
        """Build stock embeds while staying under Discord's embed size limits."""
        total_store_hits = sum(product.get("count", 0) for product in products_with_stock.values())
        total_inventory = sum(product.get("total_inventory", 0) for product in products_with_stock.values())
        embeds: List[Dict[str, Any]] = []
        total_products = len(products_with_stock)

        for product_index, (product_id, product_info) in enumerate(products_with_stock.items(), start=1):
            product_name = str(product_info["product_name"])[: self.EMBED_TITLE_LIMIT]
            retailer = product_info.get("retailer", "walgreens")
            retailer_info = self._get_retailer_info(retailer)
            image_url = str(product_info.get("image_url", "")).strip()
            source_url = str(product_info.get("source_url", "")).strip()
            store_count = product_info.get("count", 0)
            total_units = product_info.get("total_inventory", 0)
            stores = product_info.get("stores", [])

            # Get changes for this product
            product_change_map = product_changes.get(product_id, {}) if product_changes else {}
            nearest_store = self._nearest_store_text(stores)[: self.EMBED_FIELD_VALUE_LIMIT]
            description_prefix = (
                f"Near ZIP {configured_zip}\n"
                f"{store_count} stores in stock | {total_units} total units\n\n"
            )

            base_fields = [
                {"name": "Store Hits", "value": str(store_count), "inline": True},
                {"name": "Units", "value": str(total_units), "inline": True},
                {"name": "Nearest", "value": nearest_store, "inline": False},
            ]

            reserved_fields = [dict(field) for field in base_fields]
            if product_index == total_products:
                reserved_fields.insert(
                    0,
                    {
                        "name": "Check Summary",
                        "value": f"{total_inventory} total units across {total_store_hits} store hits\n🗺️ [Open map view]({FRONTEND_BASE_URL}/map)",
                        "inline": False,
                    },
                )

            # Build change summary for title if there are changes
            change_summary = ""
            if product_change_map:
                new_count = sum(1 for v in product_change_map.values() if v == "new")
                up_count = sum(1 for v in product_change_map.values() if v == "up")
                down_count = sum(1 for v in product_change_map.values() if v == "down")
                indicators = []
                if new_count:
                    indicators.append(f"New stores in-stock")
                if up_count:
                    indicators.append(f"Stock increased")
                if down_count:
                    indicators.append(f"Stock decreased")
                if indicators:
                    change_summary = f" ({', '.join(indicators)})"

            # Always include retailer name in the title
            title_prefix = f"[{retailer_info['name']}] " if retailer_info["name"] else ""
            base_embed: Dict[str, Any] = {
                "title": f"{title_prefix}{product_name}{change_summary}",
                "color": 3066993,
                "timestamp": datetime.utcnow().isoformat(),
                "author": self._brand_author(),
            }

            # Prepare stores with change indicators
            stores_with_indicators = []
            for store in stores:
                store_id = str(store.get("store_id") or "").strip()
                change_type = product_change_map.get(store_id, "same") if product_change_map else "same"
                store_copy = dict(store)
                if change_type == "new":
                    store_copy["change_indicator"] = "🆕"
                elif change_type == "up":
                    store_copy["change_indicator"] = "🔺"
                elif change_type == "down":
                    store_copy["change_indicator"] = "🔻"
                else:
                    store_copy["change_indicator"] = ""
                stores_with_indicators.append(store_copy)

            # Use product image as thumbnail if available, otherwise use retailer icon
            if image_url:
                base_embed["thumbnail"] = {"url": image_url}
            elif retailer_info["icon"]:
                base_embed["thumbnail"] = {"url": retailer_info["icon"]}

            if source_url:
                base_embed["url"] = source_url

            budget_embed = dict(base_embed)
            budget_embed["fields"] = reserved_fields

            chunk_limit = self._store_chunk_limit(
                base_embed=budget_embed,
                description_prefix=description_prefix,
                configured_zip=configured_zip,
                store_count_hint=max(len(stores), 1),
            )
            chunks = self._chunk_store_lines(stores_with_indicators, limit=chunk_limit)

            for chunk_index, chunk in enumerate(chunks, start=1):
                embed = dict(base_embed)
                embed["description"] = f"{description_prefix}{chunk}"

                if len(chunks) > 1:
                    embed["footer"] = {"text": f"Page {chunk_index} of {len(chunks)} | ZIP {configured_zip}"}
                else:
                    embed["footer"] = {"text": f"ZIP {configured_zip}"}

                is_last_product = product_index == total_products
                is_last_chunk = chunk_index == len(chunks)
                if is_last_chunk:
                    embed_fields = [dict(field) for field in base_fields]
                    if is_last_product:
                        embed_fields.insert(
                            0,
                            {
                                "name": "Check Summary",
                                "value": f"{total_inventory} total units across {total_store_hits} store hits\n🗺️ [Open map view]({FRONTEND_BASE_URL}/map)",
                                "inline": False,
                            },
                        )
                    embed["fields"] = embed_fields

                embeds.append(embed)

        return embeds

    def _nearest_store_text(self, stores: List[Dict]) -> str:
        """Summarize the nearest in-stock store."""
        if not stores:
            return "No stores found"

        nearest = min(
            stores,
            key=lambda store: (
                store.get("distance") is None,
                store.get("distance") if store.get("distance") is not None else float("inf"),
            ),
        )
        address = nearest.get("address", "Address unavailable")
        distance_text = self._distance_text(nearest.get("distance"))
        
        inventory_count = nearest.get("inventory_count", 0)
        inventory_known = nearest.get("inventory_count_known", True)
        
        if inventory_known and inventory_count > 0:
            return f"{self._address_link(address)} ({distance_text}) - **{inventory_count} units**"
            
        return f"{self._address_link(address)} ({distance_text})"

    def notify_stock_found(self, products_with_stock: Dict, configured_zip: str, mention_roles: bool = True, product_changes: Optional[Dict[str, Dict[str, str]]] = None) -> bool:
        """Notify when stock is found."""
        if not products_with_stock:
            return False

        embeds = self._build_stock_embeds(products_with_stock, configured_zip, product_changes=product_changes)
        return self.send_message(embeds, mention_roles=mention_roles)

    def notify_no_stock(self, total_stores_checked: int) -> bool:
        """Notify when check completes with no stock."""
        embed = {
            "title": "No Stock Found",
            "description": f"Checked {total_stores_checked} stores",
            "color": 15158332,
            "timestamp": datetime.utcnow().isoformat(),
            "author": self._brand_author(),
        }
        return self.send_message(embed, "")

    def notify_error(self, error_message: str) -> bool:
        """Notify on error."""
        embed = {
            "title": "Stock Check Error",
            "description": f"```\n{error_message[:1000]}\n```",
            "color": 15105570,
            "timestamp": datetime.utcnow().isoformat(),
            "author": self._brand_author(),
        }
        return self.send_message(embed, "")

    def notify_check_started(self) -> bool:
        """Notify that a check has started."""
        embed = {
            "title": "Stock Check Started",
            "description": "Scanning Walgreens stores for stock...",
            "color": 3447003,
            "timestamp": datetime.utcnow().isoformat(),
            "author": self._brand_author(),
        }
        return self.send_message(embed, "")
