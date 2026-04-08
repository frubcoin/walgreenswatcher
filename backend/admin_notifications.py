"""Admin alert webhook delivery."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlparse

import requests

from database import StockDatabase

logger = logging.getLogger(__name__)


class AdminAlertService:
    """Send admin activity alerts to configured webhooks."""

    def __init__(self, db: StockDatabase):
        self.db = db

    @staticmethod
    def normalize_destinations(
        webhook_value: Optional[Union[str, Sequence[Union[str, Dict[str, Any]]]]]
    ) -> List[Dict[str, str]]:
        if webhook_value is None:
            return []

        if isinstance(webhook_value, str):
            raw_values = webhook_value.replace(",", "\n").splitlines()
        else:
            raw_values = list(webhook_value)

        normalized: List[Dict[str, str]] = []
        seen = set()
        for value in raw_values:
            if isinstance(value, dict):
                url = str(value.get("url") or value.get("webhook_url") or "").strip()
            else:
                url = str(value or "").strip()

            if not url:
                continue

            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc or url in seen:
                continue

            seen.add(url)
            normalized.append({"url": url})
        return normalized

    @staticmethod
    def _is_discord_webhook(url: str) -> bool:
        parsed = urlparse(url)
        host = str(parsed.netloc or "").lower()
        return host in {"discord.com", "www.discord.com", "discordapp.com", "www.discordapp.com"}

    @staticmethod
    def _discord_embed_color(category: str) -> int:
        if category == "new_user":
            return 0x489CD4
        return 0xFB011C

    def _send_discord_alert(self, destination_url: str, event: Dict[str, Any]) -> bool:
        actor = event.get("actor") or {}
        target = event.get("target") or {}
        metadata = event.get("metadata") or {}

        lines = [
            f"Type: `{event.get('event_type', 'unknown')}`",
            f"Summary: {event.get('summary', 'No summary')}",
        ]
        if actor.get("email"):
            lines.append(f"Actor: {actor.get('name') or actor.get('email')} ({actor.get('email')})")
        if target.get("email"):
            lines.append(f"Target: {target.get('name') or target.get('email')} ({target.get('email')})")
        if metadata:
            preview = ", ".join(f"{key}={value}" for key, value in list(metadata.items())[:5])
            if preview:
                lines.append(f"Details: {preview}")

        payload = {
            "embeds": [
                {
                    "title": str(event.get("summary") or "Admin Alert")[:256],
                    "description": "\n".join(lines)[:4096],
                    "color": self._discord_embed_color(str(event.get("category") or "")),
                    "timestamp": event.get("created_at"),
                }
            ]
        }
        response = requests.post(destination_url, json=payload, timeout=10)
        return 200 <= response.status_code < 300

    @staticmethod
    def _send_json_alert(destination_url: str, event: Dict[str, Any]) -> bool:
        response = requests.post(destination_url, json=event, timeout=10)
        return 200 <= response.status_code < 300

    def notify(self, *, category: str, event: Dict[str, Any]) -> bool:
        settings = self.db.get_admin_settings()
        destinations = self.normalize_destinations(settings.get("admin_webhook_destinations"))
        if not destinations:
            return False

        if category == "new_user" and not settings.get("alert_new_users", True):
            return False
        if category == "user_action" and not settings.get("alert_user_actions", True):
            return False

        payload = {
            "category": category,
            "event_type": event.get("event_type"),
            "summary": event.get("summary"),
            "created_at": event.get("created_at"),
            "user_email": event.get("user_email"),
            "actor": {
                "id": event.get("actor_user_id"),
                "name": event.get("actor_name"),
                "email": event.get("actor_email"),
            },
            "target": {
                "id": event.get("target_user_id"),
                "name": event.get("target_name"),
                "email": event.get("target_email"),
            },
            "metadata": event.get("metadata") or {},
        }

        success = False
        for destination in destinations:
            url = destination["url"]
            try:
                if self._is_discord_webhook(url):
                    delivered = self._send_discord_alert(url, payload)
                else:
                    delivered = self._send_json_alert(url, payload)
                success = success or delivered
            except Exception as exc:
                logger.warning("Admin alert delivery failed for %s: %s", url, exc)
        return success
