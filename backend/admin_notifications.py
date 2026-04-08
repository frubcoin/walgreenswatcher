"""Admin alert webhook delivery."""

from __future__ import annotations

import logging
from datetime import datetime
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

    @staticmethod
    def _delivery_result(url: str, *, delivered: bool, status_code: int | None = None, error: str = "") -> Dict[str, Any]:
        return {
            "url": url,
            "delivered": bool(delivered),
            "status_code": status_code,
            "error": error,
        }

    @staticmethod
    def _post_json(destination_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = requests.post(destination_url, json=payload, timeout=10)
            delivered = 200 <= response.status_code < 300
            error = ""
            if not delivered:
                error = (response.text or "").strip()[:400]
            return AdminAlertService._delivery_result(
                destination_url,
                delivered=delivered,
                status_code=response.status_code,
                error=error,
            )
        except Exception as exc:
            return AdminAlertService._delivery_result(destination_url, delivered=False, error=str(exc))

    def _send_discord_alert(self, destination_url: str, event: Dict[str, Any]) -> Dict[str, Any]:
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
        return self._post_json(destination_url, payload)

    def _send_json_alert(self, destination_url: str, event: Dict[str, Any]) -> Dict[str, Any]:
        return self._post_json(destination_url, event)

    def _build_payload(self, *, category: str, event: Dict[str, Any]) -> Dict[str, Any]:
        return {
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

    def deliver_event(
        self,
        *,
        category: str,
        event: Dict[str, Any],
        respect_preferences: bool = True,
    ) -> Dict[str, Any]:
        settings = self.db.get_admin_settings()
        destinations = self.normalize_destinations(settings.get("admin_webhook_destinations"))
        result: Dict[str, Any] = {
            "category": category,
            "attempted": 0,
            "delivered": 0,
            "destinations": [],
            "skipped_reason": "",
        }
        if not destinations:
            result["skipped_reason"] = "No admin webhook destinations configured"
            return result

        if respect_preferences and category == "new_user" and not settings.get("alert_new_users", True):
            result["skipped_reason"] = "New user alerts are disabled"
            return result
        if respect_preferences and category == "user_action" and not settings.get("alert_user_actions", True):
            result["skipped_reason"] = "User action alerts are disabled"
            return result

        payload = self._build_payload(category=category, event=event)
        result["attempted"] = len(destinations)

        for destination in destinations:
            url = destination["url"]
            try:
                if self._is_discord_webhook(url):
                    delivery = self._send_discord_alert(url, payload)
                else:
                    delivery = self._send_json_alert(url, payload)
            except Exception as exc:
                logger.warning("Admin alert delivery failed for %s: %s", url, exc)
                delivery = self._delivery_result(url, delivered=False, error=str(exc))
            result["destinations"].append(delivery)

        result["delivered"] = sum(1 for item in result["destinations"] if item.get("delivered"))
        return result

    def notify(self, *, category: str, event: Dict[str, Any]) -> bool:
        result = self.deliver_event(category=category, event=event, respect_preferences=True)
        return bool(result.get("delivered"))

    def send_test_alert(self, *, actor_user: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        actor_user = actor_user or {}
        event = {
            "event_type": "admin.webhook_test",
            "summary": "Admin webhook test",
            "created_at": datetime.utcnow().isoformat(),
            "user_email": str(actor_user.get("email") or ""),
            "actor_user_id": actor_user.get("id"),
            "actor_name": actor_user.get("name"),
            "actor_email": actor_user.get("email"),
            "target_user_id": None,
            "target_name": "",
            "target_email": "",
            "metadata": {
                "source": "admin_panel",
                "note": "Manual webhook test triggered from the admin panel",
            },
        }
        return self.deliver_event(category="user_action", event=event, respect_preferences=False)
