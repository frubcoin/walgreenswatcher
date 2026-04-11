"""Resolve Walgreens product links into inventory metadata."""

from __future__ import annotations

import random
import re
from typing import Any, Dict
from urllib.parse import urlparse

import requests

from config import USER_AGENTS

PRODUCT_ID_PATTERN = re.compile(r"ID=([A-Za-z0-9]+)-product", re.IGNORECASE)


class WalgreensProductResolver:
    """Resolve Walgreens PDP URLs into article and planogram identifiers."""

    PRODUCT_API_URL = "https://www.walgreens.com/productapi/v1/products"

    @staticmethod
    def _normalize_url(url: str) -> str:
        """Normalize Walgreens asset URLs into absolute HTTPS URLs."""
        normalized = str(url or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            return f"https:{normalized}"
        if normalized.startswith("/"):
            return f"https://www.walgreens.com{normalized}"
        return normalized

    @classmethod
    def extract_product_id(cls, product_link: str) -> str:
        """Extract the Walgreens PDP productId from a product URL."""
        match = PRODUCT_ID_PATTERN.search(product_link or "")
        if not match:
            raise ValueError("Could not find a Walgreens product ID in the link")
        return match.group(1)

    @staticmethod
    def _request_headers() -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://www.walgreens.com/",
        }

    @classmethod
    def resolve_product_link(cls, product_link: str) -> Dict[str, str]:
        """Resolve a Walgreens product link into inventory metadata."""
        product_link = str(product_link or "").strip()
        if not product_link:
            raise ValueError("Walgreens product URL required")

        hostname = (urlparse(product_link).hostname or "").lower()
        if hostname != "walgreens.com" and not hostname.endswith(".walgreens.com"):
            raise ValueError("Walgreens product links must point to walgreens.com")

        product_id = cls.extract_product_id(product_link)

        response = requests.get(
            cls.PRODUCT_API_URL,
            params={"productId": product_id},
            headers=cls._request_headers(),
            timeout=(8, 20),
        )
        response.raise_for_status()
        data: Dict[str, Any] = response.json()

        product_info = data.get("productInfo") or {}
        prod_details = data.get("prodDetails") or {}

        article_id = str(prod_details.get("articleId", "")).strip()
        planogram = str(prod_details.get("pln", "")).strip()
        name = (
            str(product_info.get("title", "")).strip()
            or str(product_info.get("displayName", "")).strip()
        )
        canonical_url = cls._normalize_url(prod_details.get("canonicalUrl", ""))
        image_url = cls._normalize_url(
            product_info.get("productImageUrl")
            or product_info.get("zoomImageUrl")
            or product_info.get("metaImage")
        )

        if not article_id or not planogram or not name:
            raise ValueError("Walgreens product metadata was incomplete for this link")

        return {
            "product_id": product_id,
            "article_id": article_id,
            "planogram": planogram,
            "name": name,
            "image_url": image_url,
            "canonical_url": canonical_url or product_link,
        }
