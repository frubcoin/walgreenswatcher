"""Resolve Ace Hardware product links into inventory metadata."""

from __future__ import annotations

from typing import Dict
from urllib.parse import urlparse

from ace import AceBrowserClient


class AceProductResolver:
    """Resolve Ace product URLs into normalized product metadata."""

    @classmethod
    def resolve_product_link(cls, product_link: str) -> Dict[str, str]:
        normalized = AceBrowserClient.normalize_url(product_link)
        if not normalized:
            raise ValueError("Ace Hardware product URL required")

        hostname = (urlparse(normalized).hostname or "").lower()
        if hostname != "www.acehardware.com" and hostname != "acehardware.com":
            raise ValueError("Ace Hardware product links must point to acehardware.com")

        context = AceBrowserClient.fetch_product_context(normalized)
        product = dict(context.get("product") or {})
        product_id = str(product.get("product_id") or AceBrowserClient.extract_product_id(normalized)).strip()
        name = str(product.get("name") or "").strip()
        image_url = str(product.get("image_url") or "").strip()
        canonical_url = str(product.get("canonical_url") or AceBrowserClient.canonical_product_url(normalized)).strip()

        if not product_id or not name:
            raise ValueError("Ace product metadata was incomplete for this link")

        return {
            "retailer": "ace",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": name,
            "image_url": image_url,
            "canonical_url": canonical_url or normalized,
        }
