"""Resolve ALDI product links into inventory metadata."""

from __future__ import annotations

from typing import Dict
from urllib.parse import urlparse

from aldi import AldiGraphqlClient


class AldiProductResolver:
    """Resolve ALDI PDP URLs into product identifiers used by availability checks."""

    @classmethod
    def resolve_product_link(cls, product_link: str) -> Dict[str, str]:
        product_link = AldiGraphqlClient.normalize_product_url(product_link)
        if not product_link:
            raise ValueError("ALDI product URL required")

        parsed = urlparse(product_link)
        hostname = (parsed.hostname or "").lower()
        if hostname != "aldi.us" and not hostname.endswith(".aldi.us"):
            raise ValueError("ALDI product links must point to aldi.us")

        metadata = AldiGraphqlClient.extract_product_metadata(product_link)
        product_id = str(metadata.get("product_id") or "").strip()
        name = str(metadata.get("name") or "").strip()
        if not product_id or not name:
            raise ValueError("ALDI product metadata was incomplete for this link")

        return {
            "retailer": "aldi",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": name,
            "image_url": str(metadata.get("image_url") or "").strip(),
            "canonical_url": str(metadata.get("canonical_url") or product_link).strip(),
        }
