"""Resolve Ace Hardware product links into inventory metadata."""

from __future__ import annotations

import logging
import re
from typing import Dict
from urllib.parse import unquote, urlparse

from ace import AceBrowserClient

logger = logging.getLogger(__name__)


class AceProductResolver:
    """Resolve Ace product URLs into normalized product metadata."""

    @staticmethod
    def _slug_fallback_name(product_link: str) -> str:
        path = urlparse(str(product_link or "").strip()).path or ""
        parts = [p for p in path.split("/") if p and not re.fullmatch(r"\d+", p) and p.lower() != "p"]
        if parts:
            slug = unquote(parts[-1]).replace("-", " ").strip()
            slug = re.sub(r"\s+", " ", slug)
            if slug:
                return slug.title()
        return "Ace Hardware Product"

    @classmethod
    def resolve_product_link(cls, product_link: str) -> Dict[str, str]:
        normalized = AceBrowserClient.normalize_url(product_link)
        if not normalized:
            raise ValueError("Ace Hardware product URL required")

        hostname = (urlparse(normalized).hostname or "").lower()
        if hostname != "www.acehardware.com" and hostname != "acehardware.com":
            raise ValueError("Ace Hardware product links must point to acehardware.com")

        product_id = AceBrowserClient.extract_product_id(normalized)
        if not product_id:
            raise ValueError("Ace Hardware product metadata was incomplete for this link")

        canonical_url = AceBrowserClient.canonical_product_url(normalized)

        # Keep link resolution instant. If the fast metadata path fails, we still
        # return usable product ids/name without dropping into a slow browser flow.
        try:
            instant_meta = AceBrowserClient.fetch_product_metadata_instant(normalized)
            return {
                "retailer": "ace",
                "product_id": product_id,
                "article_id": product_id,
                "planogram": product_id,
                "name": instant_meta.get("name") or cls._slug_fallback_name(normalized),
                "image_url": instant_meta.get("image_url") or "",
                "canonical_url": instant_meta.get("canonical_url") or canonical_url,
            }
        except Exception as exc:
            logger.warning("Ace instant metadata fetch failed during product resolve: %s", exc)

        return {
            "retailer": "ace",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": cls._slug_fallback_name(normalized),
            "image_url": "",
            "canonical_url": canonical_url or normalized,
        }
