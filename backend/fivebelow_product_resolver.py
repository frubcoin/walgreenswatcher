"""Resolve Five Below product links into inventory metadata."""

from __future__ import annotations

import json
import logging
import re
from html import unescape
from typing import Any, Dict
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - optional runtime dependency
    curl_requests = None

logger = logging.getLogger(__name__)

FIVEBELOW_PRODUCT_KEY_PATTERN = re.compile(r"/products/[^/?#]*-(?P<product_key>\d+)(?:[/?#]|$)", re.IGNORECASE)
FIVEBELOW_TITLE_SUFFIX_PATTERN = re.compile(r"\s*\|\s*Five Below\s*$", re.IGNORECASE)
FIVEBELOW_SKU_PATTERN = re.compile(r'"sku"\s*:\s*"(?P<sku>\d+)"', re.IGNORECASE)
FIVEBELOW_RESOLVE_ATTEMPTS = 2
FIVEBELOW_IMPERSONATION_CANDIDATES = ("chrome131", "firefox135", "safari17_0")


class FiveBelowProductResolver:
    """Resolve Five Below PDP URLs into inventory and product identifiers."""

    @staticmethod
    def _normalize_url(url: str) -> str:
        normalized = str(url or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            return f"https:{normalized}"
        if normalized.startswith("/"):
            return f"https://www.fivebelow.com{normalized}"
        return normalized

    @staticmethod
    def _first_non_empty(*values: Any) -> str:
        for value in values:
            normalized = str(value or "").strip()
            if normalized:
                return normalized
        return ""

    @staticmethod
    def _load_json(value: str) -> Any:
        try:
            return json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    @classmethod
    def extract_product_key(cls, product_link: str) -> str:
        match = FIVEBELOW_PRODUCT_KEY_PATTERN.search(str(product_link or "").strip())
        if not match:
            raise ValueError("Could not find a Five Below product key in the link")
        return str(match.group("product_key") or "").strip()

    @staticmethod
    def _request_headers(*, referer: str = "https://www.fivebelow.com/") -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": referer,
            "Upgrade-Insecure-Requests": "1",
            "Connection": "close",
        }

    @classmethod
    def _fetch_html(cls, product_link: str) -> str:
        last_error: Exception | None = None
        referer = "https://www.fivebelow.com/"

        if curl_requests is not None:
            for impersonation in FIVEBELOW_IMPERSONATION_CANDIDATES:
                for _ in range(FIVEBELOW_RESOLVE_ATTEMPTS):
                    try:
                        response = curl_requests.get(
                            product_link,
                            headers=cls._request_headers(referer=referer),
                            impersonate=impersonation,
                            timeout=30,
                        )
                        if response.status_code >= 400:
                            last_error = ValueError(
                                f"HTTP {response.status_code} while resolving Five Below product"
                            )
                            continue
                        if "html" in response.headers.get("content-type", "").lower():
                            return response.text
                    except Exception as exc:  # pragma: no cover - network variance
                        last_error = exc
                        logger.warning(
                            "Five Below resolver attempt failed with %s: %s",
                            impersonation,
                            exc,
                        )

        for _ in range(FIVEBELOW_RESOLVE_ATTEMPTS):
            try:
                response = requests.get(
                    product_link,
                    headers=cls._request_headers(referer=referer),
                    timeout=30,
                )
                if response.status_code >= 400:
                    last_error = ValueError(
                        f"HTTP {response.status_code} while resolving Five Below product"
                    )
                    continue
                if "html" in response.headers.get("content-type", "").lower():
                    return response.text
            except requests.RequestException as exc:
                last_error = exc

        if last_error is not None:
            raise ValueError(
                "Five Below blocked or timed out while resolving that product link. "
                "Please try again in a moment."
            ) from last_error
        raise ValueError("Five Below did not return an HTML product page for that link")

    @classmethod
    def _product_schema(cls, soup: BeautifulSoup) -> Dict[str, Any]:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            data = cls._load_json(script.string or script.get_text(" ", strip=True))
            candidates = data if isinstance(data, list) else [data]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                schema_type = str(candidate.get("@type", "")).strip().lower()
                if schema_type == "product" or candidate.get("sku") or candidate.get("name"):
                    return candidate
        return {}

    @classmethod
    def _next_data_product(cls, soup: BeautifulSoup) -> Dict[str, Any]:
        next_data_tag = soup.find("script", attrs={"id": "__NEXT_DATA__", "type": "application/json"})
        payload = cls._load_json(next_data_tag.string or next_data_tag.get_text(" ", strip=True) if next_data_tag else "")
        if not isinstance(payload, dict):
            return {}

        try:
            return (
                payload.get("props", {})
                .get("pageProps", {})
                .get("data", {})
                .get("data", {})
                .get("dataSources", {})
                .get("__master", {})
                .get("product", {})
            )
        except AttributeError:
            return {}

    @classmethod
    def resolve_product_link(cls, product_link: str) -> Dict[str, str]:
        product_link = str(product_link or "").strip()
        if not product_link:
            raise ValueError("Five Below product URL required")

        parsed = urlparse(product_link)
        hostname = (parsed.hostname or "").lower()
        if hostname != "fivebelow.com" and not hostname.endswith(".fivebelow.com"):
            raise ValueError("Five Below product links must point to fivebelow.com")

        fallback_product_key = cls.extract_product_key(product_link)
        html = cls._fetch_html(product_link)
        soup = BeautifulSoup(html, "lxml")
        product_schema = cls._product_schema(soup)
        next_product = cls._next_data_product(soup)
        variants = next_product.get("variants") or []
        primary_variant = variants[0] if variants and isinstance(variants[0], dict) else {}
        variant_attributes = primary_variant.get("attributes") or {}

        og_title = unescape((soup.find("meta", property="og:title") or {}).get("content", "")).strip()
        heading = soup.find(["h1", "title"])
        canonical_url = cls._normalize_url(
            (soup.find("link", rel="canonical") or {}).get("href", "")
        ) or cls._normalize_url(
            ((product_schema.get("offers") or {}) if isinstance(product_schema.get("offers"), dict) else {}).get("url", "")
        ) or product_link

        name = cls._first_non_empty(
            next_product.get("name"),
            product_schema.get("name"),
            og_title,
            heading.get_text(" ", strip=True) if heading else "",
        )
        name = FIVEBELOW_TITLE_SUFFIX_PATTERN.sub("", name).strip()

        image_candidates = primary_variant.get("images") or []
        schema_image = product_schema.get("image")
        if isinstance(schema_image, list):
            schema_image = schema_image[0] if schema_image else ""

        sku_match = FIVEBELOW_SKU_PATTERN.search(html)
        sku = cls._first_non_empty(
            primary_variant.get("sku"),
            product_schema.get("sku"),
            sku_match.group("sku") if sku_match else "",
        )
        product_key = cls._first_non_empty(
            next_product.get("key"),
            variant_attributes.get("styleNumber"),
            fallback_product_key,
        )
        image_url = cls._first_non_empty(
            cls._normalize_url(image_candidates[0] if image_candidates else ""),
            cls._normalize_url((soup.find("meta", property="og:image") or {}).get("content", "")),
            cls._normalize_url(schema_image),
        )

        if not sku or not name:
            raise ValueError("Five Below product metadata was incomplete for this link")

        return {
            "retailer": "fivebelow",
            "product_id": product_key or sku,
            "article_id": sku,
            "planogram": product_key or sku,
            "name": name,
            "image_url": image_url,
            "canonical_url": canonical_url,
        }
