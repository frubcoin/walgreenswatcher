"""Resolve Best Buy product links into inventory metadata."""

from __future__ import annotations

import json
import random
import re
from html import unescape
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from config import USER_AGENTS

SKU_PATH_PATTERN = re.compile(r"/(?P<sku>\d+)\.p(?:$|[?#])", re.IGNORECASE)
SKU_TEXT_PATTERN = re.compile(r"\bSKU:\s*(?P<sku>\d{5,})\b", re.IGNORECASE)
TITLE_SUFFIX_PATTERN = re.compile(r"\s*-\s*Best Buy\s*$", re.IGNORECASE)


class BestBuyProductResolver:
    """Resolve Best Buy PDP URLs into SKU metadata."""

    @staticmethod
    def _normalize_url(url: str) -> str:
        normalized = str(url or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            return f"https:{normalized}"
        if normalized.startswith("/"):
            return f"https://www.bestbuy.com{normalized}"
        return normalized

    @classmethod
    def extract_sku_from_url(cls, product_link: str) -> str:
        normalized_link = str(product_link or "").strip()
        if not normalized_link:
            return ""

        parsed = urlparse(normalized_link)
        query_sku = (parse_qs(parsed.query).get("skuId") or [""])[0].strip()
        if query_sku.isdigit():
            return query_sku

        match = SKU_PATH_PATTERN.search(parsed.path or "")
        if match:
            return match.group("sku")

        return ""

    @staticmethod
    def _request_headers(*, accept: str) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": accept,
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

    @classmethod
    def _fetch_html(cls, product_link: str) -> str:
        response = requests.get(
            product_link,
            headers=cls._request_headers(
                accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            timeout=30,
        )
        response.raise_for_status()
        return response.text

    @staticmethod
    def _load_json(value: str) -> Any:
        try:
            return json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    @classmethod
    def _analytics_metadata(cls, soup: BeautifulSoup) -> Dict[str, Any]:
        meta_tag = soup.find("meta", attrs={"name": "analytics-metadata"})
        content = unescape((meta_tag or {}).get("content", "")).strip()
        data = cls._load_json(content)
        return data if isinstance(data, dict) else {}

    @classmethod
    def _product_schema(cls, soup: BeautifulSoup) -> Dict[str, Any]:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            data = cls._load_json(script.string or script.get_text(" ", strip=True))
            candidates = data if isinstance(data, list) else [data]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                schema_type = str(candidate.get("@type", "")).strip().lower()
                if schema_type == "product" or candidate.get("sku"):
                    return candidate
        return {}

    @staticmethod
    def _first_non_empty(*values: Any) -> str:
        for value in values:
            normalized = str(value or "").strip()
            if normalized:
                return normalized
        return ""

    @classmethod
    def _extract_image_url(cls, soup: BeautifulSoup, product_schema: Dict[str, Any]) -> str:
        schema_image = product_schema.get("image")
        if isinstance(schema_image, list):
            for candidate in schema_image:
                normalized = cls._normalize_url(candidate)
                if normalized:
                    return normalized
        else:
            normalized = cls._normalize_url(schema_image)
            if normalized:
                return normalized

        preload = soup.find("link", attrs={"rel": "preload", "as": "image"})
        if preload:
            preload_href = cls._normalize_url(preload.get("href", ""))
            if preload_href:
                return preload_href

            srcset = preload.get("imagesrcset") or preload.get("imageSrcSet") or ""
            first_src = str(srcset).split(",", 1)[0].strip().split(" ", 1)[0].strip()
            normalized = cls._normalize_url(first_src)
            if normalized:
                return normalized

        for image in soup.find_all("img"):
            candidate = cls._normalize_url(image.get("src", ""))
            if candidate and "bbystatic.com" in candidate:
                return candidate

        return ""

    @classmethod
    def resolve_product_link(cls, product_link: str) -> Dict[str, str]:
        product_link = str(product_link or "").strip()
        if not product_link:
            raise ValueError("Best Buy product URL required")

        parsed = urlparse(product_link)
        hostname = (parsed.hostname or "").lower()
        if hostname != "bestbuy.com" and not hostname.endswith(".bestbuy.com"):
            raise ValueError("Best Buy product links must point to bestbuy.com")

        html = cls._fetch_html(product_link)
        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text("\n", strip=True)
        analytics_metadata = cls._analytics_metadata(soup)
        analytics_product = analytics_metadata.get("product") or {}
        product_schema = cls._product_schema(soup)

        sku = cls._first_non_empty(
            cls.extract_sku_from_url(product_link),
            analytics_product.get("skuId"),
            product_schema.get("sku"),
        )
        if not sku:
            sku_match = SKU_TEXT_PATTERN.search(page_text)
            sku = sku_match.group("sku") if sku_match else ""
        if not sku:
            raise ValueError("Could not find a Best Buy SKU in the product page")

        canonical = cls._normalize_url(
            (soup.find("link", rel="canonical") or {}).get("href", "")
        ) or product_link
        og_title = (soup.find("meta", property="og:title") or {}).get("content", "")
        heading = soup.find(["h1", "title"])
        name = cls._first_non_empty(
            product_schema.get("name"),
            og_title,
            heading.get_text(" ", strip=True) if heading else "",
        )
        name = TITLE_SUFFIX_PATTERN.sub("", name).strip()
        if not name:
            raise ValueError("Best Buy product name was missing from the page")

        image_url = cls._first_non_empty(
            cls._normalize_url((soup.find("meta", property="og:image") or {}).get("content", "")),
            cls._extract_image_url(soup, product_schema),
        )

        model = cls._first_non_empty(product_schema.get("model"))
        model_label = soup.find(string=re.compile(r"^\s*Model:\s*$", re.IGNORECASE))
        if not model and model_label:
            sibling = model_label.parent.find_next_sibling() if getattr(model_label, "parent", None) else None
            if sibling:
                model = sibling.get_text(" ", strip=True)
        if not model:
            model_match = re.search(r"\bModel:\s*(.+?)\s*(?:SKU:|Reviews|Availability|$)", page_text, re.IGNORECASE | re.DOTALL)
            if model_match:
                model = " ".join(model_match.group(1).split())

        parsed_canonical = urlparse(canonical)
        path_parts = [part for part in (parsed_canonical.path or "").split("/") if part]
        vanity_id = cls._first_non_empty(
            analytics_product.get("bsin"),
            path_parts[-1] if path_parts and not path_parts[-1].endswith(".p") else "",
        )

        return {
            "retailer": "bestbuy",
            "product_id": vanity_id or sku,
            "article_id": sku,
            "planogram": model or sku,
            "name": name,
            "image_url": image_url,
            "canonical_url": canonical,
        }
