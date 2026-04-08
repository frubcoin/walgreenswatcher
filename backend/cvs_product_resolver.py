"""Resolve CVS product links into inventory metadata."""

from __future__ import annotations

import json
import random
import re
from html import unescape
from typing import Any, Dict
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

from config import USER_AGENTS

CVS_PRODUCT_ID_PATTERN = re.compile(r"-prodid-(?P<product_id>\d+)(?:[/?#]|$)", re.IGNORECASE)
CVS_TITLE_SUFFIX_PATTERN = re.compile(r"\s*-\s*CVS Pharmacy\s*$", re.IGNORECASE)
CVS_PRODUCT_IMAGE_PATH_PATTERN = re.compile(
    r"(?P<url>(?:https?:)?//www\.cvs\.com)?(?P<path>/bizcontent/merchandising/productimages/[^\"'<>\s]+?\.(?:jpe?g|png|webp)(?:\?[^\"'<>\s]*)?)",
    re.IGNORECASE,
)
CVS_PRODUCT_TIMEOUT = (8, 15)
CVS_BOOTSTRAP_TIMEOUT = (5, 8)
CVS_RESOLVE_ATTEMPTS = 2


class CvsProductResolver:
    """Resolve CVS product URLs into a product id plus lightweight metadata."""

    @staticmethod
    def _normalize_url(url: str) -> str:
        normalized = str(url or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            return f"https:{normalized}"
        if normalized.startswith("/"):
            return f"https://www.cvs.com{normalized}"
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
    def extract_product_id(cls, product_link: str) -> str:
        match = CVS_PRODUCT_ID_PATTERN.search(str(product_link or "").strip())
        if not match:
            raise ValueError("Could not find a CVS product ID in the link")
        return match.group("product_id")

    @staticmethod
    def _request_headers(*, accept: str, referer: str = "https://www.cvs.com/") -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": accept,
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": referer,
            "Origin": "https://www.cvs.com",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "close",
        }

    @classmethod
    def _fetch_html(cls, product_link: str) -> str:
        last_error: Exception | None = None
        accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"

        for _ in range(CVS_RESOLVE_ATTEMPTS):
            session = requests.Session()
            try:
                bootstrap_headers = cls._request_headers(accept=accept)
                for bootstrap_url in ("https://www.cvs.com/", product_link):
                    try:
                        response = session.get(
                            bootstrap_url,
                            headers=bootstrap_headers,
                            timeout=CVS_BOOTSTRAP_TIMEOUT
                            if bootstrap_url == "https://www.cvs.com/"
                            else CVS_PRODUCT_TIMEOUT,
                        )
                        response.raise_for_status()
                        if bootstrap_url == product_link and "html" in response.headers.get("content-type", "").lower():
                            return response.text
                    except requests.RequestException as exc:
                        last_error = exc
                        if bootstrap_url == product_link:
                            break
            finally:
                session.close()

        if last_error is not None:
            raise ValueError(
                "CVS blocked or timed out while resolving that product link. "
                "Try again from a network CVS accepts, or add the product again after confirming the URL."
            ) from last_error
        raise ValueError("CVS did not return an HTML product page for that link")

    @staticmethod
    def _slug_fallback_name(product_link: str) -> str:
        path = urlparse(str(product_link or "").strip()).path or ""
        slug = path.rsplit("/", 1)[-1]
        slug = re.sub(r"-prodid-\d+.*$", "", slug, flags=re.IGNORECASE)
        slug = unquote(slug).replace("-", " ").strip()
        slug = re.sub(r"\s+", " ", slug)
        if not slug:
            return "CVS product"
        return slug.title()

    @classmethod
    def _product_schema(cls, soup: BeautifulSoup) -> Dict[str, Any]:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            data = cls._load_json(script.string or script.get_text(" ", strip=True))
            candidates = data if isinstance(data, list) else [data]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                schema_type = str(candidate.get("@type", "")).strip().lower()
                if schema_type == "product" or candidate.get("image") or candidate.get("name"):
                    return candidate
        return {}

    @classmethod
    def _normalize_srcset_candidate(cls, value: str) -> str:
        candidate = str(value or "").split(",", 1)[0].strip().split(" ", 1)[0].strip()
        return cls._normalize_url(candidate)

    @classmethod
    def _extract_image_url(cls, soup: BeautifulSoup, html: str, product_schema: Dict[str, Any]) -> str:
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
            normalized = cls._first_non_empty(
                cls._normalize_url(preload.get("href", "")),
                cls._normalize_srcset_candidate(preload.get("imagesrcset") or preload.get("imageSrcSet") or ""),
            )
            if normalized:
                return normalized

        for image in soup.find_all("img"):
            for attr in ("src", "data-src", "data-lazy-src", "data-zoom-src", "data-image", "data-srcset"):
                raw_value = image.get(attr, "")
                normalized = cls._normalize_srcset_candidate(raw_value) if "srcset" in attr else cls._normalize_url(raw_value)
                if normalized and "/bizcontent/merchandising/productimages/" in normalized.lower():
                    return normalized

        normalized_html = unescape((html or "").replace("\\/", "/"))
        match = CVS_PRODUCT_IMAGE_PATH_PATTERN.search(normalized_html)
        if match:
            return cls._normalize_url(match.group("url") or match.group("path") or "")

        return ""

    @classmethod
    def resolve_product_link(cls, product_link: str) -> Dict[str, str]:
        product_link = str(product_link or "").strip()
        if not product_link:
            raise ValueError("CVS product URL required")

        parsed = urlparse(product_link)
        hostname = (parsed.hostname or "").lower()
        if hostname != "cvs.com" and not hostname.endswith(".cvs.com"):
            raise ValueError("CVS product links must point to cvs.com")

        product_id = cls.extract_product_id(product_link)
        canonical_url = product_link
        name = cls._slug_fallback_name(product_link)
        image_url = ""

        try:
            html = cls._fetch_html(product_link)
        except ValueError:
            html = ""

        if html:
            soup = BeautifulSoup(html, "lxml")
            product_schema = cls._product_schema(soup)
            canonical_url = cls._normalize_url(
                (soup.find("link", rel="canonical") or {}).get("href", "")
            ) or product_link

            og_title = unescape((soup.find("meta", property="og:title") or {}).get("content", "")).strip()
            heading = soup.find(["h1", "title"])
            candidate_name = cls._first_non_empty(
                product_schema.get("name"),
                og_title,
                heading.get_text(" ", strip=True) if heading else "",
            )
            candidate_name = CVS_TITLE_SUFFIX_PATTERN.sub("", candidate_name).strip()
            if candidate_name:
                name = candidate_name

            image_url = cls._first_non_empty(
                cls._normalize_url(
                    (soup.find("meta", property="og:image") or {}).get("content", "")
                ),
                cls._extract_image_url(soup, html, product_schema),
            )

        return {
            "retailer": "cvs",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": name,
            "image_url": image_url,
            "canonical_url": canonical_url,
        }
