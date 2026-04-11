"""Resolve CVS product links into inventory metadata."""

from __future__ import annotations

import json
import logging
import random
import re
from html import unescape
from typing import Any, Dict, List
from urllib.parse import unquote, urlparse, urlsplit

import requests
from bs4 import BeautifulSoup
try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - optional runtime dependency
    curl_requests = None
from scrapling.engines._browsers._stealth import StealthySession

from config import CVS_PROXY_URLS, USER_AGENTS

logger = logging.getLogger(__name__)

CVS_PRODUCT_ID_PATTERN = re.compile(r"-prodid-(?P<product_id>\d+)(?:[/?#]|$)", re.IGNORECASE)
CVS_TITLE_SUFFIX_PATTERN = re.compile(r"\s*-\s*CVS Pharmacy\s*$", re.IGNORECASE)
CVS_PRODUCT_IMAGE_PATH_PATTERN = re.compile(
    r"(?P<url>(?:https?:)?//www\.cvs\.com)?(?P<path>/bizcontent/merchandising/productimages/(?:large|high_res)/[^\"'<>\s]+?\.(?:jpe?g|png|webp)(?:\?[^\"'<>\s]*)?)",
    re.IGNORECASE,
)
CVS_PRODUCT_TIMEOUT = (8, 15)
CVS_BOOTSTRAP_TIMEOUT = (5, 8)
CVS_RESOLVE_ATTEMPTS = 2
CURL_IMPERSONATION_TARGET = "chrome"


class CvsProductResolver:
    """Resolve CVS product URLs into a product id plus lightweight metadata."""
    _proxy_urls_override: List[str] = []

    @classmethod
    def set_proxy_urls_override(cls, raw_value: Any) -> List[str]:
        from cvs_scraper import CvsStockChecker
        cls._proxy_urls_override = CvsStockChecker.normalize_proxy_urls(raw_value)
        return list(cls._proxy_urls_override)

    @classmethod
    def _proxy_candidates(cls) -> List[str]:
        proxies = list(cls._proxy_urls_override) if cls._proxy_urls_override else list(CVS_PROXY_URLS)
        if not proxies:
            return []
        if len(proxies) <= 1:
            return proxies
        start = random.randrange(len(proxies))
        return proxies[start:] + proxies[:start]

    @staticmethod
    def _proxy_label(proxy_url: str) -> str:
        parsed = urlsplit(str(proxy_url or "").strip())
        if not parsed.hostname:
            return "configured proxy"
        if parsed.port:
            return f"{parsed.hostname}:{parsed.port}"
        return parsed.hostname

    @staticmethod
    def _new_session(proxy_url: str = "") -> requests.Session:
        if curl_requests is not None:
            session = curl_requests.Session(impersonate=CURL_IMPERSONATION_TARGET)
        else:
            session = requests.Session()
        if proxy_url:
            session.proxies.update({"http": proxy_url, "https": proxy_url})
        return session

    @staticmethod
    def _normalize_url(url: str) -> str:
        normalized = str(url or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            return f"https:{normalized}"
        if normalized.startswith("/"):
            return f"https://www.cvs.com{normalized}"
        parsed = urlparse(normalized)
        hostname = (parsed.hostname or "").lower()
        if (
            hostname in {"localhost", "127.0.0.1", "0.0.0.0"}
            and str(parsed.path or "").lower().startswith("/bizcontent/merchandising/productimages/")
        ):
            return f"https://www.cvs.com{parsed.path}"
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
        proxy_candidates = cls._proxy_candidates() or [None]
        last_error: Exception | None = None
        
        # Ensure direct fetch is also tried if proxies are provided
        if proxy_candidates and proxy_candidates[0] is not None:
             proxy_candidates.append(None)

        for proxy in proxy_candidates:
            proxy_label = cls._proxy_label(proxy) if proxy else "direct server IP"
            logger.info("CVS resolver attempting Scrapling fetch via %s", proxy_label)
            try:
                # Use Scrapling for robust Cloudflare/bot bypass
                with StealthySession(
                    proxy=proxy,
                    headless=True,
                    real_chrome=True,
                    solve_cloudflare=True,
                    timeout=30_000,
                ) as session:
                    # Some CVS pages like certain referers
                    response = session.fetch(product_link, referer="https://www.google.com/")
                    if response.status < 400:
                        return response.text
                    
                    last_error = ValueError(f"HTTP {response.status} via {proxy_label}")
                    logger.warning("CVS resolver blocked (HTTP %d) via %s", response.status, proxy_label)
            except Exception as exc:
                last_error = exc
                logger.warning("CVS resolver Scrapling fetch failed via %s: %s", proxy_label, exc)

        if last_error:
            raise last_error
        raise ValueError("CVS resolution failed on all available routes")

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
    def _extract_image_url(cls, soup: BeautifulSoup, html: str, product_schema: Dict[str, Any], product_link: str = "") -> str:
        # 1. Try product schema image first
        schema_image = product_schema.get("image")
        if isinstance(schema_image, list):
            for candidate in schema_image:
                normalized = cls._normalize_url(candidate)
                if normalized and "cvs.com" in normalized:
                    return normalized
        else:
            normalized = cls._normalize_url(schema_image)
            if normalized and "cvs.com" in normalized:
                return normalized

        # 2. Look for preload links
        for preload in soup.find_all("link", attrs={"rel": "preload", "as": "image"}):
            normalized = cls._first_non_empty(
                cls._normalize_url(preload.get("href", "")),
                cls._normalize_srcset_candidate(preload.get("imagesrcset") or preload.get("imageSrcSet") or ""),
            )
            if normalized and "/bizcontent/merchandising/productimages/" in normalized.lower():
                return normalized

        # 3. Look for meta og:image
        og_image = soup.find("meta", property="og:image")
        if og_image:
            normalized = cls._normalize_url(og_image.get("content", ""))
            if normalized and "/bizcontent/merchandising/productimages/" in normalized.lower():
                return normalized

        # 4. Look for high_res images in the page
        for image in soup.find_all("img"):
            for attr in ("src", "data-src", "data-lazy-src", "data-zoom-src", "data-image", "data-srcset"):
                raw_value = image.get(attr, "")
                normalized = cls._normalize_srcset_candidate(raw_value) if "srcset" in attr else cls._normalize_url(raw_value)
                if normalized and "/bizcontent/merchandising/productimages/high_res/" in normalized.lower():
                    return normalized

        # 5. Look for any productimages path
        for image in soup.find_all("img"):
            for attr in ("src", "data-src", "data-lazy-src", "data-zoom-src", "data-image", "data-srcset"):
                raw_value = image.get(attr, "")
                normalized = cls._normalize_srcset_candidate(raw_value) if "srcset" in attr else cls._normalize_url(raw_value)
                if normalized and "/bizcontent/merchandising/productimages/" in normalized.lower():
                    return normalized

        # 6. Search in raw HTML
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

        # Append cgaa Googlebot bypass hint if not present
        if "cgaa=" not in product_link:
            sep = "&" if "?" in product_link else "?"
            product_link = f"{product_link}{sep}cgaa=QWxsb3dHb29nbGVUb0FjY2Vzc0NWU1BhZ2Vz"

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
                cls._extract_image_url(soup, html, product_schema, product_link),
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
