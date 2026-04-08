"""Resolve CVS product links into inventory metadata."""

from __future__ import annotations

import random
import re
from html import unescape
from typing import Dict
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

from config import USER_AGENTS

CVS_PRODUCT_ID_PATTERN = re.compile(r"-prodid-(?P<product_id>\d+)(?:[/?#]|$)", re.IGNORECASE)
CVS_TITLE_SUFFIX_PATTERN = re.compile(r"\s*-\s*CVS Pharmacy\s*$", re.IGNORECASE)
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
            canonical_url = cls._normalize_url(
                (soup.find("link", rel="canonical") or {}).get("href", "")
            ) or product_link

            og_title = unescape((soup.find("meta", property="og:title") or {}).get("content", "")).strip()
            heading = soup.find(["h1", "title"])
            candidate_name = og_title or (heading.get_text(" ", strip=True) if heading else "")
            candidate_name = CVS_TITLE_SUFFIX_PATTERN.sub("", candidate_name).strip()
            if candidate_name:
                name = candidate_name

            image_url = cls._normalize_url(
                (soup.find("meta", property="og:image") or {}).get("content", "")
            )
            if not image_url:
                preload = soup.find("link", attrs={"rel": "preload", "as": "image"})
                if preload:
                    image_url = cls._normalize_url(preload.get("href", ""))

        return {
            "retailer": "cvs",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": name,
            "image_url": image_url,
            "canonical_url": canonical_url,
        }
