"""Shared Ace Hardware browser helpers built around Scrapling."""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
import socket
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

from scrapling.engines._browsers._stealth import StealthySession

logger = logging.getLogger(__name__)

ACE_STORES_API_URL = "https://www.acehardware.com/api/commerce/storefront/locationUsageTypes/SP/locations"
ACE_INVENTORY_API_URL = "https://www.acehardware.com/getProductDetailInventory"
ACE_RADIUS_METERS = 48280.3  # 30 miles
ACE_PRODUCT_ID_PATTERN = re.compile(r"/(?P<product_id>[A-Z]*\d+)(?:[/?#]|$)", re.IGNORECASE)
ACE_DEBUG_DIR = Path(__file__).resolve().parent / "output" / "ace-debug"
ACE_BROWSER_TIMEOUT_MS = 30_000
ACE_BROWSER_WAIT_MS = 2_000
ACE_DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "X-Vol-Catalog": "1",
    "X-Vol-Currency": "USD",
    "X-Vol-Locale": "en-US",
    "X-Vol-Master-Catalog": "1",
    "X-Vol-Site": "37138",
    "X-Vol-Tenant": "24645",
}
ACE_HTML_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36"
    ),
}

ACE_SEC_HEADERS = {
    "sec-ch-ua": '"Chromium";v="141", "Not?A_Brand";v="8"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

_zip_geocode_cache: Dict[str, Dict[str, float]] = {}
_store_candidates_cache: Dict[str, List[Dict[str, Any]]] = {}


def _convert_proxy_format(proxy_url: str) -> str:
    """Handle both ip:port and ip:port:user:pass formats."""
    if not proxy_url:
        return proxy_url
    if "://" in proxy_url:
        return proxy_url
    parts = proxy_url.split(":")
    if len(parts) >= 4:
        host = parts[0]
        port = parts[1]
        username = parts[2]
        password = ":".join(parts[3:])
        return f"http://{username}:{password}@{host}:{port}"
    return f"http://{proxy_url}"


class AceBrowserError(ValueError):
    """Raised when the Ace browser flow fails."""


class AceBrowserClient:
    """Shared Ace PDP/session logic for resolver and inventory checks."""

    _proxy_urls_override: Optional[List[str]] = None
    _store_cache_db: Any = None

    @staticmethod
    def normalize_url(product_link: str) -> str:
        normalized = str(product_link or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            normalized = f"https:{normalized}"
        parsed = urlparse(normalized)
        if not parsed.scheme:
            normalized = f"https://{normalized.lstrip('/')}"
            parsed = urlparse(normalized)
        if (parsed.hostname or "").lower() == "acehardware.com":
            normalized = f"https://www.acehardware.com{parsed.path or ''}"
            if parsed.query and "isMainPDPRequested=true" in parsed.query:
                normalized = f"{normalized}?isMainPDPRequested=true"
        return normalized

    @staticmethod
    def canonical_product_url(product_link: str) -> str:
        normalized = AceBrowserClient.normalize_url(product_link)
        if not normalized:
            return ""
        parsed = urlparse(normalized)
        return f"https://www.acehardware.com{parsed.path or ''}"

    @staticmethod
    def product_url_with_main_pdp(product_link: str) -> str:
        canonical = AceBrowserClient.canonical_product_url(product_link)
        return f"{canonical}?isMainPDPRequested=true" if canonical else ""

    @staticmethod
    def extract_product_id(product_link: str) -> str:
        link = str(product_link or "").strip()
        parsed = urlparse(link)
        
        # 1. Check for variationProductCode in query string (priority)
        from urllib.parse import parse_qs
        qs = parse_qs(parsed.query)
        v_codes = qs.get("variationProductCode") or qs.get("variationproductcode")
        if v_codes and v_codes[0]:
            return str(v_codes[0]).strip()
            
        # 2. Extract from path using pattern
        match = ACE_PRODUCT_ID_PATTERN.search(link)
        if not match:
            # Fallback for simple /p/ID or path/ID if pattern search fails
            path_parts = parsed.path.strip("/").split("/")
            if path_parts:
                last_part = path_parts[-1]
                if any(c.isdigit() for c in last_part):
                    return last_part
            raise ValueError("Could not find an Ace Hardware product id in the link")
        return str(match.group("product_id") or "").strip()

    @classmethod
    def _try_direct_api(
        cls,
        product_url: str,
        zip_code: str,
        *,
        product_hints: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """High-speed inventory check using Ace's direct commerce APIs."""
        base_product = cls._product_metadata_from_hints(product_url, product_hints)
        product_id = cls._first_non_empty(
            base_product.get("product_id"),
            cls.extract_product_id(product_url),
        )
        geocoded = cls.geocode_zip(zip_code)
        lat, lng = geocoded["lat"], geocoded["lng"]

        proxies = cls.proxy_candidates()
        random.shuffle(proxies)
        # Prioritize direct server path, then proxies
        candidates = [None] + proxies
        cached_stores = cls._get_cached_store_candidates(zip_code)

        for proxy_url in candidates:
            proxy_label = cls.proxy_label(proxy_url)
            try:
                logger.info("Ace direct-API attempt via %s", proxy_label)
                stores = cls._clone_store_candidates(cached_stores) if cached_stores else cls._direct_api_fetch_stores(lat, lng, proxy_url)
                if not stores:
                    logger.warning("Ace direct-API: No stores nearby for %s", zip_code)
                    return None
                    
                if not cached_stores:
                    cls._set_cached_store_candidates(zip_code, stores)
                    cached_stores = cls._clone_store_candidates(stores)
                # Fetch exact inventory counts from all candidate stores concurrently
                # We use a lower worker count and add jitter to avoid bot detection
                stores_checked = 0
                stores_failed = 0
                
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = [
                        executor.submit(cls._direct_api_fetch_inventory_for_store, product_id, store, proxy_url)
                        for store in stores
                    ]
                    for future in futures:
                        try:
                            if not future.result():
                                stores_failed += 1
                            stores_checked += 1
                        except Exception:
                            stores_failed += 1
                            stores_checked += 1

                # If more than 50% of the stores failed (e.g. 403 Forbidden), 
                # we consider the whole direct-API attempt a failure to trigger fallback.
                if stores_checked > 0 and (stores_failed / stores_checked) > 0.5:
                    logger.warning("Ace direct-API: High failure rate (%d/%d), triggering fallback", stores_failed, stores_checked)
                    return None


                # Only count stores that have confirmed positive stock from the inventory API
                in_stock_stores = {
                    s["code"]: {
                        "store_id": s["code"],
                        "name": s.get("name") or f"Ace Hardware #{s['code']}",
                        "inventory_count": int(s.get("stock_count", 0)),
                        "inventory_count_known": True,
                        "availability_mode": "exact",
                        "pickup_available": True,
                        "delivery_available": any(
                            str(ft.get("code") or "").upper() == "DL" 
                            for ft in s.get("fulfillmentTypes", [])
                        ),
                        "availability_text": f"{s.get('stock_count', 0)} In Stock"
                    }
                    for s in stores
                    if int(s.get("stock_count", 0)) > 0
                }

                logger.info("Ace direct-API success via %s: found %d stores with stock", proxy_label, len(in_stock_stores))
                product_metadata = dict(base_product)
                try:
                    fetched_metadata = cls._fetch_product_metadata_via_api(product_url, proxy_url)
                    product_metadata.update({key: value for key, value in fetched_metadata.items() if value})
                except Exception as exc:
                    logger.info(
                        "Ace direct-API metadata enrichment failed via %s for %s: %s",
                        proxy_label,
                        product_id,
                        exc,
                    )
                return {
                    "product": product_metadata,
                    "store_candidates": stores,
                    "stores": in_stock_stores,
                    "proxy": proxy_label
                }

            except Exception as e:
                logger.warning("Ace direct-API attempt failed via %s: %s", proxy_label, e)
                continue

        return None

    @classmethod
    def _direct_api_fetch_stores(cls, lat: float, lng: float, proxy: Optional[str]) -> List[Dict[str, Any]]:
        filter_str = f"geo near({lat},{lng},{ACE_RADIUS_METERS})"
        params = {
            "filter": filter_str,
            "pageSize": "100",
            "responseFields": "items(code,name,address,geo,fulfillmentTypes,supportsInventory)"
        }
        headers = {
            **ACE_DEFAULT_HEADERS,
            **ACE_SEC_HEADERS,
            "User-Agent": ACE_HTML_HEADERS["User-Agent"],
            "Cookie": "ak_bmsc=1;",
            "Referer": "https://www.acehardware.com/store-locator"
        }
        
        try:
            # Use curl_cffi for impersonation to avoid 403s
            if curl_requests:
                session = curl_requests.Session(impersonate="chrome")
                if proxy:
                    p_url = _convert_proxy_format(proxy)
                    session.proxies = {"http": p_url, "https": p_url}
                response = session.get(ACE_STORES_API_URL, params=params, headers=headers, timeout=12)
                session.close()
            else:
                p_url = _convert_proxy_format(proxy) if proxy else None
                proxies = {"http": p_url, "https": p_url} if p_url else None
                response = requests.get(ACE_STORES_API_URL, params=params, headers=headers, proxies=proxies, timeout=12)
                
            response.raise_for_status()
            data = response.json()
            return data.get("items") or []
        except Exception as e:
            logger.debug("Ace direct-API store fetch failed: %s", e)
            raise

    @classmethod
    def _direct_api_fetch_inventory_for_store(cls, product_id: str, store: Dict[str, Any], proxy: Optional[str]) -> bool:
        store_code = store.get("code")
        if not store_code:
            return True

        # Add jitter to avoid rapid-fire bot detection
        time.sleep(random.uniform(0.12, 0.38))

        payload = {
            "productCode": product_id,
            "storeCode": store_code,
            "quantity": 1
        }
        
        # This API is more sensitive, so we use exact matching headers
        headers = {
            **ACE_DEFAULT_HEADERS,
            **ACE_SEC_HEADERS,
            "User-Agent": ACE_HTML_HEADERS["User-Agent"],
            "Content-Type": "application/json",
            "Origin": "https://www.acehardware.com",
            "Referer": f"https://www.acehardware.com/p/{product_id}",
            "Cookie": "ak_bmsc=1;"
        }
        
        try:
            # Use curl_cffi for impersonation to avoid 403s
            if curl_requests:
                session = curl_requests.Session(impersonate="chrome")
                if proxy:
                    p_url = _convert_proxy_format(proxy)
                    session.proxies = {"http": p_url, "https": p_url}
                response = session.post(ACE_INVENTORY_API_URL, json=payload, headers=headers, timeout=10)
                session.close()
            else:
                p_url = _convert_proxy_format(proxy) if proxy else None
                proxies = {"http": p_url, "https": p_url} if p_url else None
                response = requests.post(ACE_INVENTORY_API_URL, json=payload, headers=headers, proxies=proxies, timeout=10)
                
            response.raise_for_status()
            data = response.json()
            
            # structure: {"storeInventory": {"stockAvailable": 12, ...}, ...}
            store_inv = data.get("storeInventory") or {}
            store["stock_count"] = int(store_inv.get("stockAvailable") or 0)
            return True
        except Exception as e:
            logger.debug("Failed to fetch Ace inventory for store %s: %s", store_code, e)
            store["stock_count"] = 0
            # If we explicitly got Forbidden, it's a failure of the method, not a "0 stock" result
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 403:
                return False
            # For other unexpected errors, we also mark as failure to be safe
            return False

    @classmethod
    def fetch_product_metadata_instant(cls, product_link: str) -> Dict[str, Any]:
        """Fetch product metadata without browser automation."""
        product_id = cls.extract_product_id(product_link)

        last_error: Optional[Exception] = None
        proxy_candidates = cls.proxy_candidates()

        for proxy in proxy_candidates:
            try:
                return cls._fetch_product_metadata_via_requests_html(product_link, proxy)
            except Exception as exc:
                last_error = exc
                logger.warning("Ace instant HTML fetch failed via %s: %s", cls.proxy_label(proxy), exc)

        for proxy in proxy_candidates:
            try:
                return cls._fetch_product_metadata_via_api(product_link, proxy)
            except Exception as exc:
                last_error = exc
                logger.warning("Ace instant API fetch failed via %s: %s", cls.proxy_label(proxy), exc)

        if last_error is not None:
            raise last_error
        raise ValueError("Ace instant metadata fetch failed before any route could be attempted")

    @classmethod
    def _fetch_product_metadata_via_api(cls, product_link: str, proxy: Optional[str]) -> Dict[str, Any]:
        product_id = cls.extract_product_id(product_link)
        url = f"https://www.acehardware.com/api/commerce/catalog/storefront/products/{product_id}"
        proxy_config = {"http": proxy, "https": proxy} if proxy else None
        response = requests.get(url, headers=ACE_DEFAULT_HEADERS, proxies=proxy_config, timeout=10)
        response.raise_for_status()
        data = response.json()

        content = data.get("content") or {}
        image_url = ""
        product_images = (content.get("productImages") or [])
        if product_images:
            image_url = product_images[0].get("imageUrl") or ""
        if image_url and not image_url.startswith("http"):
            image_url = (
                f"https:{image_url}"
                if image_url.startswith("//")
                else f"https://www.acehardware.com{image_url}"
            )

        return {
            "retailer": "ace",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": unescape(content.get("productName") or "").strip(),
            "image_url": image_url,
            "canonical_url": cls.canonical_product_url(product_link),
        }

    @staticmethod
    def _first_non_empty(*values: Any) -> str:
        for value in values:
            normalized = str(value or "").strip()
            if normalized:
                return normalized
        return ""

    @staticmethod
    def _load_json(value: Any) -> Any:
        try:
            return json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    @staticmethod
    def _normalize_asset_url(value: Any) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            return f"https:{normalized}"
        if normalized.startswith("/"):
            return f"https://www.acehardware.com{normalized}"
        return normalized

    @classmethod
    def _parse_instant_product_metadata(cls, html: str, product_link: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html or "", "lxml")

        product_schema: Dict[str, Any] = {}
        inline_product: Dict[str, Any] = {}
        page_image = ""
        page_image_node = soup.select_one("img.mz-productimages-mainimage[src]")
        if page_image_node:
            page_image = cls._normalize_asset_url(page_image_node.get("src", ""))
        for script in soup.find_all("script"):
            script_text = script.string or script.get_text(" ", strip=True)
            if not script_text:
                continue

            if script.get("type") == "application/ld+json" and not product_schema:
                data = cls._load_json(script_text)
                candidates = data if isinstance(data, list) else [data]
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        continue
                    if str(candidate.get("@type") or "").strip().lower() == "product":
                        product_schema = candidate
                        break

            if not inline_product and script_text.startswith('{"mainImage"'):
                data = cls._load_json(script_text)
                if isinstance(data, dict):
                    inline_product = data

            if product_schema and inline_product:
                break

        content = inline_product.get("content") or {}
        main_image = inline_product.get("mainImage") or {}
        product_images = content.get("productImages") or []
        schema_image = product_schema.get("image")
        if isinstance(schema_image, list):
            schema_image = schema_image[0] if schema_image else ""

        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()

        canonical_link = soup.find("link", rel="canonical")
        canonical_url = cls._first_non_empty(
            cls.normalize_url(canonical_link.get("href", "") if canonical_link else ""),
            cls.canonical_product_url(product_link),
        )
        product_id = cls._first_non_empty(
            inline_product.get("productCode"),
            product_schema.get("sku"),
            cls.extract_product_id(product_link),
        )
        name = cls._first_non_empty(
            content.get("productName"),
            inline_product.get("name"),
            product_schema.get("name"),
            title,
        )
        image_url = cls._first_non_empty(
            page_image,
            cls._normalize_asset_url(main_image.get("imageUrl") or main_image.get("src")),
            cls._normalize_asset_url((product_images[0] or {}).get("imageUrl") if product_images else ""),
            cls._normalize_asset_url(schema_image),
        )

        if not product_id or not name:
            raise ValueError("Ace instant metadata extraction was incomplete")

        return {
            "retailer": "ace",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": unescape(name),
            "image_url": image_url,
            "canonical_url": canonical_url,
        }

    @classmethod
    def _fetch_product_metadata_via_requests_html(cls, product_link: str, proxy: Optional[str]) -> Dict[str, Any]:
        fetch_url = cls.product_url_with_main_pdp(product_link) or cls.canonical_product_url(product_link)
        headers = {
            **ACE_HTML_HEADERS,
            "Referer": "https://www.google.com/"
        }
        
        # Use curl_cffi for impersonation to avoid 403s
        if curl_requests:
            session = curl_requests.Session(impersonate="chrome")
            if proxy:
                session.proxies = {"http": proxy, "https": proxy}
            response = session.get(fetch_url, headers=headers, timeout=20, allow_redirects=True)
            session.close()
        else:
            proxy_config = {"http": proxy, "https": proxy} if proxy else None
            response = requests.get(fetch_url, headers=headers, proxies=proxy_config, timeout=20, allow_redirects=True)
            
        response.raise_for_status()
        html = response.text or ""
        if not html.strip():
            raise ValueError("Ace instant HTML fetch returned an empty response body")
        if "Just a moment..." in html and "cf-challenge" in html.lower():
            raise ValueError("Ace instant HTML fetch returned a Cloudflare challenge")
        return cls._parse_instant_product_metadata(html, product_link)

    @classmethod
    def _fetch_product_metadata_via_scrapling(cls, product_link: str, proxy: Optional[str]) -> Dict[str, Any]:
        fetch_url = cls.product_url_with_main_pdp(product_link) or cls.canonical_product_url(product_link)
        with StealthySession(
            proxy=proxy,
            headless=False,
            real_chrome=True,
            solve_cloudflare=True,
            disable_resources=False,
            network_idle=True,
            timeout=ACE_BROWSER_TIMEOUT_MS,
            wait=ACE_BROWSER_WAIT_MS,
        ) as session:
            response = session.fetch(fetch_url, referer="https://www.google.com/")
            if response.status >= 400:
                raise ValueError(f"Ace Scrapling metadata fetch returned HTTP {response.status}")
            html = (response.body or b"").decode("utf-8", errors="replace")
            if not html.strip():
                raise ValueError("Ace Scrapling metadata fetch returned an empty response body")
            return cls._parse_instant_product_metadata(html, product_link)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_mkdir(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _debug_screenshot_path(cls, label: str) -> Path:
        cls._safe_mkdir(ACE_DEBUG_DIR)
        safe_label = re.sub(r"[^a-z0-9_-]+", "-", str(label or "debug").lower()).strip("-") or "debug"
        return ACE_DEBUG_DIR / f"{safe_label}_{int(time.time() * 1000)}.png"

    @classmethod
    def save_debug_screenshot(cls, page: Any, label: str) -> str:
        target = cls._debug_screenshot_path(label)
        try:
            page.screenshot(path=str(target), full_page=True)
            return str(target)
        except Exception as exc:  # pragma: no cover - browser variance
            logger.warning("Failed to save Ace debug screenshot %s: %s", target, exc)
            return ""

    @classmethod
    def normalize_proxy_urls(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            raw_values = value.replace(",", "\n").splitlines()
        else:
            raw_values = list(value)

        normalized: List[str] = []
        seen = set()
        for item in raw_values:
            entry = str(item or "").strip()
            if not entry:
                continue
            if "://" not in entry and entry.count(":") == 3:
                host, port, username, password = entry.split(":", 3)
                entry = f"http://{username}:{password}@{host}:{port}"
            elif "://" not in entry:
                entry = f"http://{entry}"
            if entry not in seen:
                seen.add(entry)
                normalized.append(entry)
        return normalized

    @classmethod
    def set_proxy_urls_override(cls, value: Any) -> None:
        proxies = cls.normalize_proxy_urls(value)
        cls._proxy_urls_override = proxies or None

    @classmethod
    def set_store_cache_db(cls, db: Any) -> None:
        cls._store_cache_db = db

    @classmethod
    def configured_proxy_urls(cls) -> List[str]:
        if cls._proxy_urls_override is not None:
            return list(cls._proxy_urls_override)
        return cls.normalize_proxy_urls(os.getenv("ACE_PROXY_URLS") or os.getenv("CVS_PROXY_URLS"))

    @classmethod
    def proxy_candidates(cls) -> List[Optional[str]]:
        proxies = cls.configured_proxy_urls()
        return proxies or [None]

    @staticmethod
    def proxy_label(proxy_url: Optional[str]) -> str:
        if not proxy_url:
            return "direct"
        parsed = urlparse(proxy_url)
        host = parsed.hostname or proxy_url
        if parsed.port:
            return f"http://{host}:{parsed.port}"
        return f"http://{host}"

    @staticmethod
    def geocode_zip(zip_code: str) -> Dict[str, float]:
        normalized_zip = str(zip_code or "").strip()
        if not normalized_zip:
            raise ValueError("ZIP code is required for Ace store lookup")
        cached = _zip_geocode_cache.get(normalized_zip)
        if cached:
            return dict(cached)

        response = requests.get(f"https://api.zippopotam.us/us/{normalized_zip}", timeout=15)
        response.raise_for_status()
        payload = response.json()
        place = (payload.get("places") or [{}])[0]
        location = {
            "lat": float(place["latitude"]),
            "lng": float(place["longitude"]),
        }
        _zip_geocode_cache[normalized_zip] = dict(location)
        return location

    @staticmethod
    def _normalized_zip_cache_key(zip_code: Any) -> str:
        return str(zip_code or "").strip()

    @staticmethod
    def _clone_store_candidates(store_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [dict(item) for item in (store_items or [])]

    @classmethod
    def _get_cached_store_candidates(cls, zip_code: Any) -> List[Dict[str, Any]]:
        cache_key = cls._normalized_zip_cache_key(zip_code)
        if not cache_key:
            return []
        cached = _store_candidates_cache.get(cache_key)
        if cached:
            return cls._clone_store_candidates(cached)

        db = cls._store_cache_db
        if db is None:
            return []

        try:
            persisted = db.get_ace_store_candidates(cache_key)
        except Exception as exc:
            logger.warning("Failed to read persisted Ace store candidates for %s: %s", cache_key, exc)
            return []

        if persisted:
            _store_candidates_cache[cache_key] = cls._clone_store_candidates(persisted)
            return cls._clone_store_candidates(persisted)
        return []

    @classmethod
    def _set_cached_store_candidates(cls, zip_code: Any, store_items: List[Dict[str, Any]]) -> None:
        cache_key = cls._normalized_zip_cache_key(zip_code)
        if not cache_key or not store_items:
            return
        _store_candidates_cache[cache_key] = cls._clone_store_candidates(store_items)
        db = cls._store_cache_db
        if db is None:
            return
        try:
            db.store_ace_store_candidates(cache_key, store_items)
        except Exception as exc:
            logger.warning("Failed to persist Ace store candidates for %s: %s", cache_key, exc)

    @classmethod
    def _product_metadata_from_hints(
        cls,
        product_link: str,
        product_hints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        hints = dict(product_hints or {})
        canonical_url = cls._first_non_empty(
            cls.normalize_url(hints.get("canonical_url")),
            cls.normalize_url(hints.get("source_url")),
            cls.canonical_product_url(product_link),
        )
        product_id = cls._first_non_empty(
            hints.get("product_id"),
            hints.get("article_id"),
        )
        if not product_id:
            try:
                product_id = cls.extract_product_id(product_link)
            except Exception:
                product_id = ""

        return {
            "retailer": "ace",
            "product_id": product_id,
            "article_id": product_id,
            "planogram": product_id,
            "name": cls._first_non_empty(hints.get("name")),
            "image_url": cls._normalize_asset_url(hints.get("image_url")),
            "canonical_url": canonical_url,
        }

    @staticmethod
    def build_store_lookup(store_items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        for item in store_items:
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            address = item.get("address") or {}
            lookup[code] = {
                "store_id": code,
                "name": str(item.get("name") or f"Ace Hardware #{code}").strip(),
                "address": ", ".join(
                    part
                    for part in (
                        str(address.get("address1") or "").strip(),
                        " ".join(
                            part
                            for part in (
                                str(address.get("cityOrTown") or "").strip(),
                                str(address.get("stateOrProvince") or "").strip(),
                                str(address.get("postalOrZipCode") or "").strip(),
                            )
                            if part
                        ).strip(),
                    )
                    if part
                ),
                "latitude": AceBrowserClient._safe_float((item.get("geo") or {}).get("lat")),
                "longitude": AceBrowserClient._safe_float((item.get("geo") or {}).get("lng")),
            }
        return lookup

    @staticmethod
    def _normalize_match_key(name: str, address: str) -> str:
        text = f"{name}|{address}".lower()
        text = re.sub(r"[^a-z0-9]+", "", text)
        return text

    @staticmethod
    def _human_pause(page: Any, minimum_ms: int = 150, maximum_ms: int = 450) -> None:
        try:
            page.wait_for_timeout(random.randint(minimum_ms, maximum_ms))
        except Exception:
            return

    @classmethod
    def _human_scroll(cls, page: Any, delta_y: int) -> None:
        steps = max(1, min(8, abs(int(delta_y)) // 180 or 1))
        step_delta = int(delta_y / steps)
        remainder = int(delta_y - (step_delta * steps))
        try:
            for index in range(steps):
                current_delta = step_delta + (remainder if index == steps - 1 else 0)
                page.mouse.wheel(0, current_delta)
                cls._human_pause(page, 90, 220)
        except Exception:
            return

    @classmethod
    def _humanize_page(cls, page: Any) -> None:
        try:
            viewport = page.viewport_size or {"width": 1440, "height": 900}
            width = max(int(viewport.get("width") or 1440), 600)
            height = max(int(viewport.get("height") or 900), 500)

            start_x = random.randint(140, min(width - 140, 420))
            start_y = random.randint(110, min(height - 180, 260))
            page.mouse.move(start_x, start_y, steps=random.randint(8, 18))
            cls._human_pause(page, 180, 320)
            cls._human_scroll(page, random.randint(260, 520))
            cls._human_pause(page, 180, 360)
            page.mouse.move(
                random.randint(width // 3, max(width // 3, width - 180)),
                random.randint(140, max(180, height - 220)),
                steps=random.randint(10, 22),
            )
            cls._human_pause(page, 120, 280)
            cls._human_scroll(page, -random.randint(120, 260))
            cls._human_pause(page, 180, 340)
        except Exception:
            return

    @classmethod
    def _move_mouse_to_locator(cls, page: Any, locator: Any) -> None:
        try:
            box = locator.bounding_box()
            if not box:
                return
            target_x = box["x"] + (box["width"] * random.uniform(0.35, 0.65))
            target_y = box["y"] + (box["height"] * random.uniform(0.35, 0.65))
            page.mouse.move(target_x, target_y, steps=random.randint(12, 24))
            cls._human_pause(page, 100, 220)
        except Exception:
            return

    @classmethod
    def _human_click_locator(cls, page: Any, locator: Any, *, double_attempt: bool = True) -> bool:
        try:
            locator.scroll_into_view_if_needed(timeout=2500)
        except Exception:
            pass

        cls._human_pause(page, 120, 240)
        cls._move_mouse_to_locator(page, locator)

        try:
            locator.hover(timeout=2000)
            cls._human_pause(page, 110, 260)
        except Exception:
            pass

        try:
            locator.click(timeout=5000)
            cls._human_pause(page, 260, 520)
            return True
        except Exception:
            if not double_attempt:
                return False

        try:
            box = locator.bounding_box()
            if not box:
                return False
            target_x = box["x"] + (box["width"] * random.uniform(0.4, 0.6))
            target_y = box["y"] + (box["height"] * random.uniform(0.4, 0.6))
            page.mouse.click(target_x, target_y, delay=random.randint(55, 140))
            cls._human_pause(page, 260, 520)
            return True
        except Exception:
            return False

    @classmethod
    def _maybe_dismiss_cookie_banner(cls, page: Any) -> None:
        for text in ("Necessary Cookies Only", "Accept All Cookies"):
            try:
                locator = page.locator(f"text={text}").first
                if locator.is_visible(timeout=1000):
                    cls._human_click_locator(page, locator)
                    return
            except Exception:
                continue

    @classmethod
    def _maybe_click_view_details(cls, page: Any) -> bool:
        for text in ("VIEW DETAILS", "View Details"):
            try:
                locator = page.locator(f"text={text}").first
                if locator.is_visible(timeout=1000):
                    if cls._human_click_locator(page, locator):
                        cls._human_pause(page, 1600, 2600)
                        return True
            except Exception:
                continue
        return False

    @staticmethod
    def _inline_product_payload(page: Any) -> Dict[str, Any]:
        payload = page.evaluate(
            """
() => {
  const scripts = Array.from(document.querySelectorAll('script'));
  const result = { ldjson: null, inlineProduct: null, title: document.title || '', canonicalUrl: '' };
  const canonical = document.querySelector('link[rel="canonical"]');
  if (canonical?.href) result.canonicalUrl = canonical.href;

  for (const script of scripts) {
    const text = (script.textContent || '').trim();
    if (!text) continue;
    if (!result.ldjson && script.type === 'application/ld+json') {
      try {
        const parsed = JSON.parse(text);
        const items = Array.isArray(parsed) ? parsed : [parsed];
        const product = items.find(item => item && typeof item === 'object' && String(item['@type'] || '').toLowerCase() === 'product');
        if (product) result.ldjson = product;
      } catch (e) {}
    }
    if (!result.inlineProduct && text.startsWith('{"mainImage"')) {
      try {
        result.inlineProduct = JSON.parse(text);
      } catch (e) {}
    }
  }
  return result;
}
            """
        )
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def extract_product_metadata(cls, page: Any, product_url: str) -> Dict[str, str]:
        payload = cls._inline_product_payload(page)
        ldjson = payload.get("ldjson") or {}
        inline = payload.get("inlineProduct") or {}
        content = inline.get("content") or {}
        main_image = inline.get("mainImage") or {}
        image_candidates = content.get("productImages") or []
        schema_image = ldjson.get("image")
        if isinstance(schema_image, list):
            schema_image = schema_image[0] if schema_image else ""

        name = cls._first_non_empty(
            content.get("productName"),
            inline.get("name"),
            ldjson.get("name"),
            page.evaluate(
                "() => (document.querySelector('h1')?.textContent || '').trim()"
            ),
            payload.get("title", ""),
        )
        image_url = cls._first_non_empty(
            cls._normalize_asset_url(main_image.get("imageUrl") or main_image.get("src")),
            cls._normalize_asset_url((image_candidates[0] or {}).get("imageUrl") if image_candidates else ""),
            cls._normalize_asset_url(schema_image),
            cls._normalize_asset_url(
                page.evaluate(
                    "() => document.querySelector('meta[property=\"og:image\"]')?.content || ''"
                )
            ),
        )
        canonical_url = cls._first_non_empty(
            cls.normalize_url(payload.get("canonicalUrl")),
            cls.normalize_url((ldjson.get("offers") or {}).get("url") if isinstance(ldjson.get("offers"), dict) else ""),
            cls.canonical_product_url(product_url),
        )
        product_id = cls._first_non_empty(
            str(inline.get("productCode") or "").strip(),
            str(ldjson.get("sku") or "").strip(),
            cls.extract_product_id(product_url),
        )

        if not product_id or not name:
            raise AceBrowserError("Ace product metadata was incomplete on the product page")

        return {
            "product_id": product_id,
            "name": unescape(name),
            "image_url": image_url,
            "canonical_url": canonical_url,
        }

    @staticmethod
    def _browser_fetch_store_inventory(page: Any, product_id: str, store_codes: List[str]) -> List[Dict[str, Any]]:
        # Batch the requests to avoid triggering rate-limits or bot detection (e.g. 10 at a time)
        payload = page.evaluate(
            """
async ({ product_id, store_codes, headers }) => {
  const url = '/getProductDetailInventory';
  const results = [];
  const BATCH_SIZE = 5;
  
  for (let i = 0; i < store_codes.length; i += BATCH_SIZE) {
    const batch = store_codes.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(async (storeCode) => {
      try {
        const resp = await fetch(url, {
          method: 'POST',
          credentials: 'include',
          headers: {
            'Content-Type': 'application/json',
            'X-Vol-Catalog': headers['X-Vol-Catalog'],
            'X-Vol-Currency': headers['X-Vol-Currency'],
            'X-Vol-Site': headers['X-Vol-Site'],
            'X-Vol-Tenant': headers['X-Vol-Tenant']
          },
          body: JSON.stringify({ productCode: product_id, storeCode, quantity: 1 })
        });
        const data = await resp.json();
        return { storeCode, data, ok: resp.ok };
      } catch (e) {
        return { storeCode, error: e.message, ok: false };
      }
    }));
    results.push(...batchResults);
    // Pause between batches to be stealthy
    if (i + BATCH_SIZE < store_codes.length) {
      await new Promise(r => setTimeout(r, 200));
    }
  }
  return results;
}
            """,
            {
                "product_id": product_id,
                "store_codes": store_codes,
                "headers": ACE_DEFAULT_HEADERS,
            },
        )
        return payload if isinstance(payload, list) else []

    @staticmethod
    def _browser_fetch_store_candidates(page: Any, lat: float, lng: float) -> List[Dict[str, Any]]:
        payload = page.evaluate(
            """
async ({ lat, lng, radius, headers }) => {
  const filter = `geo near(${lat},${lng},${radius})`;
  const responseFields = 'items(code,name,address,geo,regularHours,fulfillmentTypes,supportsInventory)';
  const url = `/api/commerce/storefront/locationUsageTypes/SP/locations?filter=${encodeURIComponent(filter)}&pageSize=100&responseFields=${encodeURIComponent(responseFields)}`;
  const response = await fetch(url, {
    method: 'GET',
    credentials: 'include',
    headers,
  });
  const contentType = response.headers.get('content-type') || '';
  const text = await response.text();
  return { ok: response.ok, status: response.status, contentType, text };
}
            """,
            {
                "lat": lat,
                "lng": lng,
                "radius": ACE_RADIUS_METERS,
                "headers": ACE_DEFAULT_HEADERS,
            },
        )
        if not isinstance(payload, dict):
            raise AceBrowserError("Ace browser store lookup returned an unexpected payload")
        text = str(payload.get("text") or "")
        content_type = str(payload.get("contentType") or "")
        if "json" not in content_type.lower():
            raise AceBrowserError(
                f"Ace browser store lookup returned non-JSON content (HTTP {payload.get('status')}): {text[:200]}"
            )
        data = json.loads(text)
        items = data.get("items") or []
        if not isinstance(items, list) or not items:
            raise AceBrowserError("Ace browser store lookup did not return any nearby stores")
        return items



    @classmethod
    def fetch_product_context(
        cls,
        product_url: str,
        *,
        zip_code: Optional[str] = None,
        product_hints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        product_url = cls.canonical_product_url(product_url)
        if not product_url:
            raise AceBrowserError("Ace product URL required")

        geocoded_location = cls.geocode_zip(zip_code) if zip_code else None
        last_error: Optional[Exception] = None
        base_product = cls._product_metadata_from_hints(product_url, product_hints)
        needs_page_metadata = not base_product.get("name")

        for proxy in cls.proxy_candidates():
            debug_screenshot = ""
            proxy_label = cls.proxy_label(proxy)
            try:
                context: Dict[str, Any] = {
                    "product": dict(base_product),
                    "store_candidates": [],
                    "store_cards": [],
                    "stores": {},
                    "proxy": proxy_label,
                    "debug_screenshot": "",
                }

                with StealthySession(
                    proxy=proxy,
                    headless=False,
                    real_chrome=True,
                    solve_cloudflare=True,
                    disable_resources=False,
                    network_idle=True,
                    timeout=ACE_BROWSER_TIMEOUT_MS,
                    wait=ACE_BROWSER_WAIT_MS,
                ) as session:

                    def action(page: Any) -> None:
                        nonlocal debug_screenshot
                        if needs_page_metadata:
                            cls._maybe_dismiss_cookie_banner(page)
                            cls._humanize_page(page)
                            fetched_product = cls.extract_product_metadata(page, product_url)
                            context["product"].update({key: value for key, value in fetched_product.items() if value})
                        context["cookies"] = [dict(cookie) for cookie in page.context.cookies()]

                        if not geocoded_location:
                            return

                        cached_stores = cls._get_cached_store_candidates(zip_code)
                        if cached_stores:
                            context["store_candidates"] = cached_stores
                        else:
                            context["store_candidates"] = cls._browser_fetch_store_candidates(
                                page,
                                geocoded_location["lat"],
                                geocoded_location["lng"],
                            )
                            cls._set_cached_store_candidates(zip_code, context["store_candidates"])
                        store_lookup = cls.build_store_lookup(context["store_candidates"])
                        if not store_lookup:
                            raise AceBrowserError("Ace did not return any store candidates for that ZIP")

                        # Fetch real inventory counts from within the browser context
                        store_codes = list(store_lookup.keys())
                        product_id = cls._first_non_empty(
                            context["product"].get("product_id"),
                            cls.extract_product_id(product_url),
                        )
                        inventory_results = cls._browser_fetch_store_inventory(page, product_id, store_codes)
                        inventory_lookup = { r["storeCode"]: (r.get("data") or {}).get("storeInventory") or {} for r in inventory_results if r.get("ok") }

                        # Summarize availability based on inventory and fulfillmentTypes
                        # SP = In Store Pickup, DL = Delivery
                        for candidate in context["store_candidates"]:
                            loc_code = str(candidate.get("code") or "").strip()
                            if not loc_code:
                                continue
                            
                            # Use exact inventory if extracted from browser fetch
                            store_inv = inventory_lookup.get(loc_code) or {}
                            stock_count = int(store_inv.get("stockAvailable") or 0)
                            is_exact = loc_code in inventory_lookup
                            
                            f_types = candidate.get("fulfillmentTypes") or []

                            # Strictly only show if inventory API returned a positive stock count
                            has_pickup = stock_count > 0
                            has_delivery = stock_count > 0 and any(
                                str(ft.get("code") or "").upper() == "DL" 
                                for ft in f_types
                                if isinstance(ft, dict)
                            )
                            
                            if has_pickup or has_delivery:
                                context["stores"][loc_code] = {
                                    **candidate,
                                    "inventory_count": stock_count if is_exact else 1,
                                    "inventory_count_known": is_exact,
                                    "availability_mode": "exact" if is_exact else "fulfillment",
                                    "pickup_available": has_pickup,
                                    "delivery_available": has_delivery,
                                    "supports_inventory": bool(candidate.get("supportsInventory")),
                                    "fulfillment_types": [
                                        {
                                            "code": str(ft.get("code") or "").upper(),
                                            "name": str(ft.get("name") or "").strip(),
                                        }
                                        for ft in f_types
                                        if isinstance(ft, dict)
                                    ],
                                    "availability_text": f"{stock_count} In Stock" if is_exact else "Available"
                                }
                                if not is_exact:
                                    if has_pickup and has_delivery:
                                        context["stores"][loc_code]["availability_text"] = "Pickup & Delivery Available"
                                    elif has_pickup:
                                        context["stores"][loc_code]["availability_text"] = "Available for Pickup"
                                    elif has_delivery:
                                        context["stores"][loc_code]["availability_text"] = "Available for Delivery"
                            else:
                                logger.debug("Ace store %s lacks both SP and DL fulfillment", loc_code)

                        logger.info(
                            "Ace check complete: found %d stores with stock for pid=%s",
                            len(context["stores"]),
                            context["product"].get("product_id")
                        )

                    response = session.fetch(product_url, page_action=action)
                    if response.status >= 400:
                        raise AceBrowserError(f"Ace returned HTTP {response.status} for the product page")

                if context.get("product"):
                    if debug_screenshot and not context.get("debug_screenshot"):
                        context["debug_screenshot"] = debug_screenshot
                    return context
                raise AceBrowserError("Ace product flow completed without product metadata")
            except Exception as exc:
                last_error = exc
                logger.warning("Ace browser flow failed via %s: %s", proxy_label, exc)

        if last_error is not None:
            raise AceBrowserError(str(last_error)) from last_error
        raise AceBrowserError("Ace browser flow failed before any route could be attempted")
