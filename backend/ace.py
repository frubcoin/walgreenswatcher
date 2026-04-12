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
from bs4 import BeautifulSoup
from scrapling.engines._browsers._stealth import StealthySession

logger = logging.getLogger(__name__)

ACE_LOCATION_API_BASE = "https://www.acehardware.com/api/commerce/storefront/locationUsageTypes/SP/locations"
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
        "Chrome/134.0.0.0 Safari/537.36"
    ),
}

_zip_geocode_cache: Dict[str, Dict[str, float]] = {}


class AceBrowserError(ValueError):
    """Raised when the Ace browser flow fails."""


class AceBrowserClient:
    """Shared Ace PDP/session logic for resolver and inventory checks."""

    _proxy_urls_override: Optional[List[str]] = None

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
        proxy_config = {"http": proxy, "https": proxy} if proxy else None
        response = requests.get(
            fetch_url,
            headers=ACE_HTML_HEADERS,
            proxies=proxy_config,
            timeout=20,
            allow_redirects=True,
        )
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
    ) -> Dict[str, Any]:
        product_url = cls.canonical_product_url(product_url)
        if not product_url:
            raise AceBrowserError("Ace product URL required")

        geocoded_location = cls.geocode_zip(zip_code) if zip_code else None
        last_error: Optional[Exception] = None

        for proxy in cls.proxy_candidates():
            debug_screenshot = ""
            proxy_label = cls.proxy_label(proxy)
            try:
                context: Dict[str, Any] = {
                    "product": {},
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
                        cls._maybe_dismiss_cookie_banner(page)
                        cls._humanize_page(page)

                        context["product"] = cls.extract_product_metadata(page, product_url)
                        context["cookies"] = [dict(cookie) for cookie in page.context.cookies()]

                        if not geocoded_location:
                            return

                        context["store_candidates"] = cls._browser_fetch_store_candidates(
                            page,
                            geocoded_location["lat"],
                            geocoded_location["lng"],
                        )
                        store_lookup = cls.build_store_lookup(context["store_candidates"])
                        if not store_lookup:
                            raise AceBrowserError("Ace did not return any store candidates for that ZIP")

                        # Summarize availability based on fulfillmentTypes
                        # SP = In Store Pickup, DL = Delivery
                        for candidate in context["store_candidates"]:
                            loc_code = str(candidate.get("code") or "").strip()
                            if not loc_code:
                                continue
                            
                            fulfillment_types = candidate.get("fulfillmentTypes") or []
                            has_pickup = any(
                                str(ft.get("code") or "").upper() == "SP" 
                                for ft in fulfillment_types 
                                if isinstance(ft, dict)
                            )
                            has_delivery = any(
                                str(ft.get("code") or "").upper() == "DL" 
                                for ft in fulfillment_types 
                                if isinstance(ft, dict)
                            )
                            
                            if has_pickup or has_delivery:
                                context["stores"][loc_code] = {
                                    **candidate,
                                    "inventory_count": 1,  # Binary status
                                    "pickup_available": has_pickup,
                                    "delivery_available": has_delivery,
                                    "availability_text": "Available"
                                }
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
