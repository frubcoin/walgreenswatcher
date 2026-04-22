"""Helpers for ALDI product metadata and Instacart-backed availability."""

from __future__ import annotations

import ast
import json
import logging
import math
import random
import re
import secrets
import threading
import time
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urljoin, urlparse, urlsplit

import requests
from bs4 import BeautifulSoup

from config import CVS_PROXY_URLS, USER_AGENTS

logger = logging.getLogger(__name__)

ALDI_BASE_URL = "https://www.aldi.us"
ALDI_GRAPHQL_URL = f"{ALDI_BASE_URL}/graphql"
ALDI_PRODUCT_ID_PATTERN = re.compile(r"/products/(?P<product_id>\d+)(?:[/?#-]|$)", re.IGNORECASE)
ALDI_APOLLO_STATE_PATTERN = re.compile(
    r'<script id="node-apollo-state" type="application/json">(?P<state>.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
ALDI_RUNTIME_SCRIPT_PATTERN = re.compile(
    r'<script[^>]+src="(?P<src>[^"]*runtime\.webpack_bundle-[^"]+\.js)"',
    re.IGNORECASE,
)
ALDI_OPERATION_CHUNK_NAME_PATTERN = re.compile(r'51488:"operation-hashes"')
ALDI_OPERATION_CHUNK_HASH_PATTERN = re.compile(r'51488:"(?P<hash>[a-f0-9]{16})"')
ALDI_OPERATION_MANIFEST_PATTERN = re.compile(r"JSON\.parse\('(?P<json>.*)'\)", re.DOTALL)
ALDI_DEFAULT_ZONE_ID = "0"
ALDI_STORE_SERVICE_PRIORITY = {"pickup": 0, "delivery": 1, "instore": 2}
ALDI_STORE_CACHE_TTL_SECONDS = 900

_operation_hash_cache: Dict[str, Any] = {"expires_at": 0.0, "hashes": {}}
_operation_hash_cache_lock = threading.RLock()


class AldiGraphqlClient:
    """Small client for ALDI's public Instacart storefront GraphQL layer."""

    # Shared proxy list — set at startup from the admin-panel CVS_PROXY_URLS setting.
    _proxy_urls_override: List[str] = []

    @staticmethod
    def normalize_proxy_urls(raw_value: Any) -> List[str]:
        if isinstance(raw_value, (list, tuple, set)):
            candidates = [str(item or "").strip() for item in raw_value]
        else:
            candidates = re.split(r"[\r\n,;]+", str(raw_value or ""))
        normalized: List[str] = []
        seen: set = set()
        for candidate in candidates:
            value = str(candidate or "").strip().strip("'\"")
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    @classmethod
    def set_proxy_urls_override(cls, raw_value: Any) -> List[str]:
        cls._proxy_urls_override = cls.normalize_proxy_urls(raw_value)
        return list(cls._proxy_urls_override)

    @classmethod
    def _configured_proxy_urls(cls) -> List[str]:
        if cls._proxy_urls_override:
            return list(cls._proxy_urls_override)
        return cls.normalize_proxy_urls(CVS_PROXY_URLS)

    @classmethod
    def _proxy_candidates(cls) -> List[str]:
        proxies = cls._configured_proxy_urls()
        if not proxies:
            return [""]
        start = secrets.randbelow(len(proxies))
        rotated = proxies[start:] + proxies[:start]
        return rotated + [""]  # always include a direct fallback

    @staticmethod
    def _proxy_label(proxy_url: str) -> str:
        parsed = urlsplit(str(proxy_url or "").strip())
        if not parsed.hostname:
            return "direct"
        return f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname

    @staticmethod
    def _convert_proxy_format(proxy_url: str) -> str:
        """Convert `host:port:user:pass` shorthand to a full http:// URL."""
        if not proxy_url:
            return proxy_url
        if "://" in proxy_url:
            return proxy_url
        parts = proxy_url.split(":")
        if len(parts) >= 4:
            host, port, username = parts[0], parts[1], parts[2]
            password = ":".join(parts[3:])
            return f"http://{username}:{password}@{host}:{port}"
        return proxy_url

    @classmethod
    def _new_session(cls, proxy_url: str = "") -> requests.Session:
        session = requests.Session()
        if proxy_url:
            converted = cls._convert_proxy_format(proxy_url)
            session.proxies.update({"http": converted, "https": converted})
        return session
    @staticmethod
    def normalize_product_url(value: Any) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("//"):
            return f"https:{normalized}"
        if normalized.startswith("/"):
            return urljoin(ALDI_BASE_URL, normalized)
        return normalized

    @classmethod
    def extract_product_id(cls, product_link: str) -> str:
        match = ALDI_PRODUCT_ID_PATTERN.search(str(product_link or "").strip())
        return str(match.group("product_id") or "").strip() if match else ""

    @staticmethod
    def _headers(
        *,
        accept: str = "application/json",
        referer: str = ALDI_BASE_URL,
        token: str = "",
    ) -> Dict[str, str]:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": accept,
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": referer,
            "Origin": ALDI_BASE_URL,
            "x-client-identifier": "web",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @classmethod
    def fetch_product_page(cls, product_url: str) -> str:
        """Fetch an ALDI product page, trying each configured proxy before falling back direct."""
        headers = cls._headers(
            accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            referer=ALDI_BASE_URL,
        )
        last_exc: Exception | None = None
        for proxy_url in cls._proxy_candidates():
            label = cls._proxy_label(proxy_url)
            try:
                session = cls._new_session(proxy_url)
                response = session.get(product_url, headers=headers, timeout=30)
                if response.status_code in (403, 407):
                    logger.warning("ALDI product page blocked via %s (HTTP %s)", label, response.status_code)
                    last_exc = requests.HTTPError(response=response)
                    continue
                response.raise_for_status()
                logger.debug("ALDI product page fetched via %s", label)
                return response.text
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (403, 407):
                    logger.warning("ALDI product page blocked via %s: %s", label, exc)
                    last_exc = exc
                    continue
                raise
            except Exception as exc:
                logger.warning("ALDI product page fetch failed via %s: %s", label, exc)
                last_exc = exc
                continue
        if last_exc:
            raise last_exc
        raise RuntimeError("ALDI product page: no proxy candidates available")

    @classmethod
    def extract_apollo_state(cls, html: str) -> Dict[str, Any]:
        match = ALDI_APOLLO_STATE_PATTERN.search(html or "")
        if not match:
            return {}
        try:
            return json.loads(unescape(unquote(match.group("state"))))
        except (TypeError, ValueError, json.JSONDecodeError):
            logger.debug("Failed to decode ALDI Apollo state", exc_info=True)
            return {}

    @staticmethod
    def extract_auth_token(apollo_state: Dict[str, Any]) -> str:
        guest_entries = apollo_state.get("CreateImplicitGuestUser") or {}
        if not isinstance(guest_entries, dict):
            return ""
        for entry in guest_entries.values():
            if not isinstance(entry, dict):
                continue
            token = (
                ((entry.get("createImplicitGuestUser") or {}).get("authToken") or {}).get("token")
            )
            if token:
                return str(token).strip()
        return ""

    @staticmethod
    def _first_non_empty(*values: Any) -> str:
        for value in values:
            normalized = str(value or "").strip()
            if normalized:
                return normalized
        return ""

    @classmethod
    def _product_schema(cls, soup: BeautifulSoup) -> Dict[str, Any]:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or script.get_text(" ", strip=True))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            graphs = []
            if isinstance(data, dict):
                graphs.extend(data.get("@graph") or [])
                graphs.append(data)
            elif isinstance(data, list):
                graphs.extend(data)
            for candidate in graphs:
                if not isinstance(candidate, dict):
                    continue
                if str(candidate.get("@type", "")).lower() == "product":
                    return candidate
        return {}

    @classmethod
    def _landing_product(cls, apollo_state: Dict[str, Any], product_id: str) -> Dict[str, Any]:
        landing_entries = apollo_state.get("LandingProductMeta") or {}
        if not isinstance(landing_entries, dict):
            return {}
        for entry in landing_entries.values():
            products = (entry or {}).get("landingProducts") or []
            for product in products:
                if str((product or {}).get("id") or "") == str(product_id):
                    return product or {}
        return {}

    @classmethod
    def _item_product(cls, apollo_state: Dict[str, Any], product_id: str) -> Dict[str, Any]:
        item_entries = apollo_state.get("Items") or {}
        if not isinstance(item_entries, dict):
            return {}
        for entry in item_entries.values():
            for item in ((entry or {}).get("items") or []):
                if str((item or {}).get("productId") or "") == str(product_id):
                    return item or {}
        return {}

    @classmethod
    def extract_product_metadata(
        cls,
        product_url: str,
        *,
        html: str = "",
    ) -> Dict[str, Any]:
        product_url = cls.normalize_product_url(product_url)
        product_id = cls.extract_product_id(product_url)
        if not product_id:
            raise ValueError("Could not find an ALDI product id in the link")

        html = html or cls.fetch_product_page(product_url)
        soup = BeautifulSoup(html, "lxml")
        apollo_state = cls.extract_apollo_state(html)
        schema = cls._product_schema(soup)
        landing_product = cls._landing_product(apollo_state, product_id)
        item_product = cls._item_product(apollo_state, product_id)

        title = unescape((soup.find("title").get_text(" ", strip=True) if soup.find("title") else "")).strip()
        title = re.sub(r"\s+Same-Day Delivery or Pickup\s+\|\s+ALDI\s*$", "", title, flags=re.IGNORECASE).strip()
        canonical = cls.normalize_product_url((soup.find("link", rel="canonical") or {}).get("href", "")) or product_url

        schema_image = schema.get("image")
        if isinstance(schema_image, list):
            schema_image = schema_image[0] if schema_image else ""
        landing_image = (((landing_product.get("viewSection") or {}).get("productImage") or {}).get("url"))
        item_image = (((item_product.get("viewSection") or {}).get("itemImage") or {}).get("url"))

        return {
            "product_id": product_id,
            "name": cls._first_non_empty(
                item_product.get("name"),
                landing_product.get("name"),
                schema.get("name"),
                title,
                "ALDI Product",
            ),
            "size": cls._first_non_empty(item_product.get("size"), schema.get("size")),
            "image_url": cls.normalize_product_url(
                cls._first_non_empty(item_image, landing_image, schema_image)
            ),
            "canonical_url": canonical,
            "auth_token": cls.extract_auth_token(apollo_state),
        }

    @classmethod
    def _operation_manifest_url(cls, html: str) -> str:
        runtime_match = ALDI_RUNTIME_SCRIPT_PATTERN.search(html or "")
        if not runtime_match:
            raise ValueError("ALDI storefront did not expose a runtime script")

        runtime_url = cls.normalize_product_url(runtime_match.group("src"))
        response = requests.get(
            runtime_url,
            headers=cls._headers(accept="application/javascript", referer=ALDI_BASE_URL),
            timeout=30,
        )
        response.raise_for_status()
        runtime_js = response.text
        if not ALDI_OPERATION_CHUNK_NAME_PATTERN.search(runtime_js):
            raise ValueError("ALDI operation hash chunk was not present in the runtime manifest")

        hash_match = ALDI_OPERATION_CHUNK_HASH_PATTERN.search(runtime_js)
        if not hash_match:
            raise ValueError("ALDI operation hash chunk fingerprint was not present in the runtime manifest")
        return urljoin(
            runtime_url,
            f"operation-hashes-{hash_match.group('hash')}-v3.webpack_chunk.js",
        )

    @classmethod
    def operation_hashes(cls, html: str) -> Dict[str, str]:
        with _operation_hash_cache_lock:
            if (
                _operation_hash_cache.get("hashes")
                and float(_operation_hash_cache.get("expires_at", 0.0)) > time.monotonic()
            ):
                return dict(_operation_hash_cache["hashes"])

        manifest_url = cls._operation_manifest_url(html)
        response = requests.get(
            manifest_url,
            headers=cls._headers(accept="application/javascript", referer=ALDI_BASE_URL),
            timeout=30,
        )
        response.raise_for_status()
        match = ALDI_OPERATION_MANIFEST_PATTERN.search(response.text)
        if not match:
            raise ValueError("ALDI operation hash manifest could not be parsed")

        manifest_json = ast.literal_eval("'" + match.group("json") + "'")
        hashes = json.loads(manifest_json)
        if not isinstance(hashes, dict):
            raise ValueError("ALDI operation hash manifest was not an object")

        with _operation_hash_cache_lock:
            _operation_hash_cache["hashes"] = dict(hashes)
            _operation_hash_cache["expires_at"] = time.monotonic() + ALDI_STORE_CACHE_TTL_SECONDS
        return dict(hashes)

    @classmethod
    def graphql_get(
        cls,
        operation_name: str,
        variables: Dict[str, Any],
        operation_hashes: Dict[str, str],
        *,
        token: str = "",
        referer: str = ALDI_BASE_URL,
        allow_partial: bool = False,
    ) -> Dict[str, Any]:
        operation_hash = operation_hashes.get(operation_name)
        if not operation_hash:
            raise ValueError(f"ALDI operation hash missing for {operation_name}")

        params = {
            "operationName": operation_name,
            "variables": json.dumps(variables, separators=(",", ":")),
            "extensions": json.dumps(
                {"persistedQuery": {"version": 1, "sha256Hash": operation_hash}},
                separators=(",", ":"),
            ),
        }
        headers = cls._headers(referer=referer, token=token)

        last_exc: Exception | None = None
        for proxy_url in cls._proxy_candidates():
            label = cls._proxy_label(proxy_url)
            try:
                session = cls._new_session(proxy_url)
                response = session.get(ALDI_GRAPHQL_URL, params=params, headers=headers, timeout=30)
                if response.status_code in (403, 407):
                    logger.warning("ALDI GraphQL %s blocked via %s (HTTP %s)", operation_name, label, response.status_code)
                    last_exc = requests.HTTPError(response=response)
                    continue
                response.raise_for_status()
                payload = response.json()
                if payload.get("errors") and not (allow_partial and payload.get("data")):
                    raise ValueError(f"ALDI GraphQL {operation_name} returned errors: {payload['errors']}")
                if payload.get("errors"):
                    logger.debug("ALDI GraphQL %s returned partial data with errors: %s", operation_name, payload["errors"])
                return payload.get("data") or {}
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (403, 407):
                    logger.warning("ALDI GraphQL %s blocked via %s: %s", operation_name, label, exc)
                    last_exc = exc
                    continue
                raise
            except ValueError:
                raise
            except Exception as exc:
                logger.warning("ALDI GraphQL %s failed via %s: %s", operation_name, label, exc)
                last_exc = exc
                continue
        if last_exc:
            raise last_exc
        raise RuntimeError(f"ALDI GraphQL {operation_name}: no proxy candidates available")

    @staticmethod
    def distance_miles(lat1: Any, lon1: Any, lat2: Any, lon2: Any) -> Optional[float]:
        try:
            a_lat = math.radians(float(lat1))
            a_lon = math.radians(float(lon1))
            b_lat = math.radians(float(lat2))
            b_lon = math.radians(float(lon2))
        except (TypeError, ValueError):
            return None

        d_lat = b_lat - a_lat
        d_lon = b_lon - a_lon
        hav = (
            math.sin(d_lat / 2) ** 2
            + math.cos(a_lat) * math.cos(b_lat) * math.sin(d_lon / 2) ** 2
        )
        return round(3958.7613 * 2 * math.asin(math.sqrt(hav)), 2)

    @staticmethod
    def _format_address(view_section: Dict[str, Any]) -> str:
        address = (view_section or {}).get("address") or {}
        return ", ".join(
            part
            for part in (
                str(address.get("lineOneString") or "").strip(),
                str(address.get("lineTwoString") or "").strip(),
            )
            if part
        ) or "Address unavailable"

    @classmethod
    def fetch_stores(
        cls,
        *,
        postal_code: str,
        latitude: float,
        longitude: float,
        token: str,
        operation_hashes: Dict[str, str],
        referer: str,
    ) -> List[Dict[str, Any]]:
        collection = cls.graphql_get(
            "ShopCollectionScoped",
            {
                "retailerSlug": "aldi",
                "postalCode": str(postal_code),
                "coordinates": {"latitude": float(latitude), "longitude": float(longitude)},
                "addressId": None,
                "allowCanonicalFallback": True,
            },
            operation_hashes,
            token=token,
            referer=referer,
        )
        shops = ((collection.get("shopCollection") or {}).get("shops") or [])
        if not shops:
            raise ValueError(f"No ALDI shops were returned for ZIP {postal_code}")

        by_location: Dict[str, Dict[str, Any]] = {}
        for shop in shops:
            location_id = str((shop or {}).get("retailerLocationId") or "").strip()
            shop_id = str((shop or {}).get("id") or "").strip()
            if not location_id or not shop_id:
                continue
            service_type = str((shop or {}).get("serviceType") or "").strip().lower()
            current = by_location.get(location_id)
            if current and ALDI_STORE_SERVICE_PRIORITY.get(current["service_type"], 99) <= ALDI_STORE_SERVICE_PRIORITY.get(service_type, 99):
                continue
            by_location[location_id] = {
                "store_id": location_id,
                "shop_id": shop_id,
                "service_type": service_type,
                "retailer_inventory_session_token": str(
                    (shop or {}).get("retailerInventorySessionToken") or ""
                ),
            }

        stores: List[Dict[str, Any]] = []
        for store in by_location.values():
            address_data = cls.graphql_get(
                "GetRetailerLocationAddress",
                {"id": store["store_id"]},
                operation_hashes,
                referer=referer,
            )
            location = address_data.get("retailerLocation") or {}
            coordinates = location.get("coordinates") or {}
            view_section = location.get("viewSection") or {}
            store_lat = coordinates.get("latitude")
            store_lon = coordinates.get("longitude")
            name = str(view_section.get("locationDisplayNameString") or "").strip() or f"ALDI #{store['store_id']}"
            stores.append(
                {
                    **store,
                    "name": name,
                    "address": cls._format_address(view_section),
                    "latitude": store_lat,
                    "longitude": store_lon,
                    "distance": cls.distance_miles(latitude, longitude, store_lat, store_lon),
                }
            )

        if not stores:
            raise ValueError(f"No usable ALDI stores were returned for ZIP {postal_code}")
        return sorted(
            stores,
            key=lambda store: (
                store.get("distance") is None,
                store.get("distance") if store.get("distance") is not None else float("inf"),
                store.get("store_id", ""),
            ),
        )

    @classmethod
    def fetch_item(
        cls,
        *,
        product_id: str,
        store: Dict[str, Any],
        postal_code: str,
        operation_hashes: Dict[str, str],
        referer: str,
    ) -> Dict[str, Any]:
        item_id = f"items_{store['store_id']}-{product_id}"
        data = cls.graphql_get(
            "Items",
            {
                "ids": [item_id],
                "postalCode": str(postal_code),
                "shopId": str(store["shop_id"]),
                "zoneId": ALDI_DEFAULT_ZONE_ID,
            },
            operation_hashes,
            referer=referer,
            allow_partial=True,
        )
        items = data.get("items") or []
        return items[0] if items else {}
