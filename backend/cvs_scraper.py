"""CVS stock checker using the retail inventory endpoint seen on PDP pages."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import time
from typing import Any, Dict, Iterable, List
from urllib.parse import urlsplit

import requests
try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - optional runtime dependency
    curl_requests = None
try:
    import zendriver as zd
    from zendriver import cdp as zendriver_cdp
except ImportError:  # pragma: no cover - optional runtime dependency
    zd = None
    zendriver_cdp = None

from config import CVS_PROXY_URLS, TARGET_ZIP_CODE

PROGRESS_UI_YIELD_SECONDS = 0.02

logger = logging.getLogger(__name__)
CURL_IMPERSONATION_TARGET = "chrome"
CVS_INVENTORY_PUBLIC_API_KEY = "a2ff75c6-2da7-4299-929d-d670d827ab4a"
CVS_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/141.0.0.0 Safari/537.36"
)
CVS_BROWSER_SEC_CH_UA = '"Chromium";v="141", "Not?A_Brand";v="8"'
CVS_BROWSER_SEC_CH_UA_MOBILE = "?0"
CVS_BROWSER_SEC_CH_UA_PLATFORM = '"Windows"'


class CvsStockChecker:
    """Check CVS local inventory using the PDP inventory service."""

    INVENTORY_URLS = (
        "https://www.cvs.com/RETAGPV3/Inventory/V1/getStoreDetailsAndInventory",
    )

    def __init__(self) -> None:
        self.progress_callback = None
        self.current_zip_code = TARGET_ZIP_CODE

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        if self.progress_callback:
            self.progress_callback(progress_info)

    @staticmethod
    def _proxy_candidates() -> List[str]:
        proxies = list(CVS_PROXY_URLS)
        if len(proxies) <= 1:
            return proxies
        start = secrets.randbelow(len(proxies))
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
    def _request_headers(
        *,
        referer: str,
        purpose: str,
        api_key: str = "",
        include_api_header: bool = False,
    ) -> Dict[str, str]:
        headers = {
            "User-Agent": CVS_BROWSER_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Sec-CH-UA": CVS_BROWSER_SEC_CH_UA,
            "Sec-CH-UA-Mobile": CVS_BROWSER_SEC_CH_UA_MOBILE,
            "Sec-CH-UA-Platform": CVS_BROWSER_SEC_CH_UA_PLATFORM,
        }
        if purpose == "document":
            headers.update(
                {
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;q=0.9,"
                        "image/avif,image/webp,image/apng,*/*;q=0.8"
                    ),
                    "Referer": "https://www.cvs.com/",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin" if referer != "https://www.cvs.com/" else "none",
                    "Upgrade-Insecure-Requests": "1",
                }
            )
            return headers

        headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://www.cvs.com",
                "Referer": referer,
                "Priority": "u=1, i",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }
        )
        if include_api_header and api_key:
            headers["x-api-key"] = api_key
        return headers

    @staticmethod
    def _extract_api_key(html: str) -> str:
        patterns = (
            r'"x-api-key"\s*:\s*"([^"]+)"',
            r'"apiKey"\s*:\s*"([^"]+)"',
            r"['\"]x-api-key['\"]\s*,\s*['\"]([^'\"]+)['\"]",
        )
        for pattern in patterns:
            match = re.search(pattern, html or "", re.IGNORECASE)
            if match:
                return str(match.group(1) or "").strip()
        return ""

    @staticmethod
    def _zendriver_browser_executable_path() -> str | None:
        configured_path = os.getenv("CVS_ZENDRIVER_BROWSER_EXECUTABLE_PATH", "").strip()
        if configured_path:
            return configured_path

        for candidate in (
            shutil.which("google-chrome"),
            shutil.which("google-chrome-stable"),
            shutil.which("chromium"),
            shutil.which("chromium-browser"),
            shutil.which("chrome"),
            shutil.which("msedge"),
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ):
            if candidate and os.path.exists(candidate):
                return candidate
        return None

    @staticmethod
    def _run_async(coro: Any) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    @staticmethod
    def _remote_value(result: Any) -> Any:
        if isinstance(result, tuple) and result:
            first = result[0]
            return getattr(first, "value", None)
        return getattr(result, "value", None)

    @classmethod
    def _zendriver_payload(cls, product_id: str, zip_code: str, api_key: str) -> Dict[str, Any]:
        return {
            "getStoreDetailsAndInventoryRequest": {
                "header": cls._request_body_header(api_key, secrets.token_hex(8)),
                "productId": product_id,
                "addressLine": zip_code,
            }
        }

    @classmethod
    async def _fetch_inventory_payload_via_zendriver_async(
        cls,
        *,
        product_id: str,
        zip_code: str,
        referer: str,
        api_key: str,
    ) -> Dict[str, Any]:
        if zd is None or zendriver_cdp is None:
            raise RuntimeError("zendriver is not installed")

        browser_kwargs: Dict[str, Any] = {
            "headless": True,
            "sandbox": True,
        }
        browser_executable_path = cls._zendriver_browser_executable_path()
        if browser_executable_path:
            browser_kwargs["browser_executable_path"] = browser_executable_path

        browser = await zd.start(**browser_kwargs)
        try:
            page = await browser.get(referer)
            await page.sleep(8)
            payload = cls._zendriver_payload(product_id, zip_code, api_key)
            request_args = json.dumps(
                {
                    "url": cls.INVENTORY_URLS[0],
                    "headers": {
                        "accept": "application/json",
                        "content-type": "application/json",
                        "x-api-key": api_key,
                    },
                    "payload": payload,
                }
            )
            script = """
            (async () => {
              const requestArgs = %s;
              try {
                const response = await fetch(requestArgs.url, {
                  method: 'POST',
                  headers: requestArgs.headers,
                  body: JSON.stringify(requestArgs.payload),
                  credentials: 'include'
                });
                const text = await response.text();
                let jsonBody = null;
                try {
                  jsonBody = JSON.parse(text);
                } catch (parseError) {
                  jsonBody = null;
                }
                return {
                  status: response.status,
                  text,
                  jsonBody,
                };
              } catch (error) {
                return {
                  status: 0,
                  text: String(error || 'unknown browser fetch error'),
                  jsonBody: null,
                };
              }
            })()
            """ % request_args
            result = await page.send(
                zendriver_cdp.runtime.evaluate(
                    expression=script,
                    await_promise=True,
                    return_by_value=True,
                )
            )
            response_value = cls._remote_value(result) or {}
            data = response_value.get("jsonBody")
            if cls._looks_like_inventory_response(data):
                logger.info("CVS inventory response accepted from browser page context via zendriver")
                return data

            snippet = str(response_value.get("text") or "")[:200].replace("\n", " ")
            raise ValueError(
                "zendriver browser-context inventory request failed with "
                f"HTTP {response_value.get('status') or 'unknown'}: {snippet}"
            )
        finally:
            await browser.stop()

    def _fetch_inventory_payload_via_zendriver(
        self,
        *,
        product_id: str,
        zip_code: str,
        referer: str,
        api_key: str,
    ) -> Dict[str, Any]:
        return self._run_async(
            self._fetch_inventory_payload_via_zendriver_async(
                product_id=product_id,
                zip_code=zip_code,
                referer=referer,
                api_key=api_key,
            )
        )

    def _bootstrap_session(self, session: requests.Session, product: Dict[str, Any]) -> tuple[str, str]:
        referer = str(product.get("source_url", "")).strip() or "https://www.cvs.com/"
        api_key = ""
        bootstrap_urls = [referer]
        if referer != "https://www.cvs.com/":
            bootstrap_urls.append("https://www.cvs.com/")
        for url in bootstrap_urls:
            try:
                response = session.get(
                    url,
                    headers=self._request_headers(referer=referer, purpose="document"),
                    timeout=30,
                )
                if response.status_code >= 400:
                    logger.warning("CVS bootstrap blocked for %s with HTTP %s", url, response.status_code)
                    continue
                if not api_key and "html" in response.headers.get("content-type", "").lower():
                    api_key = self._extract_api_key(response.text)
                    if api_key:
                        logger.info("CVS bootstrap extracted API key from %s", url)
            except Exception as exc:
                logger.warning("CVS bootstrap request failed for %s: %s", url, exc)
                continue
        return referer, api_key

    @staticmethod
    def _request_body_header(api_key: str, device_token: str) -> Dict[str, Any]:
        return {
            "apiKey": api_key,
            "channelName": "WEB",
            "deviceToken": device_token,
            "deviceType": "DESKTOP",
            "responseFormat": "JSON",
            "securityType": "apiKey",
            "source": "CVS_WEB",
            "appName": "CVS_WEB",
            "lineOfBusiness": "RETAIL",
            "type": "rdp",
        }

    @classmethod
    def _payload_candidates(cls, product_id: str, zip_code: str, api_key: str) -> Iterable[Dict[str, Any]]:
        device_token = secrets.token_hex(8)
        request_header = cls._request_body_header(api_key, device_token) if api_key else {}

        if request_header:
            yield {
                "getStoreDetailsAndInventoryRequest": {
                    "header": request_header,
                    "productId": product_id,
                    "geolatitude": "",
                    "geolongitude": "",
                    "addressLine": zip_code,
                }
            }
            yield {
                "getStoreDetailsAndInventoryRequest": {
                    "header": request_header,
                    "productId": product_id,
                    "addressLine": zip_code,
                }
            }

        base_variants = [
            {"productId": product_id, "zipCode": zip_code},
            {"productId": product_id, "addressLine": zip_code},
            {"productId": product_id, "zip": zip_code},
            {"prodId": product_id, "zipCode": zip_code},
            {"itemId": product_id, "zipCode": zip_code},
            {"itemid": product_id, "zipCode": zip_code},
            {"productid": product_id, "zipCode": zip_code},
        ]
        for base in base_variants:
            yield dict(base)
            yield {**base, "qty": 1}
            yield {**base, "quantity": 1}
            yield {"request": dict(base)}

    @staticmethod
    def _looks_like_inventory_response(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        response = payload.get("response")
        if not isinstance(response, dict):
            return False
        locations = payload.get("atgResponse")
        if not isinstance(locations, list):
            locations = response.get("atgResponse")
        return isinstance(locations, list)

    @staticmethod
    def _is_access_denied_response(status_code: int, snippet: str) -> bool:
        if int(status_code or 0) != 403:
            return False
        normalized = str(snippet or "").lower()
        return any(
            marker in normalized
            for marker in (
                "access denied",
                "<html",
                "noindex",
                "forbidden",
                "request unsuccessful",
            )
        )

    def _fetch_inventory_payload(self, product: Dict[str, Any], zip_code: str) -> Dict[str, Any]:
        product_id = str(
            product.get("product_id")
            or product.get("article_id")
            or product.get("id")
            or ""
        ).strip()
        if not product_id:
            raise ValueError("CVS products require a numeric product id")

        failures: List[str] = []
        referer = str(product.get("source_url", "")).strip() or "https://www.cvs.com/"
        api_key_candidates = list(dict.fromkeys(key for key in (CVS_INVENTORY_PUBLIC_API_KEY,) if key))

        if api_key_candidates:
            try:
                logger.info("CVS inventory attempting zendriver browser-context fetch")
                return self._fetch_inventory_payload_via_zendriver(
                    product_id=product_id,
                    zip_code=zip_code,
                    referer=referer,
                    api_key=api_key_candidates[0],
                )
            except Exception as exc:
                failures.append(f"zendriver browser context {type(exc).__name__}: {exc}")
                logger.warning("CVS zendriver inventory attempt failed: %s", exc)

        proxy_candidates = self._proxy_candidates()
        if proxy_candidates:
            logger.info("CVS inventory will try %s proxy candidate(s)", len(proxy_candidates))
        else:
            proxy_candidates = [""]

        blocked_routes: List[str] = []
        for proxy_url in proxy_candidates:
            session = self._new_session(proxy_url)
            proxy_label = self._proxy_label(proxy_url) if proxy_url else "direct server IP"
            if proxy_url:
                logger.info("CVS requests are using proxy routing via %s", proxy_label)
            proxy_blocked = False

            try:
                referer, api_key = self._bootstrap_session(session, product)
                api_keys = list(dict.fromkeys(key for key in (api_key, CVS_INVENTORY_PUBLIC_API_KEY) if key))
                if api_keys:
                    logger.info("CVS inventory will try %s API key candidate(s)", len(api_keys))
                if not api_key and CVS_INVENTORY_PUBLIC_API_KEY:
                    logger.info("CVS inventory using HAR-derived public API key fallback")

                for inventory_url in self.INVENTORY_URLS:
                    if proxy_blocked:
                        break
                    for api_key_candidate in api_keys:
                        if proxy_blocked:
                            break
                        for payload in self._payload_candidates(product_id, zip_code, api_key_candidate):
                            if proxy_blocked:
                                break
                            for include_api_header in (False, True):
                                try:
                                    response = session.post(
                                        inventory_url,
                                        json=payload,
                                        headers=self._request_headers(
                                            referer=referer,
                                            purpose="inventory",
                                            api_key=api_key_candidate,
                                            include_api_header=include_api_header,
                                        ),
                                        timeout=30,
                                    )
                                except Exception as exc:
                                    failures.append(f"{proxy_label} {inventory_url} {type(exc).__name__}: {exc}")
                                    logger.warning("CVS inventory request exception for %s via %s: %s", inventory_url, proxy_label, exc)
                                    continue

                                try:
                                    data = response.json()
                                except ValueError:
                                    snippet = response.text[:200].replace("\n", " ")
                                    if self._is_access_denied_response(response.status_code, snippet):
                                        logger.warning(
                                            "CVS inventory blocked at edge for %s via %s: HTTP %s %s",
                                            inventory_url,
                                            proxy_label,
                                            response.status_code,
                                            snippet,
                                        )
                                        blocked_routes.append(proxy_label)
                                        proxy_blocked = True
                                        break
                                    failures.append(f"{proxy_label} {inventory_url} HTTP {response.status_code}: {snippet}")
                                    logger.warning(
                                        "CVS inventory non-JSON response from %s via %s: HTTP %s %s",
                                        inventory_url,
                                        proxy_label,
                                        response.status_code,
                                        snippet,
                                    )
                                    continue

                                if self._looks_like_inventory_response(data):
                                    logger.info(
                                        "CVS inventory response accepted from %s via %s (api_header=%s)",
                                        inventory_url,
                                        proxy_label,
                                        include_api_header,
                                    )
                                    return data

                                header = ((data.get("response") or {}).get("header") or {}) if isinstance(data, dict) else {}
                                failures.append(
                                    f"{proxy_label} {inventory_url} HTTP {response.status_code}: "
                                    f"{header.get('statusCode') or 'unknown'} {header.get('statusDesc') or 'unexpected payload'}"
                                )
                                logger.warning(
                                    "CVS inventory unexpected payload from %s via %s: HTTP %s statusCode=%s statusDesc=%s api_header=%s",
                                    inventory_url,
                                    proxy_label,
                                    response.status_code,
                                    header.get("statusCode") or "unknown",
                                    header.get("statusDesc") or "unexpected payload",
                                    include_api_header,
                                )
            finally:
                session.close()

        unique_blocked_routes = list(dict.fromkeys(blocked_routes))
        attempted_route_labels = [
            self._proxy_label(proxy_url) if proxy_url else "direct server IP"
            for proxy_url in proxy_candidates
        ]
        if unique_blocked_routes and len(unique_blocked_routes) == len(set(attempted_route_labels)):
            raise ValueError(
                "CVS blocked every configured route from the inventory API "
                f"(HTTP 403 Access Denied): {', '.join(unique_blocked_routes)}"
            )

        failure_detail = "; ".join(failures[:4]).strip()
        if failure_detail:
            raise ValueError(f"CVS inventory API did not accept the request shape: {failure_detail}")
        raise ValueError("CVS inventory API did not return store inventory")

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _candidate_quantities(cls, store: Dict[str, Any]) -> List[int]:
        quantities = [
            cls._safe_int(store.get("Qty")),
            cls._safe_int((store.get("bopis") or {}).get("Qty")),
            cls._safe_int((store.get("bopus") or {}).get("Qty")),
            cls._safe_int((store.get("ShopMyStore") or {}).get("Qty")),
            cls._safe_int((store.get("SDD") or {}).get("Qty")),
        ]
        return [qty for qty in quantities if qty > 0]

    @classmethod
    def _inventory_count(cls, store: Dict[str, Any]) -> int:
        positive = cls._candidate_quantities(store)
        if positive:
            return max(positive)
        return max(
            cls._safe_int(store.get("Qty")),
            cls._safe_int((store.get("bopis") or {}).get("Qty")),
            cls._safe_int((store.get("bopus") or {}).get("Qty")),
            cls._safe_int((store.get("ShopMyStore") or {}).get("Qty")),
            cls._safe_int((store.get("SDD") or {}).get("Qty")),
        )

    @classmethod
    def _inventory_in_stock(cls, store: Dict[str, Any]) -> bool:
        if cls._inventory_count(store) > 0:
            return True
        status_fields = (
            store.get("bopStatus"),
            store.get("bohStatus"),
            store.get("status"),
            (store.get("bopis") or {}).get("status"),
            (store.get("bopus") or {}).get("status"),
            (store.get("ShopMyStore") or {}).get("status"),
            (store.get("SDD") or {}).get("status"),
        )
        normalized_statuses = {
            str(value or "").strip().lower()
            for value in status_fields
            if str(value or "").strip()
        }
        return any("in stock" in status for status in normalized_statuses)

    @staticmethod
    def _normalize_address(store: Dict[str, Any]) -> str:
        street = str(store.get("storeAddress", "")).strip()
        city = str(store.get("City", "")).strip()
        state = str(store.get("State", "")).strip()
        zipcode = str(store.get("Zipcode", "")).strip()
        city_state_zip = " ".join(part for part in (state, zipcode) if part)
        location = ", ".join(part for part in (city, city_state_zip) if part)
        return ", ".join(part for part in (street, location) if part) or "Address unavailable"

    @staticmethod
    def _normalize_distance(value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _store_detail(self, store: Dict[str, Any]) -> Dict[str, Any]:
        store_id = str(
            store.get("storeId")
            or (store.get("bopis") or {}).get("storeId")
            or (store.get("bopus") or {}).get("storeId")
            or (store.get("ShopMyStore") or {}).get("storeId")
            or (store.get("SDD") or {}).get("storeId")
            or ""
        ).strip()
        return {
            "store_id": store_id,
            "name": f"CVS #{store_id}" if store_id else "CVS",
            "address": self._normalize_address(store),
            "distance": self._normalize_distance(store.get("dt")),
            "inventory_count": self._inventory_count(store),
            "latitude": None,
            "longitude": None,
        }

    def check_product_availability(
        self,
        product: Dict[str, Any],
        zip_code: str,
        product_index: int = 1,
        product_total: int = 1,
    ) -> Dict[str, Any]:
        product_name = str(product.get("name") or product.get("article_id") or "CVS product").strip()
        total_units = max(2, (product_total * 2) + 2)
        base_units = 1 + ((product_index - 1) * 2)
        self.current_zip_code = zip_code

        self._emit_progress(
            {
                "phase": "fetching_inventory",
                "message": f"Requesting CVS local inventory for {product_name}",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "CVS inventory API",
                "stores_processed": 0,
                "total_stores": 0,
                "stores_with_stock_current": 0,
                "completed_units": float(base_units),
                "total_units": float(total_units),
            }
        )

        payload = self._fetch_inventory_payload(product, zip_code)
        response = payload.get("response") or {}
        header = response.get("header") or {}
        if str(header.get("statusCode") or "").strip() not in {"", "0000"}:
            raise ValueError(
                f"CVS inventory API returned {header.get('statusCode')}: {header.get('statusDesc') or 'Unknown error'}"
            )

        locations = payload.get("atgResponse") or response.get("atgResponse") or []
        total_stores = len(locations)
        availability: Dict[str, bool] = {}
        store_details: Dict[str, Dict[str, Any]] = {}
        location_ids: List[str] = []

        for index, store in enumerate(locations, start=1):
            detail = self._store_detail(store)
            store_id = detail["store_id"]
            if not store_id:
                continue

            location_ids.append(store_id)
            in_stock = self._inventory_in_stock(store)
            availability[store_id] = in_stock
            if in_stock:
                store_details[store_id] = detail

            self._emit_progress(
                {
                    "phase": "processing_results",
                    "message": f"Reviewed {index} of {max(total_stores, 1)} CVS stores for {product_name}",
                    "product": product_name,
                    "product_index": product_index,
                    "product_total": product_total,
                    "store_id": store_id,
                    "store_name": detail["name"],
                    "stores_processed": index,
                    "total_stores": total_stores,
                    "stores_with_stock_current": len(store_details),
                    "completed_units": float(base_units + 1 + (index / max(total_stores, 1))),
                    "total_units": float(total_units),
                }
            )
            if self.progress_callback and total_stores > 1:
                time.sleep(PROGRESS_UI_YIELD_SECONDS)

        self._emit_progress(
            {
                "phase": "product_complete",
                "message": f"Finished {product_name}: {len(store_details)} CVS stores in stock",
                "product": product_name,
                "product_index": product_index,
                "product_total": product_total,
                "store_name": "Product scan complete",
                "stores_processed": total_stores,
                "total_stores": total_stores,
                "stores_with_stock_current": len(store_details),
                "completed_units": float(base_units + 2),
                "total_units": float(total_units),
            }
        )

        return {
            "availability": availability,
            "stores": store_details,
            "location_ids": location_ids,
        }
