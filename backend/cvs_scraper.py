"""CVS stock checker using the retail inventory endpoint seen on PDP pages."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
import random
import re
import secrets
import shutil
import subprocess
import time
from typing import Any, Dict, Iterable, List
from urllib.parse import unquote, urlsplit, urlparse

import requests
try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - optional runtime dependency
    curl_requests = None
try:
    from scrapling.engines._browsers._stealth import StealthySession
except ImportError:  # pragma: no cover - optional runtime dependency
    StealthySession = None
try:
    import zendriver as zd
    from zendriver import cdp as zendriver_cdp
except ImportError:  # pragma: no cover - optional runtime dependency
    zd = None
    zendriver_cdp = None
try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional runtime dependency
    async_playwright = None

from config import CVS_PROXY_URLS, SEARCH_RADIUS_MILES, TARGET_ZIP_CODE

PROGRESS_UI_YIELD_SECONDS = 0.02

logger = logging.getLogger(__name__)
CURL_IMPERSONATION_TARGET = "chrome"
CVS_INVENTORY_PUBLIC_API_KEY = "a2ff75c6-2da7-4299-929d-d670d827ab4a"
CVS_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/141.0.0.0 Safari/537.36"
)
CVS_PLAYWRIGHT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)
CVS_BROWSER_SEC_CH_UA = '"Chromium";v="141", "Not?A_Brand";v="8"'
CVS_BROWSER_SEC_CH_UA_MOBILE = "?0"
CVS_BROWSER_SEC_CH_UA_PLATFORM = '"Windows"'


class CvsBlockedError(ValueError):
    """Raised when CVS edge blocks every configured route."""


class CvsDisabledError(ValueError):
    """Raised when CVS checks are disabled by configuration."""


class CvsStockChecker:
    """Check CVS local inventory using the PDP inventory service."""

    INVENTORY_URLS = (
        "https://www.cvs.com/RETAGPV3/Inventory/V1/getStoreDetailsAndInventory",
    )
    _proxy_urls_override: List[str] = []
    _store_cache_db: Any = None

    @classmethod
    def set_store_cache_db(cls, db: Any) -> None:
        cls._store_cache_db = db

    def __init__(self) -> None:
        self.progress_callback = None
        self.current_zip_code = TARGET_ZIP_CODE
        self.search_radius_miles = int(SEARCH_RADIUS_MILES or 20)
        self._blocked_until_by_product: Dict[str, float] = {}
        self._last_extracted_image_url = ""

    def _emit_progress(self, progress_info: Dict[str, Any]) -> None:
        if self.progress_callback:
            self.progress_callback(progress_info)

    @staticmethod
    def normalize_proxy_urls(raw_value: Any) -> List[str]:
        if isinstance(raw_value, (list, tuple, set)):
            candidates = [str(item or "").strip() for item in raw_value]
        else:
            candidates = re.split(r"[\r\n,;]+", str(raw_value or ""))

        normalized: List[str] = []
        seen = set()
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
    def _normalize_extracted_image_url(value: str) -> str:
        normalized = str(value or "").strip()
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
    def _convert_proxy_format(proxy_url: str) -> str:
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
        return proxy_url

    @staticmethod
    def _new_session(proxy_url: str = "") -> requests.Session:
        if curl_requests is not None:
            session = curl_requests.Session(impersonate=CURL_IMPERSONATION_TARGET)
        else:
            session = requests.Session()
        if proxy_url:
            converted = CvsStockChecker._convert_proxy_format(proxy_url)
            session.proxies.update({"http": converted, "https": converted})
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
        for env_var in (
            "CVS_ZENDRIVER_BROWSER_EXECUTABLE_PATH",
            "ZENDRIVER_BROWSER_EXECUTABLE_PATH",
            "BROWSER_EXECUTABLE_PATH",
            "CHROME_BINARY",
            "CHROMIUM_BINARY",
        ):
            configured_path = os.getenv(env_var, "").strip()
            if configured_path:
                if os.path.exists(configured_path):
                    logger.info("CVS zendriver browser path resolved from %s: %s", env_var, configured_path)
                    return configured_path
                logger.warning(
                    "CVS zendriver browser path set via %s but file does not exist: %s",
                    env_var,
                    configured_path,
                )

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
                logger.info("CVS zendriver browser auto-detected at %s", candidate)
                return candidate
        logger.warning("CVS zendriver could not find a browser executable on PATH or known locations")
        return None

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _zendriver_user_data_dir() -> str | None:
        configured = os.getenv("CVS_ZENDRIVER_USER_DATA_DIR", "").strip()
        if not configured:
            return None
        os.makedirs(configured, exist_ok=True)
        return configured

    @staticmethod
    def _blocked_cooldown_seconds() -> int:
        raw = os.getenv("CVS_BLOCK_COOLDOWN_MINUTES", "").strip()
        if not raw:
            return 30 * 60
        try:
            minutes = int(raw)
        except ValueError:
            return 30 * 60
        return max(0, minutes) * 60

    @classmethod
    def _zendriver_proxy_url(cls) -> str:
        explicit_proxy = os.getenv("CVS_ZENDRIVER_PROXY_URL", "").strip()
        if explicit_proxy:
            return cls._convert_proxy_format(explicit_proxy)
        candidates = cls._proxy_candidates()
        return cls._convert_proxy_format(str(candidates[0] if candidates else "").strip())

    @staticmethod
    def _playwright_enabled() -> bool:
        return CvsStockChecker._env_bool("CVS_PLAYWRIGHT_ENABLED", False)

    @staticmethod
    def _playwright_first() -> bool:
        return CvsStockChecker._env_bool("CVS_PLAYWRIGHT_FIRST", False)

    @staticmethod
    def _playwright_only_mode() -> bool:
        return CvsStockChecker._env_bool("CVS_PLAYWRIGHT_ONLY_MODE", False)

    @staticmethod
    def _playwright_headless() -> bool:
        return CvsStockChecker._env_bool("CVS_PLAYWRIGHT_HEADLESS", True)

    @staticmethod
    def _playwright_timeout_ms() -> int:
        raw = os.getenv("CVS_PLAYWRIGHT_TIMEOUT_MS", "").strip()
        if not raw:
            return 30000
        try:
            timeout_ms = int(raw)
        except ValueError:
            return 30000
        return max(5000, min(timeout_ms, 120000))

    @staticmethod
    def _playwright_inventory_wait_ms() -> int:
        raw = os.getenv("CVS_PLAYWRIGHT_INVENTORY_WAIT_MS", "").strip()
        if not raw:
            return 25000
        try:
            timeout_ms = int(raw)
        except ValueError:
            return 25000
        return max(5000, min(timeout_ms, 120000))

    @staticmethod
    def _playwright_timezone() -> str:
        return os.getenv("CVS_PLAYWRIGHT_TIMEZONE", "America/New_York").strip() or "America/New_York"

    @staticmethod
    def _playwright_browser_executable_path() -> str | None:
        for env_var in (
            "CVS_PLAYWRIGHT_BROWSER_EXECUTABLE_PATH",
            "PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH",
            "CVS_ZENDRIVER_BROWSER_EXECUTABLE_PATH",
            "CHROME_BINARY",
            "CHROMIUM_BINARY",
        ):
            configured_path = os.getenv(env_var, "").strip()
            if configured_path and os.path.exists(configured_path):
                logger.info("CVS Playwright browser path resolved from %s: %s", env_var, configured_path)
                return configured_path
        return CvsStockChecker._zendriver_browser_executable_path()

    @staticmethod
    def _playwright_proxy_candidates() -> List[str]:
        candidates: List[str] = []
        for env_var in ("CVS_PLAYWRIGHT_PROXY_URLS", "CVS_PROXY_URLS"):
            raw = os.getenv(env_var, "").strip()
            if raw:
                candidates.extend(part.strip() for part in raw.split(",") if part.strip())
        for env_var in ("CVS_PLAYWRIGHT_PROXY_URL", "CVS_PROXY_URL"):
            raw = os.getenv(env_var, "").strip()
            if raw:
                candidates.append(raw)
        for candidate in CvsStockChecker._proxy_candidates():
            if candidate:
                candidates.append(candidate)

        deduped: List[str] = []
        seen = set()
        for candidate in candidates:
            normalized = str(candidate or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)

        if CvsStockChecker._env_bool("CVS_PLAYWRIGHT_INCLUDE_DIRECT", True):
            deduped.append("")
        return deduped or [""]

    @staticmethod
    def _parse_playwright_proxy(proxy_value: str) -> Dict[str, str] | None:
        raw = str(proxy_value or "").strip().strip("'\"")
        if not raw:
            return None

        if "://" not in raw:
            parts = raw.split(":")
            if len(parts) >= 4:
                host = parts[0].strip()
                port = parts[1].strip()
                username = parts[2].strip()
                password = ":".join(parts[3:]).strip()
                if host and port and username:
                    return {
                        "server": f"http://{host}:{port}",
                        "username": username,
                        "password": password,
                    }

        normalized = raw if re.match(r"^[a-z]+://", raw, re.IGNORECASE) else f"http://{raw}"
        parsed = urlsplit(normalized)
        if not parsed.scheme or not parsed.hostname:
            return None

        server = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            server = f"{server}:{parsed.port}"

        result = {"server": server}
        if parsed.username:
            result["username"] = unquote(parsed.username)
        if parsed.password:
            result["password"] = unquote(parsed.password)
        return result

    @staticmethod
    def _detect_browser_challenge(text: str) -> str:
        normalized = str(text or "").lower()
        if "_incapsula_resource" in normalized or "incapsula" in normalized:
            return "incapsula"
        if "captcha" in normalized:
            return "captcha"
        if "access denied" in normalized:
            return "access_denied"
        return ""

    @staticmethod
    def _playwright_use_node_script() -> bool:
        return CvsStockChecker._env_bool("CVS_PLAYWRIGHT_USE_NODE_SCRIPT", True)

    @staticmethod
    def _playwright_node_bin() -> str:
        return os.getenv("CVS_PLAYWRIGHT_NODE_BIN", "node").strip() or "node"

    @classmethod
    def _playwright_node_timeout_seconds(cls) -> int:
        raw = os.getenv("CVS_PLAYWRIGHT_NODE_TIMEOUT_SECONDS", "").strip()
        default_timeout_seconds = 180
        if not raw:
            timeout_seconds = default_timeout_seconds
        else:
            try:
                timeout_seconds = int(raw)
            except ValueError:
                timeout_seconds = default_timeout_seconds

        configured_proxy_count = len(cls._configured_proxy_urls()) or 1
        recommended_timeout_seconds = max(60, min(480, configured_proxy_count * 45))
        timeout_seconds = max(timeout_seconds, recommended_timeout_seconds)
        try:
            return max(60, min(int(timeout_seconds), 480))
        except (TypeError, ValueError):
            return default_timeout_seconds

    @staticmethod
    def _scrapling_bootstrap_enabled() -> bool:
        return CvsStockChecker._env_bool("CVS_SCRAPLING_BOOTSTRAP_ENABLED", True)

    @staticmethod
    def _playwright_node_script_path() -> Path:
        configured = os.getenv("CVS_PLAYWRIGHT_NODE_SCRIPT_PATH", "").strip()
        if configured:
            return Path(configured).expanduser().resolve()
        return (Path(__file__).resolve().parent / "crawlee" / "cvs-xvfb-test.mjs").resolve()

    @staticmethod
    def _extract_node_script_result(output_text: str) -> Dict[str, Any]:
        marker = "__CVS_XVFB_RESULT__="
        for line in reversed(str(output_text or "").splitlines()):
            if not line.startswith(marker):
                continue
            payload_text = line[len(marker):].strip()
            try:
                payload = json.loads(payload_text)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"Could not parse CVS node-script result JSON: {payload_text[:200]}") from exc
            if isinstance(payload, dict):
                return payload
        raise ValueError("CVS node-script output did not include a machine-readable result marker")

    @staticmethod
    def _sync_cookies_to_session(session: requests.Session, cookies: Any) -> int:
        cookie_items: List[Dict[str, Any]] = []
        if isinstance(cookies, dict):
            cookie_items = [{"name": key, "value": value} for key, value in cookies.items()]
        elif isinstance(cookies, (list, tuple)):
            for cookie in cookies:
                if isinstance(cookie, dict):
                    cookie_items.append(cookie)

        applied = 0
        for cookie in cookie_items:
            name = str(cookie.get("name") or "").strip()
            if not name:
                continue
            value = str(cookie.get("value") or "")
            kwargs: Dict[str, Any] = {}
            domain = str(cookie.get("domain") or "").strip()
            path = str(cookie.get("path") or "").strip()
            if domain:
                kwargs["domain"] = domain
            if path:
                kwargs["path"] = path
            try:
                session.cookies.set(name, value, **kwargs)
                applied += 1
            except Exception:
                continue
        return applied

    @staticmethod
    def _session_proxy_url(session: requests.Session) -> str:
        proxies = getattr(session, "proxies", {}) or {}
        for key in ("https", "http"):
            value = str(proxies.get(key) or "").strip()
            if value:
                return value
        return ""

    def _bootstrap_session_via_scrapling(
        self,
        session: requests.Session,
        product: Dict[str, Any],
        *,
        referer: str,
        bootstrap_urls: List[str],
    ) -> str:
        if StealthySession is None or not self._scrapling_bootstrap_enabled():
            return ""

        proxy_url = self._session_proxy_url(session)
        proxy_label = self._proxy_label(proxy_url) if proxy_url else "direct server IP"
        last_error: Exception | None = None

        for url in bootstrap_urls:
            logger.info("CVS bootstrap attempting Scrapling fallback via %s for %s", proxy_label, url)
            try:
                with StealthySession(
                    proxy=proxy_url or None,
                    headless=True,
                    real_chrome=True,
                    solve_cloudflare=True,
                    timeout=30_000,
                ) as browser_session:
                    response = browser_session.fetch(url, referer=referer)
                    if int(getattr(response, "status", 0) or 0) >= 400:
                        last_error = ValueError(f"HTTP {response.status} via {proxy_label}")
                        logger.warning(
                            "CVS bootstrap Scrapling fallback blocked for %s via %s with HTTP %s",
                            url,
                            proxy_label,
                            response.status,
                        )
                        continue

                    html = (getattr(response, "body", b"") or b"").decode("utf-8", errors="replace")
                    if not html.strip():
                        last_error = ValueError(f"empty bootstrap HTML via {proxy_label}")
                        continue

                    challenge = self._detect_browser_challenge(html)
                    if challenge:
                        last_error = ValueError(f"challenge={challenge} via {proxy_label}")
                        logger.warning(
                            "CVS bootstrap Scrapling fallback still saw challenge %s for %s via %s",
                            challenge,
                            url,
                            proxy_label,
                        )
                        continue

                    applied = self._sync_cookies_to_session(session, getattr(response, "cookies", ()))
                    api_key = self._extract_api_key(html)
                    logger.info(
                        "CVS bootstrap Scrapling fallback succeeded via %s for %s (cookies=%s, api_key=%s)",
                        proxy_label,
                        url,
                        applied,
                        bool(api_key),
                    )
                    return api_key
            except Exception as exc:
                last_error = exc
                logger.warning("CVS bootstrap Scrapling fallback failed for %s via %s: %s", url, proxy_label, exc)

        if last_error:
            logger.info("CVS bootstrap Scrapling fallback exhausted via %s: %s", proxy_label, last_error)
        return ""

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
        user_data_dir = cls._zendriver_user_data_dir()
        if user_data_dir:
            browser_kwargs["user_data_dir"] = user_data_dir
            logger.info("CVS zendriver using persistent user data dir: %s", user_data_dir)
        if cls._env_bool("CVS_ZENDRIVER_USE_PROXY", False):
            proxy_url = cls._zendriver_proxy_url()
            if proxy_url:
                proxy_label = cls._proxy_label(proxy_url)
                proxy_arg = f"--proxy-server={proxy_url}"
                browser_kwargs["browser_args"] = [proxy_arg]
                logger.info("CVS zendriver browser-context using proxy routing via %s", proxy_label)

        try:
            browser = await zd.start(**browser_kwargs)
        except TypeError as exc:
            browser_args = browser_kwargs.pop("browser_args", None)
            if browser_args:
                logger.warning("CVS zendriver rejected browser_args, retrying with args: %s", exc)
                browser_kwargs["args"] = browser_args
                browser = await zd.start(**browser_kwargs)
            else:
                raise
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

    @staticmethod
    async def _playwright_click_check_more_stores(page: Any) -> bool:
        clicked = await page.evaluate(
            """
            () => {
              for (const selector of ['a', 'button', 'span', '[role="button"]', 'div']) {
                for (const element of document.querySelectorAll(selector)) {
                  const text = (element.textContent || '').trim().toLowerCase();
                  if (text === 'check more stores' || text.includes('check more store')) {
                    element.scrollIntoView({ behavior: 'instant', block: 'center' });
                    element.click();
                    return true;
                  }
                }
              }
              return false;
            }
            """
        )
        if clicked:
            return True

        try:
            await page.get_by_text("Check more stores", exact=False).first.click(timeout=8000)
            return True
        except Exception:
            return False

    @staticmethod
    async def _playwright_submit_store_search(page: Any) -> str:
        return str(
            await page.evaluate(
                """
                () => {
                  const dialog = document.querySelector('[role="dialog"]');
                  if (!dialog) return '';
                  for (const element of dialog.querySelectorAll('button, [role="button"]')) {
                    const text = (element.textContent || '').trim().toLowerCase();
                    if (
                      text.includes('search') ||
                      text.includes('find') ||
                      text.includes('show') ||
                      text.includes('update') ||
                      text.includes('go')
                    ) {
                      element.click();
                      return text;
                    }
                  }
                  for (const element of dialog.querySelectorAll('button')) {
                    const text = (element.textContent || '').trim().toLowerCase();
                    if (text && !text.includes('close')) {
                      element.click();
                      return `fallback:${text}`;
                    }
                  }
                  return '';
                }
                """
            )
            or ""
        ).strip()

    @classmethod
    async def _fetch_inventory_payload_via_playwright_async(
        cls,
        *,
        product: Dict[str, Any],
        product_id: str,
        zip_code: str,
        api_key: str,
    ) -> Dict[str, Any]:
        if async_playwright is None:
            raise RuntimeError("playwright is not installed")

        product_url = str(product.get("source_url", "")).strip()
        if not product_url:
            raise ValueError("CVS Playwright flow requires a source_url product page")

        timeout_ms = cls._playwright_timeout_ms()
        inventory_wait_ms = cls._playwright_inventory_wait_ms()
        executable_path = cls._playwright_browser_executable_path()
        proxy_candidates = cls._playwright_proxy_candidates()
        if proxy_candidates:
            proxy_candidates = random.sample(proxy_candidates, len(proxy_candidates))
        failures: List[str] = []
        blocked_routes: List[str] = []

        async with async_playwright() as playwright:
            for proxy_value in proxy_candidates:
                proxy_label = cls._proxy_label(proxy_value) if proxy_value else "direct browser IP"
                proxy_config = cls._parse_playwright_proxy(proxy_value)
                launch_kwargs: Dict[str, Any] = {
                    "headless": cls._playwright_headless(),
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-accelerated-2d-canvas",
                        "--no-first-run",
                        "--no-zygote",
                        "--disable-gpu",
                        "--window-size=1920,1080",
                    ],
                }
                if executable_path:
                    launch_kwargs["executable_path"] = executable_path
                if proxy_config:
                    launch_kwargs["proxy"] = proxy_config
                    logger.info("CVS Playwright browser-context using proxy routing via %s", proxy_label)

                browser = None
                context = None
                response_tasks: List[asyncio.Task[Any]] = []
                try:
                    browser = await playwright.chromium.launch(**launch_kwargs)
                    context = await browser.new_context(
                        viewport={"width": 1920, "height": 1080},
                        user_agent=CVS_PLAYWRIGHT_USER_AGENT,
                        locale="en-US",
                        timezone_id=cls._playwright_timezone(),
                        permissions=["geolocation"],
                        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                    )
                    await context.add_init_script(
                        """
                        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                        Object.defineProperty(navigator, 'plugins', {
                          get: () => {
                            const plugins = [
                              { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                              { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                              { name: 'Native Client', filename: 'internal-nacl-plugin' }
                            ];
                            plugins.__proto__ = PluginArray.prototype;
                            return plugins;
                          }
                        });
                        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
                        Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
                        Object.defineProperty(screen, 'width', { get: () => 1920 });
                        Object.defineProperty(screen, 'height', { get: () => 1080 });
                        Object.defineProperty(screen, 'availWidth', { get: () => 1920 });
                        Object.defineProperty(screen, 'availHeight', { get: () => 1040 });
                        Object.defineProperty(screen, 'colorDepth', { get: () => 24 });
                        Object.defineProperty(screen, 'pixelDepth', { get: () => 24 });
                        Object.defineProperty(window, 'outerWidth', { get: () => 1920 });
                        Object.defineProperty(window, 'outerHeight', { get: () => 1080 });
                        window.chrome = window.chrome || {
                          runtime: {
                            connect: () => {},
                            sendMessage: () => {},
                            onMessage: { addListener: () => {} }
                          },
                          loadTimes: function () {},
                          csi: function () {},
                          app: {}
                        };
                        const originalPermissionsQuery = window.navigator.permissions.query.bind(window.navigator.permissions);
                        window.navigator.permissions.query = (parameters) =>
                          parameters && parameters.name === 'notifications'
                            ? Promise.resolve({ state: Notification.permission })
                            : originalPermissionsQuery(parameters);
                        const originalGetParameter = WebGLRenderingContext.prototype.getParameter;
                        WebGLRenderingContext.prototype.getParameter = function(parameter) {
                          if (parameter === 37445) return 'Intel Inc.';
                          if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                          return originalGetParameter.call(this, parameter);
                        };
                        """
                    )
                    page = await context.new_page()
                    inventory_payload: Dict[str, Any] = {}
                    inventory_event = asyncio.Event()

                    async def handle_response(response: Any) -> None:
                        if "getStoreDetailsAndInventory" not in str(getattr(response, "url", "")):
                            return
                        if int(getattr(response, "status", 0) or 0) != 200:
                            return
                        try:
                            data = await response.json()
                        except Exception:
                            return
                        if cls._looks_like_inventory_response(data):
                            inventory_payload["data"] = data
                            inventory_event.set()

                    def response_listener(response: Any) -> None:
                        response_tasks.append(asyncio.create_task(handle_response(response)))

                    async def inventory_route_handler(route: Any) -> None:
                        request = route.request
                        body_text = request.post_data or ""
                        if body_text:
                            try:
                                body = json.loads(body_text)
                            except ValueError:
                                body = None
                            request_payload = body.get("getStoreDetailsAndInventoryRequest") if isinstance(body, dict) else None
                            if isinstance(request_payload, dict):
                                request_payload["addressLine"] = zip_code
                                request_payload["geolatitude"] = ""
                                request_payload["geolongitude"] = ""
                                if api_key and isinstance(request_payload.get("header"), dict):
                                    request_payload["header"]["apiKey"] = request_payload["header"].get("apiKey") or api_key
                                await route.continue_(post_data=json.dumps(body))
                                return
                        await route.continue_()

                    page.on("response", response_listener)
                    await page.route("**/*getStoreDetailsAndInventory*", inventory_route_handler)
                    await page.goto(product_url, wait_until="domcontentloaded", timeout=timeout_ms)
                    await page.wait_for_timeout(5000)

                    html = await page.content()
                    challenge = cls._detect_browser_challenge(html)
                    if challenge:
                        blocked_routes.append(proxy_label)
                        raise ValueError(f"CVS browser page challenge ({challenge})")

                    await page.evaluate("window.scrollTo(0, 600)")
                    await page.wait_for_timeout(1500)

                    button_clicked = await cls._playwright_click_check_more_stores(page)
                    if button_clicked:
                        await page.wait_for_selector('[role="dialog"] input[type="text"]', timeout=8000)
                        modal_input = page.locator('[role="dialog"] input[type="text"]').first
                        await modal_input.click()
                        await modal_input.press("Control+A")
                        await modal_input.press("Backspace")
                        await modal_input.type(zip_code, delay=80)
                        await page.wait_for_timeout(500)

                        search_action = await cls._playwright_submit_store_search(page)
                        if search_action:
                            logger.info("CVS Playwright dialog submit action: %s", search_action)
                        else:
                            await modal_input.press("Enter")
                    else:
                        logger.info("CVS Playwright could not find 'Check more stores'; waiting for existing inventory calls")

                    try:
                        await asyncio.wait_for(inventory_event.wait(), timeout=inventory_wait_ms / 1000)
                    except asyncio.TimeoutError as exc:
                        raise ValueError("CVS Playwright flow did not capture an inventory response in time") from exc

                    if cls._looks_like_inventory_response(inventory_payload.get("data")):
                        logger.info("CVS inventory response accepted from browser page context via Playwright")
                        return inventory_payload["data"]
                    raise ValueError("CVS Playwright flow did not return a valid inventory payload")
                except Exception as exc:
                    failures.append(f"{proxy_label} {type(exc).__name__}: {exc}")
                    logger.warning("CVS Playwright attempt failed via %s: %s", proxy_label, exc)
                finally:
                    for task in response_tasks:
                        if not task.done():
                            task.cancel()
                    if context is not None:
                        await context.close()
                    if browser is not None:
                        await browser.close()

        unique_blocked_routes = list(dict.fromkeys(blocked_routes))
        if unique_blocked_routes and len(unique_blocked_routes) == len({cls._proxy_label(value) if value else "direct browser IP" for value in proxy_candidates}):
            raise CvsBlockedError(
                "CVS Playwright flow hit blocking on every configured route: "
                + ", ".join(unique_blocked_routes)
            )

        failure_detail = "; ".join(failures[:4]).strip()
        if failure_detail:
            raise ValueError(f"CVS Playwright flow failed: {failure_detail}")
        raise ValueError("CVS Playwright flow did not return store inventory")

    def _fetch_inventory_payload_via_playwright(
        self,
        *,
        product: Dict[str, Any],
        product_id: str,
        zip_code: str,
        api_key: str,
    ) -> Dict[str, Any]:
        self._last_extracted_image_url = ""
        if not self._playwright_use_node_script():
            return self._run_async(
                self._fetch_inventory_payload_via_playwright_async(
                    product=product,
                    product_id=product_id,
                    zip_code=zip_code,
                    api_key=api_key,
                )
            )

        script_path = self._playwright_node_script_path()
        if not script_path.exists():
            raise ValueError(f"CVS Playwright node script not found: {script_path}")

        product_url = str(product.get("source_url", "")).strip()
        if not product_url:
            raise ValueError("CVS Playwright node-script flow requires a source_url product page")

        command = [
            self._playwright_node_bin(),
            str(script_path.name),
            product_url,
            zip_code,
            str(self.search_radius_miles),
        ]
        env = os.environ.copy()
        env["CVS_XVFB_API_KEY"] = api_key
        env["CVS_XVFB_HEADLESS"] = "1" if self._playwright_headless() else "0"
        env["CVS_XVFB_TIMEOUT_MS"] = str(self._playwright_timeout_ms())
        env["CVS_XVFB_INVENTORY_WAIT_MS"] = str(self._playwright_inventory_wait_ms())
        env["CVS_XVFB_RANGE_MILES"] = str(self.search_radius_miles)
        configured_proxy_urls = self._configured_proxy_urls()
        if configured_proxy_urls:
            shuffled_proxies = random.sample(configured_proxy_urls, len(configured_proxy_urls))
            env["CVS_PROXY_URLS"] = ",".join(shuffled_proxies)
            env["CVS_XVFB_PROXY_URLS"] = ",".join(shuffled_proxies)

        logger.info("CVS inventory attempting Node/Playwright browser-context fetch via %s", script_path)
        completed = subprocess.run(
            command,
            cwd=str(script_path.parent),
            capture_output=True,
            text=True,
            timeout=self._playwright_node_timeout_seconds(),
            env=env,
        )
        combined_output = "\n".join(
            part for part in (
                str(completed.stdout or "").strip(),
                str(completed.stderr or "").strip(),
            ) if part
        )

        try:
            result = self._extract_node_script_result(combined_output)
        except Exception as exc:
            snippet = combined_output[-1500:] if combined_output else ""
            raise ValueError(
                "CVS Playwright node-script run did not produce a parseable result"
                + (f": {snippet}" if snippet else "")
            ) from exc

        extracted_image_url = self._normalize_extracted_image_url(str(result.get("image_url") or "").strip())
        if extracted_image_url:
            self._last_extracted_image_url = extracted_image_url

        if result.get("ok") and self._looks_like_inventory_response(result.get("payload")):
            logger.info("CVS inventory response accepted from Node/Playwright browser-context flow")
            return result["payload"]

        attempts = result.get("attempts")
        if isinstance(attempts, list) and attempts:
            normalized_attempts = [attempt for attempt in attempts if isinstance(attempt, dict)]
            errors = [str((attempt or {}).get("error") or "").strip() for attempt in normalized_attempts]
            blocked_attempts = []
            for attempt in normalized_attempts:
                challenge_type = str((attempt or {}).get("challengeType") or "").strip()
                challenge_detected = bool((attempt or {}).get("challengeDetected"))
                error_text = str((attempt or {}).get("error") or "").strip()
                if challenge_type or challenge_detected:
                    blocked_attempts.append(attempt)
                    continue
                lowered_error = error_text.lower()
                if "page challenge" in lowered_error or "access denied" in lowered_error or "incapsula" in lowered_error:
                    blocked_attempts.append(attempt)

            if blocked_attempts and len(blocked_attempts) == len(normalized_attempts):
                route_labels = [
                    str((attempt or {}).get("proxy") or "").strip()
                    for attempt in blocked_attempts
                    if str((attempt or {}).get("proxy") or "").strip()
                ]
                challenge_labels = list(
                    dict.fromkeys(
                        str((attempt or {}).get("challengeType") or "").strip()
                        for attempt in blocked_attempts
                        if str((attempt or {}).get("challengeType") or "").strip()
                    )
                )
                challenge_summary = f" ({', '.join(challenge_labels)})" if challenge_labels else ""
                raise CvsBlockedError(
                    "CVS Playwright node-script flow hit blocking on every configured route: "
                    + ", ".join(route_labels or ["browser route"])
                    + challenge_summary
                )

            deduped_errors = list(dict.fromkeys(error for error in errors if error))
            failure_detail = "; ".join(deduped_errors)[:1000].strip()
            if failure_detail:
                raise ValueError(f"CVS Playwright node-script flow failed: {failure_detail}")

        fatal_message = str(result.get("fatal") or "").strip()
        if fatal_message:
            raise ValueError(f"CVS Playwright node-script fatal error: {fatal_message}")

        snippet = combined_output[-1000:] if combined_output else ""
        raise ValueError(
            "CVS Playwright node-script flow did not return store inventory"
            + (f": {snippet}" if snippet else "")
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

        if not api_key:
            scrapling_api_key = self._bootstrap_session_via_scrapling(
                session,
                product,
                referer=referer,
                bootstrap_urls=bootstrap_urls,
            )
            if scrapling_api_key:
                api_key = scrapling_api_key
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

    def _try_direct_api(self, product_id: str, zip_code: str, referer: str) -> Dict[str, Any] | None:
        """Attempt a direct curl_cffi POST to the CVS inventory API without any page load.

        Uses the known public API key and Chrome TLS impersonation.  Returns the
        parsed inventory payload on success, or None if the attempt fails so the
        caller can fall back to Playwright.
        """
        if curl_requests is None:
            logger.debug("CVS direct-API path skipped: curl_cffi not installed")
            return None

        api_key = CVS_INVENTORY_PUBLIC_API_KEY
        if not api_key:
            return None

        proxy_candidates = self._proxy_candidates() or []
        # Always try the direct server IP first — proxy IPs may be blocked at the
        # API level even when the server's own IP is not.
        shuffled_proxies = random.sample([p for p in proxy_candidates if p], len([p for p in proxy_candidates if p]))
        candidates = [""] + shuffled_proxies

        for proxy_url in candidates:
            proxy_label = self._proxy_label(proxy_url) if proxy_url else "direct server IP"
            try:
                session = curl_requests.Session(impersonate=CURL_IMPERSONATION_TARGET)
                if proxy_url:
                    converted = self._convert_proxy_format(proxy_url)
                    session.proxies = {"http": converted, "https": converted}

                payload = {
                    "getStoreDetailsAndInventoryRequest": {
                        "header": self._request_body_header(api_key, secrets.token_hex(8)),
                        "productId": product_id,
                        "geolatitude": "",
                        "geolongitude": "",
                        "addressLine": zip_code,
                    }
                }
                headers = self._request_headers(
                    referer=referer,
                    purpose="inventory",
                    api_key=api_key,
                    include_api_header=True,
                )

                logger.info("CVS direct-API attempt via %s", proxy_label)
                response = session.post(
                    self.INVENTORY_URLS[0],
                    json=payload,
                    headers=headers,
                    timeout=25,
                )
                session.close()

                if response.status_code == 403:
                    logger.warning("CVS direct-API blocked (403) via %s", proxy_label)
                    continue

                data = response.json()
                if self._looks_like_inventory_response(data):
                    logger.info("CVS direct-API success via %s", proxy_label)
                    return data

                logger.debug("CVS direct-API unexpected payload via %s: HTTP %s", proxy_label, response.status_code)
            except Exception as exc:
                logger.debug("CVS direct-API attempt failed via %s: %s", proxy_label, exc)
                continue

        return None

    def _fetch_inventory_payload(self, product: Dict[str, Any], zip_code: str) -> Dict[str, Any]:
        if self._env_bool("CVS_DISABLED", False):
            raise CvsDisabledError("CVS checks are disabled by CVS_DISABLED")
        product_id = str(
            product.get("product_id")
            or product.get("article_id")
            or product.get("id")
            or ""
        ).strip()
        if not product_id:
            raise ValueError("CVS products require a numeric product id")
        blocked_until = float(self._blocked_until_by_product.get(product_id, 0))
        now = time.time()
        if blocked_until > now:
            remaining_seconds = int(blocked_until - now)
            raise CvsBlockedError(
                "CVS inventory temporarily paused after edge blocking "
                f"(retry in ~{max(1, remaining_seconds // 60)} minute(s))"
            )

        failures: List[str] = []
        referer = str(product.get("source_url", "")).strip() or "https://www.cvs.com/"
        api_key_candidates = list(dict.fromkeys(key for key in (CVS_INVENTORY_PUBLIC_API_KEY,) if key))
        browser_only_mode = self._env_bool("CVS_BROWSER_ONLY_MODE", False)
        playwright_enabled = self._playwright_enabled() or self._playwright_only_mode()
        playwright_first = self._playwright_first() or self._playwright_only_mode()

        # ── Direct API fast-path ─────────────────────────────────────────────
        # Always try a raw curl_cffi POST first — no browser, no page load, no
        # Cloudflare challenge page.  If the API key + TLS fingerprint are accepted
        # we return immediately without ever spawning a browser subprocess.
        # Only skipped when CVS_BROWSER_ONLY_MODE forces browser-only behaviour.
        if not browser_only_mode:
            result = self._try_direct_api(product_id, zip_code, referer)
            if result is not None:
                return result
            logger.info("CVS direct-API fast-path did not succeed; falling back to browser automation")
        # ────────────────────────────────────────────────────────────────────

        if playwright_enabled and playwright_first and api_key_candidates:
            try:
                logger.info("CVS inventory attempting Playwright browser-context fetch")
                return self._fetch_inventory_payload_via_playwright(
                    product=product,
                    product_id=product_id,
                    zip_code=zip_code,
                    api_key=api_key_candidates[0],
                )
            except Exception as exc:
                failures.append(f"playwright browser context {type(exc).__name__}: {exc}")
                logger.warning("CVS Playwright inventory attempt failed: %s", exc)
                if self._playwright_only_mode():
                    if "403" in str(exc).lower() or "block" in str(exc).lower():
                        cooldown_seconds = self._blocked_cooldown_seconds()
                        if cooldown_seconds > 0:
                            self._blocked_until_by_product[product_id] = time.time() + cooldown_seconds
                    raise

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
                if browser_only_mode:
                    if "HTTP 403" in str(exc):
                        cooldown_seconds = self._blocked_cooldown_seconds()
                        if cooldown_seconds > 0:
                            self._blocked_until_by_product[product_id] = time.time() + cooldown_seconds
                    raise CvsBlockedError(
                        "CVS browser-context inventory fetch failed while CVS_BROWSER_ONLY_MODE is enabled"
                    ) from exc

        if playwright_enabled and not playwright_first and api_key_candidates:
            try:
                logger.info("CVS inventory attempting Playwright browser-context fetch")
                return self._fetch_inventory_payload_via_playwright(
                    product=product,
                    product_id=product_id,
                    zip_code=zip_code,
                    api_key=api_key_candidates[0],
                )
            except Exception as exc:
                failures.append(f"playwright browser context {type(exc).__name__}: {exc}")
                logger.warning("CVS Playwright inventory attempt failed: %s", exc)
                if self._playwright_only_mode():
                    if "403" in str(exc).lower() or "block" in str(exc).lower():
                        cooldown_seconds = self._blocked_cooldown_seconds()
                        if cooldown_seconds > 0:
                            self._blocked_until_by_product[product_id] = time.time() + cooldown_seconds
                    raise

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
            cooldown_seconds = self._blocked_cooldown_seconds()
            if cooldown_seconds > 0:
                self._blocked_until_by_product[product_id] = time.time() + cooldown_seconds
            raise CvsBlockedError(
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

    def _filter_locations_by_search_radius(self, locations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        radius = self._normalize_distance(self.search_radius_miles)
        if radius is None or radius <= 0:
            return list(locations)

        filtered_locations = [
            store
            for store in locations
            if (distance := self._normalize_distance((store or {}).get("dt"))) is not None and distance <= radius
        ]
        logger.info(
            "CVS inventory applying search radius %.1f miles: kept %s of %s stores",
            radius,
            len(filtered_locations),
            len(locations),
        )
        return filtered_locations

    def _store_detail(self, store: Dict[str, Any]) -> Dict[str, Any]:
        store_id = str(
            store.get("storeId")
            or (store.get("bopis") or {}).get("storeId")
            or (store.get("bopus") or {}).get("storeId")
            or (store.get("ShopMyStore") or {}).get("storeId")
            or (store.get("SDD") or {}).get("storeId")
            or ""
        ).strip()

        db = self._store_cache_db
        latitude = None
        longitude = None
        if db is not None and store_id:
            cached = db.get_cvs_store_location(store_id)
            if cached:
                latitude = cached.get("latitude")
                longitude = cached.get("longitude")

        return {
            "store_id": store_id,
            "name": f"CVS #{store_id}" if store_id else "CVS",
            "address": self._normalize_address(store),
            "distance": self._normalize_distance(store.get("dt")),
            "inventory_count": self._inventory_count(store),
            "latitude": latitude,
            "longitude": longitude,
            "pickup_available": True,
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
        self._last_extracted_image_url = ""

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

        locations_raw = list(payload.get("atgResponse") or response.get("atgResponse") or [])
        locations = self._filter_locations_by_search_radius(locations_raw)
        total_stores = len(locations)
        availability: Dict[str, bool] = {}
        store_details: Dict[str, Dict[str, Any]] = {}
        location_ids: List[str] = []

        # Update cache for all discovered stores
        db = self._store_cache_db
        if db is not None:
            # Group candidate details for ZIP cache
            candidate_items = []
            for store in locations_raw:
                try:
                    s_id = str(store.get("storeId") or "").strip()
                    if not s_id:
                        continue
                    
                    # Normalize address components
                    s_addr = str(store.get("storeAddress") or "").strip()
                    s_city = str(store.get("City") or "").strip()
                    s_state = str(store.get("State") or "").strip()
                    s_zip = str(store.get("Zipcode") or "").strip()

                    candidate_items.append({
                        "code": s_id,
                        "name": f"CVS #{s_id}",
                        "address": s_addr,
                        "city": s_city,
                        "state": s_state,
                        "zip": s_zip
                    })

                    # If no coordinates in DB, try to at least Geocode the store ZIP
                    # as a rough placeholder if available. 
                    # For now, we just ensure the record exists so we can add coords later.
                    existing = db.get_cvs_store_location(s_id)
                    if not existing:
                        db.store_cvs_store_location(
                            s_id, s_addr, s_city, s_state, s_zip, 
                            0.0, 0.0 # Placeholder
                        )
                except Exception as e:
                    logger.debug("Failed to cache CVS store location %s: %s", s_id, e)
            
            if candidate_items:
                db.store_cvs_store_candidates(zip_code, candidate_items)

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
            "_extracted_image_url": self._last_extracted_image_url,
        }
