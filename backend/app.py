"""Flask application for the hosted Walgreens Stock Watcher."""

from __future__ import annotations

import logging
import os
import secrets
import shutil
import sys
import threading
import time
import ipaddress
import json
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import urlopen

from flask import Flask, Response, abort, g, jsonify, request, send_from_directory, session
try:
    from flask_compress import Compress
except ImportError:  # pragma: no cover - optional until dependencies are installed
    Compress = None
from flask_cors import CORS
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (  # noqa: E402
    ADMIN_PANEL_PASSWORD,
    CORS_ALLOWED_ORIGINS,
    FLASK_SECRET_KEY,
    GOOGLE_CLIENT_ID,
    SESSION_COOKIE_DOMAIN,
    SESSION_COOKIE_NAME,
    SESSION_COOKIE_SAMESITE,
    SESSION_COOKIE_SECURE,
)
from admin_notifications import AdminAlertService  # noqa: E402
from ace import AceBrowserClient  # noqa: E402
from cvs_scraper import CvsStockChecker  # noqa: E402
from database import StockDatabase  # noqa: E402
from product_resolver import resolve_product_link, CvsProductResolver  # noqa: E402
from scheduler import SchedulerManager  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="../frontend", static_url_path="")
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
INLINE_SCRIPT_NONCE_TOKEN = "__CSP_NONCE__"
FRONTEND_HTML_PAGES = {
    "admin.html",
    "index.html",
    "privacy.html",
    "terms.html",
    "disclosures.html",
}
NO_CACHE_FRONTEND_FILES = {
    "runtime-config.js",
    "sw.js",
    "manifest.webmanifest",
}
STATIC_ASSET_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60
app.secret_key = FLASK_SECRET_KEY
app.config.update(
    COMPRESS_LEVEL=6,
    COMPRESS_MIN_SIZE=1024,
    COMPRESS_MIMETYPES=[
        "text/html",
        "text/css",
        "text/plain",
        "text/javascript",
        "application/javascript",
        "application/json",
        "application/xml",
        "image/svg+xml",
    ],
    SESSION_COOKIE_NAME=SESSION_COOKIE_NAME,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=SESSION_COOKIE_SECURE,
    SESSION_COOKIE_SAMESITE=SESSION_COOKIE_SAMESITE,
)
if SESSION_COOKIE_DOMAIN:
    app.config["SESSION_COOKIE_DOMAIN"] = SESSION_COOKIE_DOMAIN

if Compress is not None:
    Compress(app)
else:
    logger.warning("Flask-Compress is not installed; responses will be sent without app-level compression")

CORS(
    app,
    resources={r"/api/*": {"origins": CORS_ALLOWED_ORIGINS}},
    supports_credentials=True,
    allow_headers=["Content-Type", "X-CSRF-Token"],
)

db = StockDatabase()
admin_alerts = AdminAlertService(db)
CvsStockChecker.set_proxy_urls_override(db.get_admin_settings().get("cvs_proxy_urls"))
CvsProductResolver.set_proxy_urls_override(db.get_admin_settings().get("cvs_proxy_urls"))
AceBrowserClient.set_proxy_urls_override(db.get_admin_settings().get("cvs_proxy_urls"))
scheduler_manager = SchedulerManager(db)
scheduler_manager.start_enabled_schedulers()
SERVICE_UPTIME_HEARTBEAT_SECONDS = 30
_uptime_tracker_started = False
ADMIN_SESSION_KEY = "admin_authenticated"
WAITLIST_SESSION_KEY = "waitlisted_user_id"
CSRF_SESSION_KEY = "csrf_token"
_system_stats_lock = threading.Lock()
_last_cpu_snapshot: Optional[tuple[float, float]] = None
_last_network_snapshot: Optional[Dict[str, float]] = None
COOKIE_NOTICE_COUNTRY_CODES = {
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GB",
    "GR",
    "HU",
    "IE",
    "IS",
    "IT",
    "LI",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
}
COUNTRY_LOOKUP_CACHE_TTL_SECONDS = 24 * 60 * 60
COUNTRY_LOOKUP_TIMEOUT_SECONDS = 2.5
_country_lookup_cache_lock = threading.Lock()
_country_lookup_cache: Dict[str, tuple[float, str]] = {}


def _is_allowed_walgreens_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (normalized == "walgreens.com" or normalized.endswith(".walgreens.com"))


def _is_allowed_cvs_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (
        normalized == "cvs.com"
        or normalized.endswith(".cvs.com")
        or normalized == "cvsassets.com"
        or normalized.endswith(".cvsassets.com")
    )


def _is_allowed_fivebelow_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (normalized == "fivebelow.com" or normalized.endswith(".fivebelow.com"))


def _is_allowed_fivebelow_image_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (
        normalized == "fbres.fivebelow.com" or normalized.endswith(".fbres.fivebelow.com")
    )


def _is_allowed_ace_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (normalized == "acehardware.com" or normalized.endswith(".acehardware.com"))


def _is_allowed_ace_image_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (
        _is_allowed_ace_host(normalized)
        or normalized == "mozu.com"
        or normalized.endswith(".mozu.com")
    )





def _is_allowed_product_source_host(hostname: str) -> bool:
    return (
        _is_allowed_walgreens_host(hostname)
        or _is_allowed_cvs_host(hostname)
        or _is_allowed_fivebelow_host(hostname)
        or _is_allowed_ace_host(hostname)
    )


def _is_allowed_product_image_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return (
        _is_allowed_product_source_host(normalized)
        or _is_allowed_fivebelow_image_host(normalized)
        or _is_allowed_ace_image_host(normalized)
    )


def _normalize_external_url(
    value: Any,
    *,
    field_name: str,
    host_validator=None,
    allow_empty: bool = True,
) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        if allow_empty:
            return ""
        raise ValueError(f"{field_name} is required")

    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{field_name} must be a valid HTTP or HTTPS URL")

    hostname = (parsed.hostname or "").lower()
    if host_validator and not host_validator(hostname):
        raise ValueError(f"{field_name} must point to an allowed retailer host")

    return parsed.geturl()


def _sanitize_product_source_url(value: Any, *, allow_empty: bool = True) -> str:
    return _normalize_external_url(
        value,
        field_name="Product source URL",
        host_validator=_is_allowed_product_source_host,
        allow_empty=allow_empty,
    )


def _sanitize_product_image_url(value: Any, *, allow_empty: bool = True) -> str:
    return _normalize_external_url(
        value,
        field_name="Product image URL",
        host_validator=_is_allowed_product_image_host,
        allow_empty=allow_empty,
    )


def _build_content_security_policy(nonce: str) -> str:
    directives = {
        "default-src": ["'self'"],
        "base-uri": ["'self'"],
        "object-src": ["'none'"],
        "frame-ancestors": ["'none'"],
        "form-action": ["'self'"],
        "manifest-src": ["'self'"],
        "worker-src": ["'self'"],
        "script-src": [
            "'self'",
            f"'nonce-{nonce}'",
            "https://accounts.google.com",
            "https://unpkg.com",
        ],
        "style-src": [
            "'self'",
            "'unsafe-inline'",
            "https://fonts.googleapis.com",
            "https://unpkg.com",
        ],
        "font-src": [
            "'self'",
            "https://fonts.gstatic.com",
            "data:",
        ],
        "img-src": [
            "'self'",
            "data:",
            "https:",
        ],
        "connect-src": [
            "'self'",
            "https://api.frub.dev",
            "http://localhost:5000",
            "http://127.0.0.1:5000",
            "https://accounts.google.com",
            "https://*.google.com",
            "https://*.googleapis.com",
            "https://nominatim.openstreetmap.org",
        ],
        "frame-src": [
            "https://accounts.google.com",
            "https://*.google.com",
        ],
    }
    return "; ".join(
        f"{directive} {' '.join(sources)}" for directive, sources in directives.items()
    )


def _serve_frontend_html_file(file_name: str) -> Response:
    if file_name not in FRONTEND_HTML_PAGES:
        abort(404)

    html_path = FRONTEND_DIR / file_name
    if not html_path.is_file():
        abort(404)

    nonce = secrets.token_urlsafe(16)
    g.csp_nonce = nonce
    html = html_path.read_text(encoding="utf-8").replace(
        INLINE_SCRIPT_NONCE_TOKEN,
        nonce,
    )
    response = Response(html, mimetype="text/html")
    response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
    return response


def _apply_frontend_cache_headers(response: Response, path: str) -> Response:
    normalized_path = str(path or "").lstrip("/")
    if not normalized_path:
        return response

    if normalized_path in FRONTEND_HTML_PAGES or normalized_path in NO_CACHE_FRONTEND_FILES:
        response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
        return response

    response.headers.setdefault(
        "Cache-Control",
        f"public, max-age={STATIC_ASSET_CACHE_MAX_AGE_SECONDS}",
    )
    return response


def _session_user() -> Optional[Dict[str, Any]]:
    user_id = session.get("user_id")
    if not user_id:
        return None
    return db.get_user_by_id(int(user_id))


def _session_waitlisted_user() -> Optional[Dict[str, Any]]:
    user_id = session.get(WAITLIST_SESSION_KEY)
    if not user_id:
        return None
    return db.get_user_by_id(int(user_id))


def _clear_user_session() -> None:
    session.pop("user_id", None)


def _clear_waitlist_session() -> None:
    session.pop(WAITLIST_SESSION_KEY, None)


def _session_admin_authenticated(user: Optional[Dict[str, Any]] = None) -> bool:
    stored_user_id = session.get(ADMIN_SESSION_KEY)
    if isinstance(stored_user_id, bool) or stored_user_id in (None, ""):
        return False

    user = user or _session_user()
    if user is None:
        return False

    try:
        return int(stored_user_id) == int(user["id"])
    except (TypeError, ValueError, KeyError):
        return False


def _clear_admin_session() -> None:
    session.pop(ADMIN_SESSION_KEY, None)


def _get_or_create_csrf_token() -> str:
    token = str(session.get(CSRF_SESSION_KEY) or "").strip()
    if token:
        return token

    token = secrets.token_urlsafe(32)
    session[CSRF_SESSION_KEY] = token
    return token


def _normalized_origin(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""

    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _allowed_api_origins() -> set[str]:
    origins = {_normalized_origin(origin) for origin in CORS_ALLOWED_ORIGINS}
    current_origin = _normalized_origin(request.host_url)
    if current_origin:
        origins.add(current_origin)
    return {origin for origin in origins if origin}


def _request_origin_allowed() -> bool:
    origin = _normalized_origin(request.headers.get("Origin", ""))
    if origin:
        return origin in _allowed_api_origins()

    referer = _normalized_origin(request.headers.get("Referer", ""))
    if referer:
        return referer in _allowed_api_origins()

    return False


def _normalize_country_code(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 2 or not normalized.isalpha():
        return ""
    return normalized


def _normalize_public_ip(value: Any) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return ""

    try:
        ip = ipaddress.ip_address(candidate)
    except ValueError:
        return ""

    if (
        ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_private
        or ip.is_reserved
        or ip.is_unspecified
    ):
        return ""

    return str(ip)


def _request_ip_for_country_lookup() -> str:
    remote_addr = _normalize_public_ip(request.remote_addr)
    if remote_addr:
        return remote_addr

    # Only trust forwarded addresses when the direct peer is local/private.
    forwarded_for = str(request.headers.get("X-Forwarded-For") or "").strip()
    if not forwarded_for:
        return ""

    for candidate in forwarded_for.split(","):
        normalized = _normalize_public_ip(candidate)
        if normalized:
            return normalized

    return ""


def _lookup_country_code_for_ip(ip_address: str) -> str:
    normalized_ip = _normalize_public_ip(ip_address)
    if not normalized_ip:
        return ""

    now = time.time()
    with _country_lookup_cache_lock:
        cached = _country_lookup_cache.get(normalized_ip)
        if cached and cached[0] > now:
            return cached[1]

    country_code = ""
    try:
        with urlopen(
            f"https://api.country.is/{quote(normalized_ip, safe='')}",
            timeout=COUNTRY_LOOKUP_TIMEOUT_SECONDS,
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
            country_code = _normalize_country_code(payload.get("country"))
    except (HTTPError, URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        logger.debug("Country lookup failed for %s: %s", normalized_ip, exc)

    with _country_lookup_cache_lock:
        _country_lookup_cache[normalized_ip] = (now + COUNTRY_LOOKUP_CACHE_TTL_SECONDS, country_code)

    return country_code


def _request_country_code() -> str:
    cached = getattr(g, "request_country_code", None)
    if cached is not None:
        return cached

    country_code = _lookup_country_code_for_ip(_request_ip_for_country_lookup())
    g.request_country_code = country_code
    return country_code


def _cookie_notice_required_for_request() -> bool:
    return _request_country_code() in COOKIE_NOTICE_COUNTRY_CODES


@app.before_request
def enforce_api_csrf() -> Optional[Response]:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None
    if not request.path.startswith("/api/"):
        return None
    if not _request_origin_allowed():
        return jsonify({"error": "Invalid request origin"}), 403

    expected = str(session.get(CSRF_SESSION_KEY) or "").strip()
    provided = str(request.headers.get("X-CSRF-Token") or "").strip()
    if not expected or not provided or not secrets.compare_digest(expected, provided):
        return jsonify({"error": "CSRF validation failed"}), 403

    return None


def _admin_panel_configured() -> bool:
    return bool(ADMIN_PANEL_PASSWORD)


def _start_user_session(user: Dict[str, Any]) -> None:
    session.clear()
    session.permanent = True
    session["user_id"] = int(user["id"])


def _start_waitlist_session(user: Dict[str, Any]) -> None:
    session.clear()
    session.permanent = True
    session[WAITLIST_SESSION_KEY] = int(user["id"])


def _waitlist_message_for_user(user: Dict[str, Any]) -> str:
    email = str(user.get("email") or "").strip()
    if email:
        return f"{email} was added to the waitlist. An admin needs to approve this account before it can use the app."
    return "Your account is on the waitlist. An admin needs to approve it before you can use the app."


def _transition_denied_user_session(user: Dict[str, Any]) -> None:
    _clear_admin_session()
    if bool(user.get("is_banned")):
        session.clear()
        return
    _start_waitlist_session(user)


def _access_denied_reason_for_user(user: Dict[str, Any]) -> Optional[str]:
    if bool(user.get("is_banned")):
        ban_reason = str(user.get("ban_reason") or "").strip()
        if ban_reason:
            return f"Your account has been banned. Reason: {ban_reason}"
        return "Your account has been banned."
    if not db.is_google_email_authorized(str(user.get("email") or "")):
        return _waitlist_message_for_user(user)
    return None


def _serialized_user(user: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not user:
        return None
    return {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "picture": user.get("picture", ""),
    }


def _record_audit_event(
    event_type: str,
    summary: str,
    *,
    actor_user: Optional[Dict[str, Any]] = None,
    target_user: Optional[Dict[str, Any]] = None,
    user_email: str = "",
    metadata: Optional[Dict[str, Any]] = None,
    alert_category: Optional[str] = None,
) -> Dict[str, Any]:
    event = db.record_audit_event(
        event_type,
        summary,
        actor_user_id=int(actor_user["id"]) if actor_user else None,
        target_user_id=int(target_user["id"]) if target_user else None,
        user_email=user_email or str((actor_user or {}).get("email") or ""),
        metadata=metadata,
    )
    event["actor_name"] = str((actor_user or {}).get("name") or "")
    event["actor_email"] = str((actor_user or {}).get("email") or "")
    event["target_name"] = str((target_user or {}).get("name") or "")
    event["target_email"] = str((target_user or {}).get("email") or "")
    if alert_category:
        admin_alerts.notify(category=alert_category, event=event)
    return event


def require_auth(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        user = _session_user()
        if user is None:
            return jsonify({"error": "Authentication required"}), 401
        access_denied_reason = _access_denied_reason_for_user(user)
        if access_denied_reason:
            _transition_denied_user_session(user)
            return jsonify({"error": access_denied_reason}), 403
        return func(user, *args, **kwargs)

    return wrapper


def require_admin(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not _admin_panel_configured():
            return jsonify({"error": "Admin panel password is not configured"}), 503
        user = _session_user()
        if user is None:
            _clear_admin_session()
            return jsonify({"error": "Google sign-in required for admin access"}), 401
        access_denied_reason = _access_denied_reason_for_user(user)
        if access_denied_reason:
            _transition_denied_user_session(user)
            return jsonify({"error": access_denied_reason}), 403
        if not _session_admin_authenticated(user):
            _clear_admin_session()
            return jsonify({"error": "Admin authentication required"}), 401
        return func(*args, **kwargs)

    return wrapper


def _current_scheduler(user: Dict[str, Any]):
    return scheduler_manager.get_or_create(int(user["id"]))


@app.after_request
def apply_security_headers(response: Response) -> Response:
    nonce = getattr(g, "csp_nonce", "")
    if nonce and response.mimetype == "text/html":
        response.headers["Content-Security-Policy"] = _build_content_security_policy(nonce)

    if request.method == "GET" and request.path.startswith("/api/"):
        if request.path == "/api/public-stats":
            response.headers.setdefault("Cache-Control", "public, max-age=60")
        else:
            response.headers.setdefault("Cache-Control", "no-store")

    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return response


def _google_client_configured() -> bool:
    return bool(GOOGLE_CLIENT_ID)


def _public_auth_payload(user: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    waitlisted_user = _session_waitlisted_user()
    access_denied_reason = ""
    access_state = "signed_out"

    if user:
        access_denied_reason = _access_denied_reason_for_user(user) or ""
        if access_denied_reason:
            _clear_admin_session()
            if bool(user.get("is_banned")):
                session.clear()
                user = None
                waitlisted_user = None
            else:
                _start_waitlist_session(user)
                user = None
                waitlisted_user = _session_waitlisted_user()
                access_state = "waitlisted"
        else:
            _clear_waitlist_session()
            access_state = "approved"
    elif waitlisted_user:
        waitlist_denial = _access_denied_reason_for_user(waitlisted_user)
        if waitlist_denial:
            access_denied_reason = waitlist_denial
            if bool(waitlisted_user.get("is_banned")):
                session.clear()
                waitlisted_user = None
            else:
                access_state = "waitlisted"
        else:
            _start_user_session(waitlisted_user)
            user = waitlisted_user
            waitlisted_user = None
            access_state = "approved"

    if user:
        access_state = "approved"
    elif waitlisted_user:
        access_state = "waitlisted"

    country_code = _request_country_code()

    return {
        "authenticated": bool(user),
        "google_client_id": GOOGLE_CLIENT_ID or "",
        "google_allowlist_enabled": True,
        "admin_panel_configured": _admin_panel_configured(),
        "csrf_token": _get_or_create_csrf_token(),
        "country_code": country_code,
        "cookie_notice_required": _cookie_notice_required_for_request(),
        "access_denied_reason": access_denied_reason,
        "access_state": access_state,
        "user": _serialized_user(user),
        "waitlisted_user": _serialized_user(waitlisted_user),
    }


def _public_admin_payload() -> Dict[str, Any]:
    user = _session_user()
    if user is None:
        _clear_admin_session()
    access_denied_reason = _access_denied_reason_for_user(user) if user else None
    if user and access_denied_reason:
        _transition_denied_user_session(user)
        user = None
    password_authenticated = bool(user) and _session_admin_authenticated(user)
    if user and not password_authenticated and session.get(ADMIN_SESSION_KEY) not in (None, ""):
        _clear_admin_session()
    country_code = _request_country_code()

    return {
        "authenticated": password_authenticated,
        "configured": _admin_panel_configured(),
        "google_authenticated": bool(user),
        "password_authenticated": password_authenticated,
        "google_client_id": GOOGLE_CLIENT_ID or "",
        "csrf_token": _get_or_create_csrf_token(),
        "country_code": country_code,
        "cookie_notice_required": _cookie_notice_required_for_request(),
        "access_denied_reason": access_denied_reason or "",
        "user": _serialized_user(user),
    }


def _logout_response() -> Response:
    response = jsonify({"success": True})
    # Expire both the current session cookie and any legacy host-only variant.
    response.delete_cookie(
        SESSION_COOKIE_NAME,
        path="/",
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
        httponly=True,
    )
    if SESSION_COOKIE_DOMAIN:
        response.delete_cookie(
            SESSION_COOKIE_NAME,
            path="/",
            domain=SESSION_COOKIE_DOMAIN,
            secure=SESSION_COOKIE_SECURE,
            samesite=SESSION_COOKIE_SAMESITE,
            httponly=True,
        )
    response.headers["Clear-Site-Data"] = '"cookies", "storage"'
    return response


def _stop_user_scheduler_if_running(user_id: int) -> None:
    scheduler = scheduler_manager.get_or_create(int(user_id))
    if scheduler.is_running:
        scheduler.stop()


def _read_linux_cpu_totals() -> Optional[tuple[float, float]]:
    try:
        with open("/proc/stat", "r", encoding="utf-8") as handle:
            first_line = handle.readline().strip()
    except OSError:
        return None

    parts = first_line.split()
    if len(parts) < 5 or parts[0] != "cpu":
        return None

    try:
        values = [float(value) for value in parts[1:]]
    except ValueError:
        return None

    idle = values[3] + (values[4] if len(values) > 4 else 0.0)
    total = sum(values)
    return total, idle


def _read_linux_memory_stats() -> Dict[str, Any]:
    values: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for raw_line in handle:
                if ":" not in raw_line:
                    continue
                key, raw_value = raw_line.split(":", 1)
                numeric = raw_value.strip().split()[0]
                values[key] = int(numeric) * 1024
    except (OSError, ValueError):
        return {
            "total_bytes": 0,
            "used_bytes": 0,
            "available_bytes": 0,
            "usage_percent": 0.0,
        }

    total_bytes = int(values.get("MemTotal", 0))
    available_bytes = int(values.get("MemAvailable", values.get("MemFree", 0)))
    used_bytes = max(total_bytes - available_bytes, 0)
    usage_percent = round((used_bytes / total_bytes) * 100, 1) if total_bytes else 0.0
    return {
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "available_bytes": available_bytes,
        "usage_percent": usage_percent,
    }


def _read_linux_network_totals() -> Dict[str, int]:
    received_bytes = 0
    transmitted_bytes = 0
    try:
        with open("/proc/net/dev", "r", encoding="utf-8") as handle:
            lines = handle.readlines()[2:]
    except OSError:
        return {
            "received_bytes": 0,
            "transmitted_bytes": 0,
        }

    for line in lines:
        if ":" not in line:
            continue
        interface, raw_values = line.split(":", 1)
        if interface.strip() == "lo":
            continue
        parts = raw_values.split()
        if len(parts) < 16:
            continue
        try:
            received_bytes += int(parts[0])
            transmitted_bytes += int(parts[8])
        except ValueError:
            continue

    return {
        "received_bytes": received_bytes,
        "transmitted_bytes": transmitted_bytes,
    }


def _system_cpu_usage_percent() -> float:
    global _last_cpu_snapshot

    current = _read_linux_cpu_totals()
    if current is None:
        return 0.0

    with _system_stats_lock:
        previous = _last_cpu_snapshot
        _last_cpu_snapshot = current

    if previous is None:
        return 0.0

    total_delta = current[0] - previous[0]
    idle_delta = current[1] - previous[1]
    if total_delta <= 0:
        return 0.0

    busy_ratio = max(0.0, min(1.0, 1.0 - (idle_delta / total_delta)))
    return round(busy_ratio * 100, 1)


def _system_network_rates() -> Dict[str, float]:
    global _last_network_snapshot

    totals = _read_linux_network_totals()
    now = time.time()

    with _system_stats_lock:
        previous = _last_network_snapshot
        _last_network_snapshot = {
            "timestamp": now,
            "received_bytes": float(totals["received_bytes"]),
            "transmitted_bytes": float(totals["transmitted_bytes"]),
        }

    if not previous:
        return {
            **totals,
            "received_bytes_per_second": 0.0,
            "transmitted_bytes_per_second": 0.0,
        }

    elapsed = max(now - float(previous.get("timestamp", now)), 0.001)
    received_rate = max(float(totals["received_bytes"]) - float(previous.get("received_bytes", 0.0)), 0.0) / elapsed
    transmitted_rate = max(
        float(totals["transmitted_bytes"]) - float(previous.get("transmitted_bytes", 0.0)),
        0.0,
    ) / elapsed
    return {
        **totals,
        "received_bytes_per_second": round(received_rate, 1),
        "transmitted_bytes_per_second": round(transmitted_rate, 1),
    }


def _get_system_stats() -> Dict[str, Any]:
    disk_total, disk_used, disk_free = shutil.disk_usage("/")
    disk_usage_percent = round((disk_used / disk_total) * 100, 1) if disk_total else 0.0

    load_average = (0.0, 0.0, 0.0)
    try:
        load_average = tuple(round(value, 2) for value in os.getloadavg())
    except (AttributeError, OSError):
        pass

    return {
        "cpu": {
            "usage_percent": _system_cpu_usage_percent(),
            "load_average": {
                "one_minute": load_average[0],
                "five_minutes": load_average[1],
                "fifteen_minutes": load_average[2],
            },
        },
        "memory": _read_linux_memory_stats(),
        "disk": {
            "mount": "/",
            "total_bytes": disk_total,
            "used_bytes": disk_used,
            "free_bytes": disk_free,
            "usage_percent": disk_usage_percent,
        },
        "network": _system_network_rates(),
    }


def _admin_overview_payload(admin_user: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    settings = db.get_admin_settings()
    settings["admin_webhook_destinations"] = admin_alerts.normalize_destinations(
        settings.get("admin_webhook_destinations")
    )
    settings["cvs_proxy_urls"] = CvsStockChecker.normalize_proxy_urls(settings.get("cvs_proxy_urls"))
    users = db.list_users_for_admin()
    events = db.list_audit_events(limit=200)
    global_statistics = db.get_global_statistics()
    service_uptime = db.get_service_uptime_stats()
    system_stats = _get_system_stats()
    trending_products = db.list_trending_products_for_admin(limit=48)
    hidden_trending_products = db.list_hidden_trending_products_for_admin(limit=48)
    return {
        "settings": settings,
        "authorized_google_emails": db.list_authorized_google_emails(),
        "users": users,
        "events": events,
        "trending_products": {
            "products": trending_products,
            "count": len(trending_products),
            "hidden_products": hidden_trending_products,
            "hidden_count": len(hidden_trending_products),
            "retention_hours": 0,
            "viewer_user_id": int(admin_user["id"]) if admin_user else None,
        },
        "platform": {
            "global_statistics": global_statistics,
            "service_uptime": service_uptime,
            "system_stats": system_stats,
            "totals": {
                "users": len(users),
                "banned_users": sum(1 for user in users if user.get("is_banned")),
                "authorized_users": sum(1 for user in users if user.get("is_authorized_email")),
                "scheduler_enabled_users": sum(1 for user in users if user.get("scheduler_enabled")),
                "alert_webhooks": len(settings.get("admin_webhook_destinations") or []),
                "audit_events": len(events),
                "login_denials": sum(
                    1
                    for event in events
                    if str(event.get("event_type") or "").startswith("auth.login_denied")
                ),
                "new_users": sum(
                    1
                    for event in events
                    if str(event.get("event_type") or "") == "auth.user_created"
                ),
            },
        },
    }


def _service_uptime_heartbeat_loop() -> None:
    while True:
        try:
            db.record_service_heartbeat()
        except Exception as exc:
            logger.warning("Service uptime heartbeat failed: %s", exc)
        time.sleep(SERVICE_UPTIME_HEARTBEAT_SECONDS)


def start_service_uptime_tracker() -> None:
    global _uptime_tracker_started
    if _uptime_tracker_started:
        return

    _uptime_tracker_started = True
    try:
        db.record_service_heartbeat()
    except Exception as exc:
        logger.warning("Initial service uptime heartbeat failed: %s", exc)

    tracker_thread = threading.Thread(
        target=_service_uptime_heartbeat_loop,
        name="service-uptime-heartbeat",
        daemon=True,
    )
    tracker_thread.start()


start_service_uptime_tracker()


@app.route("/api/auth/session", methods=["GET"])
def get_auth_session():
    return jsonify(_public_auth_payload(_session_user()))


@app.route("/api/auth/google", methods=["POST"])
def google_sign_in():
    if not _google_client_configured():
        return jsonify({"error": "GOOGLE_CLIENT_ID is not configured on the backend"}), 500

    data = request.json or {}
    credential = str(data.get("credential") or "").strip()
    if not credential:
        return jsonify({"error": "Google credential is required"}), 400

    try:
        token_info = id_token.verify_oauth2_token(
            credential,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
    except Exception as exc:
        logger.warning("Google sign-in verification failed: %s", exc)
        return jsonify({"error": "Google sign-in verification failed"}), 401

    issuer = token_info.get("iss")
    if issuer not in {"accounts.google.com", "https://accounts.google.com"}:
        return jsonify({"error": "Invalid Google token issuer"}), 401

    if not token_info.get("email_verified"):
        return jsonify({"error": "Google account email is not verified"}), 401

    normalized_email = str(token_info.get("email") or "").strip().lower()
    user = db.upsert_user_from_google(
        google_sub=str(token_info.get("sub") or ""),
        email=normalized_email,
        name=str(token_info.get("name") or token_info.get("email") or "Google User"),
        picture=str(token_info.get("picture") or ""),
    )

    if bool(user.get("is_new_user")):
        _record_audit_event(
            "auth.user_created",
            f"New user joined: {user['email']}",
            target_user=user,
            user_email=user["email"],
            metadata={"name": user.get("name", ""), "created_at": user.get("created_at", "")},
            alert_category="new_user",
        )

    access_denied_reason = _access_denied_reason_for_user(user)
    if access_denied_reason:
        if bool(user.get("is_banned")):
            _record_audit_event(
                "auth.login_denied",
                f"Rejected sign-in for {user['email']}: {access_denied_reason}",
                target_user=user,
                user_email=user["email"],
                metadata={"reason": access_denied_reason},
                alert_category="user_action",
            )
            session.clear()
            payload = _public_auth_payload(None)
            payload["error"] = access_denied_reason
            payload["access_denied_reason"] = access_denied_reason
            return jsonify(payload), 403

        _record_audit_event(
            "auth.waitlist_added",
            f"Approval required for {user['email']}",
            target_user=user,
            user_email=user["email"],
            metadata={
                "reason": "approval_required",
                "email": user["email"],
                "is_new_user": bool(user.get("is_new_user")),
            },
            alert_category="user_action",
        )
        _start_waitlist_session(user)
        payload = _public_auth_payload(None)
        payload["error"] = payload.get("access_denied_reason") or access_denied_reason
        return jsonify(payload), 403

    _start_user_session(user)

    scheduler = _current_scheduler(user)
    scheduler.refresh_from_db()

    _record_audit_event(
        "auth.login",
        f"User signed in: {user['email']}",
        actor_user=user,
        user_email=user["email"],
        alert_category="user_action",
    )

    return jsonify(_public_auth_payload(user))


@app.route("/api/auth/logout", methods=["POST"])
def logout():
    user = _session_user()
    if user is not None:
        _record_audit_event(
            "auth.logout",
            f"User signed out: {user['email']}",
            actor_user=user,
            user_email=user["email"],
            alert_category="user_action",
        )
    session.clear()
    return _logout_response()


@app.route("/api/admin/session", methods=["GET"])
def get_admin_session():
    return jsonify(_public_admin_payload())


@app.route("/api/admin/login", methods=["POST"])
def admin_login():
    if not _admin_panel_configured():
        return jsonify({"error": "Admin panel password is not configured"}), 503

    user = _session_user()
    if user is None:
        _clear_admin_session()
        return jsonify({"error": "Google sign-in required before unlocking admin"}), 401

    access_denied_reason = _access_denied_reason_for_user(user)
    if access_denied_reason:
        _transition_denied_user_session(user)
        return jsonify({"error": access_denied_reason}), 403

    data = request.json or {}
    password = str(data.get("password") or "")
    if not secrets.compare_digest(password, ADMIN_PANEL_PASSWORD):
        return jsonify({"error": "Invalid admin password"}), 401

    session[ADMIN_SESSION_KEY] = int(user["id"])
    _record_audit_event(
        "admin.login",
        f"Admin panel unlocked by {user['email']}",
        actor_user=user,
        alert_category="user_action",
    )
    return jsonify(_public_admin_payload())


@app.route("/api/admin/logout", methods=["POST"])
def admin_logout():
    user = _session_user()
    if _session_admin_authenticated(user):
        _record_audit_event(
            "admin.logout",
            f"Admin panel locked by {(user or {}).get('email') or 'unknown user'}",
            actor_user=user,
            user_email=str((user or {}).get("email") or ""),
            alert_category="user_action",
        )
    _clear_admin_session()
    return jsonify({"success": True})


@app.route("/api/public-stats", methods=["GET"])
def get_public_stats():
    payload = db.get_global_statistics()
    payload["service_uptime"] = db.get_service_uptime_stats()
    return jsonify(payload)


@app.route("/api/status", methods=["GET"])
@require_auth
def get_status(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    return jsonify(
        {
            "status": scheduler.get_status(),
            "statistics": db.get_statistics(int(user["id"])),
        }
    )


@app.route("/api/check", methods=["POST"])
@require_auth
def manual_check(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    result = scheduler.manual_check()
    if result.get("success"):
        _record_audit_event(
            "user.manual_check",
            f"Manual stock check started by {user['email']}",
            actor_user=user,
            alert_category="user_action",
        )
    return jsonify(result)


@app.route("/api/progress", methods=["GET"])
@require_auth
def get_progress(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    return jsonify(scheduler.get_progress())


@app.route("/api/start", methods=["POST"])
@require_auth
def start_scheduler(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}

    webhook_value = data.get("discord_destinations")
    if webhook_value is not None:
        scheduler.set_discord_destinations(webhook_value)

    interval_minutes = data.get("check_interval_minutes")
    if interval_minutes is not None:
        try:
            scheduler.set_check_interval_minutes(interval_minutes)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    success = scheduler.start()
    if success:
        _record_audit_event(
            "user.scheduler_started",
            f"Scheduler started by {user['email']}",
            actor_user=user,
            metadata={"check_interval_minutes": scheduler.check_interval_minutes},
            alert_category="user_action",
        )
        return jsonify({"message": "Scheduler started", "success": True})
    return jsonify({"message": "Scheduler already running", "success": False}), 400


@app.route("/api/stop", methods=["POST"])
@require_auth
def stop_scheduler(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    success = scheduler.stop()
    if success:
        _record_audit_event(
            "user.scheduler_stopped",
            f"Scheduler stopped by {user['email']}",
            actor_user=user,
            alert_category="user_action",
        )
        return jsonify({"message": "Scheduler stopped", "success": True})
    return jsonify({"error": "Scheduler not running"}), 400


@app.route("/api/history", methods=["GET"])
@require_auth
def get_history(user: Dict[str, Any]):
    limit = request.args.get("limit", default=50, type=int)
    history = db.get_recent_checks(int(user["id"]), limit)
    return jsonify({"history": history, "count": len(history)})


@app.route("/api/last-check", methods=["GET"])
@require_auth
def get_last_check(user: Dict[str, Any]):
    last = db.get_last_check(int(user["id"]))
    if last:
        return jsonify(last)
    return jsonify({"message": "No checks performed yet"}), 404


@app.route("/api/configure", methods=["POST"])
@require_auth
def configure(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}

    webhook_value = data.get("discord_destinations")
    zipcode = data.get("zipcode")
    interval_minutes = data.get("check_interval_minutes")
    max_notification_distance_miles = data.get("max_notification_distance_miles")
    pokemon_background_enabled = data.get("pokemon_background_enabled")
    pokemon_background_theme = data.get("pokemon_background_theme")
    pokemon_background_tile_size = data.get("pokemon_background_tile_size")
    map_provider = data.get("map_provider")

    try:
        if webhook_value is not None:
            scheduler.set_discord_destinations(webhook_value)
        if zipcode is not None:
            scheduler.set_zipcode(zipcode)
        if interval_minutes is not None:
            scheduler.set_check_interval_minutes(interval_minutes)
        if max_notification_distance_miles is not None:
            scheduler.set_max_notification_distance_miles(max_notification_distance_miles)
        if pokemon_background_enabled is not None:
            scheduler.set_pokemon_background_enabled(pokemon_background_enabled)
        if pokemon_background_theme is not None:
            scheduler.set_pokemon_background_theme(pokemon_background_theme)
        if pokemon_background_tile_size is not None:
            scheduler.set_pokemon_background_tile_size(pokemon_background_tile_size)
        if map_provider is not None:
            scheduler.set_map_provider(map_provider)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    _record_audit_event(
        "user.settings_updated",
        f"Settings updated by {user['email']}",
        actor_user=user,
        metadata={
            "zipcode": scheduler.current_zipcode,
            "check_interval_minutes": scheduler.check_interval_minutes,
            "max_notification_distance_miles": scheduler.max_notification_distance_miles,
            "discord_webhook_count": len(scheduler.discord_destinations),
        },
        alert_category="user_action",
    )

    return jsonify(
        {
            "message": "Configuration updated",
            "discord_configured": scheduler.notifier.is_configured,
            "discord_webhook_count": len(scheduler.notifier.webhook_urls),
            "discord_destinations": scheduler.discord_destinations,
            "zipcode": scheduler.current_zipcode,
            "check_interval_minutes": scheduler.check_interval_minutes,
            "max_notification_distance_miles": scheduler.max_notification_distance_miles,
            "pokemon_background_enabled": scheduler.pokemon_background_enabled,
            "pokemon_background_theme": scheduler.pokemon_background_theme,
            "pokemon_background_tile_size": scheduler.pokemon_background_tile_size,
            "map_provider": scheduler.map_provider,
        }
    )


@app.route("/api/products/add", methods=["POST"])
@require_auth
def add_product(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}
    product_link = str(data.get("url", "")).strip()
    custom_name = str(data.get("name", "")).strip()

    if product_link:
        try:
            resolved = resolve_product_link(product_link)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400

        retailer = str(resolved.get("retailer", "walgreens")).strip() or "walgreens"
        article_id = resolved["article_id"]
        product_name = custom_name or resolved["name"]
        planogram = resolved["planogram"]
        try:
            image_url = _sanitize_product_image_url(resolved.get("image_url", ""))
        except ValueError:
            image_url = ""

        try:
            source_url = _sanitize_product_source_url(
                resolved.get("canonical_url", "") or product_link,
                allow_empty=False,
            )
        except ValueError:
            try:
                source_url = _sanitize_product_source_url(product_link, allow_empty=False)
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400
        resolved_product_id = resolved.get("product_id", "")
    else:
        retailer = str(data.get("retailer", "walgreens")).strip() or "walgreens"
        article_id = str(data.get("id", "")).strip()
        product_name = custom_name
        planogram = str(data.get("planogram", "")).strip()
        resolved_product_id = str(data.get("product_id", "")).strip()

        if not article_id or not product_name or not planogram:
            return jsonify({"error": "Product URL or product ID, name, and planogram are required"}), 400

        try:
            image_url = _sanitize_product_image_url(data.get("image_url", ""))
            source_url = _sanitize_product_source_url(data.get("source_url", ""))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    success = scheduler.add_product(
        article_id,
        retailer,
        product_name,
        planogram,
        image_url=image_url,
        source_url=source_url,
        product_id=resolved_product_id,
    )

    if not success:
        return jsonify({"error": "Product already tracked"}), 400

    _record_audit_event(
        "user.product_added",
        f"Tracked product added by {user['email']}: {product_name}",
        actor_user=user,
        metadata={
            "product_id": article_id,
            "retailer": retailer,
            "name": product_name,
        },
        alert_category="user_action",
    )

    return jsonify(
        {
            "message": "Product added",
            "id": article_id,
            "retailer": retailer,
            "name": product_name,
            "planogram": planogram,
            "image_url": image_url,
            "source_url": source_url or None,
            "product_id": resolved_product_id or None,
        }
    )


@app.route("/api/products/resolve", methods=["POST"])
@require_auth
def resolve_product(user: Dict[str, Any]):
    del user
    data = request.json or {}
    product_link = str(data.get("url", "")).strip()
    if not product_link:
        return jsonify({"error": "Product URL required"}), 400

    try:
        resolved = resolve_product_link(product_link)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(resolved)


@app.route("/api/products/trending", methods=["GET"])
@require_auth
def get_trending_products(user: Dict[str, Any]):
    limit = request.args.get("limit", default=48, type=int)
    products = db.list_trending_products(int(user["id"]), limit=limit)
    return jsonify(
        {
            "products": products,
            "count": len(products),
            "retention_hours": 0,
        }
    )


@app.route("/api/products/remove", methods=["POST"])
@require_auth
def remove_product(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}
    product_id = str(data.get("id", "")).strip()
    retailer = str(data.get("retailer", "")).strip()
    product_name = str(data.get("name", "")).strip()
    if not product_id:
        return jsonify({"error": "Product ID required"}), 400

    success = scheduler.remove_product(product_id, retailer=retailer)
    if success:
        _record_audit_event(
            "user.product_removed",
            f"Tracked product removed by {user['email']}: {product_name or product_id}",
            actor_user=user,
            metadata={"product_id": product_id, "retailer": retailer or "walgreens", "name": product_name},
            alert_category="user_action",
        )
        return jsonify({"message": "Product removed", "id": product_id, "retailer": retailer or "walgreens"})
    return jsonify({"error": "Product not found"}), 404


@app.route("/api/products/update", methods=["POST"])
@require_auth
def update_product(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}
    product_id = str(data.get("id", "")).strip()
    retailer = str(data.get("retailer", "")).strip()
    product_name = str(data.get("name", "")).strip()
    exclude_from_discord = data.get("exclude_from_discord")

    if not product_id:
        return jsonify({"error": "Product ID required"}), 400

    success = True
    if product_name:
        try:
            success = scheduler.update_product_name(product_id, product_name, retailer=retailer)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    if exclude_from_discord is not None:
        try:
            exclude_success = scheduler.set_product_discord_exclusion(product_id, bool(exclude_from_discord), retailer=retailer)
            success = success and exclude_success
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    if success:
        if product_name:
            _record_audit_event(
                "user.product_renamed",
                f"Tracked product renamed by {user['email']}: {product_name}",
                actor_user=user,
                metadata={"product_id": product_id, "retailer": retailer or "walgreens", "name": product_name},
                alert_category="user_action",
            )
        return jsonify(
            {
                "message": "Product updated",
                "id": product_id,
                "retailer": retailer or "walgreens",
                "name": product_name,
                "exclude_from_discord": exclude_from_discord,
            }
        )
    return jsonify({"error": "Product not found"}), 404


@app.route("/api/products/reorder", methods=["POST"])
@require_auth
def reorder_products(user: Dict[str, Any]):
    data = request.json or {}
    product_keys = data.get("product_keys", [])

    if not isinstance(product_keys, list) or len(product_keys) == 0:
        return jsonify({"error": "product_keys array required"}), 400

    try:
        db.reorder_tracked_products(int(user["id"]), product_keys)
        _record_audit_event(
            "user.products_reordered",
            f"Tracked products reordered by {user['email']}: {len(product_keys)} products",
            actor_user=user,
            metadata={"count": len(product_keys)},
            alert_category="user_action",
        )
        return jsonify({"message": "Products reordered", "count": len(product_keys)})
    except Exception as exc:
        logger.error("Error reordering products for user %s: %s", user.get("id"), exc)
        return jsonify({"error": "Failed to reorder products"}), 500


@app.route("/api/admin/overview", methods=["GET"])
@require_admin
def get_admin_overview():
    return jsonify(_admin_overview_payload(_session_user()))


@app.route("/api/admin/trending-products/remove", methods=["POST"])
@require_admin
def remove_trending_product_admin():
    admin_user = _session_user()
    data = request.json or {}
    product_id = str(data.get("id") or "").strip()
    retailer = str(data.get("retailer") or "").strip()
    product_name = str(data.get("name") or "").strip()

    if not product_id:
        return jsonify({"error": "Product ID required"}), 400

    removed_product = db.hide_trending_product(
        product_id,
        retailer,
        hidden_by_user_id=int(admin_user["id"]) if admin_user else None,
    )
    if removed_product is None:
        return jsonify({"error": "Trending product not found"}), 404

    _record_audit_event(
        "admin.trending_product_removed",
        f"Trending product removed from admin radar: {product_name or removed_product['name']}",
        actor_user=admin_user,
        metadata={
            "product_id": removed_product["id"],
            "retailer": removed_product["retailer"],
            "name": product_name or removed_product["name"],
            "tracked_by_count": removed_product["tracked_by_count"],
            "hidden_at": removed_product["hidden_at"],
            "trending_only": True,
        },
        alert_category="user_action",
    )
    return jsonify({"product": removed_product})


@app.route("/api/admin/trending-products/restore", methods=["POST"])
@require_admin
def restore_trending_product_admin():
    admin_user = _session_user()
    data = request.json or {}
    product_id = str(data.get("id") or "").strip()
    retailer = str(data.get("retailer") or "").strip()
    product_name = str(data.get("name") or "").strip()

    if not product_id:
        return jsonify({"error": "Product ID required"}), 400

    restored_product = db.restore_hidden_trending_product(product_id, retailer)
    if restored_product is None:
        return jsonify({"error": "Hidden trending product not found"}), 404

    _record_audit_event(
        "admin.trending_product_restored",
        f"Trending product restored to admin radar: {product_name or restored_product['name']}",
        actor_user=admin_user,
        metadata={
            "product_id": restored_product["id"],
            "retailer": restored_product["retailer"],
            "name": product_name or restored_product["name"],
            "hidden_at": restored_product["hidden_at"],
            "restored_recent_count": restored_product["restored_recent_count"],
            "trending_only": True,
        },
        alert_category="user_action",
    )
    return jsonify({"product": restored_product})


@app.route("/api/admin/settings", methods=["POST"])
@require_admin
def update_admin_settings():
    admin_user = _session_user()
    data = request.json or {}
    updates: Dict[str, Any] = {}

    if "alert_new_users" in data:
        updates["alert_new_users"] = bool(data.get("alert_new_users"))
    if "alert_user_actions" in data:
        updates["alert_user_actions"] = bool(data.get("alert_user_actions"))
    if "admin_webhook_destinations" in data:
        updates["admin_webhook_destinations"] = admin_alerts.normalize_destinations(
            data.get("admin_webhook_destinations")
        )
    if "cvs_proxy_urls" in data:
        updates["cvs_proxy_urls"] = CvsStockChecker.normalize_proxy_urls(data.get("cvs_proxy_urls"))

    settings = db.update_admin_settings(updates)
    settings["admin_webhook_destinations"] = admin_alerts.normalize_destinations(
        settings.get("admin_webhook_destinations")
    )
    settings["cvs_proxy_urls"] = CvsStockChecker.normalize_proxy_urls(settings.get("cvs_proxy_urls"))
    CvsStockChecker.set_proxy_urls_override(settings.get("cvs_proxy_urls"))
    CvsProductResolver.set_proxy_urls_override(settings.get("cvs_proxy_urls"))
    AceBrowserClient.set_proxy_urls_override(settings.get("cvs_proxy_urls"))
    scheduler_manager.refresh_all_from_db()
    for user in db.list_users_for_admin():
        if not user.get("is_authorized_email"):
            db.update_user_settings(int(user["id"]), {"scheduler_enabled": False})
            _stop_user_scheduler_if_running(int(user["id"]))
    _record_audit_event(
        "admin.settings_updated",
        "Admin settings updated",
        actor_user=admin_user,
        metadata={
            "google_allowlist_enabled": True,
            "webhook_count": len(settings.get("admin_webhook_destinations") or []),
            "cvs_proxy_count": len(settings.get("cvs_proxy_urls") or []),
            "alert_new_users": settings.get("alert_new_users"),
            "alert_user_actions": settings.get("alert_user_actions"),
        },
        alert_category="user_action",
    )
    return jsonify({"settings": settings})


@app.route("/api/admin/test-webhook", methods=["POST"])
@require_admin
def test_admin_webhook():
    admin_user = _session_user()
    data = request.json or {}
    destinations = None
    if "admin_webhook_destinations" in data:
        destinations = admin_alerts.normalize_destinations(data.get("admin_webhook_destinations"))

    result = admin_alerts.send_test_alert(actor_user=admin_user, destinations=destinations)
    _record_audit_event(
        "admin.webhook_tested",
        "Admin webhook test triggered",
        actor_user=admin_user,
        metadata={
            "attempted": result.get("attempted", 0),
            "delivered": result.get("delivered", 0),
            "skipped_reason": result.get("skipped_reason", ""),
        },
    )
    if result.get("skipped_reason"):
        return jsonify({"error": result["skipped_reason"], "result": result}), 400
    if not result.get("delivered"):
        failed_destinations = [
            item for item in result.get("destinations", []) if not item.get("delivered")
        ]
        first_failure = failed_destinations[0] if failed_destinations else {}
        failure_detail = str(first_failure.get("error") or "").strip()
        status_code = first_failure.get("status_code")
        if not failure_detail and status_code:
            failure_detail = f"Destination returned HTTP {status_code}"
        error_message = "Webhook test did not deliver to any destination"
        if failure_detail:
            error_message = f"{error_message}: {failure_detail}"
        return jsonify({"error": error_message, "result": result}), 502
    return jsonify({"result": result})


@app.route("/api/admin/authorized-emails", methods=["POST"])
@require_admin
def add_authorized_email():
    admin_user = _session_user()
    data = request.json or {}
    email = str(data.get("email") or "").strip()
    note = str(data.get("note") or "").strip()
    if not email:
        return jsonify({"error": "Email is required"}), 400

    try:
        entry = db.add_authorized_google_email(email, note=note)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    _record_audit_event(
        "admin.authorized_email_added",
        f"Authorized Google email added: {entry['email']}",
        actor_user=admin_user,
        metadata={"email": entry["email"], "note": entry.get("note", "")},
        alert_category="user_action",
    )
    return jsonify(
        {
            "entry": entry,
            "authorized_google_emails": db.list_authorized_google_emails(),
        }
    )


@app.route("/api/admin/authorized-emails/remove", methods=["POST"])
@require_admin
def remove_authorized_email():
    admin_user = _session_user()
    data = request.json or {}
    email = str(data.get("email") or "").strip()
    if not email:
        return jsonify({"error": "Email is required"}), 400

    removed = db.remove_authorized_google_email(email)
    if not removed:
        return jsonify({"error": "Authorized email not found"}), 404

    _record_audit_event(
        "admin.authorized_email_removed",
        f"Authorized Google email removed: {email.strip().lower()}",
        actor_user=admin_user,
        metadata={"email": email.strip().lower()},
        alert_category="user_action",
    )
    return jsonify({"authorized_google_emails": db.list_authorized_google_emails()})


@app.route("/api/admin/users/<int:user_id>/ban", methods=["POST"])
@require_admin
def ban_user(user_id: int):
    admin_user = _session_user()
    target_user = db.get_user_by_id(user_id)
    if target_user is None:
        return jsonify({"error": "User not found"}), 404

    data = request.json or {}
    reason = str(data.get("reason") or "").strip()
    updated_user = db.set_user_banned_state(user_id, True, reason=reason)
    if updated_user is None:
        return jsonify({"error": "User not found"}), 404

    db.update_user_settings(user_id, {"scheduler_enabled": False})
    _stop_user_scheduler_if_running(user_id)
    _record_audit_event(
        "admin.user_banned",
        f"User banned: {updated_user['email']}",
        actor_user=admin_user,
        target_user=updated_user,
        metadata={"reason": reason},
        alert_category="user_action",
    )
    return jsonify({"user": updated_user, "users": db.list_users_for_admin()})


@app.route("/api/admin/users/<int:user_id>/unban", methods=["POST"])
@require_admin
def unban_user(user_id: int):
    admin_user = _session_user()
    target_user = db.get_user_by_id(user_id)
    if target_user is None:
        return jsonify({"error": "User not found"}), 404

    updated_user = db.set_user_banned_state(user_id, False, reason="")
    if updated_user is None:
        return jsonify({"error": "User not found"}), 404

    _record_audit_event(
        "admin.user_unbanned",
        f"User unbanned: {updated_user['email']}",
        actor_user=admin_user,
        target_user=updated_user,
        alert_category="user_action",
    )
    return jsonify({"user": updated_user, "users": db.list_users_for_admin()})


@app.route("/api/admin/users/<int:user_id>/stop-scheduler", methods=["POST"])
@require_admin
def stop_user_scheduler(user_id: int):
    admin_user = _session_user()
    target_user = db.get_user_by_id(user_id)
    if target_user is None:
        return jsonify({"error": "User not found"}), 404

    scheduler = scheduler_manager.get_or_create(user_id)
    was_running = scheduler.is_running
    if was_running:
        scheduler.stop()
    else:
        db.update_user_settings(user_id, {"scheduler_enabled": False})

    _record_audit_event(
        "admin.scheduler_stopped",
        f"Scheduler stopped for {target_user['email']}",
        actor_user=admin_user,
        target_user=target_user,
        metadata={"was_running": was_running},
        alert_category="user_action",
    )
    return jsonify({"success": True, "users": db.list_users_for_admin()})


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/")
@app.route("/index.html")
def index():
    return _serve_frontend_html_file("index.html")


@app.route("/map")
def map_view():
    return _serve_frontend_html_file("index.html")


@app.route("/admin")
@app.route("/admin.html")
def admin_panel():
    return _serve_frontend_html_file("admin.html")


@app.route("/privacy.html")
@app.route("/privacy")
@app.route("/privacy-policy")
def privacy():
    return _serve_frontend_html_file("privacy.html")


@app.route("/terms.html")
@app.route("/terms")
@app.route("/terms-of-service")
def terms():
    return _serve_frontend_html_file("terms.html")


@app.route("/disclosures.html")
@app.route("/disclosures")
@app.route("/legal")
def disclosures():
    return _serve_frontend_html_file("disclosures.html")


@app.route("/<path:path>")
def serve_static(path: str):
    if path in FRONTEND_HTML_PAGES:
        return _serve_frontend_html_file(path)
    response = send_from_directory(str(FRONTEND_DIR), path)
    return _apply_frontend_cache_headers(response, path)


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def server_error(error):
    logger.error("Server error: %s", error)
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("Walgreens Stock Watcher")
    logger.info("=" * 50)
    logger.info("Starting Flask app on http://localhost:5000")
    logger.info("Open http://localhost:5000 in your browser")
    logger.info("=" * 50)

    app.run(debug=False, host="0.0.0.0", port=5000, use_reloader=False)
