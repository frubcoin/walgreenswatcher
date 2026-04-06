"""Flask application for the hosted Walgreens Stock Watcher."""

from __future__ import annotations

import logging
import os
import secrets
import sys
import threading
import time
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from flask import Flask, Response, abort, g, jsonify, request, send_from_directory, session
from flask_cors import CORS
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (  # noqa: E402
    CORS_ALLOWED_ORIGINS,
    FLASK_SECRET_KEY,
    GOOGLE_CLIENT_ID,
    SESSION_COOKIE_DOMAIN,
    SESSION_COOKIE_NAME,
    SESSION_COOKIE_SAMESITE,
    SESSION_COOKIE_SECURE,
)
from database import StockDatabase  # noqa: E402
from scheduler import SchedulerManager  # noqa: E402
from walgreens_product_resolver import WalgreensProductResolver  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="../frontend", static_url_path="")
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
INLINE_SCRIPT_NONCE_TOKEN = "__CSP_NONCE__"
FRONTEND_HTML_PAGES = {
    "index.html",
    "privacy.html",
    "terms.html",
    "disclosures.html",
}
app.secret_key = FLASK_SECRET_KEY
app.config.update(
    SESSION_COOKIE_NAME=SESSION_COOKIE_NAME,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=SESSION_COOKIE_SECURE,
    SESSION_COOKIE_SAMESITE=SESSION_COOKIE_SAMESITE,
)
if SESSION_COOKIE_DOMAIN:
    app.config["SESSION_COOKIE_DOMAIN"] = SESSION_COOKIE_DOMAIN

CORS(
    app,
    resources={r"/api/*": {"origins": CORS_ALLOWED_ORIGINS}},
    supports_credentials=True,
)

db = StockDatabase()
scheduler_manager = SchedulerManager(db)
scheduler_manager.start_enabled_schedulers()
SERVICE_UPTIME_HEARTBEAT_SECONDS = 30
_uptime_tracker_started = False


def _is_allowed_walgreens_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (normalized == "walgreens.com" or normalized.endswith(".walgreens.com"))


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
        raise ValueError(f"{field_name} must point to an allowed Walgreens host")

    return parsed.geturl()


def _sanitize_product_source_url(value: Any, *, allow_empty: bool = True) -> str:
    return _normalize_external_url(
        value,
        field_name="Product source URL",
        host_validator=_is_allowed_walgreens_host,
        allow_empty=allow_empty,
    )


def _sanitize_product_image_url(value: Any, *, allow_empty: bool = True) -> str:
    return _normalize_external_url(
        value,
        field_name="Product image URL",
        host_validator=_is_allowed_walgreens_host,
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
    return Response(html, mimetype="text/html")


def _session_user() -> Optional[Dict[str, Any]]:
    user_id = session.get("user_id")
    if not user_id:
        return None
    return db.get_user_by_id(int(user_id))


def require_auth(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        user = _session_user()
        if user is None:
            return jsonify({"error": "Authentication required"}), 401
        return func(user, *args, **kwargs)

    return wrapper


def _current_scheduler(user: Dict[str, Any]):
    return scheduler_manager.get_or_create(int(user["id"]))


@app.after_request
def apply_security_headers(response: Response) -> Response:
    nonce = getattr(g, "csp_nonce", "")
    if nonce and response.mimetype == "text/html":
        response.headers["Content-Security-Policy"] = _build_content_security_policy(nonce)

    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return response


def _google_client_configured() -> bool:
    return bool(GOOGLE_CLIENT_ID)


def _public_auth_payload(user: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "authenticated": bool(user),
        "google_client_id": GOOGLE_CLIENT_ID or "",
        "user": (
            {
                "id": user["id"],
                "email": user["email"],
                "name": user["name"],
                "picture": user.get("picture", ""),
            }
            if user
            else None
        ),
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

    user = db.upsert_user_from_google(
        google_sub=str(token_info.get("sub") or ""),
        email=str(token_info.get("email") or ""),
        name=str(token_info.get("name") or token_info.get("email") or "Google User"),
        picture=str(token_info.get("picture") or ""),
    )

    session.clear()
    session.permanent = True
    session["user_id"] = int(user["id"])

    scheduler = _current_scheduler(user)
    scheduler.refresh_from_db()

    return jsonify(_public_auth_payload(user))


@app.route("/api/auth/logout", methods=["POST"])
def logout():
    session.clear()
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
        return jsonify({"message": "Scheduler started", "success": True})
    return jsonify({"message": "Scheduler already running", "success": False}), 400


@app.route("/api/stop", methods=["POST"])
@require_auth
def stop_scheduler(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    success = scheduler.stop()
    if success:
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
    pokemon_background_enabled = data.get("pokemon_background_enabled")
    pokemon_background_theme = data.get("pokemon_background_theme")
    pokemon_background_tile_size = data.get("pokemon_background_tile_size")

    try:
        if webhook_value is not None:
            scheduler.set_discord_destinations(webhook_value)
        if zipcode is not None:
            scheduler.set_zipcode(zipcode)
        if interval_minutes is not None:
            scheduler.set_check_interval_minutes(interval_minutes)
        if pokemon_background_enabled is not None:
            scheduler.set_pokemon_background_enabled(pokemon_background_enabled)
        if pokemon_background_theme is not None:
            scheduler.set_pokemon_background_theme(pokemon_background_theme)
        if pokemon_background_tile_size is not None:
            scheduler.set_pokemon_background_tile_size(pokemon_background_tile_size)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify(
        {
            "message": "Configuration updated",
            "discord_configured": scheduler.notifier.is_configured,
            "discord_webhook_count": len(scheduler.notifier.webhook_urls),
            "discord_destinations": scheduler.discord_destinations,
            "zipcode": scheduler.current_zipcode,
            "check_interval_minutes": scheduler.check_interval_minutes,
            "pokemon_background_enabled": scheduler.pokemon_background_enabled,
            "pokemon_background_theme": scheduler.pokemon_background_theme,
            "pokemon_background_tile_size": scheduler.pokemon_background_tile_size,
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
            resolved = WalgreensProductResolver.resolve_product_link(product_link)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400

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
        product_name,
        planogram,
        image_url=image_url,
        source_url=source_url,
        product_id=resolved_product_id,
    )

    if not success:
        return jsonify({"error": "Product already tracked"}), 400

    return jsonify(
        {
            "message": "Product added",
            "id": article_id,
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
        resolved = WalgreensProductResolver.resolve_product_link(product_link)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(resolved)


@app.route("/api/products/remove", methods=["POST"])
@require_auth
def remove_product(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}
    product_id = str(data.get("id", "")).strip()
    if not product_id:
        return jsonify({"error": "Product ID required"}), 400

    success = scheduler.remove_product(product_id)
    if success:
        return jsonify({"message": "Product removed", "id": product_id})
    return jsonify({"error": "Product not found"}), 404


@app.route("/api/products/update", methods=["POST"])
@require_auth
def update_product(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}
    product_id = str(data.get("id", "")).strip()
    product_name = str(data.get("name", "")).strip()

    if not product_id:
        return jsonify({"error": "Product ID required"}), 400
    if not product_name:
        return jsonify({"error": "Product name required"}), 400

    try:
        success = scheduler.update_product_name(product_id, product_name)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    if success:
        return jsonify({"message": "Product updated", "id": product_id, "name": product_name})
    return jsonify({"error": "Product not found"}), 404


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/")
@app.route("/index.html")
def index():
    return _serve_frontend_html_file("index.html")


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
    return send_from_directory(str(FRONTEND_DIR), path)


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
