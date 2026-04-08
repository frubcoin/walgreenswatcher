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
from database import StockDatabase  # noqa: E402
from product_resolver import resolve_product_link  # noqa: E402
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
admin_alerts = AdminAlertService(db)
scheduler_manager = SchedulerManager(db)
scheduler_manager.start_enabled_schedulers()
SERVICE_UPTIME_HEARTBEAT_SECONDS = 30
_uptime_tracker_started = False
ADMIN_SESSION_KEY = "admin_authenticated"
WAITLIST_SESSION_KEY = "waitlisted_user_id"


def _is_allowed_walgreens_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return bool(normalized) and (normalized == "walgreens.com" or normalized.endswith(".walgreens.com"))





def _is_allowed_product_source_host(hostname: str) -> bool:
    return _is_allowed_walgreens_host(hostname)


def _is_allowed_product_image_host(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    return _is_allowed_product_source_host(normalized)


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
    return Response(html, mimetype="text/html")


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


def _session_admin_authenticated() -> bool:
    return bool(session.get(ADMIN_SESSION_KEY))


def _clear_admin_session() -> None:
    session.pop(ADMIN_SESSION_KEY, None)


def _admin_panel_configured() -> bool:
    return bool(ADMIN_PANEL_PASSWORD)


def _start_user_session(user: Dict[str, Any], *, preserve_admin_authenticated: bool = False) -> None:
    session.clear()
    session.permanent = True
    if preserve_admin_authenticated:
        session[ADMIN_SESSION_KEY] = True
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
        if not _session_admin_authenticated():
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

    return {
        "authenticated": bool(user),
        "google_client_id": GOOGLE_CLIENT_ID or "",
        "google_allowlist_enabled": True,
        "admin_panel_configured": _admin_panel_configured(),
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

    return {
        "authenticated": bool(user) and _session_admin_authenticated(),
        "configured": _admin_panel_configured(),
        "google_authenticated": bool(user),
        "password_authenticated": bool(user) and _session_admin_authenticated(),
        "google_client_id": GOOGLE_CLIENT_ID or "",
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


def _admin_overview_payload() -> Dict[str, Any]:
    settings = db.get_admin_settings()
    settings["admin_webhook_destinations"] = admin_alerts.normalize_destinations(
        settings.get("admin_webhook_destinations")
    )
    users = db.list_users_for_admin()
    events = db.list_audit_events(limit=200)
    global_statistics = db.get_global_statistics()
    service_uptime = db.get_service_uptime_stats()
    return {
        "settings": settings,
        "authorized_google_emails": db.list_authorized_google_emails(),
        "users": users,
        "events": events,
        "platform": {
            "global_statistics": global_statistics,
            "service_uptime": service_uptime,
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

    was_admin_authenticated = _session_admin_authenticated()
    _start_user_session(user, preserve_admin_authenticated=was_admin_authenticated)

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

    session[ADMIN_SESSION_KEY] = True
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
    if _session_admin_authenticated():
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

    _record_audit_event(
        "user.settings_updated",
        f"Settings updated by {user['email']}",
        actor_user=user,
        metadata={
            "zipcode": scheduler.current_zipcode,
            "check_interval_minutes": scheduler.check_interval_minutes,
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


@app.route("/api/products/remove", methods=["POST"])
@require_auth
def remove_product(user: Dict[str, Any]):
    scheduler = _current_scheduler(user)
    data = request.json or {}
    product_id = str(data.get("id", "")).strip()
    product_name = str(data.get("name", "")).strip()
    if not product_id:
        return jsonify({"error": "Product ID required"}), 400

    success = scheduler.remove_product(product_id)
    if success:
        _record_audit_event(
            "user.product_removed",
            f"Tracked product removed by {user['email']}: {product_name or product_id}",
            actor_user=user,
            metadata={"product_id": product_id, "name": product_name},
            alert_category="user_action",
        )
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
        _record_audit_event(
            "user.product_renamed",
            f"Tracked product renamed by {user['email']}: {product_name}",
            actor_user=user,
            metadata={"product_id": product_id, "name": product_name},
            alert_category="user_action",
        )
        return jsonify({"message": "Product updated", "id": product_id, "name": product_name})
    return jsonify({"error": "Product not found"}), 404


@app.route("/api/admin/overview", methods=["GET"])
@require_admin
def get_admin_overview():
    return jsonify(_admin_overview_payload())


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

    settings = db.update_admin_settings(updates)
    settings["admin_webhook_destinations"] = admin_alerts.normalize_destinations(
        settings.get("admin_webhook_destinations")
    )
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
            "alert_new_users": settings.get("alert_new_users"),
            "alert_user_actions": settings.get("alert_user_actions"),
        },
        alert_category="user_action",
    )
    return jsonify({"settings": settings})


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
