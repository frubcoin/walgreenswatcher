"""SQLite persistence for the hosted Walgreens watcher."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from config import APP_DATABASE_FILE, DATA_DIR, DEFAULT_CHECK_INTERVAL_MINUTES, TARGET_ZIP_CODE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_POKEMON_BACKGROUND_ENABLED = False
DEFAULT_POKEMON_BACKGROUND_THEME = "gyra"
DEFAULT_POKEMON_BACKGROUND_TILE_SIZE = 645
DEFAULT_ADMIN_ALERT_NEW_USERS = True
DEFAULT_ADMIN_ALERT_USER_ACTIONS = True


def _normalize_email(email: Any) -> str:
    return str(email or "").strip().lower()


class StockDatabase:
    """Manage users, settings, tracked products, and check history."""

    def __init__(self, file_path: str = APP_DATABASE_FILE):
        self.file_path = file_path
        self._ensure_dir()
        self._initialize_db()

    def _ensure_dir(self) -> None:
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        os.makedirs(DATA_DIR, exist_ok=True)

    @contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.file_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialize_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    google_sub TEXT NOT NULL UNIQUE,
                    email TEXT NOT NULL,
                    name TEXT NOT NULL,
                    picture TEXT DEFAULT '',
                    is_banned INTEGER NOT NULL DEFAULT 0,
                    ban_reason TEXT NOT NULL DEFAULT '',
                    banned_at TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    last_login_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id INTEGER PRIMARY KEY,
                    current_zipcode TEXT NOT NULL DEFAULT '',
                    check_interval_minutes INTEGER NOT NULL DEFAULT 60,
                    discord_destinations TEXT NOT NULL DEFAULT '[]',
                    pokemon_background_enabled INTEGER NOT NULL DEFAULT 0,
                    pokemon_background_theme TEXT NOT NULL DEFAULT 'gyra',
                    pokemon_background_tile_size INTEGER NOT NULL DEFAULT 645,
                    scheduler_enabled INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS tracked_products (
                    user_id INTEGER NOT NULL,
                    article_id TEXT NOT NULL,
                    retailer TEXT NOT NULL DEFAULT 'walgreens',
                    name TEXT NOT NULL,
                    planogram TEXT NOT NULL,
                    image_url TEXT DEFAULT '',
                    source_url TEXT DEFAULT '',
                    product_id TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, article_id),
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS check_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    total_stores_checked INTEGER NOT NULL DEFAULT 0,
                    products_found TEXT NOT NULL DEFAULT '{}',
                    has_stock INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_check_history_user_time
                    ON check_history(user_id, timestamp DESC);

                CREATE TABLE IF NOT EXISTS service_uptime_samples (
                    sample_minute TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admin_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS authorized_google_emails (
                    email TEXT PRIMARY KEY,
                    note TEXT NOT NULL DEFAULT '',
                    added_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    actor_user_id INTEGER,
                    target_user_id INTEGER,
                    user_email TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(actor_user_id) REFERENCES users(id) ON DELETE SET NULL,
                    FOREIGN KEY(target_user_id) REFERENCES users(id) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_audit_events_created_at
                    ON audit_events(created_at DESC);
                """
            )
            self._ensure_column(
                conn,
                "users",
                "is_banned",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                conn,
                "users",
                "ban_reason",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                conn,
                "users",
                "banned_at",
                "TEXT DEFAULT ''",
            )
            self._ensure_column(
                conn,
                "tracked_products",
                "retailer",
                "TEXT NOT NULL DEFAULT 'walgreens'",
            )

    @staticmethod
    def _ensure_column(
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        definition_sql: str,
    ) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in existing_columns:
            return
        conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition_sql}"
        )

    @staticmethod
    def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        return dict(row) if row is not None else None

    @staticmethod
    def _decode_json(raw_value: Any, fallback: Any) -> Any:
        if raw_value in (None, ""):
            return fallback
        try:
            return json.loads(raw_value)
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _default_admin_settings() -> Dict[str, Any]:
        return {
            "google_allowlist_enabled": True,
            "admin_webhook_destinations": [],
            "alert_new_users": DEFAULT_ADMIN_ALERT_NEW_USERS,
            "alert_user_actions": DEFAULT_ADMIN_ALERT_USER_ACTIONS,
        }

    def _ensure_user_settings(self, conn: sqlite3.Connection, user_id: int) -> None:
        conn.execute(
            """
            INSERT INTO user_settings (
                user_id,
                current_zipcode,
                check_interval_minutes,
                discord_destinations,
                pokemon_background_enabled,
                pokemon_background_theme,
                pokemon_background_tile_size,
                scheduler_enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(user_id) DO NOTHING
            """,
            (
                user_id,
                TARGET_ZIP_CODE,
                DEFAULT_CHECK_INTERVAL_MINUTES,
                "[]",
                int(DEFAULT_POKEMON_BACKGROUND_ENABLED),
                DEFAULT_POKEMON_BACKGROUND_THEME,
                DEFAULT_POKEMON_BACKGROUND_TILE_SIZE,
            ),
        )

    def upsert_user_from_google(
        self,
        google_sub: str,
        email: str,
        name: str,
        picture: str = "",
    ) -> Dict[str, Any]:
        timestamp = datetime.utcnow().isoformat()
        normalized_email = _normalize_email(email)

        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM users WHERE google_sub = ?",
                (google_sub,),
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE users
                    SET email = ?, name = ?, picture = ?, last_login_at = ?
                    WHERE id = ?
                    """,
                    (normalized_email, name, picture, timestamp, existing["id"]),
                )
                user_id = int(existing["id"])
                is_new_user = False
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO users (
                        google_sub,
                        email,
                        name,
                        picture,
                        is_banned,
                        ban_reason,
                        banned_at,
                        created_at,
                        last_login_at
                    )
                    VALUES (?, ?, ?, ?, 0, '', '', ?, ?)
                    """,
                    (google_sub, normalized_email, name, picture, timestamp, timestamp),
                )
                user_id = int(cursor.lastrowid)
                is_new_user = True

            self._ensure_user_settings(conn, user_id)
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

        payload = self._row_to_dict(row) or {}
        payload["is_new_user"] = is_new_user
        return payload

    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return self._row_to_dict(row)

    def get_user_settings(self, user_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            self._ensure_user_settings(conn, user_id)
            row = conn.execute("SELECT * FROM user_settings WHERE user_id = ?", (user_id,)).fetchone()

        if row is None:
            return {
                "current_zipcode": TARGET_ZIP_CODE,
                "check_interval_minutes": DEFAULT_CHECK_INTERVAL_MINUTES,
                "discord_destinations": [],
                "pokemon_background_enabled": DEFAULT_POKEMON_BACKGROUND_ENABLED,
                "pokemon_background_theme": DEFAULT_POKEMON_BACKGROUND_THEME,
                "pokemon_background_tile_size": DEFAULT_POKEMON_BACKGROUND_TILE_SIZE,
                "scheduler_enabled": False,
            }

        data = dict(row)
        return {
            "current_zipcode": data["current_zipcode"] or TARGET_ZIP_CODE,
            "check_interval_minutes": int(data["check_interval_minutes"] or DEFAULT_CHECK_INTERVAL_MINUTES),
            "discord_destinations": self._decode_json(data["discord_destinations"], []),
            "pokemon_background_enabled": bool(data["pokemon_background_enabled"]),
            "pokemon_background_theme": data["pokemon_background_theme"] or DEFAULT_POKEMON_BACKGROUND_THEME,
            "pokemon_background_tile_size": int(data["pokemon_background_tile_size"] or DEFAULT_POKEMON_BACKGROUND_TILE_SIZE),
            "scheduler_enabled": bool(data["scheduler_enabled"]),
        }

    def update_user_settings(self, user_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        if not updates:
            return self.get_user_settings(user_id)

        current = self.get_user_settings(user_id)
        merged = {**current, **updates}

        with self._connect() as conn:
            self._ensure_user_settings(conn, user_id)
            conn.execute(
                """
                UPDATE user_settings
                SET current_zipcode = ?,
                    check_interval_minutes = ?,
                    discord_destinations = ?,
                    pokemon_background_enabled = ?,
                    pokemon_background_theme = ?,
                    pokemon_background_tile_size = ?,
                    scheduler_enabled = ?
                WHERE user_id = ?
                """,
                (
                    merged["current_zipcode"],
                    int(merged["check_interval_minutes"]),
                    json.dumps(merged.get("discord_destinations") or []),
                    int(bool(merged.get("pokemon_background_enabled"))),
                    merged["pokemon_background_theme"],
                    int(merged["pokemon_background_tile_size"]),
                    int(bool(merged.get("scheduler_enabled"))),
                    user_id,
                ),
            )

        return self.get_user_settings(user_id)

    def list_tracked_products(self, user_id: int) -> List[Dict[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT article_id, retailer, name, planogram, image_url, source_url, product_id
                FROM tracked_products
                WHERE user_id = ?
                ORDER BY created_at ASC, article_id ASC
                """,
                (user_id,),
            ).fetchall()

        return [
            {
                "id": row["article_id"],
                "retailer": row["retailer"] or "walgreens",
                "name": row["name"],
                "planogram": row["planogram"],
                "image_url": row["image_url"] or "",
                "source_url": row["source_url"] or "",
                "product_id": row["product_id"] or "",
            }
            for row in rows
        ]

    def add_tracked_product(
        self,
        user_id: int,
        article_id: str,
        retailer: str,
        name: str,
        planogram: str,
        image_url: str = "",
        source_url: str = "",
        product_id: str = "",
    ) -> bool:
        timestamp = datetime.utcnow().isoformat()
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO tracked_products (
                        user_id, article_id, retailer, name, planogram, image_url, source_url, product_id, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        article_id,
                        retailer,
                        name,
                        planogram,
                        image_url,
                        source_url,
                        product_id,
                        timestamp,
                    ),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_tracked_product(self, user_id: int, article_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM tracked_products WHERE user_id = ? AND article_id = ?",
                (user_id, article_id),
            )
        return cursor.rowcount > 0

    def update_tracked_product_name(self, user_id: int, article_id: str, name: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE tracked_products
                SET name = ?
                WHERE user_id = ? AND article_id = ?
                """,
                (name, user_id, article_id),
            )
        return cursor.rowcount > 0

    def add_check_result(
        self,
        user_id: int,
        total_stores_checked: int,
        products_with_stock: Dict[str, Any],
        timestamp: Optional[str] = None,
    ) -> bool:
        try:
            ts = timestamp or datetime.utcnow().isoformat()
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO check_history (user_id, timestamp, total_stores_checked, products_found, has_stock)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        ts,
                        int(total_stores_checked),
                        json.dumps(products_with_stock or {}),
                        int(bool(products_with_stock)),
                    ),
                )
                conn.execute(
                    """
                    DELETE FROM check_history
                    WHERE id IN (
                        SELECT id
                        FROM check_history
                        WHERE user_id = ?
                        ORDER BY timestamp DESC
                        LIMIT -1 OFFSET 1000
                    )
                    """,
                    (user_id,),
                )
            return True
        except sqlite3.DatabaseError as exc:
            logger.error("Error adding check result for user %s: %s", user_id, exc)
            return False

    def get_last_check(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT timestamp, total_stores_checked, products_found, has_stock
                FROM check_history
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()

        if row is None:
            return None

        timestamp = row["timestamp"]
        total_stores_checked = int(row["total_stores_checked"] or 0)
        products_found = self._decode_json(row["products_found"], {})

        return {
            "timestamp": timestamp,
            "has_stock": bool(row["has_stock"]),
            "products_found": products_found,
            "check_result": {
                "timestamp": timestamp,
                "total_stores_checked": total_stores_checked,
            },
        }

    def get_recent_checks(self, user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, total_stores_checked, products_found, has_stock
                FROM check_history
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (user_id, int(limit)),
            ).fetchall()

        history: List[Dict[str, Any]] = []
        for row in rows:
            history.append(
                {
                    "timestamp": row["timestamp"],
                    "has_stock": bool(row["has_stock"]),
                    "products_found": self._decode_json(row["products_found"], {}),
                    "check_result": {
                        "timestamp": row["timestamp"],
                        "total_stores_checked": int(row["total_stores_checked"] or 0),
                    },
                }
            )
        return history

    def get_statistics(self, user_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_checks,
                    SUM(CASE WHEN has_stock = 1 THEN 1 ELSE 0 END) AS checks_with_stock,
                    MAX(timestamp) AS last_check
                FROM check_history
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()

        total_checks = int((row["total_checks"] or 0) if row else 0)
        checks_with_stock = int((row["checks_with_stock"] or 0) if row else 0)
        success_rate = (checks_with_stock / total_checks * 100) if total_checks else 0

        return {
            "total_checks": total_checks,
            "checks_with_stock": checks_with_stock,
            "success_rate": success_rate,
            "last_check": row["last_check"] if row else None,
        }

    def get_global_statistics(self) -> Dict[str, int]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_checks,
                    SUM(CASE WHEN has_stock = 1 THEN 1 ELSE 0 END) AS successful_checks
                FROM check_history
                """
            ).fetchone()

        return {
            "total_checks": int((row["total_checks"] or 0) if row else 0),
            "successful_checks": int((row["successful_checks"] or 0) if row else 0),
        }

    def get_admin_settings(self) -> Dict[str, Any]:
        defaults = self._default_admin_settings()
        with self._connect() as conn:
            rows = conn.execute("SELECT key, value FROM admin_settings").fetchall()

        stored = {row["key"]: row["value"] for row in rows}
        return {
            "google_allowlist_enabled": True,
            "admin_webhook_destinations": self._decode_json(
                stored.get("admin_webhook_destinations"),
                defaults["admin_webhook_destinations"],
            ),
            "alert_new_users": bool(
                self._decode_json(stored.get("alert_new_users"), defaults["alert_new_users"])
            ),
            "alert_user_actions": bool(
                self._decode_json(stored.get("alert_user_actions"), defaults["alert_user_actions"])
            ),
        }

    def update_admin_settings(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        if not updates:
            return self.get_admin_settings()

        current = self.get_admin_settings()
        merged = {**current, **updates}
        timestamp = datetime.utcnow().isoformat()
        serializable = {
            "google_allowlist_enabled": True,
            "admin_webhook_destinations": list(merged.get("admin_webhook_destinations") or []),
            "alert_new_users": bool(merged.get("alert_new_users", DEFAULT_ADMIN_ALERT_NEW_USERS)),
            "alert_user_actions": bool(merged.get("alert_user_actions", DEFAULT_ADMIN_ALERT_USER_ACTIONS)),
        }

        with self._connect() as conn:
            for key, value in serializable.items():
                conn.execute(
                    """
                    INSERT INTO admin_settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE
                    SET value = excluded.value, updated_at = excluded.updated_at
                    """,
                    (key, json.dumps(value), timestamp),
                )

        return self.get_admin_settings()

    def list_authorized_google_emails(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT email, note, added_at
                FROM authorized_google_emails
                ORDER BY email ASC
                """
            ).fetchall()

        return [dict(row) for row in rows]

    def add_authorized_google_email(self, email: str, note: str = "") -> Dict[str, Any]:
        normalized_email = _normalize_email(email)
        if not normalized_email:
            raise ValueError("Authorized email is required")

        timestamp = datetime.utcnow().isoformat()
        normalized_note = str(note or "").strip()

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO authorized_google_emails (email, note, added_at)
                VALUES (?, ?, ?)
                ON CONFLICT(email) DO UPDATE
                SET note = excluded.note
                """,
                (normalized_email, normalized_note, timestamp),
            )
            row = conn.execute(
                "SELECT email, note, added_at FROM authorized_google_emails WHERE email = ?",
                (normalized_email,),
            ).fetchone()

        return self._row_to_dict(row) or {}

    def remove_authorized_google_email(self, email: str) -> bool:
        normalized_email = _normalize_email(email)
        if not normalized_email:
            return False

        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM authorized_google_emails WHERE email = ?",
                (normalized_email,),
            )

        return cursor.rowcount > 0

    def is_google_email_authorized(self, email: str) -> bool:
        normalized_email = _normalize_email(email)
        if not normalized_email:
            return False

        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM authorized_google_emails WHERE email = ?",
                (normalized_email,),
            ).fetchone()
        return row is not None

    def set_user_banned_state(self, user_id: int, banned: bool, reason: str = "") -> Optional[Dict[str, Any]]:
        timestamp = datetime.utcnow().isoformat() if banned else ""
        normalized_reason = str(reason or "").strip() if banned else ""

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET is_banned = ?, ban_reason = ?, banned_at = ?
                WHERE id = ?
                """,
                (int(bool(banned)), normalized_reason, timestamp, int(user_id)),
            )
            row = conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()

        return self._row_to_dict(row)

    def list_users_for_admin(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    u.id,
                    u.google_sub,
                    u.email,
                    u.name,
                    u.picture,
                    u.is_banned,
                    u.ban_reason,
                    u.banned_at,
                    u.created_at,
                    u.last_login_at,
                    COALESCE(s.scheduler_enabled, 0) AS scheduler_enabled,
                    COALESCE(s.current_zipcode, '') AS current_zipcode,
                    COALESCE(s.check_interval_minutes, ?) AS check_interval_minutes,
                    (
                        SELECT COUNT(DISTINCT tp.article_id)
                        FROM tracked_products tp
                        WHERE tp.user_id = u.id
                    ) AS tracked_product_count,
                    COALESCE(
                        (
                            SELECT GROUP_CONCAT(product_name, '||')
                            FROM (
                                SELECT DISTINCT COALESCE(NULLIF(TRIM(tp.name), ''), tp.article_id) AS product_name
                                FROM tracked_products tp
                                WHERE tp.user_id = u.id
                                ORDER BY tp.created_at DESC, tp.article_id DESC
                            )
                        ),
                        ''
                    ) AS tracked_product_names,
                    (
                        SELECT COUNT(*)
                        FROM check_history ch
                        WHERE ch.user_id = u.id
                    ) AS total_checks,
                    (
                        SELECT MAX(ch.timestamp)
                        FROM check_history ch
                        WHERE ch.user_id = u.id
                    ) AS last_check,
                    EXISTS(
                        SELECT 1
                        FROM authorized_google_emails a
                        WHERE a.email = lower(trim(u.email))
                    ) AS is_authorized_email
                FROM users u
                LEFT JOIN user_settings s ON s.user_id = u.id
                GROUP BY
                    u.id,
                    u.google_sub,
                    u.email,
                    u.name,
                    u.picture,
                    u.is_banned,
                    u.ban_reason,
                    u.banned_at,
                    u.created_at,
                    u.last_login_at,
                    s.scheduler_enabled,
                    s.current_zipcode,
                    s.check_interval_minutes
                ORDER BY u.created_at DESC, u.id DESC
                """,
                (DEFAULT_CHECK_INTERVAL_MINUTES,),
            ).fetchall()

        users: List[Dict[str, Any]] = []
        for row in rows:
            entry = dict(row)
            entry["is_banned"] = bool(entry.get("is_banned"))
            entry["scheduler_enabled"] = bool(entry.get("scheduler_enabled"))
            entry["tracked_product_count"] = int(entry.get("tracked_product_count") or 0)
            entry["tracked_product_names"] = [
                value.strip()
                for value in str(entry.get("tracked_product_names") or "").split("||")
                if value.strip()
            ]
            entry["total_checks"] = int(entry.get("total_checks") or 0)
            entry["check_interval_minutes"] = int(
                entry.get("check_interval_minutes") or DEFAULT_CHECK_INTERVAL_MINUTES
            )
            entry["is_authorized_email"] = bool(entry.get("is_authorized_email"))
            users.append(entry)
        return users

    def record_audit_event(
        self,
        event_type: str,
        summary: str,
        *,
        actor_user_id: Optional[int] = None,
        target_user_id: Optional[int] = None,
        user_email: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        timestamp = datetime.utcnow().isoformat()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO audit_events (
                    event_type,
                    actor_user_id,
                    target_user_id,
                    user_email,
                    summary,
                    metadata_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event_type or "").strip(),
                    actor_user_id,
                    target_user_id,
                    _normalize_email(user_email),
                    str(summary or "").strip(),
                    json.dumps(metadata or {}),
                    timestamp,
                ),
            )
            row = conn.execute(
                """
                SELECT
                    id,
                    event_type,
                    actor_user_id,
                    target_user_id,
                    user_email,
                    summary,
                    metadata_json,
                    created_at
                FROM audit_events
                WHERE id = ?
                """,
                (int(cursor.lastrowid),),
            ).fetchone()

        event = self._row_to_dict(row) or {}
        event["metadata"] = self._decode_json(event.pop("metadata_json", "{}"), {})
        return event

    def list_audit_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    e.id,
                    e.event_type,
                    e.actor_user_id,
                    e.target_user_id,
                    e.user_email,
                    e.summary,
                    e.metadata_json,
                    e.created_at,
                    actor.name AS actor_name,
                    actor.email AS actor_email,
                    target.name AS target_name,
                    target.email AS target_email
                FROM audit_events e
                LEFT JOIN users actor ON actor.id = e.actor_user_id
                LEFT JOIN users target ON target.id = e.target_user_id
                ORDER BY e.created_at DESC, e.id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()

        events: List[Dict[str, Any]] = []
        for row in rows:
            event = dict(row)
            event["metadata"] = self._decode_json(event.pop("metadata_json", "{}"), {})
            events.append(event)
        return events

    def record_service_heartbeat(self, timestamp: Optional[datetime] = None) -> None:
        heartbeat_time = timestamp or datetime.utcnow()
        sample_minute = heartbeat_time.replace(second=0, microsecond=0)
        created_at = heartbeat_time.isoformat()
        retention_cutoff = (sample_minute - timedelta(days=14)).isoformat()

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO service_uptime_samples (sample_minute, created_at)
                VALUES (?, ?)
                ON CONFLICT(sample_minute) DO UPDATE SET created_at = excluded.created_at
                """,
                (sample_minute.isoformat(), created_at),
            )
            conn.execute(
                "DELETE FROM service_uptime_samples WHERE sample_minute < ?",
                (retention_cutoff,),
            )

    def get_service_uptime_stats(self, hours: int = 24) -> Dict[str, Any]:
        capped_hours = max(1, min(int(hours or 24), 24 * 14))
        now = datetime.utcnow().replace(second=0, microsecond=0)
        window_start = now - timedelta(hours=capped_hours) + timedelta(minutes=1)

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS sample_count,
                    MIN(sample_minute) AS first_sample
                FROM service_uptime_samples
                WHERE sample_minute >= ?
                """,
                (window_start.isoformat(),),
            ).fetchone()

        sample_count = int((row["sample_count"] or 0) if row else 0)
        first_sample_raw = row["first_sample"] if row else None

        if first_sample_raw:
            try:
                first_sample = datetime.fromisoformat(str(first_sample_raw))
            except ValueError:
                first_sample = now
        else:
            first_sample = now

        tracked_minutes = max(
            1,
            min(
                capped_hours * 60,
                int(((now - max(first_sample, window_start)).total_seconds() // 60) + 1),
            ),
        )
        uptime_percentage = round((sample_count / tracked_minutes) * 100, 1) if tracked_minutes else 0.0

        if tracked_minutes >= 24 * 60:
            label = f"{uptime_percentage:.1f}% uptime over last 24h"
        else:
            tracked_hours = tracked_minutes / 60
            if tracked_hours >= 1:
                label = f"{uptime_percentage:.1f}% uptime over last {tracked_hours:.1f}h"
            else:
                label = f"{uptime_percentage:.1f}% uptime tracked"

        return {
            "uptime_percentage": uptime_percentage,
            "tracked_minutes": tracked_minutes,
            "label": label,
        }

    def list_users_with_enabled_schedulers(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT u.id, u.google_sub, u.email, u.name, u.picture
                FROM users u
                JOIN user_settings s ON s.user_id = u.id
                WHERE s.scheduler_enabled = 1
                  AND COALESCE(u.is_banned, 0) = 0
                ORDER BY u.id ASC
                """
            ).fetchall()

        return [dict(row) for row in rows if self.is_google_email_authorized(str(row["email"] or ""))]
