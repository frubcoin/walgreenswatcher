"""SQLite persistence for the hosted local pick-up monitor."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from config import (
    APP_DATABASE_FILE,
    DATA_DIR,
    DEFAULT_CHECK_INTERVAL_MINUTES,
    SEARCH_RADIUS_MILES,
    TARGET_ZIP_CODE,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_POKEMON_BACKGROUND_ENABLED = False
DEFAULT_POKEMON_BACKGROUND_THEME = "gyra"
DEFAULT_POKEMON_BACKGROUND_TILE_SIZE = 645
DEFAULT_ADMIN_ALERT_NEW_USERS = True
DEFAULT_ADMIN_ALERT_USER_ACTIONS = True
TRENDING_PRODUCTS_RETENTION_HOURS = 24
DEFAULT_MAX_NOTIFICATION_DISTANCE_MILES = int(SEARCH_RADIUS_MILES or 20)


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
                    max_notification_distance_miles INTEGER NOT NULL DEFAULT 20,
                    discord_destinations TEXT NOT NULL DEFAULT '[]',
                    pokemon_background_enabled INTEGER NOT NULL DEFAULT 0,
                    pokemon_background_theme TEXT NOT NULL DEFAULT 'gyra',
                    pokemon_background_tile_size INTEGER NOT NULL DEFAULT 645,
                    scheduler_enabled INTEGER NOT NULL DEFAULT 0,
                    map_provider TEXT NOT NULL DEFAULT 'google',
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
                    PRIMARY KEY (user_id, retailer, article_id),
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

                CREATE TABLE IF NOT EXISTS trending_products (
                    user_id INTEGER NOT NULL,
                    article_id TEXT NOT NULL,
                    retailer TEXT NOT NULL DEFAULT 'walgreens',
                    name TEXT NOT NULL,
                    planogram TEXT NOT NULL,
                    image_url TEXT DEFAULT '',
                    source_url TEXT DEFAULT '',
                    product_id TEXT DEFAULT '',
                    last_tracked_at TEXT NOT NULL,
                    PRIMARY KEY (user_id, retailer, article_id),
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_trending_products_last_tracked_at
                    ON trending_products(last_tracked_at DESC);

                CREATE TABLE IF NOT EXISTS hidden_trending_products (
                    article_id TEXT NOT NULL,
                    retailer TEXT NOT NULL DEFAULT 'walgreens',
                    hidden_at TEXT NOT NULL,
                    hidden_by_user_id INTEGER,
                    PRIMARY KEY (retailer, article_id),
                    FOREIGN KEY(hidden_by_user_id) REFERENCES users(id) ON DELETE SET NULL
                );

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
            self._migrate_tracked_products_primary_key(conn)
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
            self._ensure_column(
                conn,
                "user_settings",
                "max_notification_distance_miles",
                f"INTEGER NOT NULL DEFAULT {DEFAULT_MAX_NOTIFICATION_DISTANCE_MILES}",
            )
            self._backfill_recent_trending_products(conn)
            self._prune_expired_trending_products(conn)

    @staticmethod
    def _product_key(article_id: Any, retailer: Any) -> str:
        normalized_retailer = str(retailer or "walgreens").strip().lower() or "walgreens"
        normalized_article_id = str(article_id or "").strip()
        return f"{normalized_retailer}:{normalized_article_id}"

    @staticmethod
    def _trending_retention_cutoff(now: Optional[datetime] = None) -> str:
        reference = now or datetime.utcnow()
        return (reference - timedelta(hours=TRENDING_PRODUCTS_RETENTION_HOURS)).isoformat()

    def _migrate_tracked_products_primary_key(self, conn: sqlite3.Connection) -> None:
        row = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'tracked_products'
            """
        ).fetchone()
        table_sql = str(row["sql"] if row is not None and row["sql"] is not None else "")
        normalized_sql = " ".join(table_sql.split()).lower()

        if not normalized_sql:
            return
        if "primary key (user_id, retailer, article_id)" in normalized_sql:
            return
        if "primary key (user_id, article_id)" not in normalized_sql:
            return

        conn.execute("ALTER TABLE tracked_products RENAME TO tracked_products_legacy")
        conn.executescript(
            """
            CREATE TABLE tracked_products (
                user_id INTEGER NOT NULL,
                article_id TEXT NOT NULL,
                retailer TEXT NOT NULL DEFAULT 'walgreens',
                name TEXT NOT NULL,
                planogram TEXT NOT NULL,
                image_url TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                product_id TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                PRIMARY KEY (user_id, retailer, article_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )
        conn.execute(
            """
            INSERT INTO tracked_products (
                user_id,
                article_id,
                retailer,
                name,
                planogram,
                image_url,
                source_url,
                product_id,
                created_at
            )
            SELECT
                user_id,
                article_id,
                COALESCE(NULLIF(TRIM(retailer), ''), 'walgreens'),
                name,
                planogram,
                image_url,
                source_url,
                product_id,
                created_at
            FROM tracked_products_legacy
            ORDER BY created_at ASC, article_id ASC
            """
        )
        conn.execute("DROP TABLE tracked_products_legacy")

    def _prune_expired_trending_products(
        self,
        conn: sqlite3.Connection,
        now: Optional[datetime] = None,
    ) -> None:
        conn.execute(
            "DELETE FROM trending_products WHERE last_tracked_at < ?",
            (self._trending_retention_cutoff(now),),
        )

    def _backfill_recent_trending_products(self, conn: sqlite3.Connection) -> None:
        cutoff = self._trending_retention_cutoff()
        rows = conn.execute(
            """
            SELECT
                user_id,
                article_id,
                COALESCE(NULLIF(TRIM(retailer), ''), 'walgreens') AS retailer,
                COALESCE(NULLIF(TRIM(name), ''), article_id) AS name,
                COALESCE(NULLIF(TRIM(planogram), ''), '') AS planogram,
                COALESCE(NULLIF(TRIM(image_url), ''), '') AS image_url,
                COALESCE(NULLIF(TRIM(source_url), ''), '') AS source_url,
                COALESCE(NULLIF(TRIM(product_id), ''), '') AS product_id,
                created_at AS last_tracked_at
            FROM tracked_products
            WHERE created_at >= ?
              AND COALESCE(NULLIF(TRIM(source_url), ''), '') <> ''
            """,
            (cutoff,),
        ).fetchall()
        for row in rows:
            self._upsert_trending_product(
                conn,
                int(row["user_id"]),
                str(row["article_id"] or "").strip(),
                str(row["retailer"] or "walgreens").strip() or "walgreens",
                str(row["name"] or "").strip() or str(row["article_id"] or "").strip(),
                str(row["planogram"] or "").strip(),
                image_url=str(row["image_url"] or "").strip(),
                source_url=str(row["source_url"] or "").strip(),
                product_id=str(row["product_id"] or "").strip(),
                tracked_at=str(row["last_tracked_at"] or "").strip(),
            )

    def _upsert_trending_product(
        self,
        conn: sqlite3.Connection,
        user_id: int,
        article_id: str,
        retailer: str,
        name: str,
        planogram: str,
        *,
        image_url: str = "",
        source_url: str = "",
        product_id: str = "",
        tracked_at: Optional[str] = None,
    ) -> None:
        normalized_source_url = str(source_url or "").strip()
        if not normalized_source_url:
            return
        normalized_article_id = str(article_id or "").strip()
        normalized_retailer = str(retailer or "walgreens").strip().lower() or "walgreens"
        if self._is_trending_product_hidden(conn, normalized_article_id, normalized_retailer):
            return

        conn.execute(
            """
            INSERT INTO trending_products (
                user_id,
                article_id,
                retailer,
                name,
                planogram,
                image_url,
                source_url,
                product_id,
                last_tracked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, retailer, article_id) DO UPDATE
            SET name = excluded.name,
                planogram = excluded.planogram,
                image_url = excluded.image_url,
                source_url = excluded.source_url,
                product_id = excluded.product_id,
                last_tracked_at = excluded.last_tracked_at
            """,
            (
                int(user_id),
                normalized_article_id,
                normalized_retailer,
                str(name or article_id or "").strip() or normalized_article_id,
                str(planogram or "").strip(),
                str(image_url or "").strip(),
                normalized_source_url,
                str(product_id or "").strip(),
                str(tracked_at or datetime.utcnow().isoformat()),
            ),
        )

    def _is_trending_product_hidden(
        self,
        conn: sqlite3.Connection,
        article_id: str,
        retailer: str,
    ) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM hidden_trending_products
            WHERE article_id = ? AND retailer = ?
            LIMIT 1
            """,
            (
                str(article_id or "").strip(),
                str(retailer or "walgreens").strip().lower() or "walgreens",
            ),
        ).fetchone()
        return row is not None

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

    @staticmethod
    def _add_column_if_not_exists(
        conn: sqlite3.Connection, table_name: str, column_name: str, definition_sql: str
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

    def _ensure_user_settings(self, conn: sqlite3.Connection, user_id: int) -> None:
        StockDatabase._add_column_if_not_exists(
            conn, "user_settings", "map_provider", "TEXT NOT NULL DEFAULT 'google'"
        )
        StockDatabase._add_column_if_not_exists(
            conn, "tracked_products", "exclude_from_discord", "INTEGER NOT NULL DEFAULT 0"
        )
        StockDatabase._add_column_if_not_exists(
            conn, "tracked_products", "sort_order", "INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            """
            INSERT INTO user_settings (
                user_id,
                current_zipcode,
                check_interval_minutes,
                max_notification_distance_miles,
                discord_destinations,
                pokemon_background_enabled,
                pokemon_background_theme,
                pokemon_background_tile_size,
                scheduler_enabled,
                map_provider
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'google')
            ON CONFLICT(user_id) DO NOTHING
            """,
            (
                user_id,
                TARGET_ZIP_CODE,
                DEFAULT_CHECK_INTERVAL_MINUTES,
                DEFAULT_MAX_NOTIFICATION_DISTANCE_MILES,
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
                "max_notification_distance_miles": DEFAULT_MAX_NOTIFICATION_DISTANCE_MILES,
                "discord_destinations": [],
                "pokemon_background_enabled": DEFAULT_POKEMON_BACKGROUND_ENABLED,
                "pokemon_background_theme": DEFAULT_POKEMON_BACKGROUND_THEME,
                "pokemon_background_tile_size": DEFAULT_POKEMON_BACKGROUND_TILE_SIZE,
                "scheduler_enabled": False,
                "map_provider": "google",
            }

        data = dict(row)
        return {
            "current_zipcode": data["current_zipcode"] or TARGET_ZIP_CODE,
            "check_interval_minutes": int(data["check_interval_minutes"] or DEFAULT_CHECK_INTERVAL_MINUTES),
            "max_notification_distance_miles": int(
                data.get("max_notification_distance_miles") or DEFAULT_MAX_NOTIFICATION_DISTANCE_MILES
            ),
            "discord_destinations": self._decode_json(data["discord_destinations"], []),
            "pokemon_background_enabled": bool(data["pokemon_background_enabled"]),
            "pokemon_background_theme": data["pokemon_background_theme"] or DEFAULT_POKEMON_BACKGROUND_THEME,
            "pokemon_background_tile_size": int(data["pokemon_background_tile_size"] or DEFAULT_POKEMON_BACKGROUND_TILE_SIZE),
            "scheduler_enabled": bool(data["scheduler_enabled"]),
            "map_provider": data.get("map_provider") or "google",
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
                    max_notification_distance_miles = ?,
                    discord_destinations = ?,
                    pokemon_background_enabled = ?,
                    pokemon_background_theme = ?,
                    pokemon_background_tile_size = ?,
                    scheduler_enabled = ?,
                    map_provider = ?
                WHERE user_id = ?
                """,
                (
                    merged["current_zipcode"],
                    int(merged["check_interval_minutes"]),
                    int(merged["max_notification_distance_miles"]),
                    json.dumps(merged.get("discord_destinations") or []),
                    int(bool(merged.get("pokemon_background_enabled"))),
                    merged["pokemon_background_theme"],
                    int(merged["pokemon_background_tile_size"]),
                    int(bool(merged.get("scheduler_enabled"))),
                    merged.get("map_provider", "google"),
                    user_id,
                ),
            )

        return self.get_user_settings(user_id)

    def list_tracked_products(self, user_id: int) -> List[Dict[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT article_id, retailer, name, planogram, image_url, source_url, product_id, exclude_from_discord, sort_order
                FROM tracked_products
                WHERE user_id = ?
                ORDER BY sort_order ASC, created_at ASC, article_id ASC
                """,
                (user_id,),
            ).fetchall()

        return [
            {
                "key": self._product_key(row["article_id"], row["retailer"]),
                "id": row["article_id"],
                "retailer": row["retailer"] or "walgreens",
                "name": row["name"],
                "planogram": row["planogram"],
                "image_url": row["image_url"] or "",
                "source_url": row["source_url"] or "",
                "exclude_from_discord": bool(row["exclude_from_discord"] or 0),
                "product_id": row["product_id"] or "",
                "sort_order": row["sort_order"] or 0,
            }
            for row in rows
        ]

    def list_trending_products(self, viewer_user_id: int, limit: int = 8) -> List[Dict[str, Any]]:
        capped_limit = max(1, min(int(limit or 8), 24))

        with self._connect() as conn:
            self._prune_expired_trending_products(conn)
            rows = conn.execute(
                """
                WITH grouped AS (
                    SELECT
                        article_id,
                        retailer,
                        COUNT(DISTINCT user_id) AS tracked_by_count,
                        MIN(last_tracked_at) AS first_tracked_at,
                        MAX(last_tracked_at) AS last_tracked_at
                    FROM trending_products
                    WHERE COALESCE(NULLIF(TRIM(source_url), ''), '') <> ''
                    GROUP BY article_id, retailer
                )
                SELECT
                    g.article_id,
                    g.retailer,
                    g.tracked_by_count,
                    g.first_tracked_at,
                    g.last_tracked_at,
                    EXISTS(
                        SELECT 1
                        FROM tracked_products own
                        WHERE own.user_id = ?
                          AND own.article_id = g.article_id
                          AND own.retailer = g.retailer
                    ) AS is_tracked_by_user,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.name), ''), tp.article_id)
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS name,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.planogram), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS planogram,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.image_url), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS image_url,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.source_url), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS source_url,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.product_id), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS product_id
                FROM grouped g
                ORDER BY g.tracked_by_count DESC, g.last_tracked_at DESC, lower(name) ASC, g.article_id ASC
                LIMIT ?
                """,
                (int(viewer_user_id), capped_limit),
            ).fetchall()

        return [
            {
                "key": self._product_key(row["article_id"], row["retailer"]),
                "id": row["article_id"],
                "retailer": row["retailer"] or "walgreens",
                "name": row["name"] or row["article_id"],
                "planogram": row["planogram"] or "",
                "image_url": row["image_url"] or "",
                "source_url": row["source_url"] or "",
                "product_id": row["product_id"] or "",
                "tracked_by_count": int(row["tracked_by_count"] or 0),
                "first_tracked_at": row["first_tracked_at"] or "",
                "last_tracked_at": row["last_tracked_at"] or "",
                "is_tracked_by_user": bool(row["is_tracked_by_user"]),
            }
            for row in rows
        ]

    def list_trending_products_for_admin(self, limit: int = 48) -> List[Dict[str, Any]]:
        capped_limit = max(1, min(int(limit or 48), 200))

        with self._connect() as conn:
            self._prune_expired_trending_products(conn)
            rows = conn.execute(
                """
                WITH grouped AS (
                    SELECT
                        article_id,
                        retailer,
                        COUNT(DISTINCT user_id) AS tracked_by_count,
                        MIN(last_tracked_at) AS first_tracked_at,
                        MAX(last_tracked_at) AS last_tracked_at
                    FROM trending_products
                    WHERE COALESCE(NULLIF(TRIM(source_url), ''), '') <> ''
                    GROUP BY article_id, retailer
                )
                SELECT
                    g.article_id,
                    g.retailer,
                    g.tracked_by_count,
                    g.first_tracked_at,
                    g.last_tracked_at,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.name), ''), tp.article_id)
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS name,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.planogram), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS planogram,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.image_url), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS image_url,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.source_url), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS source_url,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.product_id), ''), '')
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS product_id
                FROM grouped g
                ORDER BY g.tracked_by_count DESC, g.last_tracked_at DESC, lower(name) ASC, g.article_id ASC
                LIMIT ?
                """,
                (capped_limit,),
            ).fetchall()

        return [
            {
                "key": self._product_key(row["article_id"], row["retailer"]),
                "id": row["article_id"],
                "retailer": row["retailer"] or "walgreens",
                "name": row["name"] or row["article_id"],
                "planogram": row["planogram"] or "",
                "image_url": row["image_url"] or "",
                "source_url": row["source_url"] or "",
                "product_id": row["product_id"] or "",
                "tracked_by_count": int(row["tracked_by_count"] or 0),
                "first_tracked_at": row["first_tracked_at"] or "",
                "last_tracked_at": row["last_tracked_at"] or "",
            }
            for row in rows
        ]

    def list_hidden_trending_products_for_admin(self, limit: int = 48) -> List[Dict[str, Any]]:
        capped_limit = max(1, min(int(limit or 48), 200))
        cutoff = self._trending_retention_cutoff()

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    h.article_id,
                    h.retailer,
                    h.hidden_at,
                    h.hidden_by_user_id,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.name), ''), tp.article_id)
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                        ORDER BY tp.created_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS name,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.planogram), ''), '')
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                        ORDER BY tp.created_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS planogram,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.image_url), ''), '')
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                        ORDER BY tp.created_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS image_url,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.source_url), ''), '')
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                        ORDER BY tp.created_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS source_url,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.product_id), ''), '')
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                        ORDER BY tp.created_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS product_id,
                    (
                        SELECT COUNT(DISTINCT tp.user_id)
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                          AND COALESCE(NULLIF(TRIM(tp.source_url), ''), '') <> ''
                          AND tp.created_at >= ?
                    ) AS tracked_by_count,
                    (
                        SELECT MAX(tp.created_at)
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                          AND COALESCE(NULLIF(TRIM(tp.source_url), ''), '') <> ''
                          AND tp.created_at >= ?
                    ) AS last_tracked_at,
                    (
                        SELECT COALESCE(NULLIF(TRIM(u.email), ''), '')
                        FROM users u
                        WHERE u.id = h.hidden_by_user_id
                        LIMIT 1
                    ) AS hidden_by_email
                FROM hidden_trending_products h
                ORDER BY h.hidden_at DESC, lower(COALESCE(name, h.article_id)) ASC, h.article_id ASC
                LIMIT ?
                """,
                (cutoff, cutoff, capped_limit),
            ).fetchall()

        return [
            {
                "key": self._product_key(row["article_id"], row["retailer"]),
                "id": row["article_id"],
                "retailer": row["retailer"] or "walgreens",
                "name": row["name"] or row["article_id"],
                "planogram": row["planogram"] or "",
                "image_url": row["image_url"] or "",
                "source_url": row["source_url"] or "",
                "product_id": row["product_id"] or "",
                "tracked_by_count": int(row["tracked_by_count"] or 0),
                "last_tracked_at": row["last_tracked_at"] or "",
                "hidden_at": row["hidden_at"] or "",
                "hidden_by_email": row["hidden_by_email"] or "",
            }
            for row in rows
        ]

    def hide_trending_product(
        self,
        article_id: str,
        retailer: str = "",
        *,
        hidden_by_user_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        normalized_article_id = str(article_id or "").strip()
        normalized_retailer = str(retailer or "walgreens").strip().lower() or "walgreens"
        if not normalized_article_id:
            return None

        with self._connect() as conn:
            self._prune_expired_trending_products(conn)
            row = conn.execute(
                """
                WITH grouped AS (
                    SELECT
                        article_id,
                        retailer,
                        COUNT(DISTINCT user_id) AS tracked_by_count,
                        MAX(last_tracked_at) AS last_tracked_at
                    FROM trending_products
                    WHERE article_id = ?
                      AND retailer = ?
                    GROUP BY article_id, retailer
                )
                SELECT
                    g.article_id,
                    g.retailer,
                    g.tracked_by_count,
                    g.last_tracked_at,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.name), ''), tp.article_id)
                        FROM trending_products tp
                        WHERE tp.article_id = g.article_id
                          AND tp.retailer = g.retailer
                        ORDER BY tp.last_tracked_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS name
                FROM grouped g
                """,
                (normalized_article_id, normalized_retailer),
            ).fetchone()
            if row is None:
                return None

            hidden_at = datetime.utcnow().isoformat()
            conn.execute(
                """
                INSERT INTO hidden_trending_products (
                    article_id,
                    retailer,
                    hidden_at,
                    hidden_by_user_id
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(retailer, article_id) DO UPDATE
                SET hidden_at = excluded.hidden_at,
                    hidden_by_user_id = excluded.hidden_by_user_id
                """,
                (
                    normalized_article_id,
                    normalized_retailer,
                    hidden_at,
                    int(hidden_by_user_id) if hidden_by_user_id is not None else None,
                ),
            )
            conn.execute(
                """
                DELETE FROM trending_products
                WHERE article_id = ? AND retailer = ?
                """,
                (normalized_article_id, normalized_retailer),
            )

        return {
            "key": self._product_key(normalized_article_id, normalized_retailer),
            "id": normalized_article_id,
            "retailer": normalized_retailer,
            "name": row["name"] or normalized_article_id,
            "tracked_by_count": int(row["tracked_by_count"] or 0),
            "last_tracked_at": row["last_tracked_at"] or "",
            "hidden_at": hidden_at,
        }

    def restore_hidden_trending_product(self, article_id: str, retailer: str = "") -> Optional[Dict[str, Any]]:
        normalized_article_id = str(article_id or "").strip()
        normalized_retailer = str(retailer or "walgreens").strip().lower() or "walgreens"
        if not normalized_article_id:
            return None

        with self._connect() as conn:
            hidden_row = conn.execute(
                """
                SELECT
                    h.article_id,
                    h.retailer,
                    h.hidden_at,
                    (
                        SELECT COALESCE(NULLIF(TRIM(tp.name), ''), tp.article_id)
                        FROM tracked_products tp
                        WHERE tp.article_id = h.article_id
                          AND tp.retailer = h.retailer
                        ORDER BY tp.created_at DESC, tp.user_id DESC
                        LIMIT 1
                    ) AS name
                FROM hidden_trending_products h
                WHERE h.article_id = ? AND h.retailer = ?
                LIMIT 1
                """,
                (normalized_article_id, normalized_retailer),
            ).fetchone()
            if hidden_row is None:
                return None

            conn.execute(
                """
                DELETE FROM hidden_trending_products
                WHERE article_id = ? AND retailer = ?
                """,
                (normalized_article_id, normalized_retailer),
            )

            cutoff = self._trending_retention_cutoff()
            tracked_rows = conn.execute(
                """
                SELECT
                    user_id,
                    article_id,
                    retailer,
                    COALESCE(NULLIF(TRIM(name), ''), article_id) AS name,
                    COALESCE(NULLIF(TRIM(planogram), ''), '') AS planogram,
                    COALESCE(NULLIF(TRIM(image_url), ''), '') AS image_url,
                    COALESCE(NULLIF(TRIM(source_url), ''), '') AS source_url,
                    COALESCE(NULLIF(TRIM(product_id), ''), '') AS product_id,
                    created_at
                FROM tracked_products
                WHERE article_id = ?
                  AND retailer = ?
                  AND COALESCE(NULLIF(TRIM(source_url), ''), '') <> ''
                  AND created_at >= ?
                ORDER BY created_at ASC, user_id ASC
                """,
                (normalized_article_id, normalized_retailer, cutoff),
            ).fetchall()

            for row in tracked_rows:
                self._upsert_trending_product(
                    conn,
                    int(row["user_id"]),
                    str(row["article_id"] or "").strip(),
                    str(row["retailer"] or "walgreens").strip() or "walgreens",
                    str(row["name"] or "").strip() or normalized_article_id,
                    str(row["planogram"] or "").strip(),
                    image_url=str(row["image_url"] or "").strip(),
                    source_url=str(row["source_url"] or "").strip(),
                    product_id=str(row["product_id"] or "").strip(),
                    tracked_at=str(row["created_at"] or "").strip(),
                )

        return {
            "key": self._product_key(normalized_article_id, normalized_retailer),
            "id": normalized_article_id,
            "retailer": normalized_retailer,
            "name": hidden_row["name"] or normalized_article_id,
            "hidden_at": hidden_row["hidden_at"] or "",
            "restored_recent_count": len(tracked_rows),
        }

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
                self._upsert_trending_product(
                    conn,
                    int(user_id),
                    article_id,
                    retailer,
                    name,
                    planogram,
                    image_url=image_url,
                    source_url=source_url,
                    product_id=product_id,
                    tracked_at=timestamp,
                )
                self._prune_expired_trending_products(conn)
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_tracked_product(self, user_id: int, article_id: str, retailer: str = "") -> bool:
        with self._connect() as conn:
            if retailer:
                cursor = conn.execute(
                    """
                    DELETE FROM tracked_products
                    WHERE user_id = ? AND article_id = ? AND retailer = ?
                    """,
                    (user_id, article_id, retailer),
                )
            else:
                cursor = conn.execute(
                    "DELETE FROM tracked_products WHERE user_id = ? AND article_id = ?",
                    (user_id, article_id),
                )
        return cursor.rowcount > 0

    def update_tracked_product_name(
        self,
        user_id: int,
        article_id: str,
        name: str,
        retailer: str = "",
    ) -> bool:
        with self._connect() as conn:
            if retailer:
                cursor = conn.execute(
                    """
                    UPDATE tracked_products
                    SET name = ?
                    WHERE user_id = ? AND article_id = ? AND retailer = ?
                    """,
                    (name, user_id, article_id, retailer),
                )
                conn.execute(
                    """
                    UPDATE trending_products
                    SET name = ?
                    WHERE user_id = ? AND article_id = ? AND retailer = ?
                    """,
                    (name, user_id, article_id, retailer),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE tracked_products
                    SET name = ?
                    WHERE user_id = ? AND article_id = ?
                    """,
                    (name, user_id, article_id),
                )
                conn.execute(
                    """
                    UPDATE trending_products
                    SET name = ?
                    WHERE user_id = ? AND article_id = ?
                    """,
                    (name, user_id, article_id),
                )
        return cursor.rowcount > 0

    def update_product_discord_exclusion(
        self,
        user_id: int,
        article_id: str,
        exclude_from_discord: bool,
        retailer: str = "",
    ) -> bool:
        with self._connect() as conn:
            exclude_value = 1 if exclude_from_discord else 0
            if retailer:
                cursor = conn.execute(
                    """
                    UPDATE tracked_products
                    SET exclude_from_discord = ?
                    WHERE user_id = ? AND article_id = ? AND retailer = ?
                    """,
                    (exclude_value, user_id, article_id, retailer),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE tracked_products
                    SET exclude_from_discord = ?
                    WHERE user_id = ? AND article_id = ?
                    """,
                    (exclude_value, user_id, article_id),
                )
        return cursor.rowcount > 0

    def reorder_tracked_products(
        self,
        user_id: int,
        product_keys: List[str],
    ) -> bool:
        """Update sort_order for tracked products based on the provided ordered list of product keys."""
        with self._connect() as conn:
            for sort_order, product_key in enumerate(product_keys):
                parts = product_key.split(":", 1)
                retailer = parts[0] if len(parts) > 1 else "walgreens"
                article_id = parts[1] if len(parts) > 1 else product_key
                conn.execute(
                    """
                    UPDATE tracked_products
                    SET sort_order = ?
                    WHERE user_id = ? AND article_id = ? AND retailer = ?
                    """,
                    (sort_order, user_id, article_id, retailer),
                )
        return True

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
        now = datetime.utcnow()
        checks_today_cutoff = now - timedelta(days=1)
        checks_week_cutoff = now - timedelta(days=7)
        recent_scan_window = 50

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_checks,
                    SUM(CASE WHEN has_stock = 1 THEN 1 ELSE 0 END) AS checks_with_stock,
                    MAX(timestamp) AS last_check,
                    SUM(CASE WHEN timestamp >= ? THEN 1 ELSE 0 END) AS checks_today,
                    SUM(CASE WHEN timestamp >= ? THEN 1 ELSE 0 END) AS checks_this_week
                FROM check_history
                WHERE user_id = ?
                """,
                (
                    checks_today_cutoff.isoformat(),
                    checks_week_cutoff.isoformat(),
                    user_id,
                ),
            ).fetchone()

            recent_rows = conn.execute(
                """
                SELECT products_found
                FROM check_history
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (user_id, recent_scan_window),
            ).fetchall()

        total_checks = int((row["total_checks"] or 0) if row else 0)
        checks_with_stock = int((row["checks_with_stock"] or 0) if row else 0)
        success_rate = (checks_with_stock / total_checks * 100) if total_checks else 0
        checks_today = int((row["checks_today"] or 0) if row else 0)
        checks_this_week = int((row["checks_this_week"] or 0) if row else 0)

        tracked_products = self.list_tracked_products(user_id)
        recent_product_hits: Dict[str, Dict[str, Any]] = {
            str(product.get("key") or self._product_key(product["id"], product.get("retailer"))): {
                "id": str(product["id"]),
                "name": str(product["name"] or product["id"]),
                "retailer": str(product.get("retailer") or "walgreens"),
                "recent_hits": 0,
            }
            for product in tracked_products
        }

        for history_row in recent_rows:
            products_found = self._decode_json(history_row["products_found"], {})
            for product_id in products_found.keys():
                if product_id in recent_product_hits:
                    recent_product_hits[product_id]["recent_hits"] += 1
                    continue

                legacy_walgreens_key = self._product_key(product_id, "walgreens")
                if legacy_walgreens_key in recent_product_hits:
                    recent_product_hits[legacy_walgreens_key]["recent_hits"] += 1

        most_active_product = None
        least_active_product = None
        if recent_product_hits:
            activity_values = list(recent_product_hits.values())
            most_active_product = sorted(
                activity_values,
                key=lambda item: (-int(item["recent_hits"]), str(item["name"]).lower(), str(item["id"])),
            )[0]
            least_active_product = sorted(
                activity_values,
                key=lambda item: (int(item["recent_hits"]), str(item["name"]).lower(), str(item["id"])),
            )[0]

        return {
            "total_checks": total_checks,
            "checks_with_stock": checks_with_stock,
            "success_rate": success_rate,
            "last_check": row["last_check"] if row else None,
            "checks_today": checks_today,
            "checks_this_week": checks_this_week,
            "recent_scan_window": len(recent_rows),
            "most_active_product": most_active_product,
            "least_active_product": least_active_product,
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
                        SELECT COUNT(DISTINCT tp.retailer || ':' || tp.article_id)
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
