"""Merge users and related data from an older backup SQLite DB into the active DB."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from config import APP_DATABASE_FILE


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _rows(conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> list[Dict[str, Any]]:
    return [dict(row) for row in conn.execute(query, tuple(params)).fetchall()]


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _default_str(value: Any, default: str = "") -> str:
    return str(value if value is not None else default)


def _default_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def _upsert_users(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    dry_run: bool,
) -> tuple[Dict[int, int], Dict[str, int]]:
    id_map: Dict[int, int] = {}
    counts = {"users_inserted": 0, "users_updated": 0}

    if not _has_table(src, "users"):
        return id_map, counts

    for user in _rows(src, "SELECT * FROM users ORDER BY id ASC"):
        google_sub = _default_str(user.get("google_sub")).strip()
        if not google_sub:
            continue

        existing = dst.execute(
            "SELECT id FROM users WHERE google_sub = ?",
            (google_sub,),
        ).fetchone()

        params = (
            _default_str(user.get("email")).strip().lower(),
            _default_str(user.get("name")).strip(),
            _default_str(user.get("picture")).strip(),
            _bool_int(user.get("is_banned")),
            _default_str(user.get("ban_reason")).strip(),
            _default_str(user.get("banned_at")).strip(),
            _default_str(user.get("created_at")).strip(),
            _default_str(user.get("last_login_at")).strip(),
        )

        if existing is None:
            if not dry_run:
                cursor = dst.execute(
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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (google_sub, *params),
                )
                new_id = int(cursor.lastrowid)
            else:
                new_id = -1
            id_map[_default_int(user.get("id"))] = new_id
            counts["users_inserted"] += 1
            continue

        new_id = int(existing["id"])
        id_map[_default_int(user.get("id"))] = new_id
        if not dry_run:
            dst.execute(
                """
                UPDATE users
                SET email = ?,
                    name = ?,
                    picture = ?,
                    is_banned = ?,
                    ban_reason = ?,
                    banned_at = ?,
                    created_at = CASE
                        WHEN created_at = '' OR created_at IS NULL THEN ?
                        WHEN ? = '' THEN created_at
                        WHEN ? < created_at THEN ?
                        ELSE created_at
                    END,
                    last_login_at = CASE
                        WHEN last_login_at = '' OR last_login_at IS NULL THEN ?
                        WHEN ? = '' THEN last_login_at
                        WHEN ? > last_login_at THEN ?
                        ELSE last_login_at
                    END
                WHERE id = ?
                """,
                (
                    *params[:6],
                    params[6],
                    params[6],
                    params[6],
                    params[6],
                    params[7],
                    params[7],
                    params[7],
                    params[7],
                    new_id,
                ),
            )
        counts["users_updated"] += 1

    return id_map, counts


def _import_user_settings(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    id_map: Dict[int, int],
    dry_run: bool,
) -> int:
    if not _has_table(src, "user_settings"):
        return 0

    imported = 0
    for row in _rows(src, "SELECT * FROM user_settings"):
        dst_user_id = id_map.get(_default_int(row.get("user_id")))
        if not dst_user_id:
            continue
        if not dry_run:
            dst.execute(
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    current_zipcode = excluded.current_zipcode,
                    check_interval_minutes = excluded.check_interval_minutes,
                    discord_destinations = excluded.discord_destinations,
                    pokemon_background_enabled = excluded.pokemon_background_enabled,
                    pokemon_background_theme = excluded.pokemon_background_theme,
                    pokemon_background_tile_size = excluded.pokemon_background_tile_size,
                    scheduler_enabled = excluded.scheduler_enabled
                """,
                (
                    dst_user_id,
                    _default_str(row.get("current_zipcode")).strip(),
                    _default_int(row.get("check_interval_minutes"), 60),
                    _default_str(row.get("discord_destinations"), "[]"),
                    _bool_int(row.get("pokemon_background_enabled")),
                    _default_str(row.get("pokemon_background_theme"), "gyra").strip() or "gyra",
                    _default_int(row.get("pokemon_background_tile_size"), 645),
                    _bool_int(row.get("scheduler_enabled")),
                ),
            )
        imported += 1
    return imported


def _import_tracked_products(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    id_map: Dict[int, int],
    dry_run: bool,
) -> int:
    if not _has_table(src, "tracked_products"):
        return 0

    imported = 0
    for row in _rows(src, "SELECT * FROM tracked_products ORDER BY user_id ASC, created_at ASC"):
        dst_user_id = id_map.get(_default_int(row.get("user_id")))
        if not dst_user_id:
            continue
        if not dry_run:
            dst.execute(
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, article_id) DO UPDATE SET
                    retailer = excluded.retailer,
                    name = excluded.name,
                    planogram = excluded.planogram,
                    image_url = excluded.image_url,
                    source_url = excluded.source_url,
                    product_id = excluded.product_id
                """,
                (
                    dst_user_id,
                    _default_str(row.get("article_id")).strip(),
                    _default_str(row.get("retailer"), "walgreens").strip() or "walgreens",
                    _default_str(row.get("name")).strip(),
                    _default_str(row.get("planogram")).strip(),
                    _default_str(row.get("image_url")).strip(),
                    _default_str(row.get("source_url")).strip(),
                    _default_str(row.get("product_id")).strip(),
                    _default_str(row.get("created_at")).strip(),
                ),
            )
        imported += 1
    return imported


def _import_check_history(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    id_map: Dict[int, int],
    dry_run: bool,
) -> int:
    if not _has_table(src, "check_history"):
        return 0

    existing = {
        (
            row["user_id"],
            row["timestamp"],
            row["total_stores_checked"],
            row["products_found"],
            row["has_stock"],
        )
        for row in _rows(
            dst,
            "SELECT user_id, timestamp, total_stores_checked, products_found, has_stock FROM check_history",
        )
    }

    imported = 0
    for row in _rows(src, "SELECT * FROM check_history ORDER BY timestamp ASC, id ASC"):
        dst_user_id = id_map.get(_default_int(row.get("user_id")))
        if not dst_user_id:
            continue
        signature = (
            dst_user_id,
            _default_str(row.get("timestamp")).strip(),
            _default_int(row.get("total_stores_checked")),
            _default_str(row.get("products_found"), "{}"),
            _bool_int(row.get("has_stock")),
        )
        if signature in existing:
            continue
        if not dry_run:
            dst.execute(
                """
                INSERT INTO check_history (
                    user_id,
                    timestamp,
                    total_stores_checked,
                    products_found,
                    has_stock
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                signature,
            )
        existing.add(signature)
        imported += 1
    return imported


def _import_authorized_google_emails(src: sqlite3.Connection, dst: sqlite3.Connection, dry_run: bool) -> int:
    if not _has_table(src, "authorized_google_emails"):
        return 0

    imported = 0
    for row in _rows(src, "SELECT * FROM authorized_google_emails ORDER BY email ASC"):
        if not dry_run:
            dst.execute(
                """
                INSERT INTO authorized_google_emails (email, note, added_at)
                VALUES (?, ?, ?)
                ON CONFLICT(email) DO UPDATE SET
                    note = excluded.note
                """,
                (
                    _default_str(row.get("email")).strip().lower(),
                    _default_str(row.get("note")).strip(),
                    _default_str(row.get("added_at")).strip(),
                ),
            )
        imported += 1
    return imported


def _import_admin_settings(src: sqlite3.Connection, dst: sqlite3.Connection, dry_run: bool) -> int:
    if not _has_table(src, "admin_settings"):
        return 0

    imported = 0
    for row in _rows(src, "SELECT * FROM admin_settings ORDER BY key ASC"):
        if not dry_run:
            dst.execute(
                """
                INSERT INTO admin_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (
                    _default_str(row.get("key")).strip(),
                    _default_str(row.get("value")).strip(),
                    _default_str(row.get("updated_at")).strip(),
                ),
            )
        imported += 1
    return imported


def _import_audit_events(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    id_map: Dict[int, int],
    dry_run: bool,
) -> int:
    if not _has_table(src, "audit_events"):
        return 0

    existing = {
        (
            row["event_type"],
            row["actor_user_id"],
            row["target_user_id"],
            row["user_email"],
            row["summary"],
            row["metadata_json"],
            row["created_at"],
        )
        for row in _rows(
            dst,
            """
            SELECT event_type, actor_user_id, target_user_id, user_email, summary, metadata_json, created_at
            FROM audit_events
            """,
        )
    }

    imported = 0
    for row in _rows(src, "SELECT * FROM audit_events ORDER BY created_at ASC, id ASC"):
        signature = (
            _default_str(row.get("event_type")).strip(),
            id_map.get(_default_int(row.get("actor_user_id"))) if row.get("actor_user_id") is not None else None,
            id_map.get(_default_int(row.get("target_user_id"))) if row.get("target_user_id") is not None else None,
            _default_str(row.get("user_email")).strip().lower(),
            _default_str(row.get("summary")).strip(),
            _default_str(row.get("metadata_json"), "{}"),
            _default_str(row.get("created_at")).strip(),
        )
        if signature in existing:
            continue
        if not dry_run:
            dst.execute(
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
                signature,
            )
        existing.add(signature)
        imported += 1
    return imported


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import users and related data from an old Walgreens Watcher SQLite backup.",
    )
    parser.add_argument("source_db", help="Path to the old backup watcher.sqlite3")
    parser.add_argument(
        "--dest-db",
        default=APP_DATABASE_FILE,
        help=f"Destination DB path. Defaults to {APP_DATABASE_FILE}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without writing changes.",
    )
    args = parser.parse_args()

    source_db = Path(args.source_db).resolve()
    dest_db = Path(args.dest_db).resolve()

    if not source_db.is_file():
        raise SystemExit(f"Source DB not found: {source_db}")
    if not dest_db.is_file():
        raise SystemExit(f"Destination DB not found: {dest_db}")
    if source_db == dest_db:
        raise SystemExit("Source and destination DB paths must be different.")

    src = _connect(source_db)
    dst = _connect(dest_db)
    try:
        if args.dry_run:
            dst.execute("BEGIN")
        id_map, user_counts = _upsert_users(src, dst, args.dry_run)
        stats = {
            **user_counts,
            "user_settings_imported": _import_user_settings(src, dst, id_map, args.dry_run),
            "tracked_products_imported": _import_tracked_products(src, dst, id_map, args.dry_run),
            "check_history_imported": _import_check_history(src, dst, id_map, args.dry_run),
            "authorized_google_emails_imported": _import_authorized_google_emails(src, dst, args.dry_run),
            "admin_settings_imported": _import_admin_settings(src, dst, args.dry_run),
            "audit_events_imported": _import_audit_events(src, dst, id_map, args.dry_run),
        }
        if args.dry_run:
            dst.rollback()
        else:
            dst.commit()
    finally:
        src.close()
        dst.close()

    print(f"Source DB: {source_db}")
    print(f"Destination DB: {dest_db}")
    print("Mode:", "dry-run" if args.dry_run else "import")
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
