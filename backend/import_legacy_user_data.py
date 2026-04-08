"""Import legacy single-user JSON data into a hosted per-user SQLite account."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from config import APP_DATABASE_FILE, DATA_DIR


def _load_json(path: Path, fallback: Any) -> Any:
    if not path.is_file():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"Failed to read {path}: {exc}") from exc


def _prompt(prompt: str) -> str:
    return input(prompt).strip()


def _find_user(conn: sqlite3.Connection, email: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, email, name, google_sub, created_at, last_login_at
        FROM users
        WHERE lower(trim(email)) = lower(trim(?))
        ORDER BY id DESC
        LIMIT 1
        """,
        (email,),
    ).fetchone()


def _ensure_user_settings(conn: sqlite3.Connection, user_id: int) -> None:
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
        VALUES (?, '', 60, '[]', 0, 'gyra', 645, 0)
        ON CONFLICT(user_id) DO NOTHING
        """,
        (int(user_id),),
    )


def import_settings(conn: sqlite3.Connection, user_id: int, settings: Dict[str, Any]) -> None:
    _ensure_user_settings(conn, user_id)
    conn.execute(
        """
        UPDATE user_settings
        SET current_zipcode = ?,
            check_interval_minutes = ?,
            discord_destinations = ?,
            pokemon_background_enabled = ?,
            pokemon_background_theme = ?,
            pokemon_background_tile_size = ?
        WHERE user_id = ?
        """,
        (
            str(settings.get("current_zipcode") or "").strip(),
            int(settings.get("check_interval_minutes") or 60),
            json.dumps(settings.get("discord_destinations") or []),
            int(bool(settings.get("pokemon_background_enabled"))),
            str(settings.get("pokemon_background_theme") or "gyra").strip() or "gyra",
            int(settings.get("pokemon_background_tile_size") or 645),
            int(user_id),
        ),
    )


def import_products(conn: sqlite3.Connection, user_id: int, products: Dict[str, Dict[str, Any]]) -> int:
    created_at = datetime.utcnow().isoformat()
    imported = 0
    for article_id, product in (products or {}).items():
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO tracked_products (
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
            VALUES (?, ?, 'walgreens', ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                str(article_id).strip(),
                str(product.get("name") or "").strip(),
                str(product.get("planogram") or "").strip(),
                str(product.get("image_url") or "").strip(),
                str(product.get("source_url") or "").strip(),
                str(product.get("product_id") or "").strip(),
                created_at,
            ),
        )
        imported += int(cursor.rowcount or 0)
    return imported


def main() -> None:
    data_dir = Path(DATA_DIR)
    db_path = Path(APP_DATABASE_FILE)
    tracked_products = _load_json(data_dir / "tracked_products.json", {})
    app_settings = _load_json(data_dir / "app_settings.json", {})

    if not db_path.is_file():
        raise SystemExit(f"Database not found at {db_path}")

    if not tracked_products and not app_settings:
        raise SystemExit("No legacy JSON data found to import.")

    email = _prompt("Google account email to receive the imported data: ")
    if not email:
        raise SystemExit("Email is required.")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        user = _find_user(conn, email)
        if user is None:
            raise SystemExit(
                f"No user record found for {email}. Sign in once through the app first, then rerun this importer."
            )

        import_settings(conn, int(user["id"]), app_settings)
        imported_products = import_products(conn, int(user["id"]), tracked_products)
        conn.commit()

        total_products = conn.execute(
            "SELECT COUNT(*) FROM tracked_products WHERE user_id = ?",
            (int(user["id"]),),
        ).fetchone()[0]
    finally:
        conn.close()

    print(f"Imported data into user #{user['id']} ({user['email']}).")
    print(f"Added {imported_products} tracked product(s).")
    print(f"User now has {total_products} tracked product(s) in SQLite.")


if __name__ == "__main__":
    main()
