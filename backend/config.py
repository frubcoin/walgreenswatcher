"""Configuration management for Walgreens Stock Watcher."""

from __future__ import annotations

import os
from typing import List

from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: str = "") -> List[str]:
    value = os.getenv(name, default)
    return [part.strip() for part in value.split(",") if part.strip()]


# App / Auth Configuration
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "change-me-in-production")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "walgreens_watcher_session")
SESSION_COOKIE_SECURE = _env_bool("SESSION_COOKIE_SECURE", False)
SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
SESSION_COOKIE_DOMAIN = os.getenv("SESSION_COOKIE_DOMAIN", "").strip() or None
CORS_ALLOWED_ORIGINS = _env_csv(
    "CORS_ALLOWED_ORIGINS",
    "http://localhost:5000,http://127.0.0.1:5000,http://localhost:8788,http://127.0.0.1:8788",
)

# Discord Configuration
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DISCORD_ROLE_ID = os.getenv("DISCORD_ROLE_TO_MENTION", "")

# Walgreens Configuration
DEFAULT_TRACKED_PRODUCTS = {
    "000000000012449025": {
        "name": "Pokemon Trading Card Game, Elite Trainer Box",
        "planogram": "40000405020",
        "product_id": "300455939",
        "image_url": "https://pics.walgreens.com/prodimg/6784463/900.jpg",
        "source_url": "https://www.walgreens.com/store/c/pokemon-trading-card-game,-elite-trainer-box/ID=300455939-product",
    }
}

WALGREENS_PRODUCT_NAMES = {
    article_id: product["name"] for article_id, product in DEFAULT_TRACKED_PRODUCTS.items()
}

# Location Configuration
TARGET_ZIP_CODE = "55555"
SEARCH_RADIUS_MILES = 20

# Rate Limiting Configuration
RATE_LIMIT_DELAY = 2
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5

# Scheduling Configuration
DEFAULT_CHECK_INTERVAL_MINUTES = 60

# Headers for requests
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# Storage
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
APP_DATABASE_FILE = os.path.join(DATA_DIR, "watcher.sqlite3")
