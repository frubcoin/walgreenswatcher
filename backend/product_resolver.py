"""Retailer-aware product resolver entry points."""

from __future__ import annotations

from typing import Dict
from urllib.parse import urlparse

from bestbuy_product_resolver import BestBuyProductResolver
from walgreens_product_resolver import WalgreensProductResolver

SUPPORTED_RETAILERS = {"walgreens", "bestbuy"}


def detect_product_retailer(product_link: str) -> str:
    parsed = urlparse(str(product_link or "").strip())
    hostname = (parsed.hostname or "").lower()

    if hostname == "walgreens.com" or hostname.endswith(".walgreens.com"):
        return "walgreens"
    if hostname == "bestbuy.com" or hostname.endswith(".bestbuy.com"):
        return "bestbuy"
    raise ValueError("Only Walgreens and Best Buy product links are supported right now")


def resolve_product_link(product_link: str) -> Dict[str, str]:
    retailer = detect_product_retailer(product_link)
    if retailer == "walgreens":
        resolved = WalgreensProductResolver.resolve_product_link(product_link)
    elif retailer == "bestbuy":
        resolved = BestBuyProductResolver.resolve_product_link(product_link)
    else:
        raise ValueError(f"Unsupported retailer: {retailer}")

    resolved["retailer"] = retailer
    return resolved
