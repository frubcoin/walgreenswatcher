"""Retailer-aware product resolver entry points."""

from __future__ import annotations

from typing import Dict
from urllib.parse import urlparse


from walgreens_product_resolver import WalgreensProductResolver

SUPPORTED_RETAILERS = {"walgreens"}


def detect_product_retailer(product_link: str) -> str:
    parsed = urlparse(str(product_link or "").strip())
    hostname = (parsed.hostname or "").lower()

    if hostname == "walgreens.com" or hostname.endswith(".walgreens.com"):
        return "walgreens"
    raise ValueError("Only Walgreens product links are supported right now")


def resolve_product_link(product_link: str) -> Dict[str, str]:
    retailer = detect_product_retailer(product_link)
    if retailer == "walgreens":
        resolved = WalgreensProductResolver.resolve_product_link(product_link)
    else:
        raise ValueError(f"Unsupported retailer: {retailer}")

    resolved["retailer"] = retailer
    return resolved
