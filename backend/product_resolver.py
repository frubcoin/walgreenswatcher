"""Retailer-aware product resolver entry points."""

from __future__ import annotations

from typing import Dict
from urllib.parse import urlparse

from ace_product_resolver import AceProductResolver
from cvs_product_resolver import CvsProductResolver
from fivebelow_product_resolver import FiveBelowProductResolver
from walgreens_product_resolver import WalgreensProductResolver

SUPPORTED_RETAILERS = {"walgreens", "cvs", "fivebelow", "ace"}


def detect_product_retailer(product_link: str) -> str:
    parsed = urlparse(str(product_link or "").strip())
    hostname = (parsed.hostname or "").lower()

    if hostname == "walgreens.com" or hostname.endswith(".walgreens.com"):
        return "walgreens"
    if hostname == "cvs.com" or hostname.endswith(".cvs.com"):
        return "cvs"
    if hostname == "fivebelow.com" or hostname.endswith(".fivebelow.com"):
        return "fivebelow"
    if hostname == "acehardware.com" or hostname.endswith(".acehardware.com"):
        return "ace"
    raise ValueError("Only Walgreens, CVS, Five Below, and Ace Hardware product links are supported right now")


def resolve_product_link(product_link: str) -> Dict[str, str]:
    retailer = detect_product_retailer(product_link)
    if retailer == "walgreens":
        resolved = WalgreensProductResolver.resolve_product_link(product_link)
    elif retailer == "cvs":
        resolved = CvsProductResolver.resolve_product_link(product_link)
    elif retailer == "fivebelow":
        resolved = FiveBelowProductResolver.resolve_product_link(product_link)
    elif retailer == "ace":
        resolved = AceProductResolver.resolve_product_link(product_link)
    else:
        raise ValueError(f"Unsupported retailer: {retailer}")

    resolved["retailer"] = retailer
    return resolved
