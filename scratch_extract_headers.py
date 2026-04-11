import sys
import os
import json

# Add backend to path
sys.path.insert(0, os.path.abspath('backend'))

from scrapling.engines._browsers._stealth import StealthySession

URL = "https://www.acehardware.com/departments/home-and-decor/novelty-items/toys-and-games/9125200"

def action(page):
    print("Page Title:", page.title())
    # We want to find the headers used in API calls.
    # We can inspect the network requests if possible, or look for inline config.
    # Often Kibo sites have an inline JS object with these IDs.
    config = page.evaluate("() => window.Hyngage || window.KiboCommerce || {}")
    print("UI Config:", json.dumps(config))
    
    # Try to find common Kibo IDs in the page text
    content = page.content()
    for key in ["X-Vol-Site", "X-Vol-Tenant"]:
        if key in content:
            print(f"Found {key} in content!")

with StealthySession(headless=True) as session:
    session.fetch(URL, page_action=action)
