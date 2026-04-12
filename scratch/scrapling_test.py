from scrapling.engines._browsers._stealth import StealthySession
import logging

logging.basicConfig(level=logging.DEBUG)

with StealthySession(headless=True, solve_cloudflare=True) as s:
    r = s.fetch("https://www.acehardware.com/departments/home-and-decor/novelty-items/toys-and-games/9125200")
    print(f"Status: {r.status}")
    print(f"Body length: {len(r.body)}")
    print(f"Cookies: {s.context.cookies()}")
