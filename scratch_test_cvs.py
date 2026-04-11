import os
import sys
import json
import logging

# Ensure backend modules can be imported
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/backend'))

from cvs_product_resolver import CvsProductResolver

CVS_URL = "https://www.cvs.com/shop/gillette-venus-comfortglide-white-tea-3-blade-razor-2-razor-blade-refills-prodid-1010317?skuId=482401&cgaa=QWxsb3dHb29nbGVUb0FjY2Vzc0NWU1BhZ2Vz"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Mock proxies for testing
PROXIES = [
    "http://107540_AEsKv:Pc4YqUtJWg@155.117.149.186:61232",
    "http://107540_AEsKv:Pc4YqUtJWg@154.16.13.115:61234",
    "http://107540_AEsKv:Pc4YqUtJWg@45.45.154.91:61234",
    "http://107540_AEsKv:Pc4YqUtJWg@140.233.248.195:61234",
]

# Inject proxies into config or monkeypatch CvsProductResolver
import cvs_product_resolver
cvs_product_resolver.CVS_PROXY_URLS = PROXIES

if __name__ == "__main__":
    print(f"=== Testing CVS Resolver for {CVS_URL} ===")
    try:
        resolved = CvsProductResolver.resolve_product_link(CVS_URL)
        print("\nSUCCESS!")
        print(json.dumps(resolved, indent=2))
    except Exception as e:
        print(f"\nFAILED: {e}")
        import traceback
        traceback.print_exc()
