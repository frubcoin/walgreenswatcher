import sys
sys.path.append('backend')
from config import CVS_PROXY_URLS
from aldi import AldiGraphqlClient

print('PROXIES:', CVS_PROXY_URLS)
url = 'https://www.aldi.us/store/aldi/products/21770730'
try:
    print('Testing fallback...')
    html = AldiGraphqlClient.fetch_product_page(url)
    print('SUCCESS, length:', len(html))
except Exception as e:
    print('FAILED:', str(e))
