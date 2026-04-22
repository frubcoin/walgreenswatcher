import sys
import json
sys.path.append('backend')
from aldi import AldiGraphqlClient

url = 'https://www.aldi.us/store/aldi/products/21770730'
html = AldiGraphqlClient.fetch_product_page(url)
operations = AldiGraphqlClient.operation_hashes(html)
apollo_state = AldiGraphqlClient.extract_apollo_state(html)
token = AldiGraphqlClient.extract_auth_token(apollo_state)

postal_code = '60601'
lat, lon = 41.884, -87.632
stores = AldiGraphqlClient.fetch_stores(latitude=lat, longitude=lon, postal_code=postal_code, operation_hashes=operations, referer=url, token=token)
if stores:
    store = stores[0]
    item = AldiGraphqlClient.fetch_item(product_id='21770730', store=store, postal_code=postal_code, operation_hashes=operations, referer=url)
    print("ITEM:")
    print(json.dumps(item.get('availability', {}), indent=2))
else:
    print('No stores found.')
