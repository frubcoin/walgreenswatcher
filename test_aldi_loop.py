import sys
sys.path.append('backend')
from aldi import AldiGraphqlClient
success, fails = 0, 0
for i in range(10):
    try:
        html = AldiGraphqlClient.fetch_product_page('https://www.aldi.us/store/aldi/products/21770730')
        success += 1
    except Exception as e:
        fails += 1
print(f'SUCCESS: {success}, FAILS: {fails}')
