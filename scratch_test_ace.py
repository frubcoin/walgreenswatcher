import os
import sys

# Ensure backend modules can be imported
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/backend'))

from ace_scraper import AceStockChecker

if __name__ == "__main__":
    checker = AceStockChecker()
    product = {
        "article_id": "9125200",
        "name": "Squishmallows",
        "source_url": "https://www.acehardware.com/departments/home-and-decor/novelty-items/toys-and-games/9125200"
    }
    
    # 85282 is near the user's test locations in Tempe
    result = checker.check_product_availability(product, zip_code="85282")
    
    print("FINISHED CHECK!")
    print(f"Products tracked: {result}")
