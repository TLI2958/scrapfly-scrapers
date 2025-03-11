"""
This example run script shows how to run the Aliexpress.com scraper defined in ./aliexpress.py
It scrapes product data and product search and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import asyncio
import json
import ebay
import re
from datetime import datetime
from pathlib import Path
from loguru import logger as log


output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)


# from chatgpt: generate fdbk url
def generate_ebay_review_url(username, item_id, page_number=1):
    base_url = "https://www.ebay.com/fdbk/mweb_profile"
    params = {
        "fdbkType": "FeedbackReceivedAsSeller",
        "item_id": item_id,
        "username": username,
        "filter": "feedback_page%3ARECEIVED_AS_SELLER",
        "q": item_id,
        "sort": "RELEVANCE",
        "page_id_item": page_number,
        "filter_image_item": "false",
        "page_id": page_number,
    }
    return f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"

kws = [''
        # 'tea',
        # 'tincture',
        # 'supplement',
        # 'drink'
        ]
# prefix = 'https://www.ebay.com/sch/i.html?_nkw=california+poppy+'
prefix = 'https://www.ebay.com/sch/i.html?_nkw=lemon+balm+'

async def run():
    # enable scrapfly cache only during development
    ebay.BASE_CONFIG["cache"] = False
    ebay.BASE_CONFIG["country"] = "US"

    print("running Ebay.com scrape and saving results to ./results directory")

    for k in kws:   
        url = prefix + k
        search_results = await ebay.scrape_search(url.rstrip(), max_pages=5)
        output.joinpath(f"search_products.json").write_text(json.dumps(search_results, indent=2, cls=DateTimeEncoder))

        # urls = [product["url"] for product in search_results]
        # products = await ebay.scrape_products(urls)
        # output.joinpath(f"search_{k}_products.json").write_text(json.dumps(products, indent=2))
        products = json.loads(output.joinpath(f"search_products.json").read_text())
        urls = [generate_ebay_review_url(re.sub(' ', '', product["seller_name"].lower()), product["id"]) for product in products]
        reviews = await ebay.scrape_all_reviews(urls, max_pages= 10)
        output.joinpath(f"search_reviews.json").write_text(json.dumps(reviews, indent=2))
        log.success(f"search_reviews.json saved")
        

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSONEncoder subclass that knows how to encode datetime values."""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()  # Convert datetime objects to ISO-8601 string format
        return super(DateTimeEncoder, self).default(o)  # Default behaviour for other types



if __name__ == "__main__":
    asyncio.run(run())
