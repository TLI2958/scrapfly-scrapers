"""
This example run script shows how to run the iherb.com scraper defined in ./iherb.py
It scrapes product data and product search and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import asyncio
import json
from pathlib import Path
import iherb
from loguru import logger as log


output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

kws = [
        "tea", 
        "tincture",
        "supplement", 
        "drink"
        ]

prefix = "https://www.iherb.com/search?kw=california%20poppy%20"
async def run():
    # enable scrapfly cache for basic use
    iherb.BASE_CONFIG["cache"] = True
    iherb.BASE_CONFIG["country"] = "US"

    print("running iherb scrape and saving results to ./results directory")

    for i, k in enumerate(kws):   
        url = prefix + k
        search = await iherb.scrape_search(url, max_pages=3)
        output.joinpath(f"search_{k}.json").write_text(json.dumps(search, indent=2))
        
        log.success(f"search_{k}.json saved")

        # urls = [product["url"] for product in search]
        # products = await iherb.scrape_products(urls)
        # output.joinpath(f"search_{k}_products.json").write_text(json.dumps(products, indent=2))
        
        # for product in search:
        #     url = product["url"]
        #     brand = product.get('brand', '')
        #     product_data = await iherb.scrape_product(url)
        #     product_data = product_data[0]
        #     ASIN = product_data.get('asin', '')

            # if not ASIN:
            #     continue
            # else:
            #     review_url = url.split(f'dp/{ASIN}')[0]  + 'product-reviews/' + ASIN + '/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
            #     reviews = await iherb.scrape_reviews(review_url, ASIN, max_pages=3)
            #     for review in reviews:
            #         review.update(product_data)
            #         review.update({'brand': brand})

            #         with output.joinpath(f"search_california_poppy_{k}_products_reviews.json").open('a', encoding='utf-8') as file:
            #             file.write(json.dumps(review, indent=2) + ",\n") 


if __name__ == "__main__":
    asyncio.run(run())
