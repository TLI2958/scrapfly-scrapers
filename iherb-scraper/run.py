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
        # "tea", 
        # "tincture",
        "supplement", 
        # "drink"
        ]

prefix = "https://www.iherb.com/search?kw=california%20poppy%20"
async def run():
    # enable scrapfly cache for basic use
    iherb.BASE_CONFIG["cache"] = True
    iherb.BASE_CONFIG["country"] = "US"

    print("running iherb scrape and saving results to ./results directory")

    for i, k in enumerate(kws):   
        url = prefix + k
        # search = await iherb.scrape_search(url, max_pages=5)
        # output.joinpath(f"search_{k}_extra.json").write_text(json.dumps(search, indent=2))
        
        # log.success(f"search_{k}.json saved")
        search = json.loads(output.joinpath(f"search_{k}_extra.json").read_text())
        # search = [{"url": "https://www.iherb.com/pr/california-gold-nutrition-sport-organic-mct-oil-unflavored-12-fl-oz-355-ml/99738"}]

        # urls = [product["url"] if product["url"].startswith("https://www.iherb.com/pr/") else '/pr/'.join(product["review_url"].split('/r/')) for product in search]

        # urls = [product["url"] if product["url"].startswith("https://www.iherb.com/pr/") else '/pr/'.join(product["review_url"].split('/r/')) for product in search]
        # products = await iherb.scrape_products(urls)
        # output.joinpath(f"search_{k}_products_w_brand_extra.json").write_text(json.dumps(products, indent=2))
        # log.success(f"search_{k}_products_w_brand_extra.json saved")
        
        urls = [product["review_url"] for product in search]
        reviews = await iherb.scrape_all_reviews(urls, max_pages= 10)
        output.joinpath(f"search_{k}_reviews_extra.json").write_text(json.dumps(reviews, indent=2))
        
        log.success(f"search_{k}_reviews_extra.json saved")
if __name__ == "__main__":
    asyncio.run(run())
