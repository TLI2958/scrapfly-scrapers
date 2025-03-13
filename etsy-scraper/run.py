"""
This example run script shows how to run the etsy.com scraper defined in ./etsy.py
It scrapes ads data and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import asyncio
import json
from pathlib import Path
import etsy
from loguru import logger as log

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)
urls = [
# 'https://www.etsy.com/search/home-and-living/food-and-drink?q=%22california+poppy%22&category_path=891%2C930'
# 'https://www.etsy.com/search/home-and-living/outdoor-and-garden/plants/herbs?explicit=1&q=%26quot%3BCalifornia+poppy%26quot%3B&category_path=891%2C1105%2C1120%2C1126&ship_to=US',
'https://www.etsy.com/search?q=california+poppy+tea&ref=search_bar'
# 'https://www.etsy.com/search/home-and-living/food-and-drink/coffee-and-tea/tea?q=%22California+poppy%22&explicit=1&category_path=891%2C930%2C951%2C955&ship_to=US'
]

async def run():
    etsy.BASE_CONFIG["cache"] = False
    log.info("running Etsy scrape and saving results to ./results directory")
    for i, url in enumerate(urls):
        # product_data = await etsy.scrape_product(url, search_data = None, max_review_pages= 10)
        search_data, product_data = await etsy.scrape_search_and_products(
            search_url=url,
            max_pages=1,
            max_review_pages= 5
        )
        with open(output.joinpath(f"search_null.json"), "w", encoding="utf-8") as file:
            json.dump(search_data, file, indent=2, ensure_ascii=False)

        with open(output.joinpath(f"products_null.json"), "w", encoding="utf-8") as file:
            json.dump(product_data, file, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    asyncio.run(run())
