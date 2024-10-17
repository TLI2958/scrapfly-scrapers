"""
This example run script shows how to run the Walmart.com scraper defined in ./walmart.py
It scrapes product data and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""

import asyncio
import json
from pathlib import Path
import walmart

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)


async def run():
    # enable scrapfly cache for basic use
    walmart.BASE_CONFIG["cache"] = False

    print("running Walmart scrape and saving results to ./results directory")
    # search_data = await walmart.scrape_search(
    #     query="california+poppy", sort="best_seller", max_pages=3
    # )
    # with open(output.joinpath("search_california_poppy.json"), "a", encoding="utf-8") as file:
    #     json.dump(search_data, file, indent=2, ensure_ascii=False)

    with open(output.joinpath("search_california_poppy.json"), "r", encoding="utf-8") as file:
        search_data = json.load(file) 
    # search_data = [{'usItemId': '3931738164'}]
    product_and_reviews = await walmart.scrape_product_and_reviews(search_data)
    
    # with open(output.joinpath("Walmart_product_and_reviews_california_poppy.json"), "a", encoding="utf-8") as file:
    #     json.dump(product_and_reviews, file, indent=2, ensure_ascii=False)

    # products_data = await walmart.scrape_products(
    #     urls=[
    #         "https://www.walmart.com/ip/28931700",
    #         ]
    # )
    # with open(output.joinpath("products.json"), "w", encoding="utf-8") as file:
    #     json.dump(products_data, file, indent=2, ensure_ascii=False)


    
    # product_id = [search_data['usItemId'] if search_data['usItemId'] else None]
    # product_id = 28931700
    # product_reviews = await walmart.scrape_reviews(product_id)
    # with open(output.joinpath("reviews_null.json"), "w", encoding="utf-8") as file:
    #     json.dump(product_reviews, file, indent=2, ensure_ascii=False)
    


if __name__ == "__main__":
    asyncio.run(run())
