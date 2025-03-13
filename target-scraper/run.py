"""
This example run script shows how to run the target.com scraper defined in ./target.py
It scrapes product data and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""

import asyncio
import json
from pathlib import Path
import target

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)


async def run():
    # enable scrapfly cache for basic use
    target.BASE_CONFIG["cache"] = True
    target.BASE_CONFIG["country"] = "US"
    

    print("running target scrape and saving results to ./results directory")
    kw = [
        # "tincture", 
        #   "drink", 
        #   "tea", 
          "supplement"
         ]
    
    for k in kw:
        # search_data = await target.scrape_search(
        #     query="california+poppy" + "+" + k,  max_pages=10
        # )
        # with open(output.joinpath(f"search_california_poppy_{k}.json"), "a", encoding="utf-8") as file:
        #     json.dump(search_data, file, indent=2, ensure_ascii=False)
        with open(output.joinpath(f"search_california_poppy_{k}.json"), "r", encoding="utf-8") as file:
            search_data = json.load(file)   
            
        # search_data = [{'usItemId': '3931738164'}]
        # res = ['https://www.target.com/p/yogi-tea-elderberry-lemon-balm-immune-stress-16ct/-/A-81503691#lnk=sametab']
        # review = await target.scrape_reviews(res)
        product_and_reviews = await target.scrape_product_and_reviews(search_data, key = k)
    
        with open(output.joinpath(f"target_product_and_reviews_california_poppy_{k}.json"), "a", encoding="utf-8") as file:
            json.dump(product_and_reviews, file, indent=2, ensure_ascii=False)

    # products_data = await target.scrape_products(
    #     urls=[
    #         "https://www.target.com/ip/28931700",
    #         ]
    # )
    # with open(output.joinpath("products.json"), "w", encoding="utf-8") as file:
    #     json.dump(products_data, file, indent=2, ensure_ascii=False)


    
    # product_id = [search_data['usItemId'] if search_data['usItemId'] else None]
    # product_id = 28931700
    # product_reviews = await target.scrape_reviews(product_id)
    # with open(output.joinpath("reviews_null.json"), "w", encoding="utf-8") as file:
    #     json.dump(product_reviews, file, indent=2, ensure_ascii=False)
    


if __name__ == "__main__":
    asyncio.run(run())
