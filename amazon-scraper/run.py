"""
This example run script shows how to run the Amazon.com scraper defined in ./amazon.py
It scrapes product data and product search and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import asyncio
import json
from pathlib import Path
import amazon

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

urls = ["https://www.amazon.com/s?k=california+poppy+tincture"]
async def run():
    # enable scrapfly cache for basic use
    amazon.BASE_CONFIG["cache"] = True
    amazon.BASE_CONFIG["country"] = "US"

    print("running Amazon scrape and saving results to ./results directory")

    for i, url in enumerate(urls):    
        search = await amazon.scrape_search(url, max_pages=3)
        output.joinpath(f"search_{i}.json").write_text(json.dumps(search, indent=2))
        
        for product in search:
            url = product["url"].split('ref')[0]
            brand = product.get('brand', '')
            product_data = await amazon.scrape_product(url)
            product_data = product_data[0]
            ASIN = product_data.get('asin', '')

            if not ASIN:
                continue
            else:
                review_url = url.split(f'dp/{ASIN}')[0]  + 'product-reviews/' + ASIN + '/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
                reviews = await amazon.scrape_reviews(review_url, ASIN, max_pages=3)
                for review in reviews:
                    review.update(product_data)
                    review.update({'brand': brand})

                    with output.joinpath(f"search_california_poppy_tincture_products_reviews.json").open('a', encoding='utf-8') as file:
                        file.write(json.dumps(review, indent=2) + ",\n") 


if __name__ == "__main__":
    asyncio.run(run())
