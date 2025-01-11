import asyncio
import json
from pathlib import Path
import amazon

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

kws = [
        # "tea", 
        # "tincture",
        "supplement", 
        # "drink"
        ]

prefix = "https://www.amazon.com/s?k=california+poppy+"

async def run():
    # enable scrapfly cache for basic use
    amazon.BASE_CONFIG["cache"] = False
    amazon.BASE_CONFIG["country"] = "US"

    print("running Amazon scrape and saving results to ./results directory")

    for i, k in enumerate(kws):   
        url = prefix + k
        # url = prefix + k
        # search = await amazon.scrape_search(url, max_pages=3)
        search = json.loads(output.joinpath(f"search_{k}.json").read_text())
        # output.joinpath(f"search_{k}.json").write_text(json.dumps(search, indent=2))
        
        # log.success(f"search_{k}.json saved")

        # urls = [product["url"] for product in search]
        # products = await iherb.scrape_products(urls)
        # output.joinpath(f"search_{k}_products.json").write_text(json.dumps(products, indent=2))
        
        for product in search:
            url = product["url"]
            # brand = product.get('brand', '')
            product_data = await amazon.scrape_product(url)
            if not product_data:
                continue
            product_data = product_data[0]
            with output.joinpath(f"search_california_poppy_{k}_products_only.json").open('a', encoding='utf-8') as file:
                file.write(json.dumps(product_data, indent=2) + ",\n") 

            # if not ASIN:
            #     continue
            # else:
            #     review_url = url.split(f'dp/{ASIN}')[0]  + 'product-reviews/' + ASIN + '/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
            #     reviews = await amazon.scrape_reviews(review_url, ASIN, max_pages=5)
            #     for review in reviews:
            #         review.update(product_data)
            #         review.update({'brand': brand})

            #         with output.joinpath(f"search_california_poppy_{k}_products_reviews.json").open('a', encoding='utf-8') as file:
            #             file.write(json.dumps(review, indent=2) + ",\n") 


if __name__ == "__main__":
    asyncio.run(run())
