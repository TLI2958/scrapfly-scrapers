"""
This is an example web scraper for Walmart.com.

To run this scraper set env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""

import os
import json
import math
import re
from typing import Dict, List, TypedDict, Optional
from urllib.parse import urlencode
from loguru import logger as log
from lxml import html
from parsel import Selector
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse
from pathlib import Path

SCRAPFLY = ScrapflyClient(key=os.environ["SCRAPFLY_KEY"])

BASE_CONFIG = {
    # bypass walmart.com web scraping blocking
    "asp": True,
    # set the proxy country to US
    "country": "US",
}

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

def parse_product(response: ScrapeApiResponse):
    """parse product data from walmart product pages"""
    sel = response.selector
    # data = sel.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
    # data = json.loads(data)
    # _product_raw = data["props"]["pageProps"]["initialData"]["data"]["product"]

    metadata = sel.xpath('//script[@type="application/ld+json" and @data-seo-id="schema-org-product"]/text()').get()
    meta_data = json.loads(metadata)

    wanted_product_keys = [
        "name",
        "sku",
        "description",
        'image',
        'brand',
        'offers',
        "aggregateRating",
    ]

    product = {k: v for k, v in meta_data.items() if k in wanted_product_keys}

    return {"product": product}


def parse_search(response: ScrapeApiResponse) -> List[Dict]:
    """parse product listing data from search pages"""
    sel = response.selector
    data = sel.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
    data = json.loads(data)
    total_results = data["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["count"]
    results = data["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["items"]
    return {"results": results, "total_results": total_results}



def parse_reviews(respones: ScrapeApiResponse):
    """parse product reviews from product pages"""
    sel = respones.selector
    # will select left/right columns separately, hence undesirable None values
    # review_boxes = sel.xpath('//div[contains(@class,"overflow-visible" and contains(@class, "dark-gray"))]/*').getall()
    parsed = []
    key = ['customer_name', 'review_date', 'star_rating', 'review_title', 'review_text']
    
    # solve non-unique 
    customer_name = sel.xpath('//span[contains(@class, "f7") and contains(@class, "b") and contains(@class, "mv0")]/text()').getall()
    review_date = sel.xpath('//div[contains(@class, "f7") and contains(@class, "gray") and contains(@class, "mt1")]/text()').getall()
    star_rating = sel.xpath('//div[contains(@class, "w_ExHd")]/following-sibling::span[contains(@class, "w_iUH7")]/text()').getall()

    review_body = sel.xpath('//div[contains(@class, "overflow-visible")][not(contains(@class, "undefined"))]').getall()
    review_title = sel.xpath('//div[contains(@class, "overflow-visible")][not(contains(@class, "undefined"))]//h3[contains(@class, "w_kV33")]/text()').getall()
    review_text = sel.xpath('//div[contains(@class, "overflow-visible")][not(contains(@class, "undefined"))]//span[contains(@class, "tl-m") and contains(@class, "db-m")]/text()').getall()

    
    j, k = 0, 0
    for i in range(len(review_body)):
        has_title = re.search(r'w_kV33', review_body[i])
        has_text = re.search(r'tl-m db-m', review_body[i])

        if has_title and has_text: 
            parsed.append(dict(zip(key, [customer_name[i], review_date[i], star_rating[i], review_title[j], review_text[k]])))
            j, k = j + 1, k + 1
        elif has_text:
            parsed.append(dict(zip(key, [customer_name[i], review_date[i], star_rating[i], '', review_text[k]])))
            k = k + 1
        elif has_title:
            parsed.append(dict(zip(key, [customer_name[i], review_date[i], star_rating[i], review_title[j], ''])))
            j = j + 1
        else:
            parsed.append(dict(zip(key, [customer_name[i], review_date[i], star_rating[i], '', ''])))
        try:
            print(parsed[-1])    
        except:
            continue
    return parsed


async def scrape_products(res = None) -> List[Dict]:
    """scrape product data from product pages
    res: metadata of products from scrape_search"""
    # add the product pages to a scraping list
    result = []
    urls = [f"https://www.walmart.com/ip/{e['usItemId']}" for e in res if e.get('usItemId', 0) != 0]
    to_scrape = [ScrapeConfig(url, **BASE_CONFIG) for url in urls]
    async for response in SCRAPFLY.concurrent_scrape(to_scrape):
        result.append(parse_product(response))  
        if len(result)%10 == 0:
            log.info('scraped product data from product pages...')

    return result


async def scrape_reviews(res = None, max_pages: Optional[int] = None):
    """scrape product reviews from product pages
        res: metadata of product from scrape_products"""
    total_reviews = int(res.get('product',{}).get("aggregateRating",{}).get('reviewCount', 0))
    id = res['product']['sku']
    first_page = await SCRAPFLY.async_scrape(ScrapeConfig(f"https://www.walmart.com/reviews/product/{id}?", **BASE_CONFIG))
    log.info(f"scraping the first review page from https://www.walmart.com/reviews/product/{id}?")

    reviews = parse_reviews(first_page)
    _reviews_per_page = 10


    total_pages = int(math.ceil(total_reviews / _reviews_per_page))
    if max_pages and total_pages > max_pages:
        total_pages = max_pages
    
    log.info(f"found total {total_reviews} reviews across {total_pages} pages -> scraping")
    other_pages = []
    for page in range(2, total_pages + 1):
        url = f"https://www.walmart.com/reviews/product/{id}?page={page}"
        other_pages.append(ScrapeConfig(url, **BASE_CONFIG))


    async for result in SCRAPFLY.concurrent_scrape(other_pages):
        page_reviews = parse_reviews(result)
        reviews.extend(page_reviews)
    log.info(f"scraped total {len(reviews)} reviews")
    return reviews

async def scrape_product_and_reviews(search_data, key = ''):
    """scrape product and reviews concurrently"""
    result = await scrape_products(search_data)
    with open(output.joinpath(f"Walmart_products_california_poppy_{key}.json"), "a", encoding="utf-8",) as file:
        json.dump(result, file, indent=2, ensure_ascii=False)
        
    # with open(output.joinpath(f"Walmart_products_california_poppy_{key}.json"), "r", encoding="utf-8") as file:
    #     result = json.load(file) 
        
    result_combined = []
    for product in result:
        product_reviews = await scrape_reviews(product, max_pages=5)
        product['product_reviews'] = product_reviews
        result_combined.append(product)
        log.info(f'scraped reviews for product {product["product"]["sku"]}')
        with open(output.joinpath(f"Walmart_product_and_reviews_california_poppy_{key}.json"), "a", encoding="utf-8") as file:
            json.dump(result_combined, file, indent=2, ensure_ascii=False)
            
    return result_combined

async def scrape_search(
    query: str = "",
    sort: TypedDict(
        "SortOptions",
        {"best_seller": str, "best_match": str, "price_low": str, "price_high": str},
    ) = "best_match",
    max_pages: int = None,
):
    """scrape single walmart search page"""

    def make_search_url(page):
        url = "https://www.walmart.com/search?" + urlencode(
            {
                "q": query,
                "page": page,
                sort: sort,
                "affinityOverride": "default",
            }
        )
        return url

    # scrape the first search page
    log.info(f"scraping the first search page with the query ({query})")
    first_page = await SCRAPFLY.async_scrape(
        ScrapeConfig(make_search_url(1), **BASE_CONFIG)
    )
    data = parse_search(first_page)
    search_data = data["results"]
    total_results = data["total_results"]

    # find total page count to scrape
    total_pages = math.ceil(total_results / 40)
    # walmart sets the max search results to 25 pages
    if total_pages > 25:
        total_pages = 25
    if max_pages and max_pages < total_pages:
        total_pages = max_pages

    # then add the remaining pages to a scraping list and scrape them concurrently
    log.info(f"scraping search pagination, remaining ({total_pages - 1}) more pages")
    other_pages = [
        ScrapeConfig(make_search_url(page), **BASE_CONFIG)
        for page in range(2, total_pages + 1)
    ]
    async for response in SCRAPFLY.concurrent_scrape(other_pages):
        search_data.extend(parse_search(response)["results"])
    log.success(f"scraped {len(search_data)} product listings from search pages")
    return search_data


