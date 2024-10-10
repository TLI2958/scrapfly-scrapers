"""
This is an example web scraper for Walmart.com.

To run this scraper set env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""

import os
import json
import math
from typing import Dict, List, TypedDict, Optional
from urllib.parse import urlencode
from loguru import logger as log
from lxml import html
from parsel import Selector
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse

SCRAPFLY = ScrapflyClient(key=os.environ["SCRAPFLY_KEY"])

BASE_CONFIG = {
    # bypass walmart.com web scraping blocking
    "asp": True,
    # set the proxy country to US
    "country": "US",
}


def parse_product(response: ScrapeApiResponse):
    """parse product data from walmart product pages"""
    sel = response.selector
    data = sel.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
    data = json.loads(data)
    _product_raw = data["props"]["pageProps"]["initialData"]["data"]["product"]
    # There's a lot of product data, including private meta keywords, so we need to do some filtering:
    wanted_product_keys = [
        "availabilityStatus",
        "averageRating",
        "brand",
        "id",
        "imageInfo",
        "manufacturerName",
        "name",
        "orderLimit",
        "orderMinLimit",
        "priceInfo",
        "shortDescription",
        "type",
    ]
    product = {k: v for k, v in _product_raw.items() if k in wanted_product_keys}
    reviews_raw = data["props"]["pageProps"]["initialData"]["data"]["reviews"]
    return {"product": product, "reviews": reviews_raw}


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
    review_boxes = sel.xpath('//div[contains(@class,"overflow-visible" and contains(@class, "dark-gray"))]/*')
    overall_rating = sel.xpath('//div[contains(@class, "mb2")]//span[contains(@class, "w_iUH7")]/text()').get().split()[0]
    total_reviews = sel.xpath('//div[contains(@class, "mb2")]//span[contains(@class, "ml2")]/text()').get().split()[0]
    
    parsed = []
    for review in review_boxes:
        customer_name = review.xpath('.//span[contains(@class, "f7") and contains(@class, "b") and contains(@class, "mv0")]/text()').get()
        review_date = review.xpath('//div[contains(@class, "f7") and contains(@class, "gray")]/text()').get()
        star_rating = review.xpath('.//span[contains(@class, "w_iUH7")]/text()').get()
        
        review_title = review.xpath('.//h3[contains(@class, "f5") and contains(@class, "b")]/text()').get()
        review_text = review.xpath('.//span[contains(@class, "tl-m") and contains(@class, "db-m")]/text()').get()
        parsed.append({'overall_rating': overall_rating, 
                       'total_reviews': total_reviews,
                       'reviewer': customer_name, 'date': review_date, 
                       'rating': star_rating, 'title': review_title, 'text': review_text})
        
    return parsed


async def scrape_products(urls: List[str]) -> List[Dict]:
    """scrape product data from product pages"""
    # add the product pages to a scraping list
    result = []
    to_scrape = [ScrapeConfig(url, **BASE_CONFIG) for url in urls]
    async for response in SCRAPFLY.concurrent_scrape(to_scrape):
        result.append(parse_product(response))
    log.success(f"scraped {len(result)} product pages")
    return result

async def scrape_reviews(id, max_pages: Optional[int] = None):
    """scrape product reviews from product pages"""
    # add the product pages to a scraping list

    first_page = await SCRAPFLY.async_scrape(ScrapeConfig(f"https://www.walmart.com/reviews/product/{id}", **BASE_CONFIG))
    log.info(f"scraping the first review page from https://www.walmart.com/reviews/product/{id}")

    reviews = parse_reviews(first_page)
    total_reviews = int(reviews[0]['total_reviews'])

    _reviews_per_page = max(len(reviews), 1)

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
