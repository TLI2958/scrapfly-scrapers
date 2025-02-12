"""
This is an example web scraper for target.com.

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
    # bypass target.com web scraping blocking
    "asp": True,
    # set the proxy country to US
    "country": "US",
    "proxy_pool": "public_residential_pool"

}

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

def parse_product(response: ScrapeApiResponse):
    """parse product data from target product pages"""
    url = response.result['config']['url']
    
    sel = response.selector
    product = {}
    product['url'] = url
    for spec in sel.xpath('//div[@data-test="item-details-specifications"]/div'):
        key = spec.xpath('.//b/text()').get()
        val = spec.xpath('.//b/following-sibling::text()[2]').get()
        if val is None:
            val = spec.xpath('.//b/following-sibling::text()[1]').get()
        if key and val:
            product[key] = val
    # maybe: still results in ':'
    print(product)
    return {"product": product}

def parse_search(result: ScrapeApiResponse) -> List:
    """Parse search result page for product previews"""
    previews = []
    sel = result.selector
    total = sel.xpath('//div[@data-test="lp-resultsCount"]/h2/span/text()').get()
    if total:
        total = re.sub(',', '', total).split()[0].strip()
        total = int(total)

    product_boxes = sel.xpath('//div[@data-test="product-details"]')

    for details in product_boxes:
        product_data = {
            'total_results': total,
            'url': details.xpath('.//a[@data-test="product-title"]/@href').get(),
            'brand': details.xpath('.//a[contains(@data-test, "brand")]/text()').get(),
            'name': details.xpath('.//a[@data-test="product-title"]/@aria-label').get(),
            'rating_value': details.xpath('.//span[@data-test="ratings"]/span/text()').get(),
            'review_count': details.xpath('.//span[@data-test="rating-count"]/text()').get(),
            'price': details.xpath('.//span[@data-test="current-price"]/span/text()').get(),
        }


        previews.append(product_data)
    print(previews)
    log.info(f"parsed {len(previews)} product previews from search page {result.context['url']}")
    return previews


def parse_reviews(response: ScrapeApiResponse):
    """parse product reviews from product pages"""
    sel = response.selector
    # will select left/right columns separately, hence undesirable None values
    # review_boxes = sel.xpath('//div[contains(@class,"overflow-visible" and contains(@class, "dark-gray"))]/*').getall()
    parsed = []
    key = ['url', 'customer_name', 'review_date', 'star_rating', 'review_title', 'review_text']
    
    # solve non-unique
    url = response.result['config']['url']
    customer_name = sel.xpath('//span[contains(@class, "f7") and contains(@class, "b") and contains(@class, "mv0")]/text()').getall()
    # need revising
    review_date = sel.xpath('//div[contains(@class, "f7") and contains(@class, "justify")]/text()').getall()
    star_rating = sel.xpath('//div[contains(@class, "w_ExHd")]/following-sibling::span[contains(@class, "w_iUH7")]/text()').getall()

    review_body = sel.xpath('//div[contains(@class, "overflow-visible")][not(contains(@class, "undefined"))]').getall()
    review_title = sel.xpath('//div[contains(@class, "overflow-visible")][not(contains(@class, "undefined"))]//h3[contains(@class, "w_kV33")]/text()').getall()
    review_text = sel.xpath('//div[contains(@class, "overflow-visible")][not(contains(@class, "undefined"))]//span[contains(@class, "tl-m") and contains(@class, "db-m")]/text()').getall()

    j, k = 0, 0
    for i in range(len(review_body)):
        has_title = re.search(r'w_kV33', review_body[i])
        has_text = re.search(r'tl-m db-m', review_body[i])

        if has_title and has_text: 
            parsed.append(dict(zip(key, [url, customer_name[i], review_date[i], star_rating[i], review_title[j], review_text[k]])))
            j, k = j + 1, k + 1
        elif has_text:
            parsed.append(dict(zip(key, [url, customer_name[i], review_date[i], star_rating[i], '', review_text[k]])))
            k = k + 1
        elif has_title:
            parsed.append(dict(zip(key, [url, customer_name[i], review_date[i], star_rating[i], review_title[j], ''])))
            j = j + 1
        else:
            parsed.append(dict(zip(key, [url, customer_name[i], review_date[i], star_rating[i], '', ''])))
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
    prefix = 'https://www.target.com'
    urls = [prefix + e['url'] for e in res]
    to_scrape = [ScrapeConfig(url, 
                                    js_scenario=[
                                        {"scroll": {"selector": '//div[@data-test="@web/site-top-of-funnel/ProductDetailCollapsible-Specifications"]'}},
                                        {"click": {"selector": '//div[@data-test="@web/site-top-of-funnel/ProductDetailCollapsible-Specifications"]/button'}},

                                        {"condition": {
                                            "selector": "//div[@data-test='item-details-specifications']",
                                            "selector_state": "not_existing",
                                            "timeout": 500,
                                            "action": "exit_success"
                                        }
                                    },

                                    {
                                        "wait_for_selector": {
                                            "selector": "//div[@data-test='item-details-specifications']",
                                            "state": "visible",
                                            "timeout": 500
                                        }
                                    }
                                ],
                              render_js = True,
                              **BASE_CONFIG,
                             ) for url in urls]

    for i in range(0, len(to_scrape), 20):
        to_scrape_slice = to_scrape[i: i+20]
        async for response in SCRAPFLY.concurrent_scrape(to_scrape_slice):
            result.append(parse_product(response))  
        if len(result)%10 == 0:
            log.info('scraped product data from product pages...')

    return result


async def scrape_reviews(res = None, max_pages: Optional[int] = None):
    """scrape product reviews from product pages
        res: metadata of product from scrape_products"""
    total_reviews = int(res.get('product',{}).get("aggregateRating",{}).get('reviewCount', 0))
    id = res['product']['sku']
    first_page = await SCRAPFLY.async_scrape(ScrapeConfig(f"https://www.target.com/reviews/product/{id}?", 
                                                          js_scenario = 
                                                          [ {"scroll": {"selector": "bottom"}},
                                                            *[{"click": 
                                                                {"selector": '//div[@data-test="load-more-btn"]/button',
                                                                'ignore_if_not_visible': True}},
                                                                {"wait": 500},]*3],
                                                        render_js = True,
                                                        **BASE_CONFIG))
    log.info(f"scraping the first review page from https://www.target.com/reviews/product/{id}?")

    reviews = parse_reviews(first_page)
    _reviews_per_page = 10


    total_pages = int(math.ceil(total_reviews / _reviews_per_page))
    if max_pages and total_pages > max_pages:
        total_pages = max_pages
    
    log.info(f"found total {total_reviews} reviews across {total_pages} pages -> scraping")
    other_pages = []
    for page in range(6, total_pages + 1):
        url = f"https://www.target.com/reviews/product/{id}?page={page}"
        other_pages.append(ScrapeConfig(url, **BASE_CONFIG))

    async for result in SCRAPFLY.concurrent_scrape(other_pages):
        page_reviews = parse_reviews(result)
        reviews.extend(page_reviews)
    log.info(f"scraped total {len(reviews)} reviews")
    return reviews

async def scrape_product_and_reviews(search_data, key = ''):
    """scrape product and reviews concurrently"""
    result = await scrape_products(search_data)
    with open(output.joinpath(f"target_products_california_poppy_{key}.json"), "a", encoding="utf-8",) as file:
        json.dump(result, file, indent=2, ensure_ascii=False)

    # log.success(f'scraped {len(result)} products...')
    # with open(output.joinpath(f"target_products_california_poppy_{key}.json"), "r", encoding="utf-8") as file:
    #     result = json.load(file) 
        
    # result_combined = []
    # for product in result:
    #     product_reviews = await scrape_reviews(product, max_pages=20)
    #     product['product_reviews'] = product_reviews
    #     result_combined.append(product)
    #     log.info(f'scraped reviews for product {product["product"]["sku"]}')
    #     with open(output.joinpath(f"target_product_and_reviews_california_poppy_{key}.json"), "a", encoding="utf-8") as file:
    #         json.dump(result_combined, file, indent=2, ensure_ascii=False)
            
    # return result_combined

async def scrape_search(
    query: str = "",
    max_pages: int = None,
):
    """scrape single target search page"""

    def make_search_url(page):
        url = "https://www.target.com/s?" + urlencode(
            {
                "searchTerm": query,
                "Nao": page,
                "moveTo": "product-list-grid",
            }
        )
        return url

    # scrape the first search page
    log.info(f"scraping the first search page with the query ({query})")
    # todo: add js_scenario of scroll to the bottom of the page
    first_page = await SCRAPFLY.async_scrape(
        ScrapeConfig(make_search_url(1), 
                      js_scenario=[{"wait": 1000},
                                    {"scroll": {"selector": "bottom"}},
                                    {"wait": 1000},
                                     {
                                        "condition": {
                                            "selector": "//div[@data-testid='product-description']",
                                            "selector_state": "not_existing",
                                            "timeout": 500,
                                            "action": "exit_success"
                                        }
                                    },
                                    {
                                        "wait_for_selector": {
                                            "selector": "//div[@data-testid='product-description']",
                                            "state": "visible",
                                            "timeout": 500
                                        }
                                    }
                                    
                                ],
                     render_js = True,
                     **BASE_CONFIG)
    )
    data = parse_search(first_page)
    total_results = data[0]["total_results"]

    # find total page count to scrape
    total_pages = math.ceil(total_results / 24)
    
    if total_pages > 25:
        total_pages = 25
    if max_pages and max_pages < total_pages:
        total_pages = max_pages

    # then add the remaining pages to a scraping list and scrape them concurrently
    log.info(f"scraping search pagination, remaining ({total_pages - 1}) more pages")
    other_pages = [
        ScrapeConfig(make_search_url((page-1)*24), 
                      js_scenario=[ {"wait": 1000},
                                    {"scroll": {"selector": "bottom"}},
                                    {"wait": 1000},
                                     {
                                        "condition": {
                                            "selector": "//div[@data-testid='product-description']",
                                            "selector_state": "not_existing",
                                            "timeout": 500,
                                            "action": "exit_success"
                                        }
                                    },
                                    {
                                        "wait_for_selector": {
                                            "selector": "//div[@data-testid='product-description']",
                                            "state": "visible",
                                            "timeout": 500
                                        }
                                    }],
                                
                     render_js = True,
                     **BASE_CONFIG)
        for page in range(2, total_pages + 1)
    ]
    async for response in SCRAPFLY.concurrent_scrape(other_pages):
        data.extend(parse_search(response))
    log.success(f"scraped {len(data)} product listings from search pages")
    return data



