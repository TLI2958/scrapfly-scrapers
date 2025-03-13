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
    tot_rating = sel.xpath('//a[@data-test="ratingCountLink"]//span[@data-test="ratings"]/span/text()').get()
    # will select /right columns separately, hence undesirable None values
    parsed = []
    if not tot_rating:
        overall_rating, total_reviews = None, 0
        return parsed
    
    overall_rating, total_reviews = tot_rating.split('out of')[0], tot_rating.split('with')[1] 
    url  = response.result['config']['url']
    key = ['overall_rating', 'total_reviews', 'url', 'title', 'rating', 'recommend', 'user', 'data', 'content']
    for box in sel.xpath('//div[@data-test="reviews-list"]/div'):
        title = box.xpath('.//h4[@data-test="review-card--title"]/text()').get()
        rating = box.xpath('.//span[@data-test="ratings"]/span/text()').get()
        recommend = box.xpath('.//div[@data-test="review-card--recommendation"]/span[2]/text()').get()
        user = box.xpath('.//span[@data-test="review-card--username"]/text()').get()
        date = box.xpath('.//span[@data-test="review-card--reviewTime"]/text()').get()
        content = box.xpath('.//div[@data-test="review-card--text"]/text()').get()
        review = dict(zip(key, [overall_rating, total_reviews, url, title, rating, recommend, user, date, content]))
        parsed.append(review)
    try:
        print(parsed)
    except:
        pass
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
                                         {"condition": {
                                            "selector": '//div[@data-test="@web/site-top-of-funnel/ProductDetailCollapsible-Specifications"]',
                                            "selector_state": "not_existing",
                                            "timeout": 1000,
                                            "action": "exit_success"
                                        }
                                    },
                                        
                                        # {"scroll": {"selector": '//div[@data-test="@web/site-top-of-funnel/ProductDetailCollapsible-Specifications"]'}},
                                        {"click": {"selector": '//div[@data-test="@web/site-top-of-funnel/ProductDetailCollapsible-Specifications"]/button'}},
                                    {
                                        "wait_for_selector": {
                                            "selector": "//div[@data-test='item-details-specifications']",
                                            "state": "visible",
                                            "timeout": 1000
                                        }
                                    }
                                ],
                              render_js = True,
                              **BASE_CONFIG,
                             ) for url in urls]

    for i in range(0, len(to_scrape), 20):
        to_scrape_slice = to_scrape[i: i+20]
        async for response in SCRAPFLY.concurrent_scrape(to_scrape_slice):
            try:
                parse_product(response)
                result.append(parse_product(response))  
            except:
                continue
        if len(result)%10 == 0:
            log.info('scraped product data from product pages...')

    return result


async def scrape_reviews(res = None):
    """scrape product reviews from product pages
        res: metadata of product from scrape_products"""
    url = res['product']['url']
    # print(url)
    first_page = await SCRAPFLY.async_scrape(ScrapeConfig(url, 
                                                          js_scenario = 
                                                          [ {"scroll": {"selector": "bottom"}},
                                                            {"wait": 1000},
                                                            {"condition": {
                                                                "selector": '//div[@data-test="hide-show-reviews-btn"]/button',
                                                                "selector_state": "not_existing",
                                                                "timeout": 1000,
                                                                "action": "exit_success"
                                                            }
                                                            },
                                                            {"click":
                                                                    {"selector": '//div[@data-test="hide-show-reviews-btn"]/button',
                                                                    'ignore_if_not_visible': True}},
                                                            {"wait": 500},
                                                            {"click": {"selector": '//div[@data-test="load-more-btn"]/button', 
                                                                    "ignore_if_not_visible": True}},
                                                            {"wait": 500},
                                                            {"click": {"selector": '//div[@data-test="load-more-btn"]/button', 
                                                                    "ignore_if_not_visible": True}},
                                                            {"wait": 500},
                                                            ],
                                                        render_js = True,
                                                        **BASE_CONFIG))
    log.info(f"scraping reviews from {url}")
    reviews = parse_reviews(first_page)
    log.info(f"scraped {len(reviews)} reviews from {url}")
    return reviews

async def scrape_product_and_reviews(search_data = None, key = ''):
    """scrape product and reviews concurrently"""
    # result = await scrape_products(search_data)
    # with open(output.joinpath(f"target_products_california_poppy_{key}.json"), "a", encoding="utf-8",) as file:
    #     json.dump(result, file, indent=2, ensure_ascii=False)

    # log.success(f'scraped {len(result)} products...')
    with open(output.joinpath(f"target_products_california_poppy_{key}.json"), "r", encoding="utf-8") as file:
        result = json.load(file) 
        
    result_combined = []
    for product in result:
        if product['product'].get('UPC', None):
            print(product['product']['UPC'])   
            product_reviews = await scrape_reviews(product)
            if not product_reviews:
                continue
            product['product_reviews'] = product_reviews
            result_combined.append(product)
            log.info(f'scraped reviews for product {product["product"]["UPC"]}')
            with open(output.joinpath(f"target_product_and_reviews_california_poppy_{key}.json"), "a", encoding="utf-8") as file:
                json.dump(result_combined, file, indent=2, ensure_ascii=False)
                
    return result_combined

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



