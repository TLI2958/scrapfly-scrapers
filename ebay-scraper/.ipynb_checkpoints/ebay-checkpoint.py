"""
This is an example web scraper for Ebay.com used in scrapfly blog article:
https://scrapfly.io/blog/how-to-scrape-ebay/

To run this scraper set env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import json
import math
import os
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import dateutil
from loguru import logger as log

from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient, ScrapflyScrapeError

SCRAPFLY = ScrapflyClient(key=os.environ["SCRAPFLY_KEY"])
BASE_CONFIG = {
    # Ebay.com requires Anti Scraping Protection bypass feature.
    # for more: https://scrapfly.io/docs/scrape-api/anti-scraping-protection
    "asp": True,
    "country": "US",  # change country for geo details like currency and shipping
    "lang": ["en-US"],

}


output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)


# from chatgpt: update page number
def update_page_number(url, new_page_number):
    url = re.sub(r"page_id_item=\d+", f"page_id_item={new_page_number}", url)
    url = re.sub(r"page_id=\d+", f"page_id={new_page_number}", url)
    return url


def _find_json_objects(text: str, decoder=json.JSONDecoder()):
    """Find JSON objects in text, and generate decoded JSON data"""
    pos = 0
    while True:
        match = text.find("{", pos)
        if match == -1:
            break
        try:
            result, index = decoder.raw_decode(text[match:])
            yield result
            pos = match + index
        except ValueError:
            pos = match + 1


def parse_product(result: ScrapeApiResponse):
    """Parse Ebay's product listing page for core product data"""
    sel = result.selector
    css_join = lambda css: "".join(sel.css(css).getall()).strip()  # join all selected elements
    css = lambda css: sel.css(css).get("").strip()  # take first selected element and strip of leading/trailing spaces

    item = {}
    item["url"] = css('link[rel="canonical"]::attr(href)')
    item["id"] = item["url"].split("/itm/")[1].split("?")[0]  # we can take ID from the URL
    item["price_original"] = css(".x-price-primary>span::text")
    item["price_converted"] = css(".x-price-approx__price ::text")  # ebay automatically converts price for some regions

    item["name"] = css_join("h1 span::text")
    item["seller_name"] = sel.xpath("//div[contains(@class,'info__about-seller')]/a/span/text()").get()
    item["seller_url"] = sel.xpath("//div[contains(@class,'info__about-seller')]/a/@href").get().split("?")[0]
    item["photos"] = sel.css('.ux-image-filmstrip-carousel-item.image img::attr("src")').getall()  # carousel images
    item["photos"].extend(sel.css('.ux-image-carousel-item.image img::attr("src")').getall())  # main image
    # description is an iframe (independant page). We can keep it as an URL or scrape it later.
    item["description_url"] = css("iframe#desc_ifr::attr(src)")
    # feature details from the description table:
    feature_table = sel.css("div.ux-layout-section--features")
    features = {}
    for feature in feature_table.css("dl.ux-labels-values"):
        # iterate through each label of the table and select first sibling for value:
        label = "".join(feature.css(".ux-labels-values__labels-content > div > span::text").getall()).strip(":\n ")
        value = "".join(feature.css(".ux-labels-values__values-content > div > span *::text").getall()).strip(":\n ")
        features[label] = value
    item["features"] = features
    return item


async def scrape_product(url: str) -> Dict:
    """Scrape ebay.com product listing page for product data"""
    page = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    product = parse_product(page)
    product["variants"] = parse_variants(page)
    return product


async def scrape_products(urls: List[str]) -> List:
    """scrape multiple iherb.com products"""
    products = []
    log.info(f"scraping {len(urls)} products")
    _to_scrape = [ScrapeConfig(url, **BASE_CONFIG, render_js=True
                               ) for url in urls]
    async for result in SCRAPFLY.concurrent_scrape(_to_scrape):
        try:
            res = parse_product(result)
            with output.joinpath(f"products_on_time_.json").open('a', encoding='utf-8') as file:
                file.write(json.dumps(res, indent=2) + ",\n")
            products.append(res)
        except Exception as e:
            log.error(f'Error parsing product: {e}')

    return products

# todo: tailor review parser
def parse_reviews(result: ScrapeApiResponse) -> List:
    """parse review from single review page"""

    sel = result.selector
    try:
        total_reviews = sel.xpath('//div[@class="fdbk-filter"]//span/label/text()').getall()[0]
    except:
        total_reviews = 0

    review_boxes = sel.xpath('//li[@class="fdbk-container"]')
    parsed = []
    for box in review_boxes:    
        user = box.xpath('.//div[@class="fdbk-container__details__info__username"]/span/text()').get()
        rating = box.xpath('.//div[@class="fdbk-container__details__info__icon"]/svg/@aria-label').get()
        review_text = box.xpath('.//div[@class="fdbk-container__details__top"]/div[@class="fdbk-container__details__comment"]/span/text()').get()
        parsed.append({ 
            'review_url': result.context['url'],
            "total_reviews": total_reviews,
            'user': user,
            'rating': rating,
            'text': review_text,
        })
    print(parsed)
    return parsed

async def scrape_reviews(url: str, max_pages: Optional[int] = None) -> List:
    """scrape product reviews of a given URL of an ebay product"""

    log.info(f"scraping review page: {url}")
    first_page_result = await SCRAPFLY.async_scrape(ScrapeConfig(url, 
                                #     js_scenario=[
                                #     {
                                #         "wait": 500
                                #     }
                                # ], render_js = True,
                                **BASE_CONFIG))
    try:
        reviews = parse_reviews(first_page_result)
    except:
        reviews = []
    if not reviews:
        return []
    # find total reviews
    _reviews_per_page = len(reviews)
    total_reviews = reviews[0].get('total_reviews', 0)
    if total_reviews:
        total_reviews = int(total_reviews.split('(')[-1].rstrip(')'))
    # raise ValueError
    total_pages = int(math.ceil(int(total_reviews) / _reviews_per_page)) if total_reviews > 0 and _reviews_per_page > 0 else max_pages if _reviews_per_page > 0 else 0
    print(total_pages)
    if max_pages and total_pages > max_pages:
        total_pages = max_pages

    log.info(f"found total {total_reviews} reviews across {total_pages} pages -> scraping")
    other_pages = []
    for page in range(2, total_pages + 1):
        url = update_page_number(url, page)
        other_pages.append(ScrapeConfig(url, 
                                #          js_scenario=[
                                #     {
                                #         "wait": 500
                                #     }
                                # ], render_js = True,
                                        **BASE_CONFIG))

    async for result in SCRAPFLY.concurrent_scrape(other_pages):
        try:
            page_reviews = parse_reviews(result)
            with output.joinpath(f"reviews_on_time_.json").open('a', encoding='utf-8') as file:
                file.write(json.dumps(page_reviews, indent=2) + ",")
            reviews.extend(page_reviews)
        except:
            continue
            
    log.info(f"scraped total {len(reviews)} reviews for url {url}")
    return reviews


async def scrape_all_reviews(urls: List[str], max_pages: Optional[int] = None):
    """scrape all reviews of multiple iherb.com products"""
    reviews = []
    for url in urls:
        reviews.extend(await scrape_reviews(url, max_pages))
    return reviews


def parse_search(result: ScrapeApiResponse) -> List[Dict]:
    """Parse ebay.com search result page for product previews"""
    previews = []
    for box in result.selector.css(".srp-results li.s-item"):
        css = lambda css: box.css(css).get("").strip() or None  # get first CSS match
        css_all = lambda css: box.css(css).getall()  # get all CSS matches
        css_re = lambda css, pattern: box.css(css).re_first(pattern, default="").strip()  # get first css regex match
        css_int = lambda css: int(box.css(css).re_first(r"(\d+)", default="0")) if box.css(css) else None
        css_float = lambda css: float(box.css(css).re_first(r"(\d+\.*\d*)", default="0.0")) if box.css(css) else None
        auction_end = css_re(".s-item__time-end::text", r"\((.+?)\)") or None
        if auction_end:
            auction_end = dateutil.parser.parse(auction_end.replace("Today", ""))        
        item = {
            "url": css("a.s-item__link::attr(href)").split("?")[0],
            "title": css(".s-item__title span::text"),
            "price": css(".s-item__price::text"),
            "shipping": css_float(".s-item__shipping::text"),
            "auction_end": auction_end,
            "bids": css_int(".s-item__bidCount::text"),
            "location": css(".s-item__itemLocation::text"),
            "subtitles": css_all(".s-item__subtitle::text"),
            "condition": css(".SECONDARY_INFO::text"),
            "photo": css("img::attr(data-src)") or css("img::attr(src)"),
            "rating": css_float(".s-item__reviews .clipped::text"),
            "rating_count": css_int(".s-item__reviews-count span::text"),
        }
        previews.append(item)
    return previews


def _get_url_parameter(url: str, param: str, default=None) -> Optional[str]:
    """get url parameter value"""
    query_params = dict(parse_qsl(urlparse(url).query))
    return query_params.get(param) or default


def _update_url_param(url: str, **params):
    """adds url parameters or replaces them with new values"""
    parsed_url = urlparse(url)
    query_params = dict(parse_qsl(parsed_url.query))
    query_params.update(params)
    updated_url = parsed_url._replace(query=urlencode(query_params))
    return urlunparse(updated_url)


async def scrape_search(url: str, max_pages: Optional[int] = None) -> List[Dict]:
    """Scrape Ebay's search for product preview data for given"""
    log.info("Scraping search for {}", url)

    first_page = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    results = parse_search(first_page)
    # find total amount of results for concurrent pagination
    total_results = first_page.selector.css(".srp-controls__count-heading>span::text").get()
    total_results = int(total_results.replace(",", "").replace(".", ""))
    items_per_page = int(_get_url_parameter(first_page.context["url"], "_ipg", default=60))
    total_pages = math.ceil(total_results / items_per_page)
    if max_pages and total_pages > max_pages:
        total_pages = max_pages
    other_pages = [
        ScrapeConfig(_update_url_param(first_page.context["url"], _pgn=i), **BASE_CONFIG)
        for i in range(2, total_pages + 1)
    ]
    log.info("Scraping search pagination of {} total pages for {}", len(other_pages), url)
    async for result in SCRAPFLY.concurrent_scrape(other_pages):
        if not isinstance(result, ScrapflyScrapeError):
            try:
                results.extend(parse_search(result))
            except Exception as e:
                log.error(f"failed to parse search: {result.context['url']}: {e}")
        else:
            log.error(f"failed to scrape {result.api_response.config['url']}, got: {result.message}")
    return results
