"""
This is an example web scraper for iherb.com used in scrapfly blog article:
https://SCRAPFLY.io/blog/how-to-scrape-iherb/

To run this scraper set env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://SCRAPFLY.io/dashboard"
"""
import json
import math
import os
import re
from pathlib import Path
from typing import Dict, List, TypedDict, Optional
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

from loguru import logger as log
from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient

SCRAPFLY = ScrapflyClient(key=os.environ["SCRAPFLY_KEY"])
BASE_CONFIG = {
    # iherb.com requires Anti Scraping Protection bypass feature.
    # for more: https://SCRAPFLY.io/docs/scrape-api/anti-scraping-protection
    "asp": True,
    # to change region see change the country code
    "country": "US",
}

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)


def extract_facts_table(panel) -> Dict[str, List[Dict[str, str]]]:
    """Parse the supplement facts table from the right panel."""
    rows = panel.xpath('.//div[@class="supplement-facts-container"]//tr')
    facts = {"nutrition_fact": []}
    key_value_rows = {}
    
    for row in rows:
        cells = row.xpath('.//td').getall()
        if len(cells) == 1:
            try:
                text = row.xpath('.//td/text()').get().strip()
                if 'Serving Size' in text or 'Servings Per Container' in text:
                    key, value = text.split(":", 1)
                    key_value_rows[key.strip()] = value.strip()
            except:
                continue
        elif len(cells) == 3:
            if cells[1].find('Amount Per Serving') != -1:
                continue
            facts["nutrition_fact"].append({
                    "name": re.sub(r'^<td>|</td>$', '', cells[0]).strip(),
                    "amount": re.sub(r'^<td>|</td>$', '', cells[1]).strip(),
                    "%dv": re.sub(r'^<td>|</td>$', '', cells[2]).strip(),
                })
    return {**key_value_rows, **facts}



class ProductPreview(TypedDict):
    """result generated by search scraper"""

    url: str
    title: str
    price: str
    real_price: str
    rating: str
    rating_count: str


def parse_search(result: ScrapeApiResponse) -> List[ProductPreview]:
    """Parse search result page for product previews"""
    previews = []
    sel = result.selector
    product_boxes = sel.xpath('//div[contains(@class, "product-cell-container")]')

    for box in product_boxes:
        product_data = {
            'url': box.xpath('.//a[@href]/@href').get(),
            'name': box.xpath('.//div[@class="product-title"]/bdi/text()').get(),
            'rating_value': box.xpath('.//meta[@itemprop="ratingValue"]/@content').get(),
            'review_count': box.xpath('.//meta[@itemprop="reviewCount"]/@content').get(),
            'review_url': box.xpath('.//div[@class="rating"]/a[contains(@class, "stars") and contains(@class, "scroll-to")]/@href').get(),
            'price': box.xpath('.//div[contains(@class, "product-price") and contains(@class, "text-nowrap")]//span[contains(@class, "price")]/bdi/text()').get(),
            'sku': box.xpath('.//div[@itemprop="sku"]/@content').get(),
        }

        previews.append(product_data)
    log.info(f"parsed {len(previews)} product previews from search page {result.context['url']}")
    return previews


async def scrape_search(url: str, max_pages: Optional[int] = None) -> List[ProductPreview]:
    """Scrape iherb search pages product previews"""
    log.info(f"{url}: scraping first page")
    # first, scrape the first page and find total pages:
    first_result = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    results = parse_search(first_result)
    
    _paging_meta = first_result.selector.xpath('//span[@id="product-count"]/text()').get()
    _total_results = _paging_meta.split('results')[0].split()[-1]
    _results_per_page = _paging_meta.split('of')[0].split()[-1]
    total_pages = math.ceil(int(_total_results) / int(_results_per_page))
    if max_pages and total_pages > max_pages:
        total_pages = max_pages

    # now we can scrape remaining pages concurrently
    log.info(f"{url}: found {total_pages}, scraping them concurrently")
    other_pages = [
        ScrapeConfig(
            first_result.context["url"]+f'&p={page}', 
            **BASE_CONFIG
        )
        for page in range(2, total_pages + 1)
    ]
    async for result in SCRAPFLY.concurrent_scrape(other_pages):
        results.extend(parse_search(result))

    log.info(f"{url}: found total of {len(results)} products")
    return results


class Review(TypedDict):
    title: str
    text: str
    location_and_date: str
    verified: bool
    rating: float


def parse_reviews(result: ScrapeApiResponse) -> List[Review]:
    """parse review from single review page"""

    sel = result.selector
    try:
        data = sel.xpath('//*[@id="__NEXT_DATA__"]/text()').get()
        info = data.split("calculatedRating")[1]
        avg_rating, total_reviews = info.split('"count":')[0].strip(':"'), info.split('"count":')[1].split(',')[0]
    except:
        avg_rating, total_reviews = None, 0

    review_boxes = sel.xpath('//div[contains(@class, "MuiBox-root") and @id="reviews"]/div')
    parsed = []
    for box in review_boxes:    
        date = box.xpath('.//span[@data-testid="review-posted-date"]/text()').get().split('on')[-1]
        rating = box.xpath('.//ul[@data-testid="review-rating"]//path/@fill').getall()
        rating = f'{len(rating)} star(s)'
        title = box.xpath('.//span[@data-testid="review-title"]/text()').get()
        badges = box.xpath('.//div[@data-testid="review-badge-info"]/div').getall()
        verified, rewarded = any([i.find('Verified') != -1 for i in badges]), any([i.find('Rewarded') != -1 for i in badges])  
        review_text = box.xpath('.//div[@data-testid="review-text"]//p/text()').get()
        parsed.append({ 
            'review_url': result.context['url'].split('?')[0],
            "avg_rating": avg_rating,
            "total_reviews": total_reviews,
            'title': title,
            'rating': rating,
            'text': review_text,
            'location_and_date': date,
            'verified': verified,
            'rewarded': rewarded
        })
    # print(parsed)
    return parsed


async def scrape_reviews(url: str, max_pages: Optional[int] = None) -> List[Review]:
    """scrape product reviews of a given URL of an iherb product"""
    if max_pages > 10:
        raise ValueError("max_pages cannot be greater than 10 as iherb paging stops at 10 pages. Try splitting search through multiple filters and sorting to get more results")

    # scrape first review page
    log.info(f"scraping review page: {url}")
    first_page_result = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    reviews = parse_reviews(first_page_result)

    # find total reviews
    _reviews_per_page = max(len(reviews), 1)
    total_reviews = reviews[0].get('total_reviews', 0)
    # raise ValueError
    total_pages = int(math.ceil(int(total_reviews) / _reviews_per_page))
    if max_pages and total_pages > max_pages:
        total_pages = max_pages

    log.info(f"found total {total_reviews} reviews across {total_pages} pages -> scraping")
    other_pages = []
    for page in range(6, total_pages + 1):
        url = url + f'?sort=6&isshowtranslated=true&p={page}'
        other_pages.append(ScrapeConfig(url, **BASE_CONFIG))

    async for result in SCRAPFLY.concurrent_scrape(other_pages):
        try:
            page_reviews = parse_reviews(result)
            with output.joinpath(f"reviews_on_time_.json").open('a', encoding='utf-8') as file:
                file.write(json.dumps(page_reviews, indent=2) + ",")
            reviews.extend(page_reviews)
        except:
            continue
            
    log.info(f"scraped total {len(reviews)} reviews for url {url.split('?')[0]}")
    return reviews



class Product(TypedDict):
    """type hint storage of iherbs product information"""
    name: str
    asin: str
    style: str
    description: str
    stars: str
    rating_count: str
    features: List[str]
    images: List[str]
    info_table: Dict[str, str]


def parse_product(result) -> Product:
    """parse uherb product page for essential product data"""

    sel = result.selector
    if sel.xpath('//div[@class="product-collapse-container"]').get():
        log.info(f"collapsed product page found for {result.context['url']}")
        collapsed = sel.xpath('//div[@class="switch-language-content "]')
        
        description_container = collapsed.xpath('.//div[@id="overview"]/div[@class="overview-info"]')
        description  = (('\n'.join(description_container.xpath('.//ul/li/text()').getall()) or '') + '.\n' + 
                            ('\n'.join(description_container.xpath('.//p/text()').getall()) or ''))
        details_container = collapsed.xpath('//div[@id="details"]/div[@class="details-info"]')
        direction = details_container.xpath('//h3[strong[contains(text(), "Suggested")]]/following-sibling::div/p/text()').getall()
        warnings = details_container.xpath('//h3[strong[text()="Warnings"]]/following-sibling::div/p/text()').getall()
        disclaimer = details_container.xpath('//h3[strong[text()="Disclaimer"]]/following-sibling::div/p/text()').getall()

        info_container = collapsed.xpath('//div[@id="product-supplement-facts"]/div[@class="ingredient-info"]')
        ingredients = info_container.xpath('//h3[strong[contains(text(), "Other")]]/following-sibling::div/p/text()').getall()   
        info_table = extract_facts_table(info_container)
        
    else:
        log.info(f"plain product page found for {result.context['url']}")
        product_overview = sel.xpath('//div[@id="product-overview"]')
        left_panel = product_overview.xpath('.//div[contains(@class, "col-xs-24") and contains(@class, "col-md-14")]') or product_overview.xpath('.//div[@class="col-xs-24 "]')
        right_panel = product_overview.xpath('.//div[contains(@class, "col-xs-24") and contains(@class, "col-md-10")]')

        description_container = left_panel.xpath('//h3[strong[text()="Description"]]/following-sibling::div')
        description  = (('\n'.join(description_container.xpath('.//ul/li/text()').getall()) or '') + '.\n' + 
                            ('\n'.join(description_container.xpath('.//p/text()').getall()) or ''))
        direction = left_panel.xpath('//h3[strong[contains(text(), "Suggested")]]/following-sibling::div/p/text()').getall()
        ingredients = left_panel.xpath('//h3[strong[contains(text(), "Other")]]/following-sibling::div/p/text()').getall()
        warnings = left_panel.xpath('//h3[strong[text()="Warnings"]]/following-sibling::div/p/text()').getall()
        disclaimer = left_panel.xpath('//h3[strong[text()="Disclaimer"]]/following-sibling::div/p/text()').getall()

        info_table = extract_facts_table(right_panel)

    parsed = {
        'url': result.context['url'],
        'description': description,
        'direction':direction,
        'ingredients': ingredients,
        'warnings': warnings,
        'disclaimer': disclaimer,
        'info_table': info_table
    }
    
    print(parsed)

    log.info(f"parsed product page for {result.context['url']}")
    return parsed


async def scrape_products(urls: List[str]) -> List[Product]:
    """scrape multiple iherb.com products"""
    products = []

    log.info(f"scraping {len(urls)} products")
    _to_scrape = [ScrapeConfig(url, **BASE_CONFIG, render_js=True
                               ) for url in urls]
    async for result in SCRAPFLY.concurrent_scrape(_to_scrape):
        res = parse_product(result)
        with output.joinpath(f"products_on_time_.json").open('a', encoding='utf-8') as file:
            file.write(json.dumps(res, indent=2) + ",\n")
        products.append(res)

    return products


async def scrape_all_reviews(urls: List[str], max_pages: Optional[int] = None):
    """scrape all reviews of multiple iherb.com products"""
    reviews = []
    for url in urls:
        reviews.extend(await scrape_reviews(url, max_pages))
    return reviews