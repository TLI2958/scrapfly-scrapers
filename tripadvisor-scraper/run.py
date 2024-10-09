"""
This example run script shows how to run the tripadvisor scraper defined in ./tripadvisor.py
It scrapes hotel data and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import asyncio
import json
from pathlib import Path
import tripadvisor

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)


async def run():
    # enable scrapfly cache for basic use
    tripadvisor.BASE_CONFIG["cache"] = False

    # print("running Tripadvisor scrape and saving results to ./results directory")
    # result_location = await tripadvisor.scrape_location_data(query="Malta")
    # output.joinpath("location.json").write_text(json.dumps(result_location, indent=2, ensure_ascii=False))

    # result_search = await tripadvisor.scrape_search(query="Malta", max_pages=2)
    # output.joinpath("search.json").write_text(json.dumps(result_search, indent=2, ensure_ascii=False))
    url = "https://www.tripadvisor.com/Attraction_Review-g60763-d105127-Reviews-Central_Park-New_York_City_New_York.html"
    att = url.split('Attraction_Review-')[-1].split('Reviews-')[1].split('-')[0]

    result_attraction = await tripadvisor.scrape_hotel(
        url,
        max_review_pages= 100,
    )

    output.joinpath(f"reviews_{att}.json").write_text(json.dumps(result_attraction, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(run())
