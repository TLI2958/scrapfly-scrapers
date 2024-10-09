"""
This example run script shows how to run the trustpilot.com scraper defined in ./trustpilot.py
It scrapes ads data and saves it to ./results/

To run this script set the env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import asyncio
import json
from pathlib import Path
import trustpilot

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

urls=[
            # "https://www.trustpilot.com/review/www.mountainroseherbs.com",
            "https://www.trustpilot.com/review/fosterfarms.com",
        ]
async def run():
    trustpilot.BASE_CONFIG["cache"] = True

    print("running Trustpilot scrape and saving results to ./results directory")

    # search_data = await trustpilot.scrape_search(
    #     url="https://www.trustpilot.com/categories/electronics_technology", max_pages=3
    # )
    # with open(output.joinpath("search.json"), "w", encoding="utf-8") as file:
    #     json.dump(search_data, file, indent=2, ensure_ascii=False)

    companies_data = await trustpilot.scrape_company(
        urls= urls
    )
    with open(output.joinpath("companies_1.json"), "w", encoding="utf-8") as file:
        json.dump(companies_data, file, indent=2, ensure_ascii=False)

    for i, url in enumerate(urls):
        reviews_data = await trustpilot.scrape_reviews(
            url= url,
            max_pages=100,
        )

        file_path = output.joinpath(f"reviews_1.json")

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(reviews_data, file, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    asyncio.run(run())
