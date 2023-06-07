import os

import pandas as pd
from scrapy.crawler import CrawlerProcess

from isitgoingtohell.scraping.spiders.aljazeera_spider import AlJazeeraSpider
from isitgoingtohell.scraping.spiders.bbc_spider import BbcSpider
from isitgoingtohell.scraping.spiders.reuters_spider import ReutersSpider
from isitgoingtohell.scraping.spiders.guardian_spider import GuardianSpider


def run_spiders(spiders, output_csv: str) -> None:
    process = CrawlerProcess(
        settings={
            "OUTPUT_CSV": output_csv,
            "ITEM_PIPELINES": {
                "isitgoingtohell.scraping.pipelines.RegionPipeline": 300,
                "isitgoingtohell.scraping.pipelines.DateRestrictorPipeline": 400,
                "isitgoingtohell.scraping.pipelines.CsvWriterPipeline": 900,
            },
        }
    )

    for spider in spiders:
        process.crawl(spider)

    process.start()


def load_data(output_csv: str) -> pd.DataFrame:
    news_df = pd.read_csv(output_csv, delimiter="\t")

    news_df.drop_duplicates(subset=["headline"], inplace=True)

    # as we write to .csv in all scrapers None gets treated as a string..
    # remove items with no region
    news_df = news_df[news_df["region"] != "None"]

    # remove items with no date
    news_df = news_df[news_df["date"] != "None"]

    news_df.reset_index(inplace=True, drop=True)

    return news_df


def scrape_news() -> pd.DataFrame:
    # all scrapers will write to this csv.
    # we load data from it then remove it
    output_csv = "output_news.csv"

    # scrape news
    spiders = [GuardianSpider, ReutersSpider, BbcSpider, AlJazeeraSpider]
    run_spiders(spiders, output_csv)

    # load data, remove dupicates etc
    news_df = load_data(output_csv)

    # # remove csv file
    # os.remove(output_csv)

    return news_df
