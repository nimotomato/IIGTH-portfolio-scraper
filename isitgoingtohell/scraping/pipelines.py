# Define your item pipelines here
#

import os
from datetime import datetime

REGIONS = ["africa", "asia", "europe", "oceania", "north america", "south america"]

filename = "items"


class RegionPipeline:
    # Homogenizes naming convention of geographical confinements on planet Terra

    def __init__(self):
        pass

    def process_item(self, item, spider):
        if item["region"] == "us_and_canada":
            item["region"] = "north america"
        if item["region"] == "usa":
            item["region"] = "north america"
        elif item["region"] == "middle_east":
            item["region"] = "asia"
        elif item["region"] == "australia":
            item["region"] = "oceania"
        elif item["region"] == "latin_america":
            item["region"] = "south america"
        elif item["region"] == "latin":
            item["region"] = "south america"
        elif item["region"] == "bolivia":
            item["region"] = "south america"
        elif item["region"] == "brazil":
            item["region"] = "south america"
        elif item["region"] == "colombia":
            item["region"] = "south america"
        elif item["region"] == "south":
            item["region"] = "south america"
        elif item["region"] == "china":
            item["region"] = "asia"
        elif item["region"] == "asia pacific":
            item["region"] = "asia"
        elif item["region"] == "united kingdom":
            item["region"] = "europe"
        elif item["region"] == "uk":
            item["region"] = "europe"
        elif item["region"] == "united states":
            item["region"] = "north america"
        elif item["region"] == "middle east":
            item["region"] = "asia"
        elif item["region"] == "middle":
            item["region"] = "asia"
        elif item["region"] == "afghanistan":
            item["region"] = "asia"
        elif item["region"] == "sudan":
            item["region"] = "africa"
        elif item["region"] == "israel":
            item["region"] = "asia"
        elif item["region"] == "india":
            item["region"] = "asia"
        elif item["region"] == "asia pacific":
            item["region"] = "asia"
        elif item["region"] == "russia":
            item["region"] = "europe"
        else:
            if item["region"] not in REGIONS:
                item["region"] = None

        return item


class DateRestrictorPipeline:
    # The scraper goes deep and returns old articles. However, these are not numerous enough to be useful. This filters out all older than start_date
    def __init__(self):
        pass

    def process_item(self, item, spider):
        start_date = datetime.strptime("2023-01-01", "%Y-%m-%d")

        if isinstance(item["date"], str):
            formatted_newsitem_date = datetime.strptime(item["date"], "%Y-%m-%d")

        if formatted_newsitem_date < start_date:
            item["date"] = None

        return item


class CsvWriterPipeline(object):
    def open_spider(self, spider):
        if not os.path.exists(spider.settings["OUTPUT_CSV"]):
            with open(spider.settings["OUTPUT_CSV"], "w") as f:
                f.write("headline\tdate\tregion\tsource\n")

        self.file = open(spider.settings["OUTPUT_CSV"], "a")

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        self.file.write(
            f'{item["headline"]}\t{item["date"]}\t{item["region"]}\t{item["source"]}\n'
        )
        return item
