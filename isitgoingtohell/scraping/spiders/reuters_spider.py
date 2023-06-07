"""scraper for www.reuters.com"""

from datetime import datetime

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from isitgoingtohell.scraping.items import NewsHeadline


class ReutersSpider(CrawlSpider):
    name = "reuters_crawl"
    allowed_domains = ["www.reuters.com"]
    start_urls = [
        "https://www.reuters.com/world/",
        "https://www.reuters.com/world/africa/",
        "https://www.reuters.com/world/americas/",
        "https://www.reuters.com/world/asia-pacific/",
        "https://www.reuters.com/world/china/",
        "https://www.reuters.com/world/europe/",
        "https://www.reuters.com/world/india/",
        "https://www.reuters.com/world/middle-east/",
        "https://www.reuters.com/world/us/",
    ]

    le_page_details = LinkExtractor(allow=r"world/")
    rule_page_details = Rule(le_page_details, callback="parse_item", follow=True)
    rules = (rule_page_details,)

    def parse_item(self, response):
        scraper_item = NewsHeadline()

        scraper_item["headline"] = response.css("h1 ::text").get().replace("'", "")

        # Date
        date = response.css("span.date-line__date__23Ge- ::text").getall()[1]

        formatted_date = datetime.strptime(date, "%B %d, %Y")

        scraper_item["date"] = formatted_date.date()

        region = response.css("nav.article-header__tags__3-jcV ::text").get()

        scraper_item["region"] = region.lower()

        # add source
        scraper_item["source"] = "www.reuters.com"

        yield scraper_item
