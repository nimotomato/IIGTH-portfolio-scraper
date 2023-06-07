from scrapy.spiders import XMLFeedSpider
from isitgoingtohell.scraping.items import NewsHeadline


class GuardianSpider(XMLFeedSpider):
    name = "guardian_crawl"
    allowed_domains = ["www.theguardian.com"]
    start_urls = ["https://www.theguardian.com/sitemaps/news.xml"]
    iterator = "xml"
    namespaces = [
        ("sitemap", "http://www.sitemaps.org/schemas/sitemap/0.9"),
        ("news", "http://www.google.com/schemas/sitemap-news/0.9"),
    ]

    # What tag the spider groups and iterates by
    itertag = "sitemap:url"

    REGIONS = {
        "africa",
        "australia",
        "asia",
        "asia pacific",
        "middle east",
        "europe",
        "latin america",
        "oceania",
        "north america",
        "south america",
        "usa",
        "middle east",
        "russia",
        "china",
    }

    def parse_node(self, response, node):
        scraper_item = NewsHeadline()

        tags = node.xpath(
            ".//news:keywords/text()", namespaces=self.namespaces
        ).getall()

        formatted_tags = [tag.lower() for tag in tags]

        filtered_region = ""
        for tag in formatted_tags:
            for region in self.REGIONS:
                if region in tag:
                    filtered_region = region
                    break

        if filtered_region:
            scraper_item["region"] = filtered_region

            scraper_item["headline"] = (
                node.xpath(".//news:title/text()", namespaces=self.namespaces)
                .get()
                .replace("'", "")
            )

            scraper_item["date"] = (
                node.xpath(
                    ".//news:publication_date/text()", namespaces=self.namespaces
                )
                .get()
                .split("T")[0]
            )

            scraper_item["source"] = self.allowed_domains[0]

        return scraper_item
