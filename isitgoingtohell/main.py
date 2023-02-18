from logging import getLogger

from isitgoingtohell.scraping.scrape_news import scrape_news

log = getLogger("Main")


def main():

    log.info("scraping news...")
    scrape_news()

if __name__ == "__main__":
    main()
