from logging import getLogger

from isitgoingtohell.scraping.scrape_news import scrape_news
from isitgoingtohell.sentiment_analysis import analyze_sentiment

log = getLogger("Main")


def main():

    log.info("scraping news...")
    news_df = scrape_news()
    log.info(f"scraped {len(news_df)} news")

    log.info("analysing news...")
    sentiments_df = analyze_sentiment(news_df["headline"].to_list())
    log.info(sentiments_df)

if __name__ == "__main__":
    main()
