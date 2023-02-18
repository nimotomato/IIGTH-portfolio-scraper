from logging import getLogger

from isitgoingtohell.scraping.scrape_news import scrape_news
from isitgoingtohell.sentiment_analysis import analyze_sentiment
from isitgoingtohell.database import upload_news, get_unanalysed_news

log = getLogger("Main")


def main():

    # log.info("scraping news...")
    # news_df = scrape_news()
    # log.info(f"scraped {len(news_df)} news")

    log.info("getting unanalysed news...")
    unanal_df = get_unanalysed_news()


    log.info("analysing news...")
    sentiments_df = analyze_sentiment(unanal_df["headline"].to_list())

    print(sentiments_df)


    # log.info("uploading news")
    # n_uploads = upload_news(news_df)
    


    # log.info(
    #     f"REPORT: \nScraped news: {len(news_df)} \nUploaded news: {n_uploads} \nNumber duplicates: {len(news_df)-n_uploads}"
    # )

if __name__ == "__main__":
    main()
