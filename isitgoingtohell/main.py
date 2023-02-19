import logging

from isitgoingtohell.scraping.scrape_news import scrape_news
from isitgoingtohell.sentiment_analysis import analyze_sentiment
from isitgoingtohell.database import upload_news, upload_sentiments, get_unanalysed_news

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Main")


def main():
    log.info("Scraping news...")
    news_df = scrape_news()
    log.info(f"Scraped {len(news_df)} news")


    log.info("Uploading news...")
    n_news_uploads = upload_news(news_df)


    log.info("\nGetting unanalysed news...")
    unanal_news = get_unanalysed_news()


    if unanal_news:
        # Separate news as to allow analysing the headlines
        ids, headlines = zip(*unanal_news)

    
        log.info("Analysing news...")
        sentiments_df = analyze_sentiment(list(headlines))
        log.info(f"Analysed {len(sentiments_df)} news")


        # Insert labels with ids for upload
        sentiments_df["news_id"] = ids


        n_sents_uploads = upload_sentiments(sentiments_df)
        log.info(
            f"\nREPORT: \nAnalysed news: {len(sentiments_df)} \nUploaded sentiments: {n_sents_uploads} \nNumber duplicates: {len(sentiments_df)-n_sents_uploads}"
        ) 
    else:
        log.info(
            f"\nREPORT: \nAnalysed news: 0 \nUploaded sentiments: 0 \nNumber duplicates: 0"
        ) 


    log.info(
        f"\nREPORT: \nScraped news: {len(news_df)} \nUploaded news: {n_news_uploads} \nNumber duplicates: {len(news_df)-n_news_uploads}"
    )

    
if __name__ == "__main__":
    main()
