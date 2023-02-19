import os
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text

def upload_news(news_df: pd.DataFrame) -> int:
    """uploads news to postgres db
    se schema in schema/news"""
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    engine = create_engine(
        url,
        echo=False,
    )


    n_uploads = 0
    with engine.connect() as connection:
        for _, row in news_df.iterrows():
            query = (
                f"INSERT INTO news (headline, date, region, source)"
                f"VALUES {tuple(row.to_dict().values())} ON CONFLICT (headline) DO NOTHING;"
            )
            response = connection.execute(text(query))

            n_uploads += response.rowcount
        connection.commit()


    return n_uploads


def upload_sentiments(analysed_news_df: pd.DataFrame) -> int:
    """uploads analysed data to sentiments table in database"""
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    engine = create_engine(
        url,
        echo=False,
    )


    n_uploads = 0
    with engine.connect() as connection:
        for _, row in analysed_news_df.iterrows():
            query = (
                f"INSERT INTO sentiments (label, score, news_id)"
                f"VALUES {tuple(row.to_dict().values())} ON CONFLICT (news_id) DO NOTHING;"
            )
            response = connection.execute(text(query))

            # if we add an item rowcount is 1, if its a duplicate its 0
            n_uploads += response.rowcount
        connection.commit()

    return n_uploads


def get_unanalysed_news() -> List[tuple]:
    """get unanalysed data from the database"""
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    engine = create_engine(
        url,
        echo=False,
    )


    # Joins news and sentiments on news, but only for news with no sentiments (unanalysed news)
    query = (
        "SELECT news.ID, news.headline FROM news LEFT JOIN sentiments ON news.id = sentiments.news_id WHERE sentiments.news_id IS NULL;"
    )


    with engine.connect() as connection:
        #news_df = pd.read_sql_query(text(query), connection)
        news_df = connection.execute(text(query))


    return news_df.fetchall()