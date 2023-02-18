import os

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


def get_unanalysed_news() -> pd:
    """get unanalysed data from the database"""
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    engine = create_engine(
        url,
        echo=False,
    )


    query = (
        "SELECT news.* FROM news LEFT JOIN sentiments ON news.id = sentiments.news_id WHERE sentiments.news_id IS NULL;"
    )


    with engine.connect() as connection:
        news_df = pd.read_sql_query(text(query), connection)
    

    news_df.set_index('id')
    
    return news_df


def upload_analysed_news(analysed_news_df: pd.DataFrame) -> int:
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
    with engine.connect() as conn:
        for _, row in analysed_news_df.iterrows():
            query = (
                f"INSERT INTO sentiments (news_id, label, score)"
                f"VALUES {tuple(row.to_dict().values())} ON CONFLICT (headline) DO NOTHING;"
            )
            r = conn.execute(text(query))

            # if we add an item rowcount is 1, if its a dupliacte its 0
            n_uploads += r.rowcount

    return n_uploads


# def upload_data(analysed_news_df: pd.DataFrame) -> int:
#     """uploads all data to postgres db
#     see schema in schema/news"""
#     url = (
#         f"postgresql+psycopg2://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
#         f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
#     )
#     engine = create_engine(
#         url,
#         echo=False,
#     )

#     n_uploads = 0
#     with engine.connect() as conn:
#         for _, row in analysed_news_df.iterrows():
#             query = (
#                 f"INSERT INTO news (headline, date, region, source, label, score)"
#                 f"VALUES {tuple(row.to_dict().values())} ON CONFLICT (headline) DO NOTHING;"
#             )
#             r = conn.execute(text(query))

#             # if we add an item rowcount is 1, if its a dupliacte its 0
#             n_uploads += r.rowcount

#     return n_uploads