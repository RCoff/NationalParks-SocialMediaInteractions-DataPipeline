import pandas as pd
import tweepy  # Twitter API
from pythonjsonlogger import jsonlogger

import logging
import sqlite3
from typing import Any
import time

import config


def main():
    logging.info("Starting")

    auth = tweepy.AppAuthHandler(config.twitter_api_key, config.twitter_api_secret_key)
    api = tweepy.API(auth)

    query = """yellowstone -from:YellowstoneNPS -from:WYellowstoneMT 
               -from:DestYellowstone -is:retweet -is:reply -is:nullcast
            """

    conn = sqlite3.connect("Yellowstone.db")
    df = pd.read_sql("SELECT * FROM Tweets", con=conn)

    try:
        since_id = df['tweet_id'].max()
        if not since_id:
            since_id = None
    except:
        since_id = None

    # since_id = None
    results_list = []
    # Use recursion to retrieve tweets until no more are returned
    # TODO: Retrieve last tweet ID from database and use to retrieve only newer tweets
    results_list = get_search_results(query, api, geocode="44.5071129,-111.1972906,150mi", result_type="recent",
                                      count=100, tweet_mode="extended", max_id=None, since_id=since_id)

    # results = api.search(query, geocode="44.5071129,-111.1972906,150mi", result_type="recent", count=100,
    #                      tweet_mode="extended", max_id=None)
    # for result in results:
    #     results_list.append(result)
    #
    # while results.since_id:
    #     results = api.search(query, geocode="44.5071129,-111.1972906,100mi", result_type="recent", count=100,
    #                          tweet_mode="extended", max_id=results.max_id)
    #     if results:
    #         for result in results:
    #             results_list.append(result)

    rows_added_counter: int = 0
    for tweet in results_list:
        if tweet.id not in df['tweet_id'].tolist():
            query = """
                INSERT INTO Tweets (tweet_id, text, user_id, user_handle, source, source_url, created_datetime)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
            """

            values = (
                tweet.id,
                tweet.full_text,
                tweet.user.id,
                tweet.user.screen_name,
                tweet.source,
                tweet.source_url,
                tweet.created_at
            )

            conn.execute(query, values)
            conn.commit()
            rows_added_counter += 1

    print(rows_added_counter)
    logging.info("Operation Completed", extra={'Rows_Added': rows_added_counter})


def get_search_results(query: str, api: tweepy.api, **kwargs: Any) -> list:
    if kwargs['since_id'] and kwargs['max_id']:
        raise AttributeError("since_id and max_id were both specified. Only one may be specified.")

    # results: Any = None
    results_list: list = []

    for i in range(3):
        try:
            results = api.search(query, **kwargs)

            # TODO: Handle rate limit here
        except:
            logging.exception(f"An exception has occurred. Retrying... {i}", extra={'Retry': i})
            time.sleep(5 + (i * 5))
            continue
        break
    else:
        logging.critical("Not able to get results", extra={'Retries': i})
        return results_list

    results_list = [result for result in results]

    if kwargs['since_id'] and results.since_id:
        kwargs['since_id'] = results.since_id
    elif kwargs['max_id'] and results.max_id:
        kwargs['max_id'] = results.max_id
    else:
        return results_list

    results_list.extend(get_search_results(query, api, **kwargs))

    return results_list


def setup_logging():
    logging.basicConfig(filename='log.log', level=logging.DEBUG)


def setup_database():
    conn = sqlite3.connect("Yellowstone.db")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Tweets (
            tweet_id INTEGER PRIMARY KEY,
            text TEXT DEFAULT NULL,
            user_id INTEGER DEFAULT NULL,
            user_handle TEXT DEFAULT NULL,
            source TEXT DEFAULT NULL,
            source_url TEXT DEFAULT NULL,
            created_datetime TEXT DEFAULT NULL
        )
    """)

    conn.commit()


if __name__ == "__main__":
    setup_logging()
    setup_database()
    main()
