from datetime import datetime, timedelta
import logging
import os

import awswrangler as wr
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import praw
import requests
from sqlalchemy import exc, create_engine

try:
    from .schema import *
except:
    from schema import *

try:
    from .utils import *  # this works for tests
except:
    from utils import *  # this works for this script
# https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
# idfk wat im doin but it works

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    handlers=[logging.FileHandler("logs/example.log"), logging.StreamHandler()],
)
logging.getLogger("requests").setLevel(logging.WARNING)  # get rid of https debug stuff

logging.info("STARTING NBA ELT PIPELINE SCRIPT Version: 1.6.0")
# logging.warning("STARTING NBA ELT PIPELINE SCRIPT Version: 1.6.0")
# logging.error("STARTING NBA ELT PIPELINE SCRIPT Version: 1.6.0")

# helper validation function - has to be here instead of utils bc of globals().items()
def validate_schema(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """
    Schema Validation function to check whether table columns are correct before writing to SQL.
    Errors are written to Logs

    Args:
        data_df (pd.DataFrame): The Transformed Pandas DataFrame to check

        schema (list):  The corresponding columns of the Pandas DataFrame to be checked
    Returns:
        The same input DataFrame with a schema attribute that is either validated or invalidated
    """
    data_name = [k for k, v in globals().items() if v is df][0]
    try:
        if (
            len(df) == 0
        ):  # this has to be first to deal with both empty lists + valid data frames
            logging.error(f"Schema Validation Failed for {data_name}, df is empty")
            # df.schema = 'Invalidated'
            return df
        elif list(df.columns) == schema:
            logging.info(f"Schema Validation Passed for {data_name}")
            df.schema = "Validated"
            return df
        else:
            logging.error(f"Schema Validation Failed for {data_name}")
            df.schema = "Invalidated"
            return df
    except BaseException as e:
        logging.error(f"Schema Validation Failed for {data_name}, {e}")
        df.schema = "Invalidated"
        return df


logging.info("Starting Logging Function")

logging.info("LOADED FUNCTIONS")

today = datetime.now().date()
todaytime = datetime.now()
yesterday = today - timedelta(1)
day = (datetime.now() - timedelta(1)).day
month = (datetime.now() - timedelta(1)).month
year = (datetime.now() - timedelta(1)).year

if today < datetime(2022, 4, 11).date():
    season_type = "Regular Season"
elif (today >= datetime(2022, 4, 11).date()) & (today < datetime(2022, 4, 16).date()):
    season_type = "Play-In"
else:
    season_type = "Playoffs"

if __name__ == "__main__":
    logging.info("STARTING WEB SCRAPE")

    # STEP 1: Extract Raw Data
    # stats_raw = get_player_stats_data()
    # boxscores_raw = get_boxscores_data()
    injury_data_raw = get_injuries_data()
    transactions_raw = get_transactions_data()
    # adv_stats_raw = get_advanced_stats_data()
    # odds_raw = get_odds_data()
    reddit_data = get_reddit_data("nba")  # doesnt need transformation
    # opp_stats_raw = get_opp_stats_data()
    # schedule = schedule_scraper("2022", ["april", "may", "june"])
    # shooting_stats_raw = get_shooting_stats_data()
    # twitter_data = scrape_tweets("nba")
    twitter_tweepy_data = scrape_tweets_combo()

    logging.info("FINISHED WEB SCRAPE")

    # STEP 2: Transform data
    logging.info("STARTING DATA TRANSFORMATIONS")

    # stats = get_player_stats_transformed(stats_raw)
    # boxscores = get_boxscores_transformed(boxscores_raw)
    injury_data = get_injuries_transformed(injury_data_raw)
    transactions = get_transactions_transformed(transactions_raw)
    # adv_stats = get_advanced_stats_transformed(adv_stats_raw)
    # odds = get_odds_transformed(odds_raw)
    reddit_comment_data = get_reddit_comments(reddit_data["reddit_url"])
    # pbp_data = get_pbp_data_transformed(
    #     boxscores
    # )  # this uses the transformed boxscores
    # opp_stats = get_opp_stats_transformed(opp_stats_raw)
    # shooting_stats = get_shooting_stats_transformed(shooting_stats_raw)

    logging.info("FINISHED DATA TRANSFORMATIONS")

    logging.info("STARTING SCHEMA VALIDATION")

    # STEP 3: Validating Schemas - 1 for each SQL Write
    # stats = validate_schema(stats, stats_cols)
    # adv_stats = validate_schema(adv_stats, adv_stats_cols)
    # boxscores = validate_schema(boxscores, boxscores_cols)
    injury_data = validate_schema(injury_data, injury_cols)
    # opp_stats = validate_schema(opp_stats, opp_stats_cols)
    # pbp_stats = validate_schema(pbp_data, pbp_cols)
    reddit_data = validate_schema(reddit_data, reddit_cols)
    reddit_comment_data = validate_schema(reddit_comment_data, reddit_comment_cols)
    # odds = validate_schema(odds, odds_cols)
    transactions = validate_schema(transactions, transactions_cols)
    # twitter_data = validate_schema(twitter_data, twitter_cols)
    twitter_tweepy_data = validate_schema(twitter_tweepy_data, twitter_tweepy_cols)
    # schedule = validate_schema(schedule, schedule_cols)
    # shooting_stats = validate_schema(shooting_stats, shooting_stats_cols)

    logging.info("FINISHED SCHEMA VALIDATION")

    logging.info("STARTING SQL STORING")

    # STEP 4: Append Transformed Data to SQL
    conn = sql_connection(os.environ.get("RDS_SCHEMA"))

    with conn.connect() as connection:
        # write_to_sql(connection, "stats", stats, "append")
        # write_to_sql(connection, "boxscores", boxscores, "append")
        # write_to_sql(connection, "adv_stats", adv_stats, "append")
        # write_to_sql(connection, "odds", odds, "append")
        write_to_sql(connection, "reddit_data", reddit_data, "append")
        write_to_sql(connection, "reddit_comment_data", reddit_comment_data, "append")
        # write_to_sql(connection, "pbp_data", pbp_data, "append")
        # write_to_sql(connection, "opp_stats", opp_stats, "append")
        # write_to_sql(connection, "twitter_data", twitter_data, "append")
        # write_to_sql(connection, "schedule", schedule, "append")
        # write_to_sql(connection, "shooting_stats", shooting_stats, "append")

        # UPSERT TABLES
        write_to_sql_upsert(
            connection, "transactions", transactions, "upsert", ["date", "transaction"]
        )
        write_to_sql_upsert(
            connection,
            "injury_data",
            injury_data,
            "upsert",
            ["player", "team", "description"],
        )

        write_to_sql_upsert(
            connection, "twitter_tweepy_data", twitter_tweepy_data, "upsert", ["tweet_id"],
        )
        # write_to_sql_upsert(connection, "schedule", schedule, "upsert", ["away_team", "home_team", "proper_date"])

    conn.dispose()

    # STEP 5: Write to S3
    # write_to_s3("stats", stats)
    # write_to_s3("boxscores", boxscores)
    write_to_s3("injury_data", injury_data)
    write_to_s3("transactions", transactions)
    # write_to_s3("adv_stats", adv_stats)
    # write_to_s3("odds", odds)
    write_to_s3("reddit_data", reddit_data)
    write_to_s3("reddit_comment_data", reddit_comment_data)
    # write_to_s3("pbp_data", pbp_data)
    # write_to_s3("opp_stats", opp_stats)
    # write_to_s3("twitter_data", twitter_data)
    write_to_s3("twitter_tweepy_data", twitter_tweepy_data)
    # write_to_s3("schedule", schedule)
    # write_to_s3("shooting_stats", shooting_stats)

    # STEP 6: Grab Logs from previous steps & send email out detailing notable events
    logs = pd.read_csv("logs/example.log", sep=r"\\t", engine="python", header=None)
    logs = logs.rename(columns={0: "errors"})
    logs = logs.query("errors.str.contains('Failed')", engine="python")

    # STEP 7: Send Email
    send_aws_email(logs)

logging.info("FINISHED NBA ELT PIPELINE SCRIPT Version: 1.6.0")
