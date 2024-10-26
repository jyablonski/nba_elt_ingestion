import logging
import os

import pandas as pd

from src.schema import (
    adv_stats_cols,
    boxscores_cols,
    injury_cols,
    odds_cols,
    opp_stats_cols,
    pbp_cols,
    reddit_cols,
    reddit_comment_cols,
    schedule_cols,
    shooting_stats_cols,
    stats_cols,
    transactions_cols,
    twitter_tweepy_cols,
)

from src.utils import (
    get_advanced_stats_data,
    get_boxscores_data,
    get_feature_flags,
    get_injuries_data,
    get_opp_stats_data,
    get_pbp_data,
    get_player_stats_data,
    get_reddit_data,
    get_reddit_comments,
    get_shooting_stats_data,
    get_transactions_data,
    query_logs,
    schedule_scraper,
    scrape_odds,
    scrape_tweets_combo,
    sql_connection,
    write_to_slack,
    write_to_sql,
    write_to_sql_upsert,
    write_to_s3,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    handlers=[logging.FileHandler("logs/example.log"), logging.StreamHandler()],
)
logging.getLogger("requests").setLevel(logging.WARNING)  # get rid of https debug stuff
logging.info("Starting Ingestion Script Version: 1.13.1")


# helper validation function - has to be here instead of utils bc of globals().items()
def validate_schema(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """
    Schema Validation function to check whether table columns are correct before
    writing to SQL. Errors are written to Logs

    Args:
        data_df (pd.DataFrame): The Transformed Pandas DataFrame to check

        schema (list):  The corresponding columns of the Pandas DataFrame to be checked
    Returns:
        The same input DataFrame with a schema attribute that is either validated or
            invalidated
    """
    data_name = [k for k, v in globals().items() if v is df][0]
    try:
        if len(df) == 0:
            logging.error("df is empty for Schema Validation, skipping")
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


logging.info("Loaded Functions")

if __name__ == "__main__":
    logging.info("Starting Web Scrape")
    engine = sql_connection(
        rds_schema=os.environ.get("RDS_SCHEMA", default="default"),
        rds_port=os.environ.get("RDS_PORT", default=5432),
    )
    feature_flags = get_feature_flags(connection=engine)

    # STEP 1: Extract Raw Data
    stats = get_player_stats_data(feature_flags_df=feature_flags)
    boxscores = get_boxscores_data(feature_flags_df=feature_flags)
    injury_data = get_injuries_data(feature_flags_df=feature_flags)
    transactions = get_transactions_data(feature_flags_df=feature_flags)
    adv_stats = get_advanced_stats_data(feature_flags_df=feature_flags)
    odds = scrape_odds(feature_flags_df=feature_flags)
    reddit_data = get_reddit_data(feature_flags_df=feature_flags, sub="nba")
    opp_stats = get_opp_stats_data(feature_flags_df=feature_flags)
    schedule = schedule_scraper(feature_flags_df=feature_flags, year="2025")
    shooting_stats = get_shooting_stats_data(feature_flags_df=feature_flags)
    twitter_tweepy_data = scrape_tweets_combo(feature_flags_df=feature_flags)
    reddit_comment_data = get_reddit_comments(
        feature_flags_df=feature_flags, urls=reddit_data["reddit_url"]
    )
    pbp_data = get_pbp_data(
        feature_flags_df=feature_flags, df=boxscores
    )  # this uses the transformed boxscores

    logging.info("Finished Web Scrape")

    logging.info("Starting Schema Validation")

    # STEP 2: Validating Schemas - 1 for each SQL Write
    stats = validate_schema(df=stats, schema=stats_cols)
    adv_stats = validate_schema(df=adv_stats, schema=adv_stats_cols)
    boxscores = validate_schema(df=boxscores, schema=boxscores_cols)
    injury_data = validate_schema(df=injury_data, schema=injury_cols)
    opp_stats = validate_schema(df=opp_stats, schema=opp_stats_cols)
    pbp_data = validate_schema(df=pbp_data, schema=pbp_cols)
    reddit_data = validate_schema(df=reddit_data, schema=reddit_cols)
    reddit_comment_data = validate_schema(
        df=reddit_comment_data, schema=reddit_comment_cols
    )
    odds = validate_schema(df=odds, schema=odds_cols)
    twitter_tweepy_data = validate_schema(
        df=twitter_tweepy_data, schema=twitter_tweepy_cols
    )
    transactions = validate_schema(df=transactions, schema=transactions_cols)
    schedule = validate_schema(df=schedule, schema=schedule_cols)
    shooting_stats = validate_schema(df=shooting_stats, schema=shooting_stats_cols)

    logging.info("Finished Schema Validation")

    logging.info("Starting SQL Upserts")

    # STEP 3: Append Transformed Data to SQL
    with engine.begin() as connection:
        write_to_sql_upsert(
            conn=connection,
            table_name="boxscores",
            df=boxscores,
            pd_index=["player", "date"],
        )
        write_to_sql_upsert(
            conn=connection, table_name="odds", df=odds, pd_index=["team", "date"]
        )
        write_to_sql_upsert(
            conn=connection,
            table_name="pbp_data",
            df=pbp_data,
            pd_index=[
                "hometeam",
                "awayteam",
                "date",
                "timequarter",
                "numberperiod",
                "descriptionplayvisitor",
                "descriptionplayhome",
            ],
        )
        write_to_sql_upsert(
            conn=connection,
            table_name="shooting_stats",
            df=shooting_stats,
            pd_index=["player"],
        )
        write_to_sql_upsert(
            conn=connection,
            table_name="reddit_data",
            df=reddit_data,
            pd_index=["reddit_url"],
        )
        write_to_sql_upsert(
            conn=connection,
            table_name="reddit_comment_data",
            df=reddit_comment_data,
            pd_index=["md5_pk"],
        )
        write_to_sql_upsert(
            conn=connection,
            table_name="transactions",
            df=transactions,
            pd_index=["date", "transaction"],
        )
        write_to_sql_upsert(
            conn=connection,
            table_name="injury_data",
            df=injury_data,
            pd_index=["player", "team", "description"],
        )

        write_to_sql_upsert(
            conn=connection,
            table_name="twitter_tweepy_data",
            df=twitter_tweepy_data,
            pd_index=["tweet_id"],
        )
        write_to_sql_upsert(
            conn=connection, table_name="opp_stats", df=opp_stats, pd_index=["team"]
        )
        # cant upsert on these bc the column names have % and i kept getting issues
        # even after changing the col names to _pct instead etc.  no clue dude fk it
        write_to_sql(con=connection, table_name="stats", df=stats, table_type="append")
        write_to_sql(
            con=connection, table_name="adv_stats", df=adv_stats, table_type="append"
        )

        write_to_sql_upsert(
            conn=connection,
            table_name="schedule",
            df=schedule,
            pd_index=["away_team", "home_team", "proper_date"],
        )

    engine.dispose()
    logging.info("Finished SQL Upserts")

    # STEP 4: Write to S3
    logging.info("Starting Writes to S3")

    write_to_s3(file_name="stats", df=stats)
    write_to_s3(file_name="boxscores", df=boxscores)
    write_to_s3(file_name="injury_data", df=injury_data)
    write_to_s3(file_name="transactions", df=transactions)
    write_to_s3(file_name="adv_stats", df=adv_stats)
    write_to_s3(file_name="odds", df=odds)
    write_to_s3(file_name="reddit_data", df=reddit_data)
    write_to_s3(file_name="reddit_comment_data", df=reddit_comment_data)
    write_to_s3(file_name="pbp_data", df=pbp_data)
    write_to_s3(file_name="opp_stats", df=opp_stats)
    write_to_s3(file_name="twitter_tweepy_data", df=twitter_tweepy_data)
    write_to_s3(file_name="schedule", df=schedule)
    write_to_s3(file_name="shooting_stats", df=shooting_stats)

    logging.info("Finished Writes to S3")

    # STEP 5: Grab Logs from previous steps & send slack message for any failures
    logs = query_logs()
    write_to_slack(errors=logs)

    logging.info("Finished Ingestion Script Version: 1.13.1")
