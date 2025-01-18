import logging
import os

from jyablonski_common_modules.logging import create_logger
from jyablonski_common_modules.sql import create_sql_engine, write_to_sql_upsert

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
    write_to_slack,
    write_to_sql,
    write_to_s3,
)

if __name__ == "__main__":
    logger = create_logger(log_file="logs/example.log")
    logging.getLogger("requests").setLevel(
        logging.WARNING
    )  # get rid of https debug stuff
    logger.info("Starting Ingestion Script Version: 1.13.4")

    logger.info("Starting Web Scrape")
    engine = create_sql_engine(
        user=os.environ.get("RDS_USER", default="default"),
        password=os.environ.get("RDS_PW", default="default"),
        host=os.environ.get("IP", default="default"),
        database=os.environ.get("RDS_DB", default="default"),
        schema=os.environ.get("RDS_SCHEMA", default="default"),
        port=os.environ.get("RDS_PORT", default=5432),
    )
    feature_flags = get_feature_flags(connection=engine)
    source_schema = "nba_source"

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
    # twitter_tweepy_data = scrape_tweets_combo(feature_flags_df=feature_flags)
    reddit_comment_data = get_reddit_comments(
        feature_flags_df=feature_flags, urls=reddit_data["reddit_url"]
    )
    pbp_data = get_pbp_data(
        feature_flags_df=feature_flags, df=boxscores
    )  # this uses the transformed boxscores

    logger.info("Finished Web Scrape")

    logger.info("Starting Schema Validation")
    logger.info("Starting SQL Upserts")

    # STEP 2: Append Transformed Data to SQL
    with engine.begin() as connection:
        write_to_sql_upsert(
            conn=connection,
            table="aws_boxscores_source",
            schema=source_schema,
            df=boxscores,
            primary_keys=["player", "date"],
        )
        write_to_sql_upsert(
            conn=connection,
            table="aws_odds_source",
            schema=source_schema,
            df=odds,
            primary_keys=["team", "date"],
        )
        write_to_sql_upsert(
            conn=connection,
            table="aws_pbp_data_source",
            schema=source_schema,
            df=pbp_data,
            primary_keys=[
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
            table="aws_shooting_stats_source",
            schema=source_schema,
            df=shooting_stats,
            primary_keys=["player"],
        )
        write_to_sql_upsert(
            conn=connection,
            table="aws_reddit_data_source",
            schema=source_schema,
            df=reddit_data,
            primary_keys=["reddit_url"],
        )
        write_to_sql_upsert(
            conn=connection,
            table="aws_reddit_comment_data_source",
            schema=source_schema,
            df=reddit_comment_data,
            primary_keys=["md5_pk"],
        )
        write_to_sql_upsert(
            conn=connection,
            table="aws_transactions_source",
            schema=source_schema,
            df=transactions,
            primary_keys=["date", "transaction"],
        )
        write_to_sql_upsert(
            conn=connection,
            table="aws_injury_data_source",
            schema=source_schema,
            df=injury_data,
            primary_keys=["player", "team", "description"],
        )

        # write_to_sql_upsert(
        #     conn=connection,
        #     table_name="twitter_tweepy_data",
        #     df=twitter_tweepy_data,
        #     primary_keys=["tweet_id"],
        # )
        write_to_sql_upsert(
            conn=connection,
            table="aws_opp_stats_source",
            schema=source_schema,
            df=opp_stats,
            primary_keys=["team"],
        )
        # cant upsert on these bc the column names have % and i kept getting issues
        # even after changing the col names to _pct instead etc.  no clue dude fk it
        write_to_sql(con=connection, table_name="stats", df=stats, table_type="append")
        write_to_sql(
            con=connection, table_name="adv_stats", df=adv_stats, table_type="append"
        )

        write_to_sql_upsert(
            conn=connection,
            table="aws_schedule_source",
            schema=source_schema,
            df=schedule,
            primary_keys=["away_team", "home_team", "proper_date"],
        )

    engine.dispose()
    logger.info("Finished SQL Upserts")

    # STEP 3: Write to S3
    logger.info("Starting Writes to S3")

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
    # write_to_s3(file_name="twitter_tweepy_data", df=twitter_tweepy_data)
    write_to_s3(file_name="schedule", df=schedule)
    write_to_s3(file_name="shooting_stats", df=shooting_stats)

    logger.info("Finished Writes to S3")

    # STEP 4: Grab Logs from previous steps & send slack message for any failures
    logs = query_logs()
    write_to_slack(errors=logs)

    logger.info("Finished Ingestion Script Version: 1.13.4")
