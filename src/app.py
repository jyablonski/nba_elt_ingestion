import logging
import os

from jyablonski_common_modules.logging import create_logger
from jyablonski_common_modules.sql import create_sql_engine, write_to_sql_upsert

from src.aws import write_to_s3
from src.database import write_to_sql
from src.feature_flags import FeatureFlagManager
from src.scrapers import (
    get_boxscores_data,
    get_injuries_data,
    get_odds_data,
    get_opp_stats_data,
    get_pbp_data,
    get_player_adv_stats_data,
    get_player_stats_data,
    get_reddit_comments,
    get_reddit_data,
    get_schedule_data,
    get_shooting_stats_data,
    get_team_adv_stats_data,
    get_transactions_data,
)
from src.utils import generate_schedule_pull_type, query_logs, write_to_slack

if __name__ == "__main__":
    logger = create_logger(log_file="logs/example.log")
    logging.getLogger("requests").setLevel(
        logging.WARNING
    )  # get rid of https debug stuff
    logger.info("Starting Ingestion Script")

    logger.info("Starting Web Scrape")
    engine = create_sql_engine(
        user=os.environ.get("RDS_USER", default="default"),
        password=os.environ.get("RDS_PW", default="default"),
        host=os.environ.get("IP", default="default"),
        database=os.environ.get("RDS_DB", default="default"),
        schema=os.environ.get("RDS_SCHEMA", default="default"),
        port=os.environ.get("RDS_PORT", default=5432),
    )
    # load feature flags from db which implicitly get used in all of the
    # `get_*_data functions` to check if they need to run or not
    FeatureFlagManager.load(engine=engine)
    source_schema = "bronze"
    schedule_months_to_pull = generate_schedule_pull_type(
        season_type=FeatureFlagManager.get("season"),
        playoff_type=FeatureFlagManager.get("playoffs"),
    )

    # STEP 1: Extract Raw Data
    stats = get_player_stats_data()
    # get_boxscores_data(run_date=datetime(2025, 6, 22))
    boxscores = get_boxscores_data()
    injury_data = get_injuries_data()
    transactions = get_transactions_data()
    player_adv_stats = get_player_adv_stats_data()
    adv_stats = get_team_adv_stats_data()
    odds = get_odds_data()
    reddit_data = get_reddit_data(sub="nba")
    opp_stats = get_opp_stats_data()

    schedule = get_schedule_data(month_list=schedule_months_to_pull)
    shooting_stats = get_shooting_stats_data()
    reddit_comment_data = get_reddit_comments(urls=reddit_data["reddit_url"])
    pbp_data = get_pbp_data(df=boxscores)

    logger.info("Finished Web Scrape")

    logger.info("Starting SQL Upserts")

    # STEP 2: Write Data to SQL
    with engine.begin() as connection:
        write_to_sql_upsert(
            conn=connection,
            table="bbref_player_boxscores",
            schema=source_schema,
            df=boxscores,
            primary_keys=["player", "date"],
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="draftkings_game_odds",
            schema=source_schema,
            df=odds,
            primary_keys=["team", "date"],
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="bbref_player_pbp",
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
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="bbref_player_shooting_stats",
            schema=source_schema,
            df=shooting_stats,
            primary_keys=["player"],
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="bbref_player_adv_stats",
            schema=source_schema,
            df=player_adv_stats,
            primary_keys=["player", "team"],
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="reddit_posts",
            schema=source_schema,
            df=reddit_data,
            primary_keys=["reddit_url"],
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="reddit_comments",
            schema=source_schema,
            df=reddit_comment_data,
            primary_keys=["md5_pk"],
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="bbref_league_transactions",
            schema=source_schema,
            df=transactions,
            primary_keys=["date", "transaction"],
            update_timestamp_field="modified_at",
        )
        write_to_sql_upsert(
            conn=connection,
            table="bbref_player_injuries",
            schema=source_schema,
            df=injury_data,
            primary_keys=["player", "team", "description"],
            update_timestamp_field="modified_at",
        )

        write_to_sql_upsert(
            conn=connection,
            table="bbref_team_opponent_shooting_stats",
            schema=source_schema,
            df=opp_stats,
            primary_keys=["team"],
            update_timestamp_field="modified_at",
        )

        # cant upsert on these bc the column names have % and i kept getting issues
        # even after changing the col names to _pct instead etc.  no clue dude fk it
        write_to_sql(
            con=connection,
            table_name="bbref_player_stats_snapshot",
            df=stats,
            table_type="append",
        )
        write_to_sql(
            con=connection,
            table_name="bbref_team_adv_stats_snapshot",
            df=adv_stats,
            table_type="append",
        )

        write_to_sql_upsert(
            conn=connection,
            table="bbref_league_schedule",
            schema=source_schema,
            df=schedule,
            primary_keys=["away_team", "home_team", "proper_date"],
            update_timestamp_field="modified_at",
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
    write_to_s3(file_name="player_adv_stats", df=player_adv_stats)
    write_to_s3(file_name="opp_stats", df=opp_stats)
    write_to_s3(file_name="schedule", df=schedule)
    write_to_s3(file_name="shooting_stats", df=shooting_stats)

    logger.info("Finished Writes to S3")

    # STEP 4: Grab Logs from previous steps & send 1 slack message for any errors
    logs = query_logs()
    write_to_slack(errors=logs)

    logger.info("Finished Ingestion Script")
