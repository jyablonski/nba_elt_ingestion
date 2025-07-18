import logging
import os
from datetime import datetime

import click
from jyablonski_common_modules.logging import create_logger
from jyablonski_common_modules.sql import create_sql_engine, write_to_sql_upsert

from src.feature_flags import FeatureFlagManager
from src.scrapers import get_boxscores_data, get_pbp_data


# example usage: `python -m scripts.backfill --run_date 2025-01-01`
@click.command()
@click.option("--run_date", required=True, help="Run date to backfill for")
def run_backfill(run_date: str) -> None:
    """Backfill Function

    Args:
        run_date (str): Run date to pull Boxscores + PBP data for

    Returns:
        None, but writes upserts data to Postgres
    """
    logger = create_logger(log_file="logs/example.log")  # noqa
    logging.getLogger("requests").setLevel(
        logging.WARNING
    )  # get rid of https debug stuff
    click.echo(f"Running Backfill for {run_date}")

    # parse the run_date string into a datetime object
    try:
        parsed_date = datetime.strptime(run_date, "%Y-%m-%d")
        year = parsed_date.year
        month = parsed_date.month
        day = parsed_date.day
    except ValueError:
        raise click.BadParameter("run_date must be in format YYYY-MM-DD")

    engine = create_sql_engine(
        user=os.environ.get("RDS_USER", default="default"),
        password=os.environ.get("RDS_PW", default="default"),
        host=os.environ.get("IP", default="default"),
        database=os.environ.get("RDS_DB", default="default"),
        schema=os.environ.get("RDS_SCHEMA", default="default"),
        port=os.environ.get("RDS_PORT", default=5432),
    )
    FeatureFlagManager.load(engine=engine)
    source_schema = "nba_source"

    boxscores = get_boxscores_data(
        year=year,
        month=month,
        day=day,
    )

    pbp_data = get_pbp_data(df=boxscores)

    # STEP 2: Write Data to SQL
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

    print(f"Backfill for {run_date} complete")
    return


if __name__ == "__main__":
    run_backfill()
