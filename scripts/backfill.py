from datetime import datetime
import os

import click
from jyablonski_common_modules.sql import create_sql_engine, write_to_sql_upsert

from src.utils import get_boxscores_data, get_feature_flags, get_pbp_data


# example usage: `python -m scripts.backfill --run_date 2025-01-01`
@click.command()
@click.option("--run_date", required=True, help="Run date to backfill for")
def run_backfill(run_date: str) -> None:
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
    feature_flags = get_feature_flags(connection=engine)
    source_schema = "nba_source"

    boxscores = get_boxscores_data(
        feature_flags_df=feature_flags,
        year=year,
        month=month,
        day=day,
    )

    pbp_data = get_pbp_data(feature_flags_df=feature_flags, df=boxscores)

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
    return None


if __name__ == "__main__":
    run_backfill()
