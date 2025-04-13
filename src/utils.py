from datetime import date, datetime
import json
import logging
import os
import re

import awswrangler as wr
from nltk.sentiment import SentimentIntensityAnalyzer
import numpy as np
import pandas as pd
import requests
import sentry_sdk

# import tweepy

# sentry_sdk.init(os.environ.get("SENTRY_TOKEN"), traces_sample_rate=1.0)
# sentry_sdk.set_user({"email": "jyablonski9@gmail.com"})


def filter_spread(value: str) -> str:
    """
    Filter out 3-digit values from the `spread` column
    in the Scrape Odds Function such as `-108` or `-112`

    Parameters:
        value (str): The original value from the spread column.

    Returns:
        The spread value without any 3-digit values present
    """
    parts = value.split()
    filtered_parts = [
        (
            part
            if (part[0] in ["+", "-"] and float(part[1:]) <= 25)
            or (part.isdigit() and int(part) <= 25)
            else ""
        )
        for part in parts
    ]
    result = " ".join(filtered_parts).strip()

    # this last part strips out a couple extra white spaces
    return re.sub(r"\s+", " ", result)


def get_season_type(todays_date: date | None = None) -> str:
    """
    Function to generate Season Type for a given Date.
    **2025-03-16 NOTE** this has been deprecated as this logic
    belongs in the dbt project

    Args:
        todays_date (date): The Date to generate a Season Type for.  Defaults to
            today's date.

    Returns:
        The Season Type for Given Date
    """
    if todays_date is None:
        todays_date = datetime.now().date()

    if todays_date < datetime(2025, 4, 15).date():
        season_type = "Regular Season"
    elif (todays_date >= datetime(2025, 4, 16).date()) & (
        todays_date < datetime(2025, 4, 21).date()
    ):
        season_type = "Play-In"
    else:
        season_type = "Playoffs"

    return season_type


def check_schedule(date: datetime.date) -> bool:
    """
    Small Function used in Boxscores + PBP Functions to check if
    there are any games scheduled for a given date.

    Args:
        date (datetime.date): The Date to check for games on.

    Returns:
        Boolean: True if there are games scheduled, False if not.
    """
    schedule_endpoint = f"https://api.jyablonski.dev/schedule?date={date}"
    schedule_data = requests.get(schedule_endpoint).json()

    return True if len(schedule_data) > 0 else False


def add_sentiment_analysis(df: pd.DataFrame, sentiment_col: str) -> pd.DataFrame:
    """
    Function to add Sentiment Analysis columns to a DataFrame via nltk Vader Lexicon.

    Args:
        df (pd.DataFrame): The Pandas DataFrame

        sentiment_col (str): The Column in the DataFrame to run Sentiment Analysis on
            (comments / tweets etc).

    Returns:
        The same DataFrame but with the Sentiment Analysis columns attached.
    """
    try:
        analyzer = SentimentIntensityAnalyzer()
        df["compound"] = [
            analyzer.polarity_scores(x)["compound"] for x in df[sentiment_col]
        ]
        df["neg"] = [analyzer.polarity_scores(x)["neg"] for x in df[sentiment_col]]
        df["neu"] = [analyzer.polarity_scores(x)["neu"] for x in df[sentiment_col]]
        df["pos"] = [analyzer.polarity_scores(x)["pos"] for x in df[sentiment_col]]
        df["sentiment"] = np.where(df["compound"] > 0, 1, 0)
        return df
    except Exception as e:
        logging.error(f"Error Occurred while adding Sentiment Analysis, {e}")
        sentry_sdk.capture_exception(e)
        raise


def get_leading_zeroes(value: int) -> str:
    """
    Function to add leading zeroes to a month (1 (January) -> 01).
    Used in the the `write_to_s3` function.

    Args:
        value (int): The value integer (created from `datetime.now().month`)

    Returns:
        The same value integer with a leading 0 if it is less than 10
            (Nov/Dec aka 11/12 unaffected).
    """
    if len(str(value)) > 1:
        return str(value)
    else:
        return f"0{value}"


def clean_player_names(name: str) -> str:
    """
    Function to remove suffixes from a player name.

    Args:
        name (str): The raw player name you wish to alter.

    Returns:
        str: Cleaned Name w/ no suffix bs
    """
    try:
        cleaned_name = (
            name.replace(" Jr.", "")
            .replace(" Sr.", "")
            .replace(" III", "")  # III HAS TO GO FIRST, OVER II
            .replace(" II", "")  # or else Robert Williams III -> Robert WilliamsI
            .replace(" IV", "")
        )
        return cleaned_name
    except Exception as e:
        logging.error(f"Error Occurred with Clean Player Names, {e}")
        sentry_sdk.capture_exception(e)
        raise


def write_to_s3(
    file_name: str,
    df: pd.DataFrame,
    date: datetime.date = datetime.now().date(),
    bucket: str = os.environ.get("S3_BUCKET"),
) -> None:
    """
    S3 Function using awswrangler to write file.  Only supports parquet right now.

    Args:
        file_name (str): The base name of the file (boxscores, opp_stats)

        df (pd.DataFrame): The Pandas DataFrame to write to S3

        bucket (str): The Bucket to write to.  Defaults to `os.environ.get('S3_BUCKET')`

        date (datetime.date): Date to partition the data by.
            Defaults to `datetime.now().date()`

    Returns:
        Writes the Pandas DataFrame to an S3 File.

    """
    year_partition = date.year
    month_partition = get_leading_zeroes(value=date.month)
    file_name_jn = f"{file_name}-{date}"
    try:
        if len(df) == 0:
            logging.info(f"Not storing {file_name} to s3 because it's empty.")
            pass
        else:
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{bucket}/{file_name}/validated/year={year_partition}/month={month_partition}/{file_name_jn}.parquet",
                index=False,
            )
            logging.info(
                f"Storing {len(df)} {file_name} rows to S3 (s3://{bucket}/{file_name}/validated/{year_partition}/{file_name_jn}.parquet)"
            )
            pass
    except Exception as error:
        logging.error(f"S3 Storage Function Failed {file_name}, {error}")
        sentry_sdk.capture_exception(error)
        pass


def write_to_sql(con, table_name: str, df: pd.DataFrame, table_type: str) -> None:
    """
    Simple Wrapper Function to write a Pandas DataFrame to SQL

    Args:
        con (SQL Connection): The connection to the SQL DB.

        table_name (str): The Table name to write to SQL as.

        df (DataFrame): The Pandas DataFrame to store in SQL

        table_type (str): Whether the table should replace or append to an
            existing SQL Table under that name

    Returns:
        Writes the Pandas DataFrame to a Table in the Schema we connected to.

    """
    try:
        if len(df) == 0:
            logging.info(f"{table_name} is empty, not writing to SQL")
        else:
            df.to_sql(
                con=con,
                name=table_name,
                index=False,
                if_exists=table_type,
            )
            logging.info(
                f"Writing {len(df)} {table_name} rows to aws_{table_name}_source to SQL"
            )

        return None
    except Exception as error:
        logging.error(f"SQL Write Script Failed, {error}")
        sentry_sdk.capture_exception(error)


def query_logs(log_file: str = "logs/example.log") -> list:
    """
    Small Function to read Logs CSV File and grab Errors

    Args:
        log_file (str): Optional String of the Log File Name

    Returns:
        list of Error Messages to be passed into Slack Function
    """
    logs = pd.read_csv(log_file, sep=r"\\t", engine="python", header=None)
    logs = logs.rename(columns={0: "errors"})
    logs = logs.query("errors.str.contains('Failed')", engine="python")
    logs = logs["errors"].to_list()

    logging.info(f"Returning {len(logs)} Failed Logs")
    return logs


def write_to_slack(
    errors: list, webhook_url: str = os.environ.get("WEBHOOK_URL", default="default")
) -> int | None:
    """ "
    Function to write Errors out to Slack.  Requires a pre-configured `webhook_url`
    to be setup.

    Args:
        errors (list): The list of Failed Tasks + their associated errors

        webhook_url (str): Optional Parameter to specify the Webhook to send the
            errors to.  Defaults to `os.environ.get("WEBHOOK_URL")`

    Returns:
        None, but writes the Errors to Slack if there are any
    """
    try:
        date = datetime.now().date()
        num_errors = len(errors)
        str_dump = "\n".join(errors)

        if num_errors > 0:
            response = requests.post(
                webhook_url,
                data=json.dumps(
                    {
                        "text": (
                            f"\U0001f6d1 {num_errors} Errors during NBA ELT "
                            f"Ingestion on {date}: \n {str_dump}"
                        )
                    }
                ),
                headers={"Content-Type": "application/json"},
            )
            logging.info(
                f"Wrote Errors to Slack, Reponse Code {response.status_code}. "
                "Exiting ..."
            )
            return response.status_code
        else:
            logging.info("No Error Logs, not writing to Slack.  Exiting out ...")
            return None
    except Exception as e:
        logging.error(f"Error Writing to Slack, {e}")
        raise
