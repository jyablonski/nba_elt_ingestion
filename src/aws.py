from datetime import datetime
import logging
import os

import awswrangler as wr
import pandas as pd

from src.utils import get_leading_zeroes


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
        pass
