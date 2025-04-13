from datetime import datetime


import boto3
from moto import mock_s3
import pandas as pd
import pytest

from src.aws import write_to_s3
from src.utils import get_leading_zeroes


@mock_s3
def test_write_to_s3_validated(player_stats_data):
    conn = boto3.resource("s3", region_name="us-east-1")
    today = datetime.now().date()
    month_prefix = get_leading_zeroes(today.month)
    player_stats_data.schema = "Validated"
    conn.create_bucket(Bucket="moto_test_bucket")

    write_to_s3("player_stats_data", player_stats_data, bucket="moto_test_bucket")
    bucket = conn.Bucket("moto_test_bucket")
    contents = [_.key for _ in bucket.objects.all()]

    assert (
        contents[0]
        == f"player_stats_data/validated/year={today.year}/month={month_prefix}/player_stats_data-{today}.parquet"  # noqa: E501s
    )


@mock_s3
def test_write_to_s3_empty():
    test_file_name = "test_data"
    bucket_name = "moto_test_bucket"
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket_name)
    df = pd.DataFrame()

    # We will capture logs to check if the function correctly logs that the file is not being stored
    with pytest.raises(Exception):
        with pytest.capture_logs(level="INFO") as logs:
            write_to_s3(file_name=test_file_name, df=df, bucket=bucket_name)

            # Check the logs to ensure the message about the empty DataFrame is logged
            assert "Not storing test_data to s3 because it's empty." in logs.output

    # no object should have been loaded
    bucket = conn.Bucket(bucket_name)
    contents = [obj.key for obj in bucket.objects.all()]

    assert len(contents) == 0
