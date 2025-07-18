from datetime import datetime

import boto3
import pandas as pd
from moto import mock_aws

from src.aws import write_to_s3
from src.utils import get_leading_zeroes


@mock_aws
def test_write_to_s3_validated(player_stats_data):
    conn = boto3.resource("s3", region_name="us-east-1")
    today = datetime.now().date()
    month_prefix = get_leading_zeroes(today.month)
    player_stats_data.schema = "Validated"
    table_name = "player_stats_data"

    conn.create_bucket(Bucket="moto_test_bucket")

    write_to_s3(table_name, player_stats_data, bucket="moto_test_bucket")
    bucket = conn.Bucket("moto_test_bucket")
    contents = [_.key for _ in bucket.objects.all()]

    file_name = f"{table_name}-{today}.parquet"
    assert (
        contents[0]
        == f"{table_name}/validated/year={today.year}/month={month_prefix}/{file_name}"
    )


@mock_aws
def test_write_to_s3_empty(caplog):
    test_file_name = "test_data"
    bucket_name = "moto_test_bucket"
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket_name)
    df = pd.DataFrame()

    caplog.set_level("INFO")

    write_to_s3(file_name=test_file_name, df=df, bucket=bucket_name)

    assert "Not storing test_data to s3 because it's empty." in caplog.text

    bucket = conn.Bucket(bucket_name)
    contents = [obj.key for obj in bucket.objects.all()]
    assert len(contents) == 0
