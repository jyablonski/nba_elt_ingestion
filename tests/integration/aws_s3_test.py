from datetime import datetime


import boto3
from moto import mock_s3

from src.utils import (
    write_to_s3,
    get_leading_zeroes,
)


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
def test_write_to_s3_invalidated(player_stats_data):
    conn = boto3.resource("s3", region_name="us-east-1")
    today = datetime.now().date()
    month_prefix = get_leading_zeroes(today.month)
    player_stats_data.schema = "Invalidated"
    conn.create_bucket(Bucket="moto_test_bucket")

    write_to_s3("player_stats_data", player_stats_data, bucket="moto_test_bucket")
    bucket = conn.Bucket("moto_test_bucket")
    contents = [_.key for _ in bucket.objects.all()]

    assert (
        contents[0]
        == f"player_stats_data/invalidated/year={today.year}/month={month_prefix}/player_stats_data-{today}.parquet"  # noqa: E501s
    )
