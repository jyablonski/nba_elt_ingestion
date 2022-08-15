from datetime import datetime

import awswrangler as wr
import boto3
from moto import mock_ses, mock_s3
import numpy as np
import pandas as pd
import pytest

from src.utils import (
    write_to_s3,
    send_aws_email,
    execute_email_function,
    get_leading_zeroes,
)


@mock_ses
def test_ses_email(aws_credentials):
    ses = boto3.client("ses", region_name="us-east-1")
    logs = pd.DataFrame({"errors": ["ex1", "ex2", "ex3"]})
    send_aws_email(logs)
    assert ses.verify_email_identity(EmailAddress="jyablonski9@gmail.com")


@mock_ses
def test_ses_execution_logs(aws_credentials):
    ses = boto3.client("ses", region_name="us-east-1")
    logs = pd.DataFrame({"errors": ["ex1", "ex2", "ex3"]})
    execute_email_function(logs)
    assert ses.verify_email_identity(EmailAddress="jyablonski9@gmail.com")


@mock_ses
def test_ses_execution_no_logs(aws_credentials):
    ses = boto3.client("ses", region_name="us-east-1")
    logs = pd.DataFrame({"errors": []})
    send_aws_email(logs)
    assert ses.verify_email_identity(EmailAddress="jyablonski9@gmail.com")


@mock_s3
def test_write_to_s3_validated(player_stats_data):
    conn = boto3.resource("s3", region_name="us-east-1")
    today = datetime.now().date()
    month = datetime.now().month
    month_prefix = get_leading_zeroes(month)
    player_stats_data.schema = "Validated"
    conn.create_bucket(Bucket="moto_test_bucket")

    write_to_s3("player_stats_data", player_stats_data, bucket="moto_test_bucket")
    bucket = conn.Bucket("moto_test_bucket")
    contents = [_.key for _ in bucket.objects.all()]

    assert (
        contents[0]
        == f"player_stats_data/validated/year={datetime.now().year}/month={month_prefix}/player_stats_data-{today}.parquet"
    )

@mock_s3
def test_write_to_s3_invalidated(player_stats_data):
    conn = boto3.resource("s3", region_name="us-east-1")
    today = datetime.now().date()
    month = datetime.now().month
    month_prefix = get_leading_zeroes(month)
    player_stats_data.schema = "Invalidated"
    conn.create_bucket(Bucket="moto_test_bucket")

    write_to_s3("player_stats_data", player_stats_data, bucket="moto_test_bucket")
    bucket = conn.Bucket("moto_test_bucket")
    contents = [_.key for _ in bucket.objects.all()]

    assert (
        contents[0]
        == f"player_stats_data/invalidated/year={datetime.now().year}/month={month_prefix}/player_stats_data-{today}.parquet"
    )
