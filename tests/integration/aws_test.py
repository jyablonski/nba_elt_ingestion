from datetime import datetime


import boto3
from moto import mock_ses, mock_s3
import pandas as pd

from src.utils import (
    write_to_s3,
    send_aws_email,
    execute_email_function,
    get_leading_zeroes,
)


@mock_ses
def test_ses_email(aws_credentials, mocker):
    ses = boto3.client("ses", region_name="us-east-1")
    logs = pd.DataFrame({"errors": ["ex1", "ex2", "ex3"]})

    mocker.patch("src.utils.boto3.client").return_value = ses
    send_aws_email(logs)

    send_quota = ses.get_send_quota()

    assert ses.verify_email_identity(EmailAddress="jyablonski9@gmail.com")
    assert send_quota["ResponseMetadata"]["HTTPStatusCode"] == 200


@mock_ses
def test_ses_execution_logs(aws_credentials, mocker):
    ses = boto3.client("ses", region_name="us-east-1")
    logs = pd.DataFrame({"errors": ["ex1", "ex2", "ex3"]})

    mocker.patch("src.utils.boto3.client").return_value = ses
    execute_email_function(logs)

    send_quota = ses.get_send_quota()

    assert ses.verify_email_identity(EmailAddress="jyablonski9@gmail.com")
    assert send_quota["ResponseMetadata"]["HTTPStatusCode"] == 200


@mock_ses
def test_ses_execution_no_logs(aws_credentials, mocker):
    ses = boto3.client("ses", region_name="us-east-1")
    logs = pd.DataFrame({"errors": []})

    mocker.patch("src.utils.boto3.client").return_value = ses
    execute_email_function(logs)

    send_quota = ses.get_send_quota()

    assert ses.verify_email_identity(EmailAddress="jyablonski9@gmail.com")
    assert send_quota["ResponseMetadata"]["HTTPStatusCode"] == 200


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
