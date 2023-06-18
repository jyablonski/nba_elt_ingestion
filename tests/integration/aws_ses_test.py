import boto3
from moto import mock_ses
import pandas as pd

from src.utils import (
    send_aws_email,
    execute_email_function,
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
