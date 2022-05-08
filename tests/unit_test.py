from datetime import datetime

import awswrangler as wr
import boto3
from moto import mock_ses, mock_s3
import numpy as np
import pandas as pd
import pytest

from src.utils import write_to_sql, write_to_s3, get_leading_zeroes
from src.utils import send_aws_email, execute_email_function
from tests.schema import *

# SES TESTS
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
        == f"player_stats_data/validated/{month_prefix}/player_stats_data-{today}.parquet"
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
        == f"player_stats_data/invalidated/{month_prefix}/player_stats_data-{today}.parquet"
    )


# WRITE TO SQL TESTS
def test_player_stats_sql(setup_database, player_stats_data):
    cursor = setup_database.cursor()
    player_stats_data.to_sql(
        con=setup_database,
        name="aws_player_stats_data_source",
        index=False,
        if_exists="replace",
    )
    df_len = len(list(cursor.execute("SELECT * FROM aws_player_stats_data_source")))
    cursor.close()
    assert df_len == 384


# table has to get created from ^ first, other wise this will error out.
def test_write_to_sql(setup_database, player_stats_data):
    conn = setup_database.cursor()
    player_stats_data.schema = "Validated"
    write_to_sql(conn, "player_stats_data", player_stats_data, "append")
    df_len = len(list(conn.execute("SELECT * FROM aws_player_stats_data_source")))
    assert df_len == 384


def test_write_to_sql_no_data(setup_database):
    conn = setup_database.cursor()
    player_stats_data = pd.DataFrame({"errors": []})
    player_stats_data.schema = "Invalidated"
    write_to_sql(conn, "player_stats_data", player_stats_data, "append")
    df_len = len(list(conn.execute("SELECT * FROM aws_player_stats_data_source")))
    assert len(player_stats_data) == 0


# SCHEMA VALIDATION + ROW COUNT TESTS
def test_player_stats_rows(player_stats_data):
    assert len(player_stats_data) == 384


def test_player_stats_schema(player_stats_data):
    # player_stats_data["file_name"] = "xxx"
    assert player_stats_data.dtypes.to_dict() == stats_schema


def test_boxscores_schema(boxscores_data):
    assert boxscores_data.dtypes.to_dict() == boxscores_schema


def test_boxscores_rows(boxscores_data):
    assert len(boxscores_data) == 171


def test_opp_stats_schema(opp_stats_data):
    assert opp_stats_data.dtypes.to_dict() == opp_stats_schema


def test_opp_stats_rows(opp_stats_data):
    assert len(opp_stats_data) == 30


def test_odds_schema(odds_data):
    assert odds_data.dtypes.to_dict() == odds_schema


def test_odds_rows(odds_data):
    assert len(odds_data) == 26


def test_injuries_schema(injuries_data):
    assert injuries_data.dtypes.to_dict() == injury_schema


def test_injuries_rows(injuries_data):
    assert len(injuries_data) == 65


def test_transactions_schema(transactions_data):
    assert transactions_data.dtypes.to_dict() == transactions_schema


def test_transactions_rows(transactions_data):
    assert len(transactions_data) == 77


def test_advanced_stats_schema(advanced_stats_data):
    assert advanced_stats_data.dtypes.to_dict() == adv_stats_schema


def test_advanced_stats_rows(advanced_stats_data):
    assert len(advanced_stats_data) == 31


def test_shooting_stats_schema(shooting_stats_data):
    assert shooting_stats_data.dtypes.to_dict() == shooting_stats_schema


def test_shooting_stats_rows(shooting_stats_data):
    assert len(shooting_stats_data) == 592


def test_pbp_schema(pbp_transformed_data):
    assert pbp_transformed_data.dtypes.to_dict() == pbp_data_schema


def test_pbp_rows(pbp_transformed_data):
    assert len(pbp_transformed_data) == 1184


def test_reddit_comment_schema(reddit_comments_data):
    assert reddit_comments_data.dtypes.to_dict() == reddit_comment_data_schema


def test_reddit_comment_rows(reddit_comments_data):
    assert len(reddit_comments_data) == 1000


def test_fake_schema(boxscores_data):
    assert boxscores_data.dtypes.to_dict() != boxscores_schema_fake


def test_twitter_rows(twitter_stats_data):
    assert len(twitter_stats_data) == 1286


def test_twitter_schema(twitter_stats_data):
    assert twitter_stats_data.dtypes.to_dict() == twitter_data_schema


def test_schedule_rows(schedule_data):
    assert len(schedule_data) == 458


def test_schedule_schema(schedule_data):
    assert schedule_data.dtypes.to_dict() == schedule_schema


def test_clean_player_names_rows(clean_player_names_data):
    assert len(clean_player_names_data) == 5


# add in new test rows if you add in new name replacement conditions
def test_clean_player_names_rows(clean_player_names_data):
    assert clean_player_names_data["player"][0] == "Marcus Morris"
    assert clean_player_names_data["player"][1] == "Kelly Oubre"
    assert clean_player_names_data["player"][2] == "Gary Payton"
    assert clean_player_names_data["player"][3] == "Robert Williams"
    assert clean_player_names_data["player"][4] == "Lonnie Walker"


### DEPRECATING COLUMN TESTING AS OF 2022-02-10
# def test_pbp_cols(pbp_transformed_data):
#     assert list(pbp_transformed_data.columns) == pbp_cols

# def test_advanced_stats_cols(advanced_stats_data):
#     assert list(advanced_stats_data.columns) == adv_stats_cols

# def test_transactions_cols(transactions_data):
#     assert list(transactions_data.columns) == transactions_cols

# def test_injuries_cols(injuries_data):
#     assert list(injuries_data.columns) == injury_cols

# def test_odds_cols(odds_data):
#     assert list(odds_data.columns) == odds_cols

# def test_opp_stats_cols(opp_stats_data):
#     assert list(opp_stats_data.columns) == opp_stats_cols

# def test_boxscores_cols(boxscores_data):
#     assert list(boxscores_data.columns) == boxscores_cols

# def test_player_stats_cols(player_stats_data):
#     assert list(player_stats_data.columns) == stats_cols

### WIP for raw html testing

# def test_raw_stats_rows(player_stats_html_get):
#     assert len(player_stats_html_get) == 1

# def test_raw_stats_schema(player_stats_data_raw):
#     assert list(player_stats_data_raw.columns) == raw_stats_cols

# NEW NEW
# def test_raw_stats_rows(player_stats_html_get):
#     df = get_player_stats_data()
#     assert len(df) == 425
