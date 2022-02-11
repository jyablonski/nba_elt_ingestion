import pytest
from moto import mock_ses
import boto3
import pandas as pd
import numpy as np
from app import write_to_sql, send_aws_email, execute_email_function
from utils import adv_stats_cols, boxscores_cols, injury_cols, opp_stats_cols
from utils import pbp_cols, odds_cols, stats_cols, transactions_cols
from utils import get_player_stats_data
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
    execute_email_function(logs)
    assert ses.verify_email_identity(EmailAddress="jyablonski9@gmail.com")


# WRITE TO SQL TESTS
def test_player_stats_sql(setup_database, player_stats_data):
    df = player_stats_data
    print(len(df))
    cursor = setup_database.cursor()
    df.to_sql(
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
    write_to_sql(
        conn, player_stats_data, "append"
    )  # remember it creates f"aws_{data_name}_source" table
    df_len = len(list(conn.execute("SELECT * FROM aws_player_stats_data_source")))
    assert df_len == 384


def test_write_to_sql_no_data(setup_database):
    conn = setup_database.cursor()
    player_stats_data = pd.DataFrame({"errors": []})
    write_to_sql(
        conn, player_stats_data, "append"
    )  # remember it creates f"aws_{data_name}_source" table
    df_len = len(list(conn.execute("SELECT * FROM aws_player_stats_data_source")))
    assert len(player_stats_data) + df_len == 384
    assert len(player_stats_data) == 0


# SCHEMA VALIDATION + ROW COUNT TESTS
def test_player_stats_rows(player_stats_data):
    assert len(player_stats_data) == 384


def test_player_stats_schema(player_stats_data):
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


def test_pbp_schema(pbp_transformed_data):
    assert pbp_transformed_data.dtypes.to_dict() == pbp_data_schema


def test_pbp_rows(pbp_transformed_data):
    assert len(pbp_transformed_data) == 1184


def test_fake_schema(boxscores_data):
    assert boxscores_data.dtypes.to_dict() != boxscores_schema_fake


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

# def test_raw_stats_rows(player_stats_data_raw):
#     assert len(player_stats_data_raw) == 0

# def test_raw_stats_schema(player_stats_data_raw):
#     assert list(player_stats_data_raw.columns) == raw_stats_cols

# NEW NEW
# def test_raw_stats_rows(player_stats_html_get):
#     df = get_player_stats_data()
#     assert len(df) == 425
