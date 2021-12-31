import pytest
from moto import mock_ses
import boto3
import pandas as pd
from app import write_to_sql, send_aws_email, execute_email_function


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


def test_boxscores_sql(setup_database, boxscores_data):
    df = boxscores_data
    print(len(df))
    cursor = setup_database.cursor()
    df.to_sql(con=setup_database, name="boxscores", index=False, if_exists="replace")
    df_len = len(list(cursor.execute("SELECT * FROM boxscores")))
    cursor.close()
    assert df_len >= 20


def test_opp_stats_sql(setup_database, opp_stats_data):
    df = opp_stats_data
    print(len(df))
    cursor = setup_database.cursor()
    df.to_sql(con=setup_database, name="opp_stats", index=False, if_exists="replace")
    df_len = len(list(cursor.execute("SELECT * FROM opp_stats")))
    cursor.close()
    assert df_len == 30


def test_injuries_sql(setup_database, injuries_data):
    df = injuries_data
    print(len(df))
    cursor = setup_database.cursor()
    df.to_sql(con=setup_database, name="injuries", index=False, if_exists="replace")
    df_len = len(list(cursor.execute("SELECT * FROM injuries")))
    cursor.close()
    assert df_len >= 10


def test_transactions_sql(setup_database, transactions_data):
    df = transactions_data
    print(len(df))
    cursor = setup_database.cursor()
    df.to_sql(con=setup_database, name="transactions", index=False, if_exists="replace")
    df_len = len(list(cursor.execute("SELECT * FROM transactions")))
    cursor.close()
    assert df_len >= 50


def test_advanced_stats_sql(setup_database, advanced_stats_data):
    df = advanced_stats_data
    print(len(df))
    cursor = setup_database.cursor()
    df.to_sql(
        con=setup_database, name="advanced_stats", index=False, if_exists="replace"
    )
    df_len = len(list(cursor.execute("SELECT * FROM advanced_stats")))
    cursor.close()
    assert df_len == 31


def test_player_stats_rows(player_stats_data):
    assert len(player_stats_data) >= 300


def test_player_stats_cols(player_stats_data):
    assert len(player_stats_data.columns) == 30


def test_player_transformed_stats_rows(player_transformed_stats_data):
    assert len(player_transformed_stats_data) >= 300


def test_player_transformed_stats_cols(player_transformed_stats_data):
    assert len(player_transformed_stats_data.columns) == 30


def test_boxscores_cols(boxscores_data):
    assert len(boxscores_data.columns) == 29


def test_boxscores_rows(boxscores_data):
    assert len(boxscores_data) >= 20


def test_opp_stats_cols(opp_stats_data):
    assert len(opp_stats_data.columns) == 6


def test_opp_stats_rows(opp_stats_data):
    assert len(opp_stats_data) == 30


def test_odds_cols(odds_data):
    assert len(odds_data.columns) == 6


def test_odds_rows(odds_data):
    assert len(odds_data) == 26


def test_injuries_cols(injuries_data):
    assert len(injuries_data.columns) == 5


def test_injuries_rows(injuries_data):
    assert len(injuries_data) >= 10


def test_transactions_cols(transactions_data):
    assert len(transactions_data.columns) == 3


def test_transactions_rows(transactions_data):
    assert len(transactions_data) >= 50


def test_advanced_stats_cols(advanced_stats_data):
    assert len(advanced_stats_data.columns) == 29  ##


def test_advanced_stats_rows(advanced_stats_data):
    assert len(advanced_stats_data) == 31


def test_opp_stats_colnames(opp_stats_data):
    assert list(opp_stats_data.columns) == [
        "team",
        "fg_percent_opp",
        "threep_percent_opp",
        "threep_made_opp",
        "ppg_opp",
        "scrape_date",
    ]
