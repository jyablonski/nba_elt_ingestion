from datetime import datetime
import os

import awswrangler as wr
import boto3
from moto import mock_ses, mock_s3
import numpy as np
import pandas as pd
import pytest

from src.utils import write_to_sql, write_to_sql_upsert, sql_connection

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
    assert df_len == 630


# table has to get created from ^ first, other wise this will error out.
def test_write_to_sql(setup_database, player_stats_data):
    conn = setup_database.cursor()
    player_stats_data.schema = "Validated"
    write_to_sql(conn, "player_stats_data", player_stats_data, "append")
    df_len = len(list(conn.execute("SELECT * FROM aws_player_stats_data_source")))
    assert df_len == 630


def test_write_to_sql_no_data(setup_database):
    conn = setup_database.cursor()
    player_stats_data = pd.DataFrame({"errors": []})
    player_stats_data.schema = "Invalidated"
    write_to_sql(conn, "player_stats_data", player_stats_data, "append")
    df_len = len(list(conn.execute("SELECT * FROM aws_player_stats_data_source")))
    assert len(player_stats_data) == 0


def test_example_postgres(players_df):
    conn = sql_connection("nba_source", "postgres", "postgres", "localhost", "postgres")

    with conn.connect() as connection:
        sql = "SELECT count(*) FROM nba_source.aws_players_source"
        results = pd.read_sql_query(sql=sql, con=connection)

        write_to_sql_upsert(connection, "players", players_df, "upsert", ["player"])

        sql_upsert = "SELECT avg_ppg FROM nba_source.aws_players_source where player = 'Stephen Curry'"
        results_upsert = pd.read_sql_query(sql=sql_upsert, con=connection)

        sql_upsert_count = "SELECT count(*) FROM nba_source.aws_players_source"
        results_upsert_count = pd.read_sql_query(sql=sql_upsert_count, con=connection)

        assert results["count"][0] == 3
        assert results_upsert["avg_ppg"][0] == 29.2  # check value got updated
        assert (
            results_upsert_count["count"][0] == 3
        )  # check row count is still the same
