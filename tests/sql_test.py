from datetime import datetime

import awswrangler as wr
import boto3
from moto import mock_ses, mock_s3
import numpy as np
import pandas as pd
import pytest

from src.utils import write_to_sql, write_to_sql_upsert

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


# don't think this is possible bc i use postgres which has different syntax and constraint stuff and schemas
# mocking half of the function out seems stupid right now
# def test_write_to_sql_upsert(setup_database, schedule_upsert_data_1, schedule_upsert_data_2):
#     conn = setup_database.cursor()
#     write_to_sql_upsert(setup_database, "schedule_dummy", schedule_upsert_data_1, "upsert", pd_index = ["away_team", "home_team", "proper_date"], is_test = True)
#     write_to_sql_upsert(setup_database, "schedule_dummy", schedule_upsert_data_2, "upsert", pd_index = ["away_team", "home_team", "proper_date"], is_test = True)
#     write_to_sql_upsert(setup_database, "schedule_dummy", schedule_upsert_data_2, "upsert", pd_index = ["away_team", "home_team", "proper_date"], is_test = True)
#     # intentionally write it twice to make sure data doesn't get duplicated

#     df_len = len(list(conn.execute("SELECT * from schedule_dummy;")))
#     conn.close()
#     assert df_len == 6
