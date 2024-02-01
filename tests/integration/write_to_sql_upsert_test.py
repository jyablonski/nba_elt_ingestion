import logging

import pandas as pd
import pytest
import sqlalchemy.exc

from src.utils import write_to_sql_upsert


def test_write_to_sql_upsert_create_table(postgres_conn, caplog):
    table_name = "fake_data"
    fake_data = pd.DataFrame(data={"id": [1, 2], "col2": [3, 4]})

    caplog.clear()
    caplog.set_level(logging.INFO)

    write_to_sql_upsert(
        conn=postgres_conn, table_name=table_name, df=fake_data, pd_index=["id"]
    )

    count_check = f"select count(*) from nba_source.aws_{table_name}_source"
    count_check_results = pd.read_sql_query(sql=count_check, con=postgres_conn)

    table_check = f"select count(*) from information_schema.tables where table_name = 'aws_{table_name}_source'"
    table_check_results = pd.read_sql_query(sql=table_check, con=postgres_conn)

    # Assert on log messages
    assert "Table aws_fake_data_source not found, creating it" in caplog.text
    assert count_check_results["count"][0] == 2
    assert table_check_results["count"][0] == 1


def test_write_to_sql_upsert_check(postgres_conn, odds_upsert_check_df):
    table_name = "odds_upsert"
    write_to_sql_upsert(
        conn=postgres_conn,
        table_name=table_name,
        df=odds_upsert_check_df,
        pd_index=["team", "date"],
    )

    odds_upsert_check_df["total"] = 250
    write_to_sql_upsert(
        conn=postgres_conn,
        table_name=table_name,
        df=odds_upsert_check_df,
        pd_index=["team", "date"],
    )

    count_check = f"select count(*) from nba_source.aws_{table_name}_source"
    count_check_results = pd.read_sql_query(sql=count_check, con=postgres_conn)
    table_check = f"select total from nba_source.aws_{table_name}_source"
    table_check_results = pd.read_sql_query(sql=table_check, con=postgres_conn)

    assert count_check_results["count"][0] == 1
    assert table_check_results["total"][0] == 250


def test_write_to_sql_upsert_fail(postgres_conn, odds_upsert_check_df):
    table_name = "odds_upsert"
    odds_upsert_check_df["test"] = "my big failure"

    with pytest.raises(sqlalchemy.exc.InternalError) as exc_info:
        write_to_sql_upsert(
            conn=postgres_conn,
            table_name=table_name,
            df=odds_upsert_check_df,
            pd_index=["team", "date"],
        )

    assert "current transaction is aborted" in str(exc_info.value)
