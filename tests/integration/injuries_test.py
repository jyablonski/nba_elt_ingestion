from jyablonski_common_modules.sql import write_to_sql_upsert
import pandas as pd


def test_injury_data_upsert(postgres_conn, injuries_data):
    count_check = "SELECT count(*) FROM nba_source.aws_injury_data_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 17 records
    write_to_sql_upsert(
        conn=postgres_conn,
        table="aws_injury_data_source",
        schema="nba_source",
        df=injuries_data,
        primary_keys=["player", "team", "description"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 17
    )  # check row count is 17, 16 new and 1 upsert
