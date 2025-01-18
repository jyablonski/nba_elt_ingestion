from jyablonski_common_modules.sql import write_to_sql_upsert
import pandas as pd


def test_shooting_stats_upsert(postgres_conn, shooting_stats_data):
    count_check = "SELECT count(*) FROM nba_source.aws_shooting_stats_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 473 records
    write_to_sql_upsert(
        conn=postgres_conn,
        table="shooting_stats",
        schema="nba_source",
        df=shooting_stats_data,
        primary_keys=["player"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 474
    )  # check row count is 474, 473 new and 1 upsert
