from jyablonski_common_modules.sql import write_to_sql_upsert
import pandas as pd


def test_opp_stats_upsert(postgres_conn, opp_stats_data):
    count_check = "SELECT count(*) FROM nba_source.aws_opp_stats_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 30 records
    write_to_sql_upsert(
        conn=postgres_conn,
        table="opp_stats",
        schema="nba_source",
        df=opp_stats_data,
        primary_keys=["team"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 30
    )  # check row count is 30, 29 new and 1 upsert
