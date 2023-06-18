import pandas as pd

from src.utils import write_to_sql_upsert


def test_opp_stats_upsert(postgres_conn, opp_stats_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_opp_stats_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_opp_stats_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 30 records
    write_to_sql_upsert(postgres_conn, "opp_stats", opp_stats_data, "upsert", ["team"])

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 30
    )  # check row count is 30, 29 new and 1 upsert
