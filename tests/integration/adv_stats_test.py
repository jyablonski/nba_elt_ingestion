import pandas as pd

from src.utils import write_to_sql


def test_adv_stats_insert(postgres_conn, advanced_stats_data):
    count_check = "SELECT count(*) FROM nba_source.aws_adv_stats_source"

    # insert 30 records
    write_to_sql(postgres_conn, "adv_stats", advanced_stats_data, "replace")

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert count_check_results_after["count"][0] == 30  # check row count is 30
