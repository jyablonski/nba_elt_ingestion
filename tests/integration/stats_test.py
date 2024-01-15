import pandas as pd

from src.utils import write_to_sql


def test_stats_insert(postgres_conn, player_stats_data):
    count_check = "SELECT count(*) FROM nba_source.aws_stats_source"

    # insert 630 records
    write_to_sql(
        con=postgres_conn,
        table_name="stats",
        df=player_stats_data,
        table_type="replace",
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert count_check_results_after["count"][0] == 630  # check row count is 630
