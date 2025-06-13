import pandas as pd

from src.utils import write_to_sql


def test_stats_insert(postgres_conn, player_stats_data):
    count_check = "SELECT count(*) FROM nba_source.bbref_player_stats_snapshot"

    # insert 630 records
    write_to_sql(
        con=postgres_conn,
        table_name="bbref_player_stats_snapshot",
        df=player_stats_data,
        table_type="replace",
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert count_check_results_after["count"][0] == 377  # check row count is 377
