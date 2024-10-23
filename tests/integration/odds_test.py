import pandas as pd

from src.utils import write_to_sql_upsert


def test_odds_upsert(postgres_conn, odds_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_odds_source;")

    count_check = "SELECT count(*) FROM nba_source.aws_odds_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    write_to_sql_upsert(
        conn=postgres_conn, table_name="odds", df=odds_data, pd_index=["team", "date"]
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 3
    )  # check row count is 5, 4 new and 1 upsert
