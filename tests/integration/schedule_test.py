import pandas as pd

from src.utils import write_to_sql_upsert


def test_schedule_upsert(postgres_conn, schedule_data):
    count_check = "SELECT count(*) FROM nba_source.aws_schedule_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 229 records
    write_to_sql_upsert(
        conn=postgres_conn,
        table_name="schedule",
        df=schedule_data,
        pd_index=["away_team", "home_team", "proper_date"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    # TODO: adding in the current date filter messes, with this test, need to figure out how to handle this
    assert (
        count_check_results_after["count"][0] == 1
    )  # check row count is 1
