from jyablonski_common_modules.sql import write_to_sql_upsert
import pandas as pd


def test_pbp_upsert(postgres_conn, pbp_transformed_data):
    count_check = "SELECT count(*) FROM nba_source.aws_pbp_data_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 100 records
    write_to_sql_upsert(
        conn=postgres_conn,
        table="aws_pbp_data_source",
        schema="nba_source",
        df=pbp_transformed_data,
        primary_keys=[
            "hometeam",
            "awayteam",
            "date",
            "timequarter",
            "numberperiod",
            "descriptionplayvisitor",
            "descriptionplayhome",
        ],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 100
    )  # check row count is 100, 99 new and 1 upsert
