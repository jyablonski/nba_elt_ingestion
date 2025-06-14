from jyablonski_common_modules.sql import write_to_sql_upsert
import pandas as pd


def test_transactions_upsert(postgres_conn, transactions_data):
    count_check = "SELECT count(*) FROM nba_source.bbref_league_transactions"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 1313 records
    write_to_sql_upsert(
        conn=postgres_conn,
        table="bbref_league_transactions",
        schema="nba_source",
        df=transactions_data,
        primary_keys=["date", "transaction"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 1313
    )  # check row count is 1313, 1312 new and 1 upsert
