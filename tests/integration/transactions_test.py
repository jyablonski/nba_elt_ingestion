import pandas as pd

from src.utils import write_to_sql_upsert


def test_transactions_upsert(postgres_conn, transactions_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_transactions_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_transactions_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 1313 records
    write_to_sql_upsert(
        postgres_conn,
        "transactions",
        transactions_data,
        "upsert",
        ["date", "transaction"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 1313
    )  # check row count is 1313, 1312 new and 1 upsert
