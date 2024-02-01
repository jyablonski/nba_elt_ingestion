import pandas as pd

from src.utils import write_to_sql_upsert


def test_reddit_comment_data_upsert(postgres_conn, reddit_comments_data):
    count_check = "SELECT count(*) FROM nba_source.aws_reddit_comment_data_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 999 records
    write_to_sql_upsert(
        conn=postgres_conn,
        table_name="reddit_comment_data",
        df=reddit_comments_data,
        pd_index=["md5_pk"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 998
    )  # check row count is 998, 997 new and 1 upsert
