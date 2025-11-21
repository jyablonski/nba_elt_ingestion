from jyablonski_common_modules.sql import write_to_sql_upsert

from tests.utils.db_assertions import assert_db_row_count_change


def test_reddit_comment_data_upsert(postgres_conn, reddit_comments_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="reddit_comments",
        schema="bronze",
        expected_before=1,
        expected_after=998,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "reddit_comments",
            "schema": "bronze",
            "df": reddit_comments_data,
            "primary_keys": ["md5_pk"],
        },
    )
