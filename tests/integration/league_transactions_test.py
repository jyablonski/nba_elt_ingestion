from jyablonski_common_modules.sql import write_to_sql_upsert

from tests.utils.db_assertions import assert_db_row_count_change


def test_transactions_upsert(postgres_conn, transactions_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_league_transactions",
        schema="bronze",
        expected_before=1,
        expected_after=663,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_league_transactions",
            "schema": "bronze",
            "df": transactions_data,
            "primary_keys": ["date", "transaction"],
        },
    )
