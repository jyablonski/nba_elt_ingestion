from jyablonski_common_modules.sql import write_to_sql_upsert

from tests.utils.db_assertions import assert_db_row_count_change


def test_injury_data_upsert(postgres_conn, injuries_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_player_injuries",
        schema="bronze",
        expected_before=1,
        expected_after=17,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_player_injuries",
            "schema": "bronze",
            "df": injuries_data,
            "primary_keys": ["player", "team", "description"],
        },
    )
