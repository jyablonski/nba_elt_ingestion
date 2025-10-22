from jyablonski_common_modules.sql import write_to_sql_upsert

from tests.utils.db_assertions import assert_db_row_count_change


def test_odds_upsert(postgres_conn, odds_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="draftkings_game_odds",
        schema="nba_source",
        expected_before=1,
        expected_after=5,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "draftkings_game_odds",
            "schema": "nba_source",
            "df": odds_data,
            "primary_keys": ["team", "date"],
        },
    )
