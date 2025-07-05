from tests.utils.db_assertions import assert_db_row_count_change
from jyablonski_common_modules.sql import write_to_sql_upsert


def test_shooting_stats_upsert(postgres_conn, shooting_stats_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_player_shooting_stats",
        schema="nba_source",
        expected_before=1,
        expected_after=474,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_player_shooting_stats",
            "schema": "nba_source",
            "df": shooting_stats_data,
            "primary_keys": ["player"],
        },
    )
