from jyablonski_common_modules.sql import write_to_sql_upsert

from tests.utils.db_assertions import assert_db_row_count_change


def test_player_adv_stats_upsert(postgres_conn, player_adv_stats_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_player_adv_stats",
        schema="bronze",
        expected_before=10,
        expected_after=480,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_player_adv_stats",
            "schema": "bronze",
            "df": player_adv_stats_data,
            "primary_keys": ["player", "team"],
        },
    )
