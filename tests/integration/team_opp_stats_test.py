from jyablonski_common_modules.sql import write_to_sql_upsert

from tests.utils.db_assertions import assert_db_row_count_change


def test_opp_stats_upsert(postgres_conn, opp_stats_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_team_opponent_shooting_stats",
        schema="nba_source",
        expected_before=1,
        expected_after=30,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_team_opponent_shooting_stats",
            "schema": "nba_source",
            "df": opp_stats_data,
            "primary_keys": ["team"],
        },
    )
