from tests.utils.db_assertions import assert_db_row_count_change
from jyablonski_common_modules.sql import write_to_sql_upsert


def test_schedule_upsert(postgres_conn, schedule_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_league_schedule",
        schema="nba_source",
        expected_before=1,
        expected_after=1,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_league_schedule",
            "schema": "nba_source",
            "df": schedule_data,
            "primary_keys": ["away_team", "home_team", "proper_date"],
        },
    )
