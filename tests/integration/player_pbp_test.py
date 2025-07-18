from jyablonski_common_modules.sql import write_to_sql_upsert

from tests.utils.db_assertions import assert_db_row_count_change


def test_pbp_upsert(postgres_conn, pbp_transformed_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_player_pbp",
        schema="nba_source",
        expected_before=1,
        expected_after=100,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_player_pbp",
            "schema": "nba_source",
            "df": pbp_transformed_data,
            "primary_keys": [
                "hometeam",
                "awayteam",
                "date",
                "timequarter",
                "numberperiod",
                "descriptionplayvisitor",
                "descriptionplayhome",
            ],
        },
    )
