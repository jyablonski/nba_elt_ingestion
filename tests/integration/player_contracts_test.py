from jyablonski_common_modules.sql import write_to_sql_upsert

from src.database import filter_unchanged_rows
from tests.utils.db_assertions import assert_db_row_count_change


def test_player_contracts_upsert(postgres_conn, player_contracts_data):
    contracts_to_upsert = filter_unchanged_rows(
        conn=postgres_conn,
        schema="bronze",
        table="bbref_player_contracts",
        df=player_contracts_data,
        primary_keys=["player", "season"],
        compare_columns=["season_salary"],
    )
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_player_contracts",
        schema="bronze",
        expected_before=1,
        expected_after=491,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_player_contracts",
            "schema": "bronze",
            "df": contracts_to_upsert,
            "primary_keys": ["player", "season"],
            "update_timestamp_field": "modified_at",
        },
    )


def test_player_contracts_skips_unchanged_rows(postgres_conn, player_contracts_data):
    contracts_to_upsert = filter_unchanged_rows(
        conn=postgres_conn,
        schema="bronze",
        table="bbref_player_contracts",
        df=player_contracts_data,
        primary_keys=["player", "season"],
        compare_columns=["season_salary"],
    )
    write_to_sql_upsert(
        conn=postgres_conn,
        table="bbref_player_contracts",
        schema="bronze",
        df=contracts_to_upsert,
        primary_keys=["player", "season"],
        update_timestamp_field="modified_at",
    )

    unchanged = filter_unchanged_rows(
        conn=postgres_conn,
        schema="bronze",
        table="bbref_player_contracts",
        df=player_contracts_data,
        primary_keys=["player", "season"],
        compare_columns=["season_salary"],
    )
    assert unchanged.empty
