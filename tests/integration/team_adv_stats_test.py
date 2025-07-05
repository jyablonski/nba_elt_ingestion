from tests.utils.db_assertions import assert_db_row_count_change
from src.database import write_to_sql


def test_adv_stats_insert(postgres_conn, advanced_stats_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_team_adv_stats_snapshot",
        schema="nba_source",
        expected_before=None,  # optional for replace
        expected_after=30,
        writer=write_to_sql,
        writer_kwargs={
            "con": postgres_conn,
            "table_name": "bbref_team_adv_stats_snapshot",
            "df": advanced_stats_data,
            "table_type": "replace",
        },
    )
