from src.utils import write_to_sql
from tests.utils.db_assertions import assert_db_row_count_change


def test_stats_insert(postgres_conn, player_stats_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_player_stats_snapshot",
        schema="nba_source",
        expected_before=None,  # unknown or irrelevant for replace
        expected_after=377,
        writer=write_to_sql,
        writer_kwargs={
            "con": postgres_conn,
            "table_name": "bbref_player_stats_snapshot",
            "df": player_stats_data,
            "table_type": "replace",
        },
    )
