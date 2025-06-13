from jyablonski_common_modules.sql import write_to_sql_upsert
import pandas as pd


def test_odds_upsert(postgres_conn, odds_data):
    count_check = "SELECT count(*) FROM nba_source.draftkings_game_odds"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    write_to_sql_upsert(
        conn=postgres_conn,
        table="draftkings_game_odds",
        schema="nba_source",
        df=odds_data,
        primary_keys=["team", "date"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 21
    )  # check row count is 5, 4 new and 1 upsert
