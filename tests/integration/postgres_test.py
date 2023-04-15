import pandas as pd

from src.utils import write_to_sql, write_to_sql_upsert


def test_boxscores_upsert(postgres_conn, boxscores_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_boxscores_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_boxscores_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 145 records
    write_to_sql_upsert(
        postgres_conn, "boxscores", boxscores_data, "upsert", ["player", "date"]
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 145
    )  # check row count is 145, 144 new and 1 upsert


def test_odds_upsert(postgres_conn, odds_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_odds_source;")

    count_check = "SELECT count(*) FROM nba_source.aws_odds_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 24 records
    write_to_sql_upsert(postgres_conn, "odds", odds_data, "upsert", ["team", "date"])

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 24
    )  # check row count is 145, 144 new and 1 upsert


def test_pbp_upsert(postgres_conn, pbp_transformed_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_pbp_data_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_pbp_data_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 100 records
    write_to_sql_upsert(
        postgres_conn,
        "pbp_data",
        pbp_transformed_data,
        "upsert",
        [
            "hometeam",
            "awayteam",
            "date",
            "timequarter",
            "numberperiod",
            "descriptionplayvisitor",
            "descriptionplayhome",
        ],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 100
    )  # check row count is 100, 99 new and 1 upsert


def test_opp_stats_upsert(postgres_conn, opp_stats_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_opp_stats_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_opp_stats_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 30 records
    write_to_sql_upsert(postgres_conn, "opp_stats", opp_stats_data, "upsert", ["team"])

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 30
    )  # check row count is 30, 29 new and 1 upsert


def test_shooting_stats_upsert(postgres_conn, shooting_stats_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_shooting_stats_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_shooting_stats_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 605 records
    write_to_sql_upsert(
        postgres_conn, "shooting_stats", shooting_stats_data, "upsert", ["player"]
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 605
    )  # check row count is 605, 604 new and 1 upsert


def test_reddit_comment_data_upsert(postgres_conn, reddit_comments_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_reddit_comment_data_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_reddit_comment_data_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 999 records
    write_to_sql_upsert(
        postgres_conn, "reddit_comment_data", reddit_comments_data, "upsert", ["md5_pk"]
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 998
    )  # check row count is 999, 998 new and 1 upsert


def test_transactions_upsert(postgres_conn, transactions_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_transactions_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_transactions_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 1313 records
    write_to_sql_upsert(
        postgres_conn,
        "transactions",
        transactions_data,
        "upsert",
        ["date", "transaction"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 1313
    )  # check row count is 1313, 1312 new and 1 upsert


def test_injury_data_upsert(postgres_conn, injuries_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_injury_data_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_injury_data_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 17 records
    write_to_sql_upsert(
        postgres_conn,
        "injury_data",
        injuries_data,
        "upsert",
        ["player", "team", "description"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 17
    )  # check row count is 17, 16 new and 1 upsert


def test_twitter_tweepy_data_upsert(postgres_conn, twitter_tweepy_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_twitter_tweepy_data_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_twitter_tweepy_data_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 1060 records
    write_to_sql_upsert(
        postgres_conn,
        "twitter_tweepy_data",
        twitter_tweepy_data,
        "upsert",
        ["tweet_id"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 1060
    )  # check row count is 1060, 1059 new and 1 upsert


def test_schedule_upsert(postgres_conn, schedule_data):
    # postgres_conn.execute(statement="truncate table nba_source.aws_schedule_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_schedule_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 229 records
    write_to_sql_upsert(
        postgres_conn,
        "schedule",
        schedule_data,
        "upsert",
        ["away_team", "home_team", "proper_date"],
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 229
    )  # check row count is 229, 228 new and 1 upsert


def test_stats_insert(postgres_conn, player_stats_data):
    count_check = "SELECT count(*) FROM nba_source.aws_stats_source"

    # insert 630 records
    write_to_sql(postgres_conn, "stats", player_stats_data, "replace")

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert count_check_results_after["count"][0] == 630  # check row count is 630


def test_adv_stats_insert(postgres_conn, advanced_stats_data):
    count_check = "SELECT count(*) FROM nba_source.aws_adv_stats_source"

    # insert 30 records
    write_to_sql(postgres_conn, "adv_stats", advanced_stats_data, "replace")

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert count_check_results_after["count"][0] == 30  # check row count is 30
