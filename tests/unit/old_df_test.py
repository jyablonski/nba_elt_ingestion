import pandas as pd
import pytest

from tests.schema import (
    adv_stats_schema,
    boxscores_schema,
    boxscores_schema_fake,
    injury_schema,
    odds_schema,
    opp_stats_schema,
    pbp_data_schema,
    reddit_comment_data_schema,
    schedule_schema,
    shooting_stats_schema,
    stats_schema,
    transactions_schema,
    twitter_tweepy_schema,
)
from src.app import validate_schema


def test_validate_schema():
    # Create a sample DataFrame and schema for testing
    with pytest.raises(IndexError):
        df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
        validate_schema(df, ["col1", "col3"])


# 2023-04-15 update: these are basically obsolete now after adding
# integration tests but i'm keepin em in
def test_player_stats(player_stats_data):
    assert len(player_stats_data) == 377
    assert player_stats_data.dtypes.to_dict() == stats_schema


def test_boxscores(boxscores_data):
    assert len(boxscores_data) == 145
    assert boxscores_data.dtypes.to_dict() == boxscores_schema


def test_opp_stats(opp_stats_data):
    assert len(opp_stats_data) == 30
    assert opp_stats_data.dtypes.to_dict() == opp_stats_schema


def test_odds(odds_data):
    assert len(odds_data) == 20
    assert odds_data.dtypes.to_dict() == odds_schema


def test_injuries(injuries_data):
    assert len(injuries_data) == 17
    assert injuries_data.dtypes.to_dict() == injury_schema


def test_transactions(transactions_data):
    assert len(transactions_data) == 1313
    assert transactions_data.dtypes.to_dict() == transactions_schema


def test_advanced_stats(advanced_stats_data):
    assert len(advanced_stats_data) == 30
    assert advanced_stats_data.dtypes.to_dict() == adv_stats_schema


def test_shooting_stats(shooting_stats_data):
    assert len(shooting_stats_data) == 474
    assert shooting_stats_data.dtypes.to_dict() == shooting_stats_schema


def test_pbp(pbp_transformed_data):
    assert len(pbp_transformed_data) == 100
    assert pbp_transformed_data.dtypes.to_dict() == pbp_data_schema


def test_reddit_comment(reddit_comments_data):
    assert len(reddit_comments_data) == 998
    assert reddit_comments_data.dtypes.to_dict() == reddit_comment_data_schema
    assert reddit_comments_data["md5_pk"][0] == "69e09a5d419de1c16ad5cb156a6d3aac"


def test_fake_schema(boxscores_data):
    assert boxscores_data.dtypes.to_dict() != boxscores_schema_fake


# def test_twitter_tweepy(twitter_tweepy_data):
#     assert len(twitter_tweepy_data) == 1060
#     assert twitter_tweepy_data.dtypes.to_dict() == twitter_tweepy_schema


def test_schedule(schedule_data):
    assert len(schedule_data) == 0
    assert schedule_data.dtypes.to_dict() == schedule_schema
