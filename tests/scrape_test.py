from datetime import datetime

import awswrangler as wr
import boto3
from moto import mock_ses, mock_s3
import numpy as np
import pandas as pd
import pytest

from tests.schema import *

# SCHEMA VALIDATION + ROW COUNT TESTS
def test_player_stats(player_stats_data):
    assert len(player_stats_data) == 630
    assert player_stats_data.dtypes.to_dict() == stats_schema


def test_boxscores(boxscores_data):
    assert len(boxscores_data) == 145
    assert boxscores_data.dtypes.to_dict() == boxscores_schema


def test_opp_stats(opp_stats_data):
    assert len(opp_stats_data) == 30
    assert opp_stats_data.dtypes.to_dict() == opp_stats_schema


def test_odds(odds_data):
    assert len(odds_data) == 24
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
    assert len(shooting_stats_data) == 605
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


def test_twitter_tweepy(twitter_tweepy_data):
    assert len(twitter_tweepy_data) == 1061
    assert twitter_tweepy_data.dtypes.to_dict() == twitter_tweepy_schema


def test_schedule(schedule_data):
    assert len(schedule_data) == 458
    assert schedule_data.dtypes.to_dict() == schedule_schema


# add in new test rows if you add in new name replacement conditions
def test_clean_player_names(clean_player_names_data):
    assert len(clean_player_names_data) == 5
    assert clean_player_names_data["player"][0] == "Marcus Morris"
    assert clean_player_names_data["player"][1] == "Kelly Oubre"
    assert clean_player_names_data["player"][2] == "Gary Payton"
    assert clean_player_names_data["player"][3] == "Robert Williams"
    assert clean_player_names_data["player"][4] == "Lonnie Walker"


def test_add_sentiment_analysis(add_sentiment_analysis_df):
    assert len(add_sentiment_analysis_df) == 1000
    assert add_sentiment_analysis_df["compound"][0] == 0.0
    assert add_sentiment_analysis_df["neg"][0] == 0.0
    assert add_sentiment_analysis_df["neu"][0] == 1.0
    assert add_sentiment_analysis_df["pos"][0] == 0.0
    assert add_sentiment_analysis_df["sentiment"][0] == 0
