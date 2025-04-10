from __future__ import annotations

from datetime import datetime
import json
import logging
import os
import pickle
import socket
import time
from typing import Generator

from jyablonski_common_modules.sql import create_sql_engine
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.base import Connection

from src.utils import (
    add_sentiment_analysis,
    get_advanced_stats_data,
    get_boxscores_data,
    get_feature_flags,
    get_injuries_data,
    get_opp_stats_data,
    get_pbp_data,
    get_player_stats_data,
    get_reddit_comments,
    get_shooting_stats_data,
    get_transactions_data,
    schedule_scraper,
    scrape_odds,
)


def guard(*args, **kwargs):
    raise Exception("you're using the internet hoe")


socket.socket = guard

# pytest tests/scrape_test.py::test_player_stats - use this
# to test 1 at a time yeet
# Testing transformation functions from utils.py with custom
# csv + pickle object fixtures with edge cases


# mock s3 / mock ses
@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["USER_EMAIL"] = "jyablonski9@gmail.com"


@pytest.fixture(scope="session")
def postgres_conn() -> Generator[Connection, None, None]:
    """Fixture to connect to Docker Postgres Container"""
    # small override for local + docker testing to work fine
    if os.environ.get("ENV_TYPE") == "docker_dev":
        host = "postgres"
    else:
        host = "localhost"

    conn = create_sql_engine(
        schema="nba_source",
        user="postgres",
        password="postgres",
        host=host,
        database="postgres",
        port=5432,
    )
    with conn.begin() as conn:
        yield conn


@pytest.fixture(scope="session")
def get_feature_flags_postgres(postgres_conn) -> Generator[pd.DataFrame, None, None]:
    # test suite was shitting itself at the very beginning while trying
    # to run this without a `time.sleep()`
    time.sleep(1)
    feature_flags = get_feature_flags(postgres_conn)

    # feature_flags.to_parquet('feature_flags.parquet')
    yield feature_flags


@pytest.fixture(scope="function")
def player_stats_data(
    get_feature_flags_postgres: pd.DataFrame, mocker: MockerFixture
) -> pd.DataFrame:
    """
    Fixture to load player stats data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/stats_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    df = get_player_stats_data(feature_flags_df=get_feature_flags_postgres)
    return df


@pytest.fixture(scope="function")
def boxscores_data(
    get_feature_flags_postgres: pd.DataFrame, mocker: MockerFixture
) -> pd.DataFrame:
    """
    Fixture to load boxscores data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/boxscores_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    boxscores = get_boxscores_data(feature_flags_df=get_feature_flags_postgres)
    return boxscores


@pytest.fixture(scope="function")
def opp_stats_data(
    get_feature_flags_postgres: pd.DataFrame, mocker: MockerFixture
) -> pd.DataFrame:
    """
    Fixture to load team opponent stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/opp_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    opp_stats = get_opp_stats_data(feature_flags_df=get_feature_flags_postgres)
    return opp_stats


@pytest.fixture(scope="function")
def injuries_data(
    get_feature_flags_postgres: pd.DataFrame, mocker: MockerFixture
) -> pd.DataFrame:
    """
    Fixture to load injuries data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/injuries_dump.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    injuries = get_injuries_data(feature_flags_df=get_feature_flags_postgres)
    return injuries


@pytest.fixture(scope="function")
def transactions_data(
    get_feature_flags_postgres: pd.DataFrame, mocker: MockerFixture
) -> pd.DataFrame:
    """
    Fixture to load transactions data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/transactions_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    transactions = get_transactions_data(feature_flags_df=get_feature_flags_postgres)
    return transactions


@pytest.fixture(scope="function")
def advanced_stats_data(
    get_feature_flags_postgres: pd.DataFrame, mocker
) -> pd.DataFrame:
    """
    Fixture to load team advanced stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/opp_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    adv_stats = get_advanced_stats_data(feature_flags_df=get_feature_flags_postgres)
    return adv_stats


# with open('pbp_data.pickle', 'wb') as handle:
# pickle.dump(df, handle)


@pytest.fixture(scope="function")
def shooting_stats_data(
    get_feature_flags_postgres: pd.DataFrame, mocker
) -> pd.DataFrame:
    """
    Fixture to load shooting stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/shooting_stats.pkl")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    shooting_stats = get_shooting_stats_data(
        feature_flags_df=get_feature_flags_postgres
    )
    return shooting_stats


# has to be pickle bc odds data can be returned in list of 1 or 2 objects
@pytest.fixture(scope="function")
def odds_data(
    get_feature_flags_postgres: pd.DataFrame, mocker: MockerFixture
) -> pd.DataFrame:
    """
    Fixture to load odds data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/scrape_odds.pkl")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    odds = scrape_odds(feature_flags_df=get_feature_flags_postgres)
    return odds


@pytest.fixture(scope="function")
def pbp_transformed_data(
    get_feature_flags_postgres: pd.DataFrame, mocker
) -> pd.DataFrame:
    """
    Fixture to load boxscores data from a csv file for PBP Transform testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/boxscores_data.csv")
    boxscores_df = pd.read_csv(fname)
    boxscores_df["date"] = pd.to_datetime(boxscores_df["date"])

    fname = os.path.join(os.path.dirname(__file__), "fixtures/pbp_data.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df
    pbp_transformed = get_pbp_data(
        feature_flags_df=get_feature_flags_postgres, df=boxscores_df
    )
    return pbp_transformed


@pytest.fixture(scope="session")
def logs_data():
    """
    Fixture to load dummy error logs for testing
    """
    df = pd.DataFrame({"errors": "Test... Failure"})
    return df


@pytest.fixture()
def schedule_data(
    get_feature_flags_postgres: pd.DataFrame, mocker: MockerFixture
) -> pd.DataFrame:
    """
    Fixture to load schedule data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/schedule.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    schedule = schedule_scraper(
        feature_flags_df=get_feature_flags_postgres,
        year="2022",
        month_list=["february", "march"],
    )

    schedule = schedule.drop_duplicates(
        subset=["away_team", "home_team", "proper_date"]
    )
    return schedule


@pytest.fixture()
def reddit_comments_data(
    get_feature_flags_postgres: pd.DataFrame, mocker
) -> pd.DataFrame:
    """
    Fixture to load reddit_comments data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/reddit_comments_data.csv")
    with open(fname, "rb"):
        reddit_comments_fixture = pd.read_csv(
            fname, index_col=0
        )  # literally fuck indexes

    # mock a whole bunch of praw OOP gahbage
    mocker.patch("src.utils.praw.Reddit").return_value = 1
    mocker.patch("src.utils.praw.Reddit").return_value.submission = 1
    mocker.patch(
        "src.utils.praw.Reddit"
    ).return_value.submission.comments.list().return_value = 1
    mocker.patch("src.utils.pd.DataFrame").return_value = reddit_comments_fixture

    reddit_comments_data = get_reddit_comments(
        feature_flags_df=get_feature_flags_postgres, urls=["fake", "test"]
    )
    return reddit_comments_data


# @pytest.fixture()
# def twitter_tweepy_data(mocker: MockerFixture) -> pd.DataFrame:
#     fname = os.path.join(os.path.dirname(__file__), "fixtures/tweepy_tweets.csv")
#     twitter_csv = pd.read_csv(fname)

#     mocker.patch("src.utils.tweepy.OAuthHandler").return_value = 1
#     mocker.patch("src.utils.tweepy.API.search_tweets").return_value = 1
#     mocker.patch("src.utils.tweepy.Cursor").return_value = 1
#     mocker.patch("src.utils.tweepy.Cursor").return_value.items().return_value = 1
#     mocker.patch("src.utils.pd.DataFrame").return_value = twitter_csv

#     twitter_data = scrape_tweets_tweepy(
#         search_parameter="nba", count=1, result_type="popular"
#     )
#     twitter_data = twitter_data.drop_duplicates(subset=["tweet_id"])
#     return twitter_data


@pytest.fixture(scope="function")
def add_sentiment_analysis_df() -> pd.DataFrame:
    fname = os.path.join(os.path.dirname(__file__), "fixtures/reddit_comments_data.csv")
    df = pd.read_csv(fname)

    comments = add_sentiment_analysis(df, "comment")
    return comments


# postgres
@pytest.fixture()
def players_df() -> pd.DataFrame:
    """Small Players Fixture for Postgres Testing."""
    fname = os.path.join(
        os.path.dirname(__file__), "fixtures/postgres_players_fixture.csv"
    )
    df = pd.read_csv(fname)

    return df


# never got this workin fk it
@pytest.fixture()
def mock_globals_df(mocker: MockerFixture) -> pd.DataFrame:
    mocker.patch("src.app.globals").return_value = {"df": "df"}
    df = pd.DataFrame({"df": [1, 2], "b": [3, 4]})
    return df


@pytest.fixture(scope="function")
def feature_flags_dataframe() -> pd.DataFrame:
    """
    Fixture to load player stats data from a csv file for testing.
    """
    df = pd.DataFrame(data={"flag": ["season", "playoffs"], "is_enabled": [1, 0]})
    return df


@pytest.fixture(scope="function")
def schedule_mock_data() -> dict:
    """
    Fixture to load player stats data from a csv file for testing.
    """
    fname = os.path.join(
        os.path.dirname(__file__), "fixtures/mock_schedule_fixed_date.json"
    )
    with open(fname, "r") as file:
        mock_json_data = file.read()

    data = json.loads(mock_json_data)
    return data


@pytest.fixture(scope="function")
def odds_upsert_check_df() -> pd.DataFrame:
    fake_data = pd.DataFrame(
        data={
            "team": ["POR"],
            "spread": [-1.0],
            "total": [200],
            "moneyline": [-150],
            "date": [datetime.now().date()],
            "datetime1": [datetime.now()],
        }
    )
    return fake_data


@pytest.fixture
def mock_logging(caplog):
    caplog.set_level(logging.INFO)
    yield caplog
