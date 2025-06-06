from __future__ import annotations

from datetime import datetime
import json
import logging
import os
import pickle
import socket
from typing import Generator

from jyablonski_common_modules.sql import create_sql_engine
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.base import Connection

from src.feature_flags import FeatureFlagManager
from src.scrapers import (
    add_sentiment_analysis,
    get_advanced_stats_data,
    get_boxscores_data,
    get_injuries_data,
    get_odds_data,
    get_opp_stats_data,
    get_pbp_data,
    get_player_stats_data,
    get_reddit_comments,
    get_schedule_data,
    get_shooting_stats_data,
    get_transactions_data,
)


def guard(*args, **kwargs):
    raise Exception("you're using the internet hoe")


socket.socket = guard

# pytest tests/scrape_test.py::test_player_stats - use this
# to test 1 at a time yeet
# Testing transformation functions from utils.py with custom
# csv + pickle object fixtures with edge cases


# mock s3 / mock ses
@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["USER_EMAIL"] = "jyablonski9@gmail.com"


def get_postgres_host() -> str:
    return "postgres" if os.environ.get("ENV_TYPE") == "docker_dev" else "localhost"


@pytest.fixture(scope="session")
def postgres_engine():
    """Fixture to create a SQLAlchemy engine connected to Postgres."""
    engine = create_sql_engine(
        schema="nba_source",
        user="postgres",
        password="postgres",
        host=get_postgres_host(),
        database="postgres",
        port=5432,
    )
    return engine


@pytest.fixture(scope="session")
def postgres_conn(postgres_engine) -> Generator[Connection, None, None]:
    """Fixture to connect to Docker Postgres Container."""
    with postgres_engine.begin() as conn:
        yield conn


@pytest.fixture(scope="session", autouse=True)
def load_feature_flags(postgres_conn):
    """Autouse fixture to load feature flags from Postgres."""
    FeatureFlagManager.load(engine=postgres_conn)


@pytest.fixture(scope="function")
def player_stats_data(mocker: MockerFixture) -> pd.DataFrame:
    """
    Fixture to load player stats data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/stats_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.scrapers.requests.get").return_value.content = mock_content

    df = get_player_stats_data()
    return df


@pytest.fixture(scope="function")
def boxscores_data(mocker: MockerFixture) -> pd.DataFrame:
    """
    Fixture to load boxscores data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/boxscores_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.scrapers.requests.get").return_value.content = mock_content

    boxscores = get_boxscores_data()
    return boxscores


@pytest.fixture(scope="function")
def opp_stats_data(mocker: MockerFixture) -> pd.DataFrame:
    """
    Fixture to load team opponent stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/opp_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df

    opp_stats = get_opp_stats_data()
    return opp_stats


@pytest.fixture(scope="function")
def injuries_data(mocker: MockerFixture) -> pd.DataFrame:
    """
    Fixture to load injuries data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/injuries_dump.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df

    injuries = get_injuries_data()
    return injuries


@pytest.fixture(scope="function")
def transactions_data(mocker: MockerFixture) -> pd.DataFrame:
    """
    Fixture to load transactions data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/transactions_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.scrapers.requests.get").return_value.content = mock_content

    transactions = get_transactions_data()
    return transactions


@pytest.fixture(scope="function")
def advanced_stats_data(mocker) -> pd.DataFrame:
    """
    Fixture to load team advanced stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/opp_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df

    adv_stats = get_advanced_stats_data()
    return adv_stats


# with open('pbp_data.pickle', 'wb') as handle:
# pickle.dump(df, handle)


@pytest.fixture(scope="function")
def shooting_stats_data(mocker) -> pd.DataFrame:
    """
    Fixture to load shooting stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/shooting_stats.pkl")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df

    shooting_stats = get_shooting_stats_data()
    return shooting_stats


# has to be pickle bc odds data can be returned in list of 1 or 2 objects
@pytest.fixture(scope="function")
def odds_data(mocker: MockerFixture) -> pd.DataFrame:
    """
    Fixture to load odds data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/scrape_odds.pkl")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df

    odds = get_odds_data()
    return odds


@pytest.fixture(scope="function")
def pbp_transformed_data(mocker) -> pd.DataFrame:
    """
    Fixture to load boxscores data from a csv file for PBP Transform testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/boxscores_data.csv")
    boxscores_df = pd.read_csv(fname)
    boxscores_df["date"] = pd.to_datetime(boxscores_df["date"])

    fname = os.path.join(os.path.dirname(__file__), "fixtures/pbp_data.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df
    pbp_transformed = get_pbp_data(df=boxscores_df)
    return pbp_transformed


@pytest.fixture(scope="session")
def logs_data():
    """
    Fixture to load dummy error logs for testing
    """
    df = pd.DataFrame({"errors": "Test... Failure"})
    return df


@pytest.fixture()
def schedule_data(mocker: MockerFixture) -> pd.DataFrame:
    """
    Fixture to load schedule data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/schedule.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.scrapers.requests.get").return_value.content = mock_content

    # Note: the year + month dates here are dependent on the mock html file that's used
    schedule = get_schedule_data(year="2022", month_list=["february", "march"])

    schedule = schedule.drop_duplicates(
        subset=["away_team", "home_team", "proper_date"]
    )
    return schedule


@pytest.fixture()
def reddit_comments_data(mocker) -> pd.DataFrame:
    """
    Fixture to load reddit_comments data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/reddit_comments_data.csv")
    with open(fname, "rb"):
        reddit_comments_fixture = pd.read_csv(
            fname, index_col=0
        )  # literally fuck indexes

    # mock a whole bunch of praw OOP gahbage
    mocker.patch("src.scrapers.praw.Reddit").return_value = 1
    mocker.patch("src.scrapers.praw.Reddit").return_value.submission = 1
    mocker.patch(
        "src.scrapers.praw.Reddit"
    ).return_value.submission.comments.list().return_value = 1
    mocker.patch("src.scrapers.pd.DataFrame").return_value = reddit_comments_fixture

    reddit_comments_data = get_reddit_comments(urls=["fake", "test"])
    return reddit_comments_data


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
