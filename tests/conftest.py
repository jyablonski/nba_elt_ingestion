from __future__ import annotations

import json
import logging
import os
import pickle
import socket
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from jyablonski_common_modules.sql import create_sql_engine

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

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_mock import MockerFixture
    from sqlalchemy.engine.base import Connection


def guard(*args, **kwargs):
    raise Exception("you're using the internet hoe")


FIXTURES_DIR = Path(__file__).parent / "fixtures"
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
    return create_sql_engine(
        schema="bronze",
        user="postgres",
        password="postgres",
        host=get_postgres_host(),
        database="postgres",
        port=5432,
    )


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
    """Fixture to load player stats data from an html file for testing."""
    fname = FIXTURES_DIR / "stats_html.html"
    with fname.open("rb") as fp:
        mock_content = fp.read()

    mock_response = mocker.MagicMock()
    # The context manager returns itself, and read() is called on that
    mock_response.__enter__.return_value.read.return_value = mock_content

    mocker.patch("src.scrapers.urllib.request.urlopen", return_value=mock_response)
    return get_player_stats_data()


@pytest.fixture(scope="function")
def boxscores_data(mocker: MockerFixture) -> pd.DataFrame:
    """Fixture to load boxscores data from an html file for testing."""
    fname = FIXTURES_DIR / "boxscores_html.html"
    with fname.open("rb") as fp:
        mock_content = fp.read()

    mock_response = mocker.MagicMock()
    mock_response.__enter__.return_value.read.return_value = mock_content

    # old requests mock
    # mocker.patch("src.scrapers.requests.get").return_value.content = mock_content
    mocker.patch("src.scrapers.urllib.request.urlopen", return_value=mock_response)
    return get_boxscores_data()


@pytest.fixture(scope="function")
def opp_stats_data(mocker: MockerFixture) -> pd.DataFrame:
    """Fixture to load team opponent stats data from a pickle file for testing."""
    fname = FIXTURES_DIR / "opp_stats.pickle"
    with fname.open("rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df
    return get_opp_stats_data()


@pytest.fixture(scope="function")
def injuries_data(mocker: MockerFixture) -> pd.DataFrame:
    """Fixture to load injuries data from a pickle file for testing."""
    fname = FIXTURES_DIR / "injuries_dump.pickle"
    with fname.open("rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df
    return get_injuries_data()


@pytest.fixture(scope="function")
def transactions_data(mocker: MockerFixture) -> pd.DataFrame:
    """Fixture to load transactions data from an html file for testing."""
    fname = FIXTURES_DIR / "transactions.html"
    with fname.open("rb") as fp:
        mock_html = fp.read()

    # Mock urllib.request.urlopen
    mock_response = mocker.MagicMock()
    mock_response.read.return_value = mock_html
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = None

    mocker.patch("urllib.request.urlopen", return_value=mock_response)

    return get_transactions_data()


@pytest.fixture(scope="function")
def advanced_stats_data(mocker) -> pd.DataFrame:
    """Fixture to load team advanced stats data from a pickle file for testing."""
    fname = FIXTURES_DIR / "opp_stats.pickle"
    with fname.open("rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df
    return get_advanced_stats_data()


@pytest.fixture(scope="function")
def shooting_stats_data(mocker) -> pd.DataFrame:
    """Fixture to load shooting stats data from a pickle file for testing."""
    fname = FIXTURES_DIR / "shooting_stats.pkl"
    with fname.open("rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df
    return get_shooting_stats_data()


@pytest.fixture(scope="function")
def odds_data(mocker: MockerFixture) -> pd.DataFrame:
    """Fixture to load odds data from a pickle file for testing."""
    fname = FIXTURES_DIR / "odds_data.pkl"
    with fname.open("rb") as fp:
        df_list = pickle.load(fp)  # Should be a list of dataframes

    # Ensure it's a list (for backwards compatibility if needed)
    if not isinstance(df_list, list):
        df_list = [df_list]

    mocker.patch("src.scrapers.pd.read_html", return_value=df_list)
    return get_odds_data()


@pytest.fixture(scope="function")
def pbp_transformed_data(mocker) -> pd.DataFrame:
    """Fixture to load boxscores data from a csv file for PBP Transform testing."""
    boxscores_fname = FIXTURES_DIR / "boxscores_data.csv"
    boxscores_df = pd.read_csv(boxscores_fname)
    boxscores_df["date"] = pd.to_datetime(boxscores_df["date"])

    pbp_fname = FIXTURES_DIR / "pbp_data.pickle"
    with pbp_fname.open("rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.scrapers.pd.read_html").return_value = df
    return get_pbp_data(df=boxscores_df)


@pytest.fixture(scope="session")
def logs_data():
    """Fixture to load dummy error logs for testing"""
    return pd.DataFrame({"errors": "Test... Failure"})


@pytest.fixture()
def schedule_data(mocker: MockerFixture) -> pd.DataFrame:
    """Fixture to load schedule data from an html file for testing."""
    fname = FIXTURES_DIR / "schedule.html"
    with fname.open("rb") as fp:
        mock_content = fp.read()

    # Parse the HTML file to create a mock DataFrame that pd.read_html would return
    mock_df = pd.read_html(mock_content)[0]

    # fixture was built w/ data from 2022
    mocker.patch("src.scrapers.SEASON_YEAR", 2022)
    mocker.patch("pandas.read_html").return_value = [mock_df]

    schedule = get_schedule_data(month_list=["february", "march"])
    return schedule.drop_duplicates(subset=["away_team", "home_team", "proper_date"])


@pytest.fixture()
def reddit_comments_data(mocker) -> pd.DataFrame:
    """Fixture to load reddit_comments data from a csv file for testing."""
    fname = FIXTURES_DIR / "reddit_comments_data.csv"
    with fname.open("rb"):
        reddit_comments_fixture = pd.read_csv(fname, index_col=0)

    # mock a whole bunch of praw OOP stuff
    mocker.patch("src.scrapers.praw.Reddit").return_value = 1
    mocker.patch("src.scrapers.praw.Reddit").return_value.submission = 1
    mocker.patch(
        "src.scrapers.praw.Reddit"
    ).return_value.submission.comments.list().return_value = 1
    mocker.patch("src.scrapers.pd.DataFrame").return_value = reddit_comments_fixture

    return get_reddit_comments(urls=["fake", "test"])


@pytest.fixture(scope="function")
def add_sentiment_analysis_df() -> pd.DataFrame:
    fname = FIXTURES_DIR / "reddit_comments_data.csv"
    df = pd.read_csv(fname)
    return add_sentiment_analysis(df, "comment")


@pytest.fixture()
def players_df() -> pd.DataFrame:
    """Small Players Fixture for Postgres Testing."""
    fname = FIXTURES_DIR / "postgres_players_fixture.csv"
    return pd.read_csv(fname)


@pytest.fixture(scope="function")
def feature_flags_dataframe() -> pd.DataFrame:
    """Fixture to load player stats data from a csv file for testing."""
    return pd.DataFrame(data={"flag": ["season", "playoffs"], "is_enabled": [1, 0]})


@pytest.fixture(scope="function")
def schedule_mock_data() -> dict:
    """Fixture to load player stats data from a csv file for testing."""
    fname = FIXTURES_DIR / "mock_schedule_fixed_date.json"
    with fname.open("rb") as fp:
        mock_content = fp.read()
    return json.loads(mock_content)


@pytest.fixture(scope="function")
def odds_upsert_check_df() -> pd.DataFrame:
    return pd.DataFrame(
        data={
            "team": ["POR"],
            "spread": [-1.0],
            "total": [200],
            "moneyline": [-150],
            "date": [datetime.now().date()],
            "datetime1": [datetime.now()],
        }
    )


@pytest.fixture
def mock_logging(caplog):
    caplog.set_level(logging.INFO)
    yield caplog
