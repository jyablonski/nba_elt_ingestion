import os
import pickle
import requests
import pytest
import pytest_mock
import sqlite3
import pandas as pd
import numpy as np
import boto3

# import moto
from datetime import datetime, timedelta
from src.utils import *

## Testing transformation functions from utils.py with custom csv + pickle object fixtures with edge cases

# mock ses
@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["USER_EMAIL"] = "jyablonski9@gmail.com"


@pytest.fixture(scope="session")
def setup_database():
    """ Fixture to set up an empty in-memory database """
    conn = sqlite3.connect(":memory:")
    yield conn


@pytest.fixture(scope="session")
def player_stats_data():
    """
    Fixture to load player stats data from a csv file for testing.
    """
    fname = os.path.join(
        os.path.dirname(__file__), "fixture_csvs/player_stats_data.csv"
    )
    stats = pd.read_csv(fname)
    stats = get_player_stats_transformed(stats)
    return stats


@pytest.fixture(scope="session")
def boxscores_data():
    """
    Fixture to load boxscores data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/boxscores_data.csv")
    df = pd.read_csv(fname)
    day = (datetime.now() - timedelta(1)).day
    month = (datetime.now() - timedelta(1)).month
    year = (datetime.now() - timedelta(1)).year
    season_type = "Regular Season"
    df = get_boxscores_transformed(df)
    return df


@pytest.fixture(scope="session")
def opp_stats_data():
    """
    Fixture to load team opponent stats data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/opp_stats_data.csv")
    df = pd.read_csv(fname)
    df = get_opp_stats_transformed(df)
    return df


@pytest.fixture(scope="session")
def injuries_data():
    """
    Fixture to load injuries data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/injuries_data.csv")
    df = pd.read_csv(fname)
    df = get_injuries_transformed(df)
    return df


@pytest.fixture(scope="session")
def transactions_data():
    """
    Fixture to load transactions data from a csv file for testing.
    """
    fname = os.path.join(
        os.path.dirname(__file__), "fixture_csvs/transactions_data.csv"
    )
    transactions = pd.read_csv(fname)
    transactions = get_transactions_transformed(transactions)
    return transactions


@pytest.fixture(scope="session")
def advanced_stats_data():
    """
    Fixture to load team advanced stats data from a csv file for testing.
    """
    fname = os.path.join(
        os.path.dirname(__file__), "fixture_csvs/advanced_stats_data.csv"
    )
    df = pd.read_csv(fname)
    df = get_advanced_stats_transformed(df)
    return df


@pytest.fixture(scope="session")
def shooting_stats_data():
    """
    Fixture to load shooting stats data from a csv file for testing.
    """
    fname = os.path.join(
        os.path.dirname(__file__), "fixture_csvs/shooting_stats_data.csv"
    )
    shooting_stats = pd.read_csv(fname)
    shooting_stats = get_shooting_stats_transformed(shooting_stats)
    return shooting_stats


# has to be pickle bc odds data can be returned in list of 1 or 2 objects
@pytest.fixture(scope="session")
def odds_data():
    """
    Fixture to load odds data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/odds_data")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)
    day = (datetime.now() - timedelta(1)).day
    month = (datetime.now() - timedelta(1)).month
    year = (datetime.now() - timedelta(1)).year
    df = get_odds_transformed(df)
    return df


@pytest.fixture(scope="session")
def pbp_transformed_data():
    """
    Fixture to load boxscores data from a csv file for PBP Transform testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/pbp_data.csv")
    boxscores = pd.read_csv(fname, parse_dates=["date"])
    pbp_transformed = get_pbp_data_transformed(boxscores)
    return pbp_transformed


@pytest.fixture(scope="session")
def logs_data():
    """
    Fixture to load dummy error logs for testing
    """
    df = pd.DataFrame({"errors": "Test... Failure"})
    return df


@pytest.fixture()
def schedule_data(mocker):
    """
    Fixture to load schedule data from an html file for testing.
    *** THIS WORKS FOR ANY REQUESTS.GET MOCKING IN THE FUTURE ***
    """
    fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/schedule.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    # IM A FUCKING GENIUS HOLY SHIT IT WORKS
    # you have to first patch the requests.get response, and subsequently the return value of requests.get(url).content
    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    schedule = schedule_scraper("2022", ["february", "march"])
    return schedule


@pytest.fixture()
def twitter_stats_data(mocker):
    fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/nba_tweets.csv")
    twitter_csv = pd.read_csv(fname)

    df = mocker.patch(
        "src.utils.pd.read_csv"
    )  # mock the return value for the csv to use my fixture
    df.return_value = twitter_csv

    twint_mock = mocker.patch(
        "src.utils.twint.run.Search"
    )  # mock the twitter scrape so it doesnt run
    twint_mock.return_value = 1
    twitter_data = scrape_tweets("nba")
    return twitter_data


@pytest.fixture(scope="session")
def clean_player_names_data():
    df = pd.DataFrame(
        {
            "player": [
                "Marcus Morris Sr.",
                "Kelly Oubre Jr.",
                "Gary Payton II",
                "Robert Williams III",
                "Lonnie Walker IV",
            ]
        }
    )
    df = clean_player_names(df)
    return df


##### NEW TESTS

# @pytest.fixture()
# def player_stats_html_get(mocker):
#     fname = os.path.join(os.path.dirname(__file__), "fixture_csvs/stats_html.html")
#     with open(fname, "rb") as fp:
#         html_data = fp.read()

#     html = mocker.patch("utils.requests.get")
#     html.content = PropertyMock(return_value = html_data)
#     html.return_value = html_data

#     stats_html_get = get_player_stats_data()
#     return stats_html_get
