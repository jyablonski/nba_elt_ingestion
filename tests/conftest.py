import socket


def guard(*args, **kwargs):
    raise Exception("you're using the internet hoe")


socket.socket = guard

import os
import pickle
import sqlite3

import pandas as pd
import pytest

from src.utils import *

# pytest tests/scrape_test.py::test_player_stats - use this to test 1 at a time yeet
## Testing transformation functions from utils.py with custom csv + pickle object fixtures with edge cases

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
def setup_database():
    """Fixture to set up an empty in-memory database"""
    conn = sqlite3.connect(":memory:")
    yield conn


@pytest.fixture(scope="function")
def player_stats_data(mocker):
    """
    Fixture to load player stats data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/stats_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    df = get_player_stats_data()
    return df


@pytest.fixture(scope="function")
def boxscores_data(mocker):
    """
    Fixture to load boxscores data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/boxscores_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    boxscores = get_boxscores_data()
    return boxscores


@pytest.fixture(scope="function")
def opp_stats_data(mocker):
    """
    Fixture to load team opponent stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/opp_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    opp_stats = get_opp_stats_data()
    return opp_stats


@pytest.fixture(scope="function")
def injuries_data(mocker):
    """
    Fixture to load injuries data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/injuries_dump.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    injuries = get_injuries_data()
    return injuries


@pytest.fixture(scope="function")
def transactions_data(mocker):
    """
    Fixture to load transactions data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/transactions_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    transactions = get_transactions_data()
    return transactions


@pytest.fixture(scope="function")
def advanced_stats_data(mocker):
    """
    Fixture to load team advanced stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/opp_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    adv_stats = get_advanced_stats_data()
    return adv_stats


# with open('pbp_data.pickle', 'wb') as handle:
# pickle.dump(df, handle)


@pytest.fixture(scope="function")
def shooting_stats_data(mocker):
    """
    Fixture to load shooting stats data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/shooting_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    shooting_stats = get_shooting_stats_data()
    return shooting_stats


# has to be pickle bc odds data can be returned in list of 1 or 2 objects
@pytest.fixture(scope="function")
def odds_data(mocker):
    """
    Fixture to load odds data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/scrape_odds.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    odds = scrape_odds()
    return odds


@pytest.fixture(scope="function")
def pbp_transformed_data(mocker):
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
    pbp_transformed = get_pbp_data(boxscores_df)
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
    fname = os.path.join(os.path.dirname(__file__), "fixtures/schedule.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    # IT WORKS
    # you have to first patch the requests.get response, and subsequently the return value of requests.get(url).content
    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    schedule = schedule_scraper("2022", ["february", "march"])
    return schedule


@pytest.fixture()
def reddit_comments_data(mocker):
    """
    Fixture to load reddit_comments data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/reddit_comments_data.csv")
    with open(fname, "rb") as fp:
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

    reddit_comments_data = get_reddit_comments(["fake", "test"])
    return reddit_comments_data


@pytest.fixture()
def twitter_tweepy_data(mocker):
    fname = os.path.join(os.path.dirname(__file__), "fixtures/tweepy_tweets.csv")
    twitter_csv = pd.read_csv(fname)

    mocker.patch("src.utils.tweepy.OAuthHandler").return_value = 1
    mocker.patch("src.utils.tweepy.API.search_tweets").return_value = 1
    mocker.patch("src.utils.tweepy.Cursor").return_value = 1
    mocker.patch("src.utils.tweepy.Cursor").return_value.items().return_value = 1
    mocker.patch("src.utils.pd.DataFrame").return_value = twitter_csv

    twitter_data = scrape_tweets_tweepy(
        search_parameter="nba", count=1, result_type="popular"
    )
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


@pytest.fixture(scope="session")
def add_sentiment_analysis_df():
    fname = os.path.join(os.path.dirname(__file__), "fixtures/reddit_comments_data.csv")
    df = pd.read_csv(fname)

    comments = add_sentiment_analysis(df, "comment")
    return comments


# postgres
@pytest.fixture()
def players_df():
    """Small Players Fixture for Postgres Testing."""
    fname = os.path.join(
        os.path.dirname(__file__), "fixtures/postgres_players_fixture.csv"
    )
    df = pd.read_csv(fname)

    return df


def test_get_season_type():
    regular_season = get_season_type(date(2023, 4, 8))
    play_in = get_season_type(date(2023, 4, 15))
    playoffs = get_season_type(date(2023, 4, 21))

    assert regular_season == "Regular Season"
    assert play_in == "Play-In"
    assert playoffs == "Playoffs"


def test_get_leading_zeroes():
    month_1 = get_leading_zeroes(date(2023, 1, 1).month)
    month_9 = get_leading_zeroes(date(2023, 9, 1).month)
    month_10 = get_leading_zeroes(date(2023, 10, 1).month)

    assert month_1 == "01"
    assert month_9 == "09"
    assert month_10 == 10
