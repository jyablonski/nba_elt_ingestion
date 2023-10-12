import os
import pickle
import socket
import time

import pandas as pd
import pytest
from sqlalchemy.engine.base import Engine

from src.utils import (
    add_sentiment_analysis,
    clean_player_names,
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
    scrape_tweets_tweepy,
    sql_connection,
)
from src.app import validate_schema


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
def postgres_conn() -> Engine:
    """Fixture to connect to Docker Postgres Container"""
    # small override for local + docker testing to work fine
    if os.environ.get("ENV_TYPE") == "docker_dev":
        host = "postgres"
    else:
        host = "localhost"

    conn = sql_connection(
        rds_schema="nba_source",
        rds_user="postgres",
        rds_pw="postgres",
        rds_ip=host,
        rds_db="postgres",
    )
    with conn.begin() as conn:
        yield conn


@pytest.fixture(scope="session")
def get_feature_flags_postgres(postgres_conn: Engine) -> pd.DataFrame:
    # test suite was shitting itself at the very beginning while trying
    # to run this without a `time.sleep()`
    time.sleep(3)
    feature_flags = get_feature_flags(postgres_conn)

    # feature_flags.to_parquet('feature_flags.parquet')
    yield feature_flags


@pytest.fixture(scope="function")
def player_stats_data(get_feature_flags_postgres: pd.DataFrame, mocker) -> pd.DataFrame:
    """
    Fixture to load player stats data from an html file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/stats_html.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mocker.patch("src.utils.requests.get").return_value.content = mock_content

    df = get_player_stats_data(feature_flags_df=get_feature_flags_postgres)
    df.schema = "Validated"
    return df


@pytest.fixture(scope="function")
def boxscores_data(get_feature_flags_postgres: pd.DataFrame, mocker) -> pd.DataFrame:
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
def opp_stats_data(get_feature_flags_postgres: pd.DataFrame, mocker) -> pd.DataFrame:
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
def injuries_data(get_feature_flags_postgres: pd.DataFrame, mocker) -> pd.DataFrame:
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
def transactions_data(get_feature_flags_postgres: pd.DataFrame, mocker) -> pd.DataFrame:
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
    adv_stats.schema = "Validated"
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
    fname = os.path.join(os.path.dirname(__file__), "fixtures/shooting_stats.pickle")
    with open(fname, "rb") as fp:
        df = pickle.load(fp)

    mocker.patch("src.utils.pd.read_html").return_value = df

    shooting_stats = get_shooting_stats_data(
        feature_flags_df=get_feature_flags_postgres
    )
    return shooting_stats


# has to be pickle bc odds data can be returned in list of 1 or 2 objects
@pytest.fixture(scope="function")
def odds_data(get_feature_flags_postgres: pd.DataFrame, mocker) -> pd.DataFrame:
    """
    Fixture to load odds data from a pickle file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/scrape_odds.pickle")
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
def schedule_data(get_feature_flags_postgres: pd.DataFrame, mocker) -> pd.DataFrame:
    """
    Fixture to load schedule data from an html file for testing.
    *** THIS WORKS FOR ANY REQUESTS.GET MOCKING IN THE FUTURE ***
    """
    fname = os.path.join(os.path.dirname(__file__), "fixtures/schedule.html")
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    # IT WORKS
    # you have to first patch the requests.get response, and subsequently the
    # return value of requests.get(url).content
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


@pytest.fixture()
def twitter_tweepy_data(mocker) -> pd.DataFrame:
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
    twitter_data = twitter_data.drop_duplicates(subset=["tweet_id"])
    return twitter_data


@pytest.fixture(scope="function")
def clean_player_names_data() -> pd.DataFrame:
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
def mock_globals_df(mocker) -> pd.DataFrame:
    mocker.patch("src.app.globals").return_value = {"df": "df"}
    df = pd.DataFrame({"df": [1, 2], "b": [3, 4]})

    df = validate_schema(df, ["a", "b"])
    return df


@pytest.fixture(scope="function")
def feature_flags_dataframe() -> pd.DataFrame:
    """
    Fixture to load player stats data from a csv file for testing.
    """
    df = pd.DataFrame(data={"flag": ["season", "playoffs"], "is_enabled": [1, 0]})
    return df
