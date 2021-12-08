import os
import pytest
import pytest_mock
import sqlite3
import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta
from utils import *

## Testing transformation functions from utils with custom csv fixtures featuring edge cases

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
def player_transformed_stats_data():
    """
    Fixture to load player stats data from a csv file for testing.
    """
    fname = os.path.join(
        os.path.dirname(__file__), "fixture_csvs/player_stats_data.csv"
    )
    stats = pd.read_csv(fname)
    stats_transformed = get_player_stats_transformed(stats)
    return stats_transformed
