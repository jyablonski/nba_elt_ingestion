import os
import pytest
import pytest_mock
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

@pytest.fixture
def setup_database():
    """ Fixture to set up an empty in-memory database """
    conn = sqlite3.connect(':memory:')
    yield conn

@pytest.fixture(scope='session')
def player_stats_data():
    """
    Fixture to load player stats data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), 'fixture_csvs/player_stats_data.csv')
    stats = pd.read_csv(fname)
    stats["PTS"] = pd.to_numeric(stats["PTS"])

    stats = stats.query("Player == Player").reset_index()
    stats["Player"] = (
        stats["Player"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    stats.columns = stats.columns.str.lower()
    stats["scrape_date"] = datetime.now().date()
    stats = stats.drop("index", axis=1)
    return stats

@pytest.fixture(scope='session')
def boxscores_data():
    """
    Fixture to load boxscores data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), 'fixture_csvs/boxscores_data.csv')
    df = pd.read_csv(fname)
    day = (datetime.now() - timedelta(1)).day
    month = (datetime.now() - timedelta(1)).month
    year = (datetime.now() - timedelta(1)).year
    season_type = 'Regular Season'
    df[
            [
                "FGM",
                "FGA",
                "FGPercent",
                "threePFGMade",
                "threePAttempted",
                "threePointPercent",
                "OREB",
                "DREB",
                "TRB",
                "AST",
                "STL",
                "BLK",
                "TOV",
                "PF",
                "PTS",
                "PlusMinus",
                "GmSc",
            ]
        ] = df[
            [
                "FGM",
                "FGA",
                "FGPercent",
                "threePFGMade",
                "threePAttempted",
                "threePointPercent",
                "OREB",
                "DREB",
                "TRB",
                "AST",
                "STL",
                "BLK",
                "TOV",
                "PF",
                "PTS",
                "PlusMinus",
                "GmSc",
            ]
        ].apply(
            pd.to_numeric
        )
    df["date"] = str(year) + "-" + str(month) + "-" + str(day)
    df["date"] = pd.to_datetime(df["date"])
    df["Type"] = season_type
    df["Season"] = 2022
    df["Location"] = df["Location"].apply(lambda x: "A" if x == "@" else "H")
    df["Team"] = df["Team"].str.replace("PHO", "PHX")
    df["Team"] = df["Team"].str.replace("CHO", "CHA")
    df["Team"] = df["Team"].str.replace("BRK", "BKN")
    df["Opponent"] = df["Opponent"].str.replace("PHO", "PHX")
    df["Opponent"] = df["Opponent"].str.replace("CHO", "CHA")
    df["Opponent"] = df["Opponent"].str.replace("BRK", "BKN")
    df = df.query("Player == Player").reset_index(drop=True)
    df["Player"] = (
        df["Player"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    df.columns = df.columns.str.lower()
    return df

@pytest.fixture(scope='session')
def opp_stats_data():
    """
    Fixture to load team opponent stats data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), 'fixture_csvs/opp_stats_data.csv')
    df = pd.read_csv(fname)
    df = df[["Team", "FG%", "3P%", "3P", "PTS"]]
    df = df.rename(
        columns={
            df.columns[0]: "team",
            df.columns[1]: "fg_percent_opp",
            df.columns[2]: "threep_percent_opp",
            df.columns[3]: "threep_made_opp",
            df.columns[4]: "ppg_opp",
        }
    )
    df = df.query('team != "League Average"')
    df = df.reset_index(drop=True)
    df["scrape_date"] = datetime.now().date()
    return df

@pytest.fixture(scope='session')
def injuries_data():
    """
    Fixture to load injuries data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), 'fixture_csvs/injuries_data.csv')
    df = pd.read_csv(fname)
    df = df.rename(columns={"Update": "Date"})
    df.columns = df.columns.str.lower()
    df["scrape_date"] = datetime.now().date()
    return df

@pytest.fixture(scope='session')
def transactions_data():
    """
    Fixture to load transactions data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), 'fixture_csvs/transactions_data.csv')
    transactions = pd.read_csv(fname)
    transactions.columns = ["Date", "Transaction"]
    transactions = transactions.query(
        'Date == Date & Date != ""'
    ).reset_index()  # filters out nulls and empty values
    transactions = transactions.explode("Transaction")
    transactions["Date"] = transactions["Date"].str.replace(
        "?", "Jan 1, 2021", regex=True  # bad data 10-14-21
    )
    transactions["Date"] = pd.to_datetime(transactions["Date"])
    transactions.columns = transactions.columns.str.lower()
    transactions = transactions[["date", "transaction"]]
    transactions["scrape_date"] = datetime.now().date()
    return transactions

@pytest.fixture(scope='session')
def advanced_stats_data():
    """
    Fixture to load team advanced stats data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), 'fixture_csvs/advanced_stats_data.csv')
    df = pd.read_csv(fname)
    df.drop(columns=df.columns[0], axis=1, inplace=True)

    df.columns = [
        "Team",
        "Age",
        "W",
        "L",
        "PW",
        "PL",
        "MOV",
        "SOS",
        "SRS",
        "ORTG",
        "DRTG",
        "NRTG",
        "Pace",
        "FTr",
        "3PAr",
        "TS%",
        "bby1",  # the bby columns are because of hierarchical html formatting - they're just blank columns
        "eFG%",
        "TOV%",
        "ORB%",
        "FT/FGA",
        "bby2",
        "eFG%_opp",
        "TOV%_opp",
        "DRB%_opp",
        "FT/FGA_opp",
        "bby3",
        "Arena",
        "Attendance",
        "Att/Game",
    ]
    df.drop(["bby1", "bby2", "bby3"], axis=1, inplace=True)
    df = df.query('Team != "League Average"').reset_index()
    df["Team"] = df["Team"].str.replace("*", "", regex=True)
    df["scrape_date"] = datetime.now().date()
    df.columns = df.columns.str.lower()
    return df

@pytest.fixture(scope='session')
def odds_data():
    """
    Fixture to load odds data from a csv file for testing.
    """
    fname = os.path.join(os.path.dirname(__file__), 'fixture_csvs/odds_data.csv')
    df = pd.read_csv(fname)
    day = (datetime.now() - timedelta(1)).day
    month = (datetime.now() - timedelta(1)).month
    year = (datetime.now() - timedelta(1)).year
    data1 = df[0].copy()
    date_try = str(year) + " " + data1.columns[0]
    data1["date"] = np.where(
        date_try == "2021 Today",
        datetime.now().date(),  # if the above is true, then return this
        str(year) + " " + data1.columns[0],  # if false then return this
    )
    date_try = data1["date"].iloc[0]
    data1.columns.values[0] = "Today"
    data1.reset_index(drop=True)
    data1["Today"] = data1["Today"].str.replace(
        "LA Clippers", "LAC Clippers", regex=True
    )
    data1["Today"] = data1["Today"].str.replace("AM", "AM ", regex=True)
    data1["Today"] = data1["Today"].str.replace("PM", "PM ", regex=True)
    data1["Time"] = data1["Today"].str.split().str[0]
    data1["datetime1"] = pd.to_datetime(
        date_try.strftime("%Y-%m-%d") + " " + data1["Time"]
    ) - timedelta(hours=5)

    data2 = df[1].copy()
    data2.columns.values[0] = "Today"
    data2.reset_index(drop=True)
    data2["Today"] = data2["Today"].str.replace(
        "LA Clippers", "LAC Clippers", regex=True
    )
    data2["Today"] = data2["Today"].str.replace("AM", "AM ", regex=True)
    data2["Today"] = data2["Today"].str.replace("PM", "PM ", regex=True)
    data2["Time"] = data2["Today"].str.split().str[0]
    data2["datetime1"] = (
        pd.to_datetime(date_try.strftime("%Y-%m-%d") + " " + data2["Time"])
        - timedelta(hours=5)
        + timedelta(days=1)
    )
    data2["date"] = data2["datetime1"].dt.date

    data = data1.append(data2).reset_index(drop=True)
    data["SPREAD"] = data["SPREAD"].str[:-4]
    data["TOTAL"] = data["TOTAL"].str[:-4]
    data["TOTAL"] = data["TOTAL"].str[2:]
    data["Today"] = data["Today"].str.split().str[1:2]
    data["Today"] = pd.DataFrame(
        [str(line).strip("[").strip("]").replace("'", "") for line in data["Today"]]
    )
    data["SPREAD"] = data["SPREAD"].str.replace("pk", "-1", regex=True)
    data["SPREAD"] = data["SPREAD"].str.replace("+", "", regex=True)
    data.columns = data.columns.str.lower()
    data = data[["today", "spread", "total", "moneyline", "date", "datetime1"]]
    data = data.rename(columns={data.columns[0]: "team"})
    data = data.query("date == date.min()")  # only grab games from upcoming day
    return data
