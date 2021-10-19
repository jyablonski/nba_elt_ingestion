import pytest
import requests
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen
import os
import pandas as pd
import numpy as np
import praw
from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError

today = datetime.now().date()
todaytime = datetime.now()
yesterday = today - timedelta(1)
day = (datetime.now() - timedelta(1)).day
month = (datetime.now() - timedelta(1)).month
year = (datetime.now() - timedelta(1)).year
season_type = "Regular Season"


def test_basketball_reference_responsive(bbref_url):
    response = requests.get(bbref_url)
    assert response.status_code == 200


def test_draftkings_responsive(draftkings_url):
    response = requests.get(draftkings_url)
    assert response.status_code == 200


def test_get_player_stats():
    """
      Web Scrape function w/ BS4 that grabs aggregate season stats

      Args:
         None
      
      Returns:
         Pandas DataFrame of Player Aggregate Season stats
      """
    url = "https://www.basketball-reference.com/leagues/NBA_{}_per_game.html".format(
        year
    )
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")

    headers = [th.getText() for th in soup.findAll("tr", limit=2)[0].findAll("th")]
    headers = headers[1:]

    rows = soup.findAll("tr")[1:]
    player_stats = [
        [td.getText() for td in rows[i].findAll("td")] for i in range(len(rows))
    ]

    stats = pd.DataFrame(player_stats, columns=headers)
    stats["PTS"] = pd.to_numeric(stats["PTS"])

    stats = stats.query("Player == Player").reset_index()
    stats["Player"] = (
        stats["Player"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    stats.columns = stats.columns.str.lower()
    assert len(stats) > 0


def test_get_boxscores(month=12, day=23, year=2020):
    """
      Web Scrape function w/ BS4 that grabs box scores for certain day.

      Args:
         month (integer) - the month the game was played
         day (integer) - the day the game was played
         year (integer) - the year the game was played

      Returns:
         Pandas DataFrame of every player's boxscores for games played that day.
      """
    url = "https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={}&day={}&year={}&type=all".format(
        month, day, year
    )
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")

    headers = [th.getText() for th in soup.findAll("tr", limit=2)[0].findAll("th")]
    headers = headers[1:]
    headers[1] = "Team"
    headers[2] = "Location"
    headers[3] = "Opponent"
    headers[4] = "Outcome"
    headers[6] = "FGM"
    headers[8] = "FGPercent"
    headers[9] = "threePFGMade"
    headers[10] = "threePAttempted"
    headers[11] = "threePointPercent"
    headers[14] = "FTPercent"
    headers[15] = "OREB"
    headers[16] = "DREB"
    headers[24] = "PlusMinus"

    rows = soup.findAll("tr")[1:]
    player_stats = [
        [td.getText() for td in rows[i].findAll("td")] for i in range(len(rows))
    ]

    df = pd.DataFrame(player_stats, columns=headers)
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
    df = df.query("Player == Player").reset_index()
    df["Player"] = (
        df["Player"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    df.columns = df.columns.str.lower()
    assert len(df) > 0


def test_get_injuries():
    """
      Web Scrape function w/ pandas read_html that grabs all current injuries

      Args:
         None

      Returns:
         Pandas DataFrame of all current player injuries & their associated team
      """
    url = "https://www.basketball-reference.com/friv/injuries.fcgi"
    df = pd.read_html(url)[0]
    df = df.rename(columns={"Update": "Date"})
    df.columns = df.columns.str.lower()
    assert len(df) > 0


def test_get_transactions():
    """
      Web Scrape function w/ BS4 that retrieves NBA Trades, signings, waivers etc.

      Args: 
         None

      Returns:
         Pandas DataFrame of all season transactions, trades, player waives etc.
      """
    url = "https://www.basketball-reference.com/leagues/NBA_2022_transactions.html"
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")
    trs = soup.findAll("li")[
        70:
    ]  # theres a bunch of garbage in the first 71 rows - no matter what
    rows = []
    mylist = []
    for tr in trs:
        date = tr.find("span")
        if date is not None:  # needed bc span can be null (multi <p> elements per span)
            date = date.text
        data = tr.findAll("p")
        for p in data:
            mylist.append(p.text)
        data3 = [date] + [mylist]
        rows.append(data3)
        mylist = []
    transactions = pd.DataFrame(rows)
    transactions.columns = ["Date", "Transaction"]
    transactions = transactions.query('Date == Date & Date != ""').reset_index()
    transactions = transactions.explode("Transaction")
    transactions["Date"] = pd.to_datetime(transactions["Date"])
    transactions
    assert len(transactions) > 0


def test_get_advanced_stats():
    """
      Web Scrape function w/ pandas read_html that grabs all team advanced stats

      Args:
         None
      
      Returns:
         Pandas DataFrame of all current Team Advanced Stats
      """
    url = "https://www.basketball-reference.com/leagues/NBA_2021.html"
    df = pd.read_html(url)
    df = pd.DataFrame(df[10])
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
    # Playoff teams get a * next to them ??  fkn stupid, filter it out.
    df["Team"] = df["Team"].str.replace("*", "", regex=True)
    df.columns = df.columns.str.lower()
    assert len(df) > 0


def test_get_odds():
    """
      Web Scrape function w/ pandas read_html that grabs current day's nba odds

      Args:
         None

      Returns:
         Pandas DataFrame of NBA moneyline + spread odds for upcoming games for that day
      """
    url = "https://sportsbook.draftkings.com/leagues/basketball/88670846?category=game-lines&subcategory=game"
    df = pd.read_html(url)

    data1 = df[0].copy()
    date_try = str(year) + " " + data1.columns[0]
    data1["date"] = np.where(
        date_try == "2021 Today",
        datetime.now().date(),
        str(year) + " " + data1.columns[0],
    )  # if else to grab current date.
    # date_try = pd.to_datetime(date_try, errors="coerce", format="%Y %a %b %dth")
    date_try = data1["date"].iloc[0]
    data1.columns.values[0] = "Today"
    data1.reset_index(drop=True)
    data1["Today"] = data1["Today"].str.replace("AM", "AM ", regex=True)
    data1["Today"] = data1["Today"].str.replace("PM", "PM ", regex=True)
    data1["Time"] = data1["Today"].str.split().str[0]
    data1["datetime1"] = pd.to_datetime(
        date_try.strftime("%Y-%m-%d") + " " + data1["Time"]
    ) - timedelta(hours=5)

    data2 = df[1].copy()
    data2.columns.values[0] = "Today"
    data2.reset_index(drop=True)
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
    assert len(data) > 0


# no pbp test
