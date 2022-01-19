import os
import logging
import requests
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import praw
from bs4 import BeautifulSoup
from sqlalchemy import exc, create_engine
import boto3
from botocore.exceptions import ClientError
import twint
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

today = datetime.now().date()
todaytime = datetime.now()
yesterday = today - timedelta(1)
day = (datetime.now() - timedelta(1)).day
month = (datetime.now() - timedelta(1)).month
year = (datetime.now() - timedelta(1)).year
if datetime.now().date() < datetime(2022, 4, 11).date():
    season_type = "Regular Season"
else:
    season_type = "Playoffs"

# schemas
adv_stats_cols = [
    "index",
    "team",
    "age",
    "w",
    "l",
    "pw",
    "pl",
    "mov",
    "sos",
    "srs",
    "ortg",
    "drtg",
    "nrtg",
    "pace",
    "ftr",
    "3par",
    "ts%",
    "efg%",
    "tov%",
    "orb%",
    "ft/fga",
    "efg%_opp",
    "tov%_opp",
    "drb%_opp",
    "ft/fga_opp",
    "arena",
    "attendance",
    "att/game",
    "scrape_date",
]
boxscores_cols = [
    "player",
    "team",
    "location",
    "opponent",
    "outcome",
    "mp",
    "fgm",
    "fga",
    "fgpercent",
    "threepfgmade",
    "threepattempted",
    "threepointpercent",
    "ft",
    "fta",
    "ftpercent",
    "oreb",
    "dreb",
    "trb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
    "plusminus",
    "gmsc",
    "date",
    "type",
    "season",
]
injury_cols = ["player", "team", "date", "description", "scrape_date"]
opp_stats_cols = [
    "team",
    "fg_percent_opp",
    "threep_percent_opp",
    "threep_made_opp",
    "ppg_opp",
    "scrape_date",
]
pbp_cols = [
    "timequarter",
    "descriptionplayvisitor",
    "awayscore",
    "score",
    "homescore",
    "descriptionplayhome",
    "numberperiod",
    "hometeam",
    "awayteam",
    "scoreaway",
    "scorehome",
    "marginscore",
    "date",
]
reddit_cols = [
    "title",
    "score",
    "id",
    "url",
    "reddit_url",
    "num_comments",
    "body",
    "scrape_date",
    "scrape_time",
]
reddit_comment_cols = [
    "author",
    "comment",
    "score",
    "url",
    "flair1",
    "flair2",
    "edited",
    "scrape_date",
    "scrape_ts",
    "compound",
    "neg",
    "neu",
    "pos",
    "sentiment",
]
odds_cols = ["team", "spread", "total", "moneyline", "date", "datetime1"]
stats_cols = [
    "player",
    "pos",
    "age",
    "tm",
    "g",
    "gs",
    "mp",
    "fg",
    "fga",
    "fg%",
    "3p",
    "3pa",
    "3p%",
    "2p",
    "2pa",
    "2p%",
    "efg%",
    "ft",
    "fta",
    "ft%",
    "orb",
    "drb",
    "trb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
    "scrape_date",
]
transactions_cols = ["date", "transaction", "scrape_date"]
twitter_cols = [
    "created_at",
    "date",
    "username",
    "tweet",
    "language",
    "link",
    "likes_count",
    "retweets_count",
    "replies_count",
    "scrape_date",
    "scrape_ts",
    "compound",
    "neg",
    "neu",
    "pos",
    "sentiment",
]


def get_player_stats_data():
    """
    Web Scrape function w/ BS4 that grabs aggregate season stats
    Args:
        None
    Returns:
        Pandas DataFrame of Player Aggregate Season stats
    """
    try:
        year_stats = 2022
        url = "https://www.basketball-reference.com/leagues/NBA_{}_per_game.html".format(
            year_stats
        )
        html = requests.get(url).content
        soup = BeautifulSoup(html, "html.parser")

        headers = [th.getText() for th in soup.findAll("tr", limit=2)[0].findAll("th")]
        headers = headers[1:]

        rows = soup.findAll("tr")[1:]
        player_stats = [
            [td.getText() for td in rows[i].findAll("td")] for i in range(len(rows))
        ]

        stats = pd.DataFrame(player_stats, columns=headers)
        logging.info(
            f"General Stats Extraction Function Successful, retrieving {len(stats)} updated rows"
        )
        print(
            f"General Stats Extraction Function Successful, retrieving {len(stats)} updated rows"
        )
        return stats
    except BaseException as error:
        logging.info(f"General Stats Extraction Function Failed, {error}")
        print(f"General Stats Extraction Function Failed, {error}")
        df = []
        return df


def get_player_stats_transformed(df):
    """
    Web Scrape function w/ BS4 that transforms aggregate season stats
    Args:
        Raw Data Frame for Player Stats
    Returns:
        Pandas DataFrame of Player Aggregate Season stats
    """
    stats = df
    try:
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
        logging.info(
            f"General Stats Transformation Function Successful, retrieving {len(stats)} updated rows"
        )
        print(
            f"General Stats Transformation Function Successful, retrieving {len(stats)} updated rows"
        )
        return stats
    except BaseException as error:
        logging.info(f"General Stats Transformation Function Failed, {error}")
        print(f"General Stats Transformation Function Failed, {error}")
        df = []
        return df


def get_boxscores_data(month=month, day=day, year=year):
    """
    Function that grabs box scores from a given date in mmddyyyy format - defaults to yesterday.  values can be ex. 1 or 01

    Args:
        month (string) - month value of the game played (0 - 12)

        day (string) - day value of the game played (1 - 31)

        year (string) - year value of the game played (2021)

    Returns:
        Pandas DataFrame of Player Aggregate Season stats
    """
    url = f"https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={month}&day={day}&year={year}&type=all"

    try:
        html = requests.get(url).content
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
        return df
    except IndexError as error:
        logging.info(
            f"Box Score Extraction Function Failed, {error}, no data available for {year}-{month}-{day}"
        )
        print(
            f"Box Score Extraction Function Failed, {error}, no data available for {year}-{month}-{day}"
        )
        df = []
        return df      
    except BaseException as error:
        logging.info(
            f"Box Score Extraction Function Failed, {error}"
        )
        print(
            f"Box Score Extraction Function Failed, {error}"
        )
        df = []
        return df


def get_boxscores_transformed(df):
    try:
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
            .str.normalize("NFKD")  # this is removing all accented characters
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )
        df.columns = df.columns.str.lower()
        logging.info(
            f"Box Score Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        print(
            f"Box Score Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        return df
    except BaseException as error:
        logging.info(
            f"Box Score Function Failed, {error}, no data available for {year}-{month}-{day}"
        )
        print(
            f"Box Score Function Failed, {error}, no data available for {year}-{month}-{day}"
        )
        df = []
        return df


def get_opp_stats_data():
    """
    Web Scrape function w/ pandas read_html that grabs all current injuries

    Args:
        None

    Returns:
        Pandas DataFrame of all current player injuries & their associated team
    """
    try:
        url = "https://www.basketball-reference.com/leagues/NBA_2022.html"
        df = pd.read_html(url)[5]
        logging.info(
            f"Opp Stats Web Scrape Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        print(
            f"Opp Stats Web Scrape Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        return df
    except BaseException as error:
        logging.info(f"Opp Stats Web Scrape Function Failed, {error}")
        print(f"Opp Stats Web Scrape Function Failed, {error}")
        df = []
        return df


def get_opp_stats_transformed(df):
    try:
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
        logging.info(
            f"Opp Stats Transformation Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        print(
            f"Opp Stats Transformation Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        return df
    except BaseException as error:
        logging.info(f"Opp Stats Function Failed, {error}")
        print(f"Opp Stats Function Failed, {error}")
        df = []
        return df


def get_injuries_data():
    """
    Web Scrape function w/ pandas read_html that grabs all current injuries

    Args:
        None

    Returns:
        Pandas DataFrame of all current player injuries & their associated team
    """
    try:
        url = "https://www.basketball-reference.com/friv/injuries.fcgi"
        df = pd.read_html(url)[0]
        logging.info(
            f"Injury Web Scrape Function Successful, retrieving {len(df)} rows"
        )
        print(f"Injury Web Scrape Function Successful, retrieving {len(df)} rows")
        return df
    except BaseException as error:
        logging.info(f"Injury Web Scrape Function Failed, {error}")
        print(f"Injury Web Scrape Function Failed, {error}")
        df = []
        return df


def get_injuries_transformed(df):
    """
    Transformation Function for injuries function

    Args:
        Raw Injuries DataFrame

    Returns:
        Pandas DataFrame of all current player injuries & their associated team
    """
    try:
        df = df.rename(columns={"Update": "Date"})
        df.columns = df.columns.str.lower()
        df["scrape_date"] = datetime.now().date()
        logging.info(
            f"Injury Transformation Function Successful, retrieving {len(df)} rows"
        )
        print(f"Injury Transformation Function Successful, retrieving {len(df)} rows")
        return df
    except BaseException as error:
        logging.info(f"Injury Transformation Function Failed, {error}")
        print(f"Injury Transformation Function Failed, {error}")
        df = []
        return df


def get_transactions_data():
    """
    Web Scrape function w/ BS4 that retrieves NBA Trades, signings, waivers etc.

    Args:
        None

    Returns:
        Pandas DataFrame of all season transactions, trades, player waives etc.
    """
    try:
        url = "https://www.basketball-reference.com/leagues/NBA_2022_transactions.html"
        html = requests.get(url).content
        soup = BeautifulSoup(html, "html.parser")
        # theres a bunch of garbage in the first 50 rows - no matter what
        trs = soup.findAll("li")[70:]
        rows = []
        mylist = []
        for tr in trs:
            date = tr.find("span")
            # needed bc span can be null (multi <p> elements per span)
            if date is not None:
                date = date.text
            data = tr.findAll("p")
            for p in data:
                mylist.append(p.text)
            data3 = [date] + [mylist]
            rows.append(data3)
            mylist = []

        transactions = pd.DataFrame(rows)
        logging.info(
            f"Transactions Web Scrape Function Successful, retrieving {len(transactions)} rows"
        )
        print(
            f"Transactions Web Scrape Function Successful, retrieving {len(transactions)} rows"
        )
        return transactions
    except BaseException as error:
        logging.info(f"Transaction Web Scrape Function Failed, {error}")
        print(f"Transactions Web Scrape Failed, {error}")
        df = []
        return df


def get_transactions_transformed(df):
    """
    Transformation function for Transactions data

    Args:
        Raw Transactions DataFrame

    Returns:
        Pandas DataFrame of all Transactions data 
    """
    transactions = df
    try:
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
        logging.info(
            f"Transactions Transformation Function Successful, retrieving {len(transactions)} rows"
        )
        print(
            f"Transactions Transformation Function Successful, retrieving {len(transactions)} rows"
        )
        return transactions
    except BaseException as error:
        logging.info(f"Transaction Transformation Function Failed, {error}")
        print(f"Transactions Transformation Failed, {error}")
        df = []
        return df


def get_advanced_stats_data():
    """
    Web Scrape function w/ pandas read_html that grabs all team advanced stats

    Args:
        None

    Returns:
        Pandas DataFrame of all current Team Advanced Stats
    """
    try:
        url = "https://www.basketball-reference.com/leagues/NBA_2022.html"
        df = pd.read_html(url)
        df = pd.DataFrame(df[10])
        logging.info(
            f"Advanced Stats Web Scrape Function Successful, retrieving updated data for 30 Teams"
        )
        print(
            f"Advanced Stats Web Scrape Function Successful, retrieving updated data for 30 Teams"
        )
        return df
    except BaseException as error:
        logging.info(f"Advanced Stats Web Scrape Function Failed, {error}")
        print(f"Advanced Stats Web Scrape Function Failed, {error}")
        df = []
        return df


def get_advanced_stats_transformed(df):
    """
    Transformation function for Advanced Stats

    Args:
        Raw Advanced Stats DataFrame

    Returns:
        Pandas DataFrame of all Advanced Stats data 
    """
    try:
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
        df["scrape_date"] = datetime.now().date()
        df.columns = df.columns.str.lower()
        logging.info(
            f"Advanced Stats Transformation Function Successful, retrieving updated data for 30 Teams"
        )
        print(
            f"Advanced Stats Transformation Function Successful, retrieving updated data for 30 Teams"
        )
        return df
    except BaseException as error:
        logging.info(f"Advanced Stats Transformation Function Failed, {error}")
        print(f"Advanced Stats Transformation Function Failed, {error}")
        df = []
        return df


# leaving this as is for now
def get_odds_data():
    """
    Web Scrape function w/ pandas read_html that grabs current day's nba odds in raw format.
    There are 2 objects [0], [1] if the days are split into 2.
    AWS ECS operates in UTC time so the game start times are actually 6+ hours ahead of what they actually are

    Args:
        None
    Returns:
        Pandas DataFrame of NBA moneyline + spread odds for upcoming games for that day
    """
    try:
        url = "https://sportsbook.draftkings.com/leagues/basketball/88670846?category=game-lines&subcategory=game"
        df = pd.read_html(url)
        print(
            f"Odds Web Scrape Function Successful for {len(df)} day, retrieving {len(df)} day objects"
        )
        logging.info(
            f"Odds Web Scrape Function Successful {len(df)} day, retrieving {len(df)} day objects"
        )
        return df
    except (
        BaseException,
        ValueError,
    ) as error:  # valueerror fucked shit up apparently idfk
        print(f"Odds Function Failed for {len(df)} day, {error}")
        logging.info(f"Odds Function Failed {len(df)} day, {error}")
        df = []
        return df


# import pickle
# with open('tests/fixture_csvs/odds_data', 'wb') as fp:
#     pickle.dump(df, fp)


def get_odds_transformed(df):
    """
    Transformation function for Odds Data

    Args:
        Raw Odds DataFrame

    Returns:
        Pandas DataFrame of all Odds Data 
    """
    try:
        data1 = df[0].copy()
        data1.columns.values[0] = "Tomorrow"
        date_try = str(year) + " " + data1.columns[0]
        data1["date"] = np.where(
            date_try == "2022 Tomorrow",
            datetime.now().date(),  # if the above is true, then return this
            str(year) + " " + data1.columns[0],  # if false then return this
        )
        # )
        date_try = data1["date"].iloc[0]
        data1.reset_index(drop=True)
        data1["Tomorrow"] = data1["Tomorrow"].str.replace(
            "LA Clippers", "LAC Clippers", regex=True
        )

        data1["Tomorrow"] = data1["Tomorrow"].str.replace("AM", "AM ", regex=True)
        data1["Tomorrow"] = data1["Tomorrow"].str.replace("PM", "PM ", regex=True)
        data1["Time"] = data1["Tomorrow"].str.split().str[0]
        data1["datetime1"] = (
            pd.to_datetime(date_try.strftime("%Y-%m-%d") + " " + data1["Time"])
            - timedelta(hours=6)
            + timedelta(days=1)
        )
        if len(df) > 1:  # if more than 1 day's data appears then do this
            data2 = df[1].copy()
            data2.columns.values[0] = "Tomorrow"
            data2.reset_index(drop=True)
            data2["Tomorrow"] = data2["Tomorrow"].str.replace(
                "LA Clippers", "LAC Clippers", regex=True
            )
            data2["Tomorrow"] = data2["Tomorrow"].str.replace("AM", "AM ", regex=True)
            data2["Tomorrow"] = data2["Tomorrow"].str.replace("PM", "PM ", regex=True)
            data2["Time"] = data2["Tomorrow"].str.split().str[0]
            data2["datetime1"] = (
                pd.to_datetime(date_try.strftime("%Y-%m-%d") + " " + data2["Time"])
                - timedelta(hours=6)
                + timedelta(days=1)
            )
            data2["date"] = data2["datetime1"].dt.date

            data = data1.append(data2).reset_index(drop=True)
            data["SPREAD"] = data["SPREAD"].str[:-4]
            data["TOTAL"] = data["TOTAL"].str[:-4]
            data["TOTAL"] = data["TOTAL"].str[2:]
            data["Tomorrow"] = data["Tomorrow"].str.split().str[1:2]
            data["Tomorrow"] = pd.DataFrame(
                [
                    str(line).strip("[").strip("]").replace("'", "")
                    for line in data["Tomorrow"]
                ]
            )
            data["SPREAD"] = data["SPREAD"].str.replace("pk", "-1", regex=True)
            data["SPREAD"] = data["SPREAD"].str.replace("+", "", regex=True)
            data.columns = data.columns.str.lower()
            data = data[
                ["tomorrow", "spread", "total", "moneyline", "date", "datetime1"]
            ]
            data = data.rename(columns={data.columns[0]: "team"})
            data = data.query("date == date.min()")  # only grab games from upcoming day
            print(
                f"Odds Transformation Function Successful for {len(df)} day, retrieving {len(data)} rows"
            )
            logging.info(
                f"Odds Transformation Function Successful {len(df)} day, retrieving {len(data)} rows"
            )
            return data
        else:  # if there's only 1 day of data then just use that
            data = data1.reset_index(drop=True)
            data["SPREAD"] = data["SPREAD"].str[:-4]
            data["TOTAL"] = data["TOTAL"].str[:-4]
            data["TOTAL"] = data["TOTAL"].str[2:]
            data["Tomorrow"] = data["Tomorrow"].str.split().str[1:2]
            data["Tomorrow"] = pd.DataFrame(
                [
                    str(line).strip("[").strip("]").replace("'", "")
                    for line in data["Tomorrow"]
                ]
            )
            data["SPREAD"] = data["SPREAD"].str.replace("pk", "-1", regex=True)
            data["SPREAD"] = data["SPREAD"].str.replace("+", "", regex=True)
            data.columns = data.columns.str.lower()
            data = data[
                ["tomorrow", "spread", "total", "moneyline", "date", "datetime1"]
            ]
            data = data.rename(columns={data.columns[0]: "team"})
            data = data.query("date == date.min()")  # only grab games from upcoming day
            print(
                f"Odds Transformation Function Successful for {len(df)} day, retrieving {len(data)} rows"
            )
            logging.info(
                f"Odds Transformation Function Successful {len(df)} day, retrieving {len(data)} rows"
            )
            return data
    except BaseException as error:
        print(f"Odds Transformation Function Failed for {len(df)} day objects, {error}")
        logging.info(
            f"Odds Transformation Function Failed for {len(df)} day objects, {error}"
        )
        data = []
        return data


def get_reddit_data(sub):
    """
    Web Scrape function w/ PRAW that grabs top ~27 top posts from a given subreddit.
    Left sub as an argument in case I want to scrape multi subreddits in the future (r/nba, r/nbadiscussion, r/sportsbook etc)

    Args:
        sub (string) - subreddit to query

    Returns:
        Pandas DataFrame of all current top posts on r/nba
    """
    reddit = praw.Reddit(
        client_id=os.environ.get("reddit_accesskey"),
        client_secret=os.environ.get("reddit_secretkey"),
        user_agent="praw-app",
        username=os.environ.get("reddit_user"),
        password=os.environ.get("reddit_pw"),
    )
    try:
        subreddit = reddit.subreddit(sub)
        posts = []
        for post in subreddit.hot(limit=27):
            posts.append(
                [
                    post.title,
                    post.score,
                    post.id,
                    post.url,
                    str(f"https://www.reddit.com{post.permalink}"),
                    post.num_comments,
                    post.selftext,
                    today,
                    todaytime,
                ]
            )
        posts = pd.DataFrame(
            posts,
            columns=[
                "title",
                "score",
                "id",
                "url",
                "reddit_url",
                "num_comments",
                "body",
                "scrape_date",
                "scrape_time",
            ],
        )
        posts.columns = posts.columns.str.lower()

        print(
            f"Reddit Scrape Successful, grabbing 27 Recent popular posts from r/{sub} subreddit"
        )
        logging.info(
            f"Reddit Scrape Successful, grabbing 27 Recent popular posts from r/{sub} subreddit"
        )
        return posts
    except BaseException as error:
        logging.info(f"Reddit Scrape Function Failed, {error}")
        print(f"Reddit Scrape Function Failed, {error}")
        data = []
        return data


def get_reddit_comments(urls):
    """
    Web Scrape function w/ PRAW that iteratively extracts comments from provided reddit post urls

    Args:
        urls (Pandas Series) - The (reddit) urls to extract comments from

    Returns:
        Pandas DataFrame of all comments from the provided reddit urls
    """
    reddit = praw.Reddit(
        client_id=os.environ.get("reddit_accesskey"),
        client_secret=os.environ.get("reddit_secretkey"),
        user_agent="praw-app",
        username=os.environ.get("reddit_user"),
        password=os.environ.get("reddit_pw"),
    )
    author_list = []
    comment_list = []
    score_list = []
    flair_list1 = []
    flair_list2 = []
    edited_list = []
    url_list = []

    try:
        for i in urls:
            submission = reddit.submission(url=i)
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list():
                author_list.append(comment.author)
                comment_list.append(comment.body)
                score_list.append(comment.score)
                flair_list1.append(comment.author_flair_css_class)
                flair_list2.append(comment.author_flair_text)
                edited_list.append(comment.edited)
                url_list.append(i)

        df = pd.DataFrame(
            {
                "author": author_list,
                "comment": comment_list,
                "score": score_list,
                "url": url_list,
                "flair1": flair_list1,
                "flair2": flair_list2,
                "edited": edited_list,
                "scrape_date": datetime.now().date(),
                "scrape_ts": datetime.now(),
            }
        )

        df = df.astype({"author": str})
        # adding sentiment analysis columns
        analyzer = SentimentIntensityAnalyzer()
        df["compound"] = [
            analyzer.polarity_scores(x)["compound"] for x in df["comment"]
        ]
        df["neg"] = [analyzer.polarity_scores(x)["neg"] for x in df["comment"]]
        df["neu"] = [analyzer.polarity_scores(x)["neu"] for x in df["comment"]]
        df["pos"] = [analyzer.polarity_scores(x)["pos"] for x in df["comment"]]
        df["sentiment"] = np.where(df["compound"] > 0, 1, 0)

        print(
            f"Reddit Comment Extraction Success, retrieving {len(df)} total comments from {len(urls)} total urls"
        )
        logging.info(
            f"Reddit Comment Extraction Success, retrieving {len(df)} total comments from {len(urls)} total urls"
        )
        return df
    except BaseException as e:
        print(f"Reddit Comment Extraction Failed for url {i}, {e}")
        logging.info(f"Reddit Comment Extraction Failed for url {i}, {e}")
        df = []
        return df


# adding this in as of 2021-01-15
def scrape_tweets(search_term: str):
    try:
        c = twint.Config()
        c.Search = search_term
        c.Limit = 2500  # number of Tweets to scrape
        c.Store_csv = True  # store tweets in a csv file
        c.Output = f"{search_term}_tweets.csv"  # path to csv file
        c.Hide_output = True

        twint.run.Search(c)
        df = pd.read_csv(f"{search_term}_tweets.csv")
        df = df[
            [
                "id",
                "created_at",
                "date",
                "username",
                "tweet",
                "language",
                "link",
                "likes_count",
                "retweets_count",
                "replies_count",
            ]
        ].drop_duplicates()
        df["scrape_date"] = datetime.now().date()
        df["scrape_ts"] = datetime.now()
        df = df.query('language=="en"').groupby("id").agg("last")

        analyzer = SentimentIntensityAnalyzer()
        df["compound"] = [analyzer.polarity_scores(x)["compound"] for x in df["tweet"]]
        df["neg"] = [analyzer.polarity_scores(x)["neg"] for x in df["tweet"]]
        df["neu"] = [analyzer.polarity_scores(x)["neu"] for x in df["tweet"]]
        df["pos"] = [analyzer.polarity_scores(x)["pos"] for x in df["tweet"]]
        df["sentiment"] = np.where(df["compound"] > 0, 1, 0)
        print(f"Twitter Tweet Extraction Success, retrieving {len(df)} total tweets")
        logging.info(
            f"Twitter Tweet Extraction Success, retrieving {len(df)} total tweets"
        )
        return df
    except BaseException as e:
        print(f"Twitter Tweet Extraction Failed, {e}")
        logging.info(f"Twitter Tweet Extraction Failed, {e}")
        df = []
        return df


def get_pbp_data_transformed(df):
    """
    Web Scrape function w/ pandas read_html that uses aliases via boxscores function
    to scrape the pbp data interactively for each game played the previous day

    Args:
        df (Pandas DataFrame) - the DataFrame result from running the boxscores function.

    Returns:
        All PBP Data for the games in the input df

    """
    try:
        if len(df) > 0:
            yesterday_hometeams = (
                df.query('location == "H"')[["team"]].drop_duplicates().dropna()
            )
            yesterday_hometeams["team"] = yesterday_hometeams["team"].str.replace(
                "PHX", "PHO"
            )
            yesterday_hometeams["team"] = yesterday_hometeams["team"].str.replace(
                "CHA", "CHO"
            )
            yesterday_hometeams["team"] = yesterday_hometeams["team"].str.replace(
                "BKN", "BRK"
            )

            away_teams = (
                df.query('location == "A"')[["team", "opponent"]]
                .drop_duplicates()
                .dropna()
            )
            away_teams = away_teams.rename(
                columns={
                    away_teams.columns[0]: "AwayTeam",
                    away_teams.columns[1]: "HomeTeam",
                }
            )
        else:
            yesterday_hometeams = []

        if len(yesterday_hometeams) > 0:
            try:
                newdate = str(
                    df["date"].drop_duplicates()[0].date()
                )  # this assumes all games in the boxscores df are 1 date
                newdate = pd.to_datetime(newdate).strftime(
                    "%Y%m%d"
                )  # formatting into url format.
                pbp_list = pd.DataFrame()
                for i in yesterday_hometeams["team"]:
                    url = "https://www.basketball-reference.com/boxscores/pbp/{}0{}.html".format(
                        newdate, i
                    )
                    df = pd.read_html(url)[0]
                    df.columns = df.columns.map("".join)
                    df = df.rename(
                        columns={
                            df.columns[0]: "Time",
                            df.columns[1]: "descriptionPlayVisitor",
                            df.columns[2]: "AwayScore",
                            df.columns[3]: "Score",
                            df.columns[4]: "HomeScore",
                            df.columns[5]: "descriptionPlayHome",
                        }
                    )
                    conditions = [
                        (
                            df["HomeScore"].str.contains("Jump ball:", na=False)
                            & df["Time"].str.contains("12:00.0")
                        ),
                        (
                            df["HomeScore"].str.contains(
                                "Start of 2nd quarter", na=False
                            )
                        ),
                        (
                            df["HomeScore"].str.contains(
                                "Start of 3rd quarter", na=False
                            )
                        ),
                        (
                            df["HomeScore"].str.contains(
                                "Start of 4th quarter", na=False
                            )
                        ),
                        (
                            df["HomeScore"].str.contains(
                                "Start of 1st overtime", na=False
                            )
                        ),
                        (
                            df["HomeScore"].str.contains(
                                "Start of 2nd overtime", na=False
                            )
                        ),
                        (
                            df["HomeScore"].str.contains(
                                "Start of 3rd overtime", na=False
                            )
                        ),
                        (
                            df["HomeScore"].str.contains(
                                "Start of 4th overtime", na=False
                            )
                        ),  # if more than 4 ots then rip
                    ]
                    values = [
                        "1st Quarter",
                        "2nd Quarter",
                        "3rd Quarter",
                        "4th Quarter",
                        "1st OT",
                        "2nd OT",
                        "3rd OT",
                        "4th OT",
                    ]
                    df["Quarter"] = np.select(conditions, values, default=None)
                    df["Quarter"] = df["Quarter"].fillna(method="ffill")
                    df = df.query(
                        'Time != "Time" & Time != "2nd Q" & Time != "3rd Q" & Time != "4th Q" & Time != "1st OT" & Time != "2nd OT" & Time != "3rd OT" & Time != "4th OT"'
                    ).copy()  # use COPY to get rid of the fucking goddamn warning bc we filtered stuf out
                    # anytime you filter out values w/o copying and run code like the lines below it'll throw a warning.
                    df["HomeTeam"] = i
                    df["HomeTeam"] = df["HomeTeam"].str.replace("PHO", "PHX")
                    df["HomeTeam"] = df["HomeTeam"].str.replace("CHO", "CHA")
                    df["HomeTeam"] = df["HomeTeam"].str.replace("BRK", "BKN")
                    df = df.merge(away_teams)
                    df[["scoreAway", "scoreHome"]] = df["Score"].str.split(
                        "-", expand=True, n=1
                    )
                    df["scoreAway"] = pd.to_numeric(df["scoreAway"], errors="coerce")
                    df["scoreAway"] = df["scoreAway"].fillna(method="ffill")
                    df["scoreAway"] = df["scoreAway"].fillna(0)
                    df["scoreHome"] = pd.to_numeric(df["scoreHome"], errors="coerce")
                    df["scoreHome"] = df["scoreHome"].fillna(method="ffill")
                    df["scoreHome"] = df["scoreHome"].fillna(0)
                    df["marginScore"] = df["scoreHome"] - df["scoreAway"]
                    df["Date"] = yesterday
                    df = df.rename(
                        columns={
                            df.columns[0]: "timeQuarter",
                            df.columns[6]: "numberPeriod",
                        }
                    )
                    pbp_list = pbp_list.append(df)
                    df = pd.DataFrame()
                pbp_list.columns = pbp_list.columns.str.lower()
                pbp_list = pbp_list.query(
                    "(awayscore.notnull()) | (homescore.notnull())", engine="python"
                )
                logging.info(
                    f"PBP Data Transformation Function Successful, retrieving {len(pbp_list)} rows for {year}-{month}-{day}"
                )
                print(
                    f"PBP Data Transformation Function Successful, retrieving {len(pbp_list)} rows for {year}-{month}-{day}"
                )
                # filtering only scoring plays here, keep other all other rows in future for lineups stuff etc.
                return pbp_list
            except BaseException as error:
                logging.info(f"PBP Data Transformation Logic Failed, {error}")
                print(f"PBP Data Transformation Logic Failed, {error}")
                df = []
                return df
        else:
            df = []
            logging.info("PBP Function No Data Yesterday")
            print("PBP Function No Data Yesterday")
            return df
    except BaseException as error:
        logging.info(f"PBP Data Transformation Function Failed, {error}")
        print(f"PBP Data Transformation Function Failed, {error}")
        data = []
        return data


def sql_connection(schema="nba_source"):
    """
    SQL Connection function connecting to my postgres db with schema = nba_source where initial data in ELT lands

    Args:
        None

    Returns:
        SQL Connection variable to schema: nba_source in my PostgreSQL DB
    """
    RDS_USER = os.environ.get("RDS_USER")
    RDS_PW = os.environ.get("RDS_PW")
    RDS_IP = os.environ.get("IP")
    RDS_DB = os.environ.get("RDS_DB")
    try:
        connection = create_engine(
            f"postgresql+psycopg2://{RDS_USER}:{RDS_PW}@{RDS_IP}:5432/{RDS_DB}",
            connect_args={"options": f"-csearch_path={schema}"},
            # defining schema to connect to
            echo=False,
        )
        logging.info(f"SQL Connection to schema: {schema} Successful")
        print(f"SQL Connection to schema: {schema} Successful")
        return connection
    except exc.SQLAlchemyError as e:
        logging.info(f"SQL Connection to schema: {schema} Failed, Error: {e}")
        print(f"SQL Connection to schema: {schema} Failed, Error: {e}")
        return e


def send_aws_email(logs):
    """
    Email function utilizing boto3, has to be set up with SES in AWS and env variables passed in via Terraform.

    Args:
        None

    Returns:
        Sends an email out upon every script execution, including errors (if any)
    """
    sender = os.environ.get("USER_EMAIL")
    recipient = os.environ.get("USER_EMAIL")
    aws_region = "us-east-1"
    subject = f"NBA ELT PIPELINE - {str(len(logs))} Alert Fails for {str(today)}"
    body_html = message = """\
<h3>Errors:</h3>
                   {}""".format(
        logs.to_html()
    )

    charset = "UTF-8"
    client = boto3.client("ses", region_name=aws_region)
    try:
        response = client.send_email(
            Destination={"ToAddresses": [recipient,],},
            Message={
                "Body": {
                    "Html": {"Charset": charset, "Data": body_html,},
                    "Text": {"Charset": charset, "Data": body_html,},
                },
                "Subject": {"Charset": charset, "Data": subject,},
            },
            Source=sender,
        )
    except ClientError as e:
        print(e.response["Error"]["Message"])
    else:
        print("Email sent! Message ID:"),
        print(response["MessageId"])


def execute_email_function(logs):
    """
    Email function that executes the email function upon script finishing.

    Args:
        None

    Returns:
        Holds the actual send_email logic and executes if invoked as a script (aka on ECS)
    """
    try:
        if len(logs) > 0:
            print("Sending Email")
            send_aws_email(logs)
        elif len(logs) == 0:
            print("No Errors!")
            send_aws_email(logs)
    except BaseException as error:
        logging.info(f"Failed Email Alert, {error}")
        print(f"Failed Email Alert, {error}")
