from datetime import date, datetime, timedelta
import hashlib
import json
import logging
import os
import re
import time
from typing import Any, Callable

import awswrangler as wr
from bs4 import BeautifulSoup
from nltk.sentiment import SentimentIntensityAnalyzer
import numpy as np
import pandas as pd
import praw
import requests
from sqlalchemy.engine.base import Connection, Engine
import sentry_sdk

# import tweepy

sentry_sdk.init(os.environ.get("SENTRY_TOKEN"), traces_sample_rate=1.0)
sentry_sdk.set_user({"email": "jyablonski9@gmail.com"})


def time_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator function used to record the execution time of any
    function it's applied to.

    Args:
        func (Callable): Function to track the execution time on.

    Returns:
        Callable[..., Any]: The wrapped function that records
            the execution time.
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        total_func_time = round(time.time() - start_time, 2)
        logging.info(f"{func.__name__} took {total_func_time} seconds")

        return result

    return wrapper


def filter_spread(value: str) -> str:
    """
    Filter out 3-digit values from the `spread` column
    in the Scrape Odds Function such as `-108` or `-112`

    Parameters:
        value (str): The original value from the spread column.

    Returns:
        The spread value without any 3-digit values present
    """
    parts = value.split()
    filtered_parts = [
        (
            part
            if (part[0] in ["+", "-"] and float(part[1:]) <= 25)
            or (part.isdigit() and int(part) <= 25)
            else ""
        )
        for part in parts
    ]
    result = " ".join(filtered_parts).strip()

    # this last part strips out a couple extra white spaces
    return re.sub(r"\s+", " ", result)


def get_season_type(todays_date: date | None = None) -> str:
    """
    Function to generate Season Type for a given Date.
    **2025-03-16 NOTE** this has been deprecated as this logic
    belongs in the dbt project

    Args:
        todays_date (date): The Date to generate a Season Type for.  Defaults to
            today's date.

    Returns:
        The Season Type for Given Date
    """
    if todays_date is None:
        todays_date = datetime.now().date()

    if todays_date < datetime(2025, 4, 15).date():
        season_type = "Regular Season"
    elif (todays_date >= datetime(2025, 4, 16).date()) & (
        todays_date < datetime(2025, 4, 21).date()
    ):
        season_type = "Play-In"
    else:
        season_type = "Playoffs"

    return season_type


def check_schedule(date: datetime.date) -> bool:
    """
    Small Function used in Boxscores + PBP Functions to check if
    there are any games scheduled for a given date.

    Args:
        date (datetime.date): The Date to check for games on.

    Returns:
        Boolean: True if there are games scheduled, False if not.
    """
    schedule_endpoint = f"https://api.jyablonski.dev/schedule?date={date}"
    schedule_data = requests.get(schedule_endpoint).json()

    return True if len(schedule_data) > 0 else False


def add_sentiment_analysis(df: pd.DataFrame, sentiment_col: str) -> pd.DataFrame:
    """
    Function to add Sentiment Analysis columns to a DataFrame via nltk Vader Lexicon.

    Args:
        df (pd.DataFrame): The Pandas DataFrame

        sentiment_col (str): The Column in the DataFrame to run Sentiment Analysis on
            (comments / tweets etc).

    Returns:
        The same DataFrame but with the Sentiment Analysis columns attached.
    """
    try:
        analyzer = SentimentIntensityAnalyzer()
        df["compound"] = [
            analyzer.polarity_scores(x)["compound"] for x in df[sentiment_col]
        ]
        df["neg"] = [analyzer.polarity_scores(x)["neg"] for x in df[sentiment_col]]
        df["neu"] = [analyzer.polarity_scores(x)["neu"] for x in df[sentiment_col]]
        df["pos"] = [analyzer.polarity_scores(x)["pos"] for x in df[sentiment_col]]
        df["sentiment"] = np.where(df["compound"] > 0, 1, 0)
        return df
    except Exception as e:
        logging.error(f"Error Occurred while adding Sentiment Analysis, {e}")
        sentry_sdk.capture_exception(e)
        raise e


def get_leading_zeroes(value: int) -> str:
    """
    Function to add leading zeroes to a month (1 (January) -> 01).
    Used in the the `write_to_s3` function.

    Args:
        value (int): The value integer (created from `datetime.now().month`)

    Returns:
        The same value integer with a leading 0 if it is less than 10
            (Nov/Dec aka 11/12 unaffected).
    """
    if len(str(value)) > 1:
        return str(value)
    else:
        return f"0{value}"


def clean_player_names(name: str) -> str:
    """
    Function to remove suffixes from a player name.

    Args:
        name (str): The raw player name you wish to alter.

    Returns:
        str: Cleaned Name w/ no suffix bs
    """
    try:
        cleaned_name = (
            name.replace(" Jr.", "")
            .replace(" Sr.", "")
            .replace(" III", "")  # III HAS TO GO FIRST, OVER II
            .replace(" II", "")  # or else Robert Williams III -> Robert WilliamsI
            .replace(" IV", "")
        )
        return cleaned_name
    except Exception as e:
        logging.error(f"Error Occurred with Clean Player Names, {e}")
        sentry_sdk.capture_exception(e)
        raise e


@time_function
def get_player_stats_data(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Web Scrape function w/ BS4 that grabs aggregate season stats

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to
            check whether to run this function or not

    Returns:
        DataFrame of Player Aggregate Season stats
    """
    feature_flag = "stats"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    # stats = stats.rename(columns={"fg%": "fg_pct", "3p%": "3p_pct",
    # "2p%": "2p_pct", "efg%": "efg_pct", "ft%": "ft_pct"})
    try:
        year_stats = 2025
        url = f"https://www.basketball-reference.com/leagues/NBA_{year_stats}_per_game.html"
        html = requests.get(url).content
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
        stats["scrape_date"] = datetime.now().date()
        stats = stats.drop(columns=["index", "awards"], axis=1)
        logging.info(
            "General Stats Transformation Function Successful, "
            f"retrieving {len(stats)} updated rows"
        )
        return stats
    except Exception as error:
        logging.error(f"General Stats Extraction Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        df = pd.DataFrame()
        return df


@time_function
def get_boxscores_data(
    feature_flags_df: pd.DataFrame,
    month: int = (datetime.now() - timedelta(1)).month,
    day: int = (datetime.now() - timedelta(1)).day,
    year: int = (datetime.now() - timedelta(1)).year,
) -> pd.DataFrame:
    """
    Function that grabs box scores from a given date in mmddyyyy
    format - defaults to yesterday.  values can be ex. 1 or 01.
    Can't use `read_html` for this so this is raw web scraping baby.

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to
            check whether to run this function or not

        month (int): month value of the game played (0 - 12)

        day (int): day value of the game played (1 - 31)

        year (int): year value of the game played (2021)

    Returns:
        DataFrame of Player Aggregate Season stats
    """
    day = get_leading_zeroes(value=day)
    month = get_leading_zeroes(value=month)

    feature_flag = "boxscores"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    url = f"https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={month}&day={day}&year={year}&type=all"
    date = f"{year}-{month}-{day}"

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
        df["scrape_date"] = datetime.now().date()
        df.columns = df.columns.str.lower()
        logging.info(
            "Box Score Transformation Function Successful, "
            f"retrieving {len(df)} rows for {date}"
        )
        return df
    except IndexError:

        # if no boxscores available, check the schedule. this will log an error
        # if there are games played and the data isnt available yet, or log a
        # message that no games were found bc there were no games played on that date
        is_games_played = check_schedule(date=date)

        if is_games_played:
            logging.error(
                "Box Scores Function Failed, Box Scores aren't available yet "
                f"for {date}"
            )
        else:
            logging.info(
                f"Box Scores Function Warning, no games played on {date} so "
                "no data available"
            )

        return pd.DataFrame()

    except Exception as error:
        logging.error(f"Box Scores Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        df = pd.DataFrame()
        return df


@time_function
def get_opp_stats_data(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Web Scrape function w/ pandas read_html that grabs all
        regular season opponent team stats

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame
            to check whether to run this function or not

    Returns:
        Pandas DataFrame of all current team opponent stats
    """
    feature_flag = "opp_stats"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    year = (datetime.now() - timedelta(1)).year
    month = (datetime.now() - timedelta(1)).month
    day = (datetime.now() - timedelta(1)).day
    year_stats = 2025

    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year_stats}.html"
        df = pd.read_html(url)[5]
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
            "Opp Stats Transformation Function Successful, "
            f"retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        return df
    except Exception as error:
        logging.error(f"Opp Stats Web Scrape Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        df = pd.DataFrame()
        return df


@time_function
def get_injuries_data(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Web Scrape function w/ pandas read_html that grabs all current injuries

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check
            whether to run this function or not

    Returns:
        Pandas DataFrame of all current player injuries & their associated team
    """
    feature_flag = "injuries"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    try:
        url = "https://www.basketball-reference.com/friv/injuries.fcgi"
        df = pd.read_html(url)[0]
        df = df.rename(columns={"Update": "Date"})
        df.columns = df.columns.str.lower()
        df["scrape_date"] = datetime.now().date()
        df["player"] = (
            df["player"]
            .str.normalize("NFKD")  # this is removing all accented characters
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )
        df["player"] = df["player"].apply(clean_player_names)
        df = df.drop_duplicates()
        logging.info(
            f"Injury Transformation Function Successful, retrieving {len(df)} rows"
        )
        return df
    except Exception as error:
        logging.error(f"Injury Web Scrape Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        df = pd.DataFrame()
        return df


@time_function
def get_transactions_data(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Web Scrape function w/ BS4 that retrieves NBA Trades, signings, waivers etc.

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
            to run this function or not

    Returns:
        Pandas DataFrame of all season transactions, trades, player waives etc.
    """
    feature_flag = "transactions"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    try:
        url = "https://www.basketball-reference.com/leagues/NBA_2025_transactions.html"
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
        transactions.columns = ["Date", "Transaction"]
        transactions = transactions.query(
            'Date == Date & Date != ""'
        ).reset_index()  # filters out nulls and empty values
        transactions = transactions.explode("Transaction")
        transactions["Date"] = transactions["Date"].str.replace(
            "\\?", "October 1, 2024", regex=True  # bad data 10-14-21
        )
        transactions["Date"] = pd.to_datetime(transactions["Date"])
        transactions.columns = transactions.columns.str.lower()
        transactions = transactions[["date", "transaction"]]
        transactions["scrape_date"] = datetime.now().date()
        transactions = transactions.drop_duplicates()
        logging.info(
            "Transactions Transformation Function Successful, "
            f"retrieving {len(transactions)} rows"
        )
        return transactions
    except Exception as error:
        logging.error(f"Transaction Web Scrape Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        df = pd.DataFrame()
        return df


@time_function
def get_advanced_stats_data(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Web Scrape function w/ pandas read_html that grabs all team advanced stats

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check
            whether to run this function or not

    Returns:
        DataFrame of all current Team Advanced Stats
    """
    feature_flag = "adv_stats"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    year_stats = 2025
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year_stats}.html"
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
            "bby1",  # the bby columns are because of hierarchical html formatting
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
        df["Team"] = df["Team"].str.replace("\\*", "", regex=True)
        df["scrape_date"] = datetime.now().date()
        df.columns = df.columns.str.lower()
        logging.info(
            """
            Advanced Stats Transformation Function Successful,
            retrieving updated data for 30 Teams
            """
        )
        return df
    except Exception as error:
        logging.error(f"Advanced Stats Web Scrape Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        df = pd.DataFrame()
        return df


@time_function
def get_shooting_stats_data(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Web Scrape function w/ pandas read_html that grabs all raw shooting stats

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
            to run this function or not

    Returns:
        DataFrame of raw shooting stats
    """
    feature_flag = "shooting_stats"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    year_stats = 2025
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year_stats}_shooting.html"
        df = pd.read_html(url)[0]
        df.columns = df.columns.to_flat_index()
        df = df.rename(
            columns={
                df.columns[1]: "player",
                df.columns[6]: "mp",
                df.columns[8]: "avg_shot_distance",
                df.columns[10]: "pct_fga_2p",
                df.columns[11]: "pct_fga_0_3",
                df.columns[12]: "pct_fga_3_10",
                df.columns[13]: "pct_fga_10_16",
                df.columns[14]: "pct_fga_16_3p",
                df.columns[15]: "pct_fga_3p",
                df.columns[17]: "fg_pct_0_3",
                df.columns[18]: "fg_pct_3_10",
                df.columns[19]: "fg_pct_10_16",
                df.columns[20]: "fg_pct_16_3p",
                df.columns[22]: "pct_2pfg_ast",
                df.columns[23]: "pct_3pfg_ast",
                df.columns[24]: "dunk_pct_tot_fg",
                df.columns[25]: "dunks",
                df.columns[26]: "corner_3_ast_pct",
                df.columns[27]: "corner_3pm_pct",
                df.columns[28]: "heaves_att",
                df.columns[29]: "heaves_makes",
            }
        )[
            [
                "player",
                "mp",
                "avg_shot_distance",
                "pct_fga_2p",
                "pct_fga_0_3",
                "pct_fga_3_10",
                "pct_fga_10_16",
                "pct_fga_16_3p",
                "pct_fga_3p",
                "fg_pct_0_3",
                "fg_pct_3_10",
                "fg_pct_10_16",
                "fg_pct_16_3p",
                "pct_2pfg_ast",
                "pct_3pfg_ast",
                "dunk_pct_tot_fg",
                "dunks",
                "corner_3_ast_pct",
                "corner_3pm_pct",
                "heaves_att",
                "heaves_makes",
            ]
        ]
        df = df.query('player != "Player"').copy()
        df["mp"] = pd.to_numeric(df["mp"])
        df = (
            df.sort_values(["mp"], ascending=False)
            .groupby("player")
            .first()
            .reset_index()
            .drop("mp", axis=1)
        )
        df["player"] = (
            df["player"]
            .str.normalize("NFKD")  # this is removing all accented characters
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )
        df["player"] = df["player"].apply(clean_player_names)
        df["scrape_date"] = datetime.now().date()
        df["scrape_ts"] = datetime.now()
        logging.info(
            "Shooting Stats Transformation Function Successful, "
            f"retrieving {len(df)} rows"
        )
        return df
    except Exception as error:
        logging.error(f"Shooting Stats Web Scrape Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        df = pd.DataFrame()
        return df


@time_function
def scrape_odds(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to web scrape Gambling Odds from cover.com

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
            to run this function or not

    Returns:
        DataFrame of Gambling Odds for Today's Games
    """
    feature_flag = "odds"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    try:
        url = "https://www.covers.com/sport/basketball/nba/odds"
        df = pd.read_html(url)
        odds = df[0]
        odds["spread"] = df[3].iloc[:, 4]  # 5th column in df[3]
        # Select columns by index: First column (index 0),
        # 5th column (index 4), and 'spread'
        odds = odds.iloc[:, [0, 4, -1]]
        # Rename the selected columns
        odds = odds.rename(
            columns={
                odds.columns[0]: "datetime1",  # Rename first column
                odds.columns[1]: "moneyline",  # Rename second column
            }
        )
        # filter out any records not from today
        odds = odds.query(
            "datetime1 != 'FINAL' and datetime1 == datetime1 and datetime1.str.contains('Today')",
            engine="python",
        ).copy()
        # PK is a pick em game, so we'll set the spread to -1.0
        odds["spread"] = odds["spread"].str.replace("PK", "-1.0")
        if len(odds) == 0:
            logging.info("No Odds Records available for today's games")
            return []

        odds["spread"] = odds["spread"].apply(filter_spread)
        odds["spread"] = odds["spread"].apply(lambda x: " ".join(x.split()))
        odds["datetime1"] = odds["datetime1"].str.replace("Today, ", "")
        odds_final = odds[["datetime1", "spread", "moneyline"]].copy()

        # \b: Word boundary anchor, ensures that the match occurs at a word boundary.
        # (: Start of a capturing group.
        # [A-Z]: Character class matching any uppercase letter from 'A' to 'Z'.
        # {2,3}: Quantifier specifying that the preceding character class [A-Z]
        #       should appear 2 to 3 times.
        # ): End of the capturing group.
        # \b: Word boundary anchor, again ensuring that the match occurs at a word boundary.

        pattern = r"\b([A-Z]{2,3})\b"

        odds_final["team"] = (
            odds_final["datetime1"]
            .str.extractall(pattern)
            .unstack()
            .apply(lambda x: " ".join(x.dropna()), axis=1)
        )

        # turning the space separated elements in a list, then exploding that list
        odds_final["team"] = odds_final["team"].str.split(" ", n=1, expand=False)
        odds_final["spread"] = odds_final["spread"].str.split(" ", n=1, expand=False)
        odds_final["moneyline"] = odds_final["moneyline"].str.split(
            " ", n=1, expand=False
        )
        odds_final = odds_final.explode(["team", "spread", "moneyline"]).reset_index()
        odds_final = odds_final.drop("index", axis=1)
        odds_final["date"] = datetime.now().date()
        odds_final["spread"] = odds_final[
            "spread"
        ].str.strip()  # strip trailing and leading spaces
        odds_final["moneyline"] = odds_final["moneyline"].str.strip()
        odds_final["time"] = odds_final["datetime1"].str.split().str[1]
        odds_final["datetime1"] = pd.to_datetime(
            (datetime.now().date().strftime("%Y-%m-%d") + " " + odds_final["time"]),
            format="%Y-%m-%d %H:%M",
        )

        odds_final["total"] = 200
        odds_final["team"] = odds_final["team"].str.replace("BK", "BKN")
        odds_final["moneyline"] = odds_final["moneyline"].str.replace(
            "\\+", "", regex=True
        )
        odds_final["moneyline"] = odds_final["moneyline"].astype("int")
        odds_final = odds_final[
            ["team", "spread", "total", "moneyline", "date", "datetime1"]
        ]
        logging.info(
            f"Odds Scrape Successful, returning {len(odds_final)} records "
            f"from {len(odds_final) // 2} games Today"
        )
        return odds_final
    except Exception as e:
        logging.error(f"Odds Function Web Scrape Failed, {e}")
        sentry_sdk.capture_exception(e)
        df = pd.DataFrame()
        return df


# def get_odds_data() -> pd.DataFrame:
#     """
#     *********** DEPRECATED AS OF 2022-10-19 ***********

#     Web Scrape function w/ pandas read_html that grabs current day's
#         nba odds in raw format. There are 2 objects [0], [1] if the days
#         are split into 2.  AWS ECS operates in UTC time so the game start
#         times are actually 5-6+ hours ahead of what they actually are, so
#         there are 2 html tables.

#     Args:
#         None

#     Returns:
#         Pandas DataFrame of NBA moneyline + spread odds for upcoming games
#    for that day
#     """
#     year = (datetime.now() - timedelta(1)).year

#     try:
#         url = "https://sportsbook.draftkings.com/leagues/basketball/nba"
#         df = pd.read_html(url)
#         if len(df) == 0:
#             logging.info("Odds Transformation Failed, no Odds Data available.")
#             df = pd.DataFrame()
#             return df
#         else:
#             try:
#                 data1 = df[0].copy()
#                 data1.columns.values[0] = "Tomorrow"
#                 date_try = str(year) + " " + data1.columns[0]
#                 data1["date"] = np.where(
#                     date_try == "2022 Tomorrow",
#                     datetime.now().date(),  # if the above is true, then return this
#                     str(year) + " " + data1.columns[0],  # if false then return this
#                 )
#                 # )
#                 date_try = data1["date"].iloc[0]
#                 data1.reset_index(drop=True)
#                 data1["Tomorrow"] = data1["Tomorrow"].str.replace(
#                     "LA Clippers", "LAC Clippers", regex=True
#                 )

#                 data1["Tomorrow"] = data1["Tomorrow"].str.replace(
#                     "AM", "AM ", regex=True
#                 )
#                 data1["Tomorrow"] = data1["Tomorrow"].str.replace(
#                     "PM", "PM ", regex=True
#                 )
#                 data1["Time"] = data1["Tomorrow"].str.split().str[0]
#                 data1["datetime1"] = (
#                     pd.to_datetime(date_try.strftime("%Y-%m-%d") + " "
#                                   + data1["Time"])
#                     - timedelta(hours=6)
#                     + timedelta(days=1)
#                 )
#                 if len(df) > 1:  # if more than 1 day's data appears then do this
#                     data2 = df[1].copy()
#                     data2.columns.values[0] = "Tomorrow"
#                     data2.reset_index(drop=True)
#                     data2["Tomorrow"] = data2["Tomorrow"].str.replace(
#                         "LA Clippers", "LAC Clippers", regex=True
#                     )
#                     data2["Tomorrow"] = data2["Tomorrow"].str.replace(
#                         "AM", "AM ", regex=True
#                     )
#                     data2["Tomorrow"] = data2["Tomorrow"].str.replace(
#                         "PM", "PM ", regex=True
#                     )
#                     data2["Time"] = data2["Tomorrow"].str.split().str[0]
#                     data2["datetime1"] = (
#                         pd.to_datetime(
#                             date_try.strftime("%Y-%m-%d") + " " + data2["Time"]
#                         )
#                         - timedelta(hours=6)
#                         + timedelta(days=1)
#                     )
#                     data2["date"] = data2["datetime1"].dt.date

#                     data = pd.concat([data1, data2])
#                     data["SPREAD"] = data["SPREAD"].str[:-4]
#                     data["TOTAL"] = data["TOTAL"].str[:-4]
#                     data["TOTAL"] = data["TOTAL"].str[2:]
#                     data["Tomorrow"] = data["Tomorrow"].str.split().str[1:2]
#                     data["Tomorrow"] = pd.DataFrame(
#                         [
#                             str(line).strip("[").strip("]").replace("'", "")
#                             for line in data["Tomorrow"]
#                         ]
#                     )
#                     data["SPREAD"] = data["SPREAD"].str.replace("pk", "-1",
#                           regex=True)
#                     data["SPREAD"] = data["SPREAD"].str.replace("+", "", regex=True)
#                     data.columns = data.columns.str.lower()
#                     data = data[
#                         [
#                             "tomorrow",
#                             "spread",
#                             "total",
#                             "moneyline",
#                             "date",
#                             "datetime1",
#                         ]
#                     ]
#                     data = data.rename(columns={data.columns[0]: "team"})
#                     data = data.query(
#                         "date == date.min()"
#                     )  # only grab games from upcoming day
#                     logging.info(
#                         f"""Odds Transformation Function Successful {len(df)} day, \
#                         retrieving {len(data)} rows"""
#                     )
#                     return data
#                 else:  # if there's only 1 day of data then just use that
#                     data = data1.reset_index(drop=True)
#                     data["SPREAD"] = data["SPREAD"].str[:-4]
#                     data["TOTAL"] = data["TOTAL"].str[:-4]
#                     data["TOTAL"] = data["TOTAL"].str[2:]
#                     data["Tomorrow"] = data["Tomorrow"].str.split().str[1:2]
#                     data["Tomorrow"] = pd.DataFrame(
#                         [
#                             str(line).strip("[").strip("]").replace("'", "")
#                             for line in data["Tomorrow"]
#                         ]
#                     )
#                     data["SPREAD"] = data["SPREAD"].str.replace("pk", "-1",
#                        regex=True)
#                     data["SPREAD"] = data["SPREAD"].str.replace("+", "", regex=True)
#                     data.columns = data.columns.str.lower()
#                     data = data[
#                         [
#                             "tomorrow",
#                             "spread",
#                             "total",
#                             "moneyline",
#                             "date",
#                             "datetime1",
#                         ]
#                     ]
#                     data = data.rename(columns={data.columns[0]: "team"})
#                     data = data.query(
#                         "date == date.min()"
#                     )  # only grab games from upcoming day
#                     logging.info(
#                         f"""Odds Transformation Successful {len(df)} day, \
#                         retrieving {len(data)} rows"""
#                     )
#                     return data
#             except Exception as error:
#                 logging.error(
#                     f"Odds Transformation Failed for {len(df)} day objects, {error}"
#                 )
#                 sentry_sdk.capture_exception(error)
#                 data = pd.DataFrame()
#                 return data
#     except (
#         BaseException,
#         ValueError,
#     ) as error:  # valueerror fucked shit up apparently idfk
#         logging.error(f"Odds Function Web Scrape Failed, {error}")
#         sentry_sdk.capture_exception(error)
#         df = pd.DataFrame()
#         return df


@time_function
def get_reddit_data(feature_flags_df: pd.DataFrame, sub: str = "nba") -> pd.DataFrame:
    """
    Web Scrape function w/ PRAW that grabs top ~27 top posts from a given subreddit.
    Left sub as an argument in case I want to scrape multi subreddits in the future
    (r/nba, r/nbadiscussion, r/sportsbook etc)

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
            to run this function or not

        sub (string): subreddit to query

    Returns:
        Pandas DataFrame of all current top posts on r/nba
    """
    feature_flag = "reddit_posts"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

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
                    datetime.now().date(),
                    datetime.now(),
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

        logging.info(
            "Reddit Scrape Successful, grabbing 27 Recent "
            f"popular posts from r/{sub} subreddit"
        )
        return posts
    except Exception as error:
        logging.error(f"Reddit Scrape Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        data = pd.DataFrame()
        return data


@time_function
def get_reddit_comments(
    feature_flags_df: pd.DataFrame, urls: pd.Series
) -> pd.DataFrame:
    """
    Web Scrape function w/ PRAW that iteratively extracts comments from provided
    reddit post urls.

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
            to run this function or not

        urls (Series): The (reddit) urls to extract comments from

    Returns:
        Pandas DataFrame of all comments from the provided reddit urls
    """
    feature_flag = "reddit_comments"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

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
            # this removes all the "more comment" stubs
            # to grab ALL comments use limit=None, but it will take 100x longer
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

        df = df.query('author != "None"')  # remove deleted comments rip
        df["author"] = df["author"].astype(str)
        df = df.sort_values("score").groupby(["author", "comment", "url"]).tail(1)
        df = add_sentiment_analysis(df, "comment")

        df["edited"] = np.where(
            df["edited"] is False, 0, 1
        )  # if edited, then 1, else 0
        df["md5_pk"] = df.apply(
            lambda x: hashlib.md5(
                (str(x["author"]) + str(x["comment"]) + str(x["url"])).encode("utf8")
            ).hexdigest(),
            axis=1,
        )
        # this hash function lines up with the md5 function in postgres
        # this is needed for the upsert to work on it.
        logging.info(
            f"Reddit Comment Extraction Success, retrieving {len(df)} "
            f"total comments from {len(urls)} total urls"
        )
        return df
    except Exception as e:
        logging.error(f"Reddit Comment Extraction Failed for url {i}, {e}")
        sentry_sdk.capture_exception(e)
        df = pd.DataFrame()
        return df


# def scrape_tweets_tweepy(
#     search_parameter: str, count: int, result_type: str
# ) -> pd.DataFrame:
#     """
#     Web Scrape function w/ Tweepy to scrape Tweets made within last ~ 7 days

#     Args:
#         search_parameter (str): The string you're interested in finding Tweets for

#         count (int): Number of tweets to grab

#         result_type (str): Either mixed, recent, or popular.

#     Returns:
#         Pandas DataFrame of recent Tweets
#     """
#     auth = tweepy.OAuthHandler(
#         os.environ.get("twitter_consumer_api_key"),
#         os.environ.get("twitter_consumer_api_secret"),
#     )

#     api = tweepy.API(auth, wait_on_rate_limit=True)

#     full_tweet_df = pd.DataFrame()
#     try:
#         for tweet in tweepy.Cursor(  # result_type can be mixed, recent, or popular.
#             api.search_tweets, search_parameter, count=count, result_type=result_type
#         ).items(count):
#             df = {
#                 "api_created_at": tweet._json["created_at"],
#                 "tweet_id": tweet._json["id_str"],
#                 "username": tweet._json["user"]["screen_name"],
#                 "user_id": tweet._json["user"]["id"],
#                 "tweet": tweet._json["text"],
#                 "likes": tweet._json["favorite_count"],
#                 "retweets": tweet._json["retweet_count"],
#                 "language": tweet._json["lang"],
#                 "scrape_ts": datetime.now(),
#                 "profile_img": tweet._json["user"]["profile_image_url"],
#                 "url": f"https://twitter.com/twitter/statuses/{tweet._json['id']}",
#             }
#             full_tweet_df = pd.concat([df, full_tweet_df])

#         df = add_sentiment_analysis(df, "tweet")
#         logging.info(f"Twitter Scrape Successful, retrieving {len(df)} Tweets")
#         return df
#     except Exception as e:
#         logging.error(f"Error Occurred for Scrape Tweets Tweepy, {e}")
#         sentry_sdk.capture_exception(e)
#         df = pd.DataFrame()
#         return df


# @time_function
# def scrape_tweets_combo(feature_flags_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Web Scrape function to scrape Tweepy Tweets for both popular & mixed tweets

#     Args:
#         feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
#             to run this function or not

#     Returns:
#         Pandas DataFrame of both popular and mixed tweets.
#     """
#     feature_flag = "twitter"
#     feature_flag_check = check_feature_flag(
#         flag=feature_flag, flags_df=feature_flags_df
#     )

#     if feature_flag_check is False:
#         logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
#         df = pd.DataFrame()
#         return df

#     try:
#         df1 = scrape_tweets_tweepy("nba", 1000, "popular")
#         df2 = scrape_tweets_tweepy("nba", 5000, "mixed")

#         # so the scrape_ts column screws up with filtering duplicates out so
#         # this code ignores that column to correctly drop the duplicates
#         df_combo = pd.concat([df1, df2])
#         df_combo = df_combo.drop_duplicates(
#             subset=df_combo.columns.difference(
#                 ["scrape_ts", "likes", "retweets", "tweet"]
#             )
#         )

#         logging.info(
#             f"Grabbing {len(df1)} Popular Tweets and {len(df2)} Mixed Tweets "
#             f"for {len(df_combo)} Total, {(len(df1) + len(df2) - len(df_combo))} "
#             "were duplicates"
#         )
#         return df_combo
#     except Exception as e:
#         logging.error(f"Error Occurred for Scrape Tweets Combo, {e}")
#         sentry_sdk.capture_exception(e)
#         df = pd.DataFrame()
#         return df


@time_function
def get_pbp_data(feature_flags_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Web Scrape function w/ pandas read_html that uses aliases via boxscores function
    to scrape the pbp data iteratively for each game played the previous day.
    It assumes there is a location column in the df being passed in.

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
            to run this function or not

        df (DataFrame) - The Boxscores DataFrame

    Returns:
        All PBP Data for the games in the input df

    """
    feature_flag = "pbp"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    if len(df) > 0:
        game_date = df["date"][0]
    else:
        df = pd.DataFrame()
        logging.warning(
            "PBP Transformation Function Failed, "
            f"no data available for {datetime.now().date()}"
        )
        return df
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
                    url = f"https://www.basketball-reference.com/boxscores/pbp/{newdate}0{i}.html"
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
                    df["Quarter"] = df["Quarter"].ffill()
                    df = df.query(
                        'Time != "Time" & '
                        'Time != "2nd Q" & '
                        'Time != "3rd Q" & '
                        'Time != "4th Q" & '
                        'Time != "1st OT" & '
                        'Time != "2nd OT" & '
                        'Time != "3rd OT" & '
                        'Time != "4th OT"'
                    ).copy()
                    # use COPY to get rid of the fucking goddamn warning
                    df["HomeTeam"] = i
                    df["HomeTeam"] = df["HomeTeam"].str.replace("PHO", "PHX")
                    df["HomeTeam"] = df["HomeTeam"].str.replace("CHO", "CHA")
                    df["HomeTeam"] = df["HomeTeam"].str.replace("BRK", "BKN")
                    df = df.merge(away_teams)
                    df[["scoreAway", "scoreHome"]] = df["Score"].str.split(
                        "-", expand=True, n=1
                    )
                    df["scoreAway"] = pd.to_numeric(df["scoreAway"], errors="coerce")
                    df["scoreAway"] = df["scoreAway"].ffill()
                    df["scoreAway"] = df["scoreAway"].fillna(0)
                    df["scoreHome"] = pd.to_numeric(df["scoreHome"], errors="coerce")
                    df["scoreHome"] = df["scoreHome"].ffill()

                    df["scoreHome"] = df["scoreHome"].fillna(0)
                    df["marginScore"] = df["scoreHome"] - df["scoreAway"]
                    df["Date"] = game_date
                    df["scrape_date"] = datetime.now().date()
                    df = df.rename(
                        columns={
                            df.columns[0]: "timeQuarter",
                            df.columns[6]: "numberPeriod",
                        }
                    )
                    pbp_list = pd.concat([df, pbp_list])
                    df = pd.DataFrame()
                pbp_list.columns = pbp_list.columns.str.lower()
                pbp_list = pbp_list.query(
                    "(awayscore.notnull()) | (homescore.notnull())", engine="python"
                )
                logging.info(
                    "PBP Data Transformation Function Successful, "
                    f"retrieving {len(pbp_list)} rows for {game_date}"
                )
                # filtering only scoring plays here, keep other all other rows in future
                # for lineups stuff etc.
                return pbp_list
            except Exception as error:
                logging.error(f"PBP Transformation Function Logic Failed, {error}")
                sentry_sdk.capture_exception(error)
                df = pd.DataFrame()
                return df
        else:
            df = pd.DataFrame()
            logging.error(
                "PBP Transformation Function Failed, no data available "
                f"for {game_date}"
            )
            return df
    except Exception as error:
        logging.error(f"PBP Data Transformation Function Failed, {error}")
        sentry_sdk.capture_exception(error)
        data = pd.DataFrame()
        return data


@time_function
def schedule_scraper(
    feature_flags_df: pd.DataFrame,
    year: str,
    month_list: list[str] = [
        "october",
        "november",
        "december",
        "january",
        "february",
        "march",
        "april",
    ],
) -> pd.DataFrame:
    """
    Web Scrape Function to scrape Schedule data by iterating through a list of months

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check whether
            to run this function or not

        year (str) - The year to scrape

        month_list (list) - List of full-month names to scrape

    Returns:
        DataFrame of Schedule Data to be stored.

    """
    current_date = (
        datetime.now().date()
    )  # DO NOT REMOVE, used in df.query function later
    feature_flag = "schedule"
    feature_flag_check = check_feature_flag(
        flag=feature_flag, flags_df=feature_flags_df
    )

    if feature_flag_check is False:
        logging.info(f"Feature Flag {feature_flag} is disabled, skipping function")
        df = pd.DataFrame()
        return df

    try:
        schedule_df = pd.DataFrame()
        completed_months = []
        for month in month_list:
            url = f"https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
            html = requests.get(url).content
            soup = BeautifulSoup(html, "html.parser")

            headers = [th.getText() for th in soup.findAll("tr")[0].findAll("th")]
            headers[6] = "boxScoreLink"
            headers[7] = "isOT"
            headers = headers[1:]

            rows = soup.findAll("tr")[1:]
            date_info = [
                [th.getText() for th in rows[i].findAll("th")] for i in range(len(rows))
            ]

            game_info = [
                [td.getText() for td in rows[i].findAll("td")] for i in range(len(rows))
            ]
            date_info = [i[0] for i in date_info]

            schedule = pd.DataFrame(game_info, columns=headers)
            schedule["Date"] = date_info

            logging.info(
                f"Schedule Function Completed for {month}, retrieving {len(schedule)} "
                "rows"
            )
            completed_months.append(month)
            schedule_df = pd.concat([schedule, schedule_df])

    except IndexError:
        logging.info(
            f"{month} currently has no data in basketball-reference, "
            f"stopping the function and returning data for {' '.join(completed_months)}"
        )
    finally:
        if not schedule_df.empty:
            schedule_df = schedule_df[
                ["Start (ET)", "Visitor/Neutral", "Home/Neutral", "Date"]
            ]
            schedule_df["proper_date"] = pd.to_datetime(
                schedule_df["Date"], format="%a, %b %d, %Y"
            ).dt.date
            schedule_df.columns = schedule_df.columns.str.lower()
            schedule_df = schedule_df.rename(
                columns={
                    "start (et)": "start_time",
                    "visitor/neutral": "away_team",
                    "home/neutral": "home_team",
                }
            )
            # filtering the data to only rows beyond the current date because we already have
            # the historical records
            schedule_df = schedule_df[schedule_df["proper_date"] >= current_date]
            return schedule_df
        else:
            return pd.DataFrame()


def write_to_s3(
    file_name: str,
    df: pd.DataFrame,
    date: datetime.date = datetime.now().date(),
    bucket: str = os.environ.get("S3_BUCKET"),
) -> None:
    """
    S3 Function using awswrangler to write file.  Only supports parquet right now.

    Args:
        file_name (str): The base name of the file (boxscores, opp_stats)

        df (pd.DataFrame): The Pandas DataFrame to write to S3

        bucket (str): The Bucket to write to.  Defaults to `os.environ.get('S3_BUCKET')`

        date (datetime.date): Date to partition the data by.
            Defaults to `datetime.now().date()`

    Returns:
        Writes the Pandas DataFrame to an S3 File.

    """
    year_partition = date.year
    month_partition = get_leading_zeroes(value=date.month)
    file_name_jn = f"{file_name}-{date}"
    try:
        if len(df) == 0:
            logging.info(f"Not storing {file_name} to s3 because it's empty.")
            pass
        else:
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{bucket}/{file_name}/validated/year={year_partition}/month={month_partition}/{file_name_jn}.parquet",
                index=False,
            )
            logging.info(
                f"Storing {len(df)} {file_name} rows to S3 (s3://{bucket}/{file_name}/validated/{year_partition}/{file_name_jn}.parquet)"
            )
            pass
    except Exception as error:
        logging.error(f"S3 Storage Function Failed {file_name}, {error}")
        sentry_sdk.capture_exception(error)
        pass


def write_to_sql(con, table_name: str, df: pd.DataFrame, table_type: str) -> None:
    """
    Simple Wrapper Function to write a Pandas DataFrame to SQL

    Args:
        con (SQL Connection): The connection to the SQL DB.

        table_name (str): The Table name to write to SQL as.

        df (DataFrame): The Pandas DataFrame to store in SQL

        table_type (str): Whether the table should replace or append to an
            existing SQL Table under that name

    Returns:
        Writes the Pandas DataFrame to a Table in the Schema we connected to.

    """
    try:
        if len(df) == 0:
            logging.info(f"{table_name} is empty, not writing to SQL")
        else:
            df.to_sql(
                con=con,
                name=table_name,
                index=False,
                if_exists=table_type,
            )
            logging.info(
                f"Writing {len(df)} {table_name} rows to aws_{table_name}_source to SQL"
            )

        return None
    except Exception as error:
        logging.error(f"SQL Write Script Failed, {error}")
        sentry_sdk.capture_exception(error)


# deprecated as of 2023-10-17 rip
# def send_aws_email(logs: pd.DataFrame) -> None:
#     """
#     Email function utilizing boto3, has to be set up with SES in AWS
#     and env variables passed in via Terraform.

#     The actual email code is copied from aws/boto3 and the subject &
#     message should go in the subject / body_html variables.

#     Args:
#         logs (DataFrame): The log file name generated by the script.

#     Returns:
#         Sends an email out upon every script execution, including errors (if any)
#     """
#     sender = os.environ.get("USER_EMAIL")
#     recipient = os.environ.get("USER_EMAIL")
#     aws_region = "us-east-1"
#     subject = f"""
#     NBA ELT PIPELINE - {str(len(logs))} Alert Fails for {str(datetime.now().date())}
#     """
#     body_html = f"""\
# <h3>Errors:</h3>
#                    {logs.to_html()}"""

#     charset = "UTF-8"
#     client = boto3.client("ses", region_name=aws_region)
#     try:
#         response = client.send_email(
#             Destination={
#                 "ToAddresses": [
#                     recipient,
#                 ],
#             },
#             Message={
#                 "Body": {
#                     "Html": {
#                         "Charset": charset,
#                         "Data": body_html,
#                     },
#                     "Text": {
#                         "Charset": charset,
#                         "Data": body_html,
#                     },
#                 },
#                 "Subject": {
#                     "Charset": charset,
#                     "Data": subject,
#                 },
#             },
#             Source=sender,
#         )
#     except ClientError as e:
#         logging.error(e.response["Error"]["Message"])
#         raise e
#     else:
#         logging.info(f"Email sent! Message ID: {response['MessageId']}")
#         return None


# DEPRECATING this as of 2022-04-25 - i send emails everyday now regardless
# of pass or fail
# def execute_email_function(logs: pd.DataFrame) -> None:
#     """
#     Email function that executes the email function upon script finishing.
#     This is really not necessary; originally thought i wouldn't email
#     if no errors would found but now i send it everyday regardless.

#     Args:
#         logs (DataFrame): The log file name generated by the script.

#     Returns:
#         Holds the actual send_email logic and executes if invoked as a
#             script (aka on ECS)
#     """
#     try:
#         if len(logs) > 0:
#             logging.info("Sending Email")
#             send_aws_email(logs)
#         elif len(logs) == 0:
#             logging.info("No Errors!")
#             send_aws_email(logs)
#     except Exception as error:
#         logging.error(f"Failed Email Alert, {error}")
#         sentry_sdk.capture_exception(error)


def get_feature_flags(connection: Connection | Engine) -> pd.DataFrame:
    flags = pd.read_sql_query(sql="select * from marts.feature_flags;", con=connection)

    logging.info(f"Retrieving {len(flags)} Feature Flags")
    return flags


def check_feature_flag(flag: str, flags_df: pd.DataFrame) -> bool:
    flags_df = flags_df.query(f"flag == '{flag}'")

    if len(flags_df) > 0 and flags_df["is_enabled"].iloc[0] == 1:
        return True
    else:
        return False


def query_logs(log_file: str = "logs/example.log") -> list:
    """
    Small Function to read Logs CSV File and grab Errors

    Args:
        log_file (str): Optional String of the Log File Name

    Returns:
        list of Error Messages to be passed into Slack Function
    """
    logs = pd.read_csv(log_file, sep=r"\\t", engine="python", header=None)
    logs = logs.rename(columns={0: "errors"})
    logs = logs.query("errors.str.contains('Failed')", engine="python")
    logs = logs["errors"].to_list()

    logging.info(f"Returning {len(logs)} Failed Logs")
    return logs


def write_to_slack(
    errors: list, webhook_url: str = os.environ.get("WEBHOOK_URL", default="default")
) -> int | None:
    """ "
    Function to write Errors out to Slack.  Requires a pre-configured `webhook_url`
    to be setup.

    Args:
        errors (list): The list of Failed Tasks + their associated errors

        webhook_url (str): Optional Parameter to specify the Webhook to send the
            errors to.  Defaults to `os.environ.get("WEBHOOK_URL")`

    Returns:
        None, but writes the Errors to Slack if there are any
    """
    try:
        date = datetime.now().date()
        num_errors = len(errors)
        str_dump = "\n".join(errors)

        if num_errors > 0:
            response = requests.post(
                webhook_url,
                data=json.dumps(
                    {
                        "text": (
                            f"\U0001f6d1 {num_errors} Errors during NBA ELT "
                            f"Ingestion on {date}: \n {str_dump}"
                        )
                    }
                ),
                headers={"Content-Type": "application/json"},
            )
            logging.info(
                f"Wrote Errors to Slack, Reponse Code {response.status_code}. "
                "Exiting ..."
            )
            return response.status_code
        else:
            logging.info("No Error Logs, not writing to Slack.  Exiting out ...")
            return None
    except Exception as e:
        raise e
