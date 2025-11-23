import hashlib
import logging
import os
import urllib.request
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError

import numpy as np
import pandas as pd
import praw
from bs4 import BeautifulSoup

from src.decorators import check_feature_flag_decorator, record_function_time_decorator
from src.utils import (
    SEASON_YEAR,
    add_sentiment_analysis,
    check_schedule,
    clean_player_names,
    filter_spread,
    get_leading_zeroes,
)


@check_feature_flag_decorator(flag_name="stats")
@record_function_time_decorator
def get_player_stats_data() -> pd.DataFrame:
    """Web Scrape function w/ BS4 that grabs aggregate season stats

    Args:
        None

    Returns:
        DataFrame of Player Aggregate Season stats
    """
    # stats = stats.rename(columns={"fg%": "fg_pct", "3p%": "3p_pct",
    # "2p%": "2p_pct", "efg%": "efg_pct", "ft%": "ft_pct"})
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}_per_game.html"

        # Use urllib like pandas does to avoid blocks
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="boxscores")
@record_function_time_decorator
def get_boxscores_data(
    run_date: datetime | None = None,
) -> pd.DataFrame:
    """Boxscore Scraper Function

    Grabs box scores from a given date - defaults to yesterday.

    Args:
        run_date (datetime, optional): Date to get box scores for.
            Defaults to yesterday.

    Returns:
        DataFrame of Player box score stats

    Usage:
        `df = get_boxscores_data(run_date=datetime(2025, 6, 22))`
    """
    # Default to yesterday if no date provided
    if run_date is None:
        run_date = datetime.now() - timedelta(1)

    # Format date components
    day_str = get_leading_zeroes(value=run_date.day)
    month_str = get_leading_zeroes(value=run_date.month)
    year = run_date.year
    date = f"{year}-{month_str}-{day_str}"

    url = f"https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={month_str}&day={day_str}&year={year}&type=all"

    try:
        # html = requests.get(url).content
        # use urllib instead of requests to avoid cloudflare block
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        soup = BeautifulSoup(html, "html.parser")

        # Get headers and rename them (use find_all and get_text)
        headers = [
            th.get_text() for th in soup.find_all("tr", limit=2)[0].find_all("th")
        ]
        headers = headers[1:]

        # Create header mapping for cleaner renaming
        header_renames = {
            1: "Team",
            2: "Location",
            3: "Opponent",
            4: "Outcome",
            6: "FGM",
            8: "FGPercent",
            9: "threePFGMade",
            10: "threePAttempted",
            11: "threePointPercent",
            14: "FTPercent",
            15: "OREB",
            16: "DREB",
            24: "PlusMinus",
        }
        for idx, name in header_renames.items():
            headers[idx] = name

        rows = soup.find_all("tr")[1:]
        player_stats = [[td.get_text() for td in row.find_all("td")] for row in rows]

        df = pd.DataFrame(player_stats, columns=headers)

        # Convert numeric columns
        numeric_cols = [
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
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

        # Set date
        df["date"] = pd.to_datetime(date)

        # Location mapping
        df["Location"] = df["Location"].apply(lambda x: "A" if x == "@" else "H")

        # Team name replacements
        team_replacements = {"PHO": "PHX", "CHO": "CHA", "BRK": "BKN"}
        for col in ["Team", "Opponent"]:
            df[col] = df[col].replace(team_replacements)

        # Filter and clean player names
        df = df.query("Player == Player").reset_index(drop=True)
        df["Player"] = (
            df["Player"]
            .str.normalize("NFKD")
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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="opp_stats")
@record_function_time_decorator
def get_opp_stats_data() -> pd.DataFrame:
    """Team Opponent Stats Scraper Function

    Args:
        None

    Returns:
        Pandas DataFrame of all current team opponent stats
    """
    year = (datetime.now() - timedelta(1)).year
    month = (datetime.now() - timedelta(1)).month
    day = (datetime.now() - timedelta(1)).day

    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}.html"
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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="injuries")
@record_function_time_decorator
def get_injuries_data() -> pd.DataFrame:
    """Web Scrape function w/ pandas read_html that grabs all current injuries

    Args:
        None

    Returns:
        Pandas DataFrame of all current player injuries & their associated team
    """
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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="player_adv_stats")
@record_function_time_decorator
def get_player_adv_stats_data() -> pd.DataFrame:
    """Web Scrape function w/ pandas read_html that grabs all player adv stats

    Args:
        None

    Returns:
        Pandas DataFrame of all player adv stats
    """
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}_advanced.html"
        df = pd.read_html(url)[0]
        df = df.rename(columns={"Update": "Date"})
        df.columns = df.columns.str.lower()
        df["player"] = (
            df["player"]
            .str.normalize("NFKD")  # this is removing all accented characters
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )
        df["player"] = df["player"].apply(clean_player_names)
        df = df.drop_duplicates()
        logging.info(
            f"Player Advanced Stats Function Successful, retrieving {len(df)} rows"
        )
        return df
    except Exception as error:
        logging.error(f"Player Advanced Stats Function Failed, {error}")
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="transactions")
@record_function_time_decorator
def get_transactions_data() -> pd.DataFrame:
    """Web Scrape function w/ BS4 that retrieves NBA Trades, signings, waivers etc.

    Args:
        None

    Returns:
        Pandas DataFrame of all season transactions, trades, player waives etc.
    """
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}_transactions.html"

        # Use urllib like pandas does
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        soup = BeautifulSoup(html, "html.parser")

        # Find the ul with class="page_index"
        page_index = soup.find("ul", {"class": "page_index"})

        if not page_index:
            raise ValueError("Could not find transactions list")

        trs = page_index.find_all("li")

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
            "\\?",
            "October 1, 2024",
            regex=True,  # bad data 10-14-21
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
    except (URLError, HTTPError, ValueError) as error:
        logging.error(f"Transaction Web Scrape Function Failed, {error}")
        return pd.DataFrame()
    except Exception as error:
        logging.error(f"Transaction Web Scrape Function Failed, {error}")
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="adv_stats")
@record_function_time_decorator
def get_team_adv_stats_data() -> pd.DataFrame:
    """Web Scrape function w/ pandas read_html that grabs all team advanced stats

    Args:
        None

    Returns:
        DataFrame of all current Team Advanced Stats
    """
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}.html"
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
            "Team Advanced Stats Transformation Function Successful, "
            "retrieving updated data for 30 Teams"
        )
        return df
    except Exception as error:
        logging.error(f"Team Advanced Stats Web Scrape Function Failed, {error}")
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="shooting_stats")
@record_function_time_decorator
def get_shooting_stats_data() -> pd.DataFrame:
    """Web Scrape function w/ pandas read_html that grabs all raw shooting stats

    Args:
        None

    Returns:
        DataFrame of raw shooting stats
    """
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}_shooting.html"
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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="odds")
@record_function_time_decorator
def get_odds_data() -> pd.DataFrame:
    """Function to web scrape Gambling Odds from cover.com

    Args:
        None

    Returns:
        DataFrame of Gambling Odds for Today's Games
    """
    try:
        url = "https://www.covers.com/sport/basketball/nba/odds"
        df = pd.read_html(url)

        # df[0] has datetime and moneyline, df[1] has spread data
        odds = df[0].iloc[:, [0, 2]].copy()  # datetime and moneyline
        odds.columns = ["datetime1", "moneyline"]
        odds["spread"] = df[1].iloc[:, 2]  # spread from df[1]

        # Normalize all whitespace first (handles \xa0 and other issues)
        odds["datetime1"] = odds["datetime1"].str.replace(r"\s+", " ", regex=True)
        odds["spread"] = odds["spread"].str.replace(r"\s+", " ", regex=True)
        odds["moneyline"] = odds["moneyline"].str.replace(r"\s+", " ", regex=True)

        # Filter for today's games only
        odds = odds[
            (odds["datetime1"].notna())
            & (odds["datetime1"] != "FINAL")
            & (odds["datetime1"].str.contains("Today", na=False))
        ].copy()

        if len(odds) == 0:
            logging.info("No Odds Records available for today's games")
            return pd.DataFrame()

        # Clean datetime - remove "Today, "
        odds["datetime1"] = odds["datetime1"].str.replace(r"Today,\s*", "", regex=True)

        # Handle PK (pick 'em) games
        odds["spread"] = odds["spread"].str.replace("PK", "-1.0", regex=False)

        # Apply your filter_spread function
        odds["spread"] = odds["spread"].apply(filter_spread)
        odds["spread"] = odds["spread"].apply(lambda x: " ".join(x.split()))

        odds_final = odds[["datetime1", "spread", "moneyline"]].copy()

        # Extract teams from datetime1
        # \b: Word boundary anchor
        # [A-Z]{2,3}: Match 2-3 uppercase letters (team abbreviations)
        pattern = r"\b([A-Z]{2,3})\b"
        odds_final["team"] = (
            odds_final["datetime1"]
            .str.extractall(pattern)
            .unstack()
            .apply(lambda x: " ".join(x.dropna()), axis=1)
        )

        # Extract time BEFORE exploding to avoid contamination
        odds_final["time"] = odds_final["datetime1"].str.split().str[0]

        # Convert to lists for exploding
        odds_final["team"] = odds_final["team"].str.split()
        odds_final["spread"] = odds_final["spread"].str.split()
        odds_final["moneyline"] = odds_final["moneyline"].str.split()

        # Explode all columns at once
        odds_final = odds_final.explode(["team", "spread", "moneyline"]).reset_index(
            drop=True
        )

        # Clean up strings
        odds_final["spread"] = odds_final["spread"].str.strip()
        odds_final["moneyline"] = odds_final["moneyline"].str.strip()
        odds_final["date"] = datetime.now().date()

        # Create proper datetime using pre-extracted time
        odds_final["datetime1"] = pd.to_datetime(
            datetime.now().date().strftime("%Y-%m-%d") + " " + odds_final["time"],
            format="%Y-%m-%d %H:%M",
        )

        # Final transformations
        odds_final["total"] = 200
        odds_final["team"] = odds_final["team"].str.replace("BK", "BKN")
        odds_final["moneyline"] = odds_final["moneyline"].str.replace(
            r"\+", "", regex=True
        )
        odds_final["moneyline"] = odds_final["moneyline"].astype("int")

        # Select final columns (drop time as it's now in datetime1)
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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="reddit_posts")
@record_function_time_decorator
def get_reddit_data(sub: str = "nba") -> pd.DataFrame:
    """Web Scrape function w/ PRAW

    Grabs top ~27 top posts from a given subreddit.
    Left sub as an argument in case I want to scrape multi subreddits in the future
    (r/nba, r/nbadiscussion, r/sportsbook etc)

    Args:
        sub (string): subreddit to query

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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="reddit_comments")
@record_function_time_decorator
def get_reddit_comments(urls: pd.Series) -> pd.DataFrame:
    """Web Scrape function w/ PRAW

    Iteratively extracts comments from provided reddit post urls.

    Args:
        urls (Series): The (reddit) urls to extract comments from

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
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="pbp")
@record_function_time_decorator
def get_pbp_data(df: pd.DataFrame) -> pd.DataFrame:
    """Web Scrape function w/ pandas read_html

    Uses aliases via boxscores function to scrape the pbp data iteratively for each game
    played the previous day. It assumes there is a location column in the df being
    passed in.

    Args:
        df (DataFrame): The Boxscores DataFrame

    Returns:
        All PBP Data for the games in the input df

    """
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
                return pd.DataFrame()
        else:
            df = pd.DataFrame()
            logging.error(
                f"PBP Transformation Function Failed, no data available for {game_date}"
            )
            return df
    except Exception as error:
        logging.error(f"PBP Data Transformation Function Failed, {error}")
        return pd.DataFrame()


@check_feature_flag_decorator(flag_name="schedule")
@record_function_time_decorator
def get_schedule_data(
    month_list: list[str] = None,
    include_past_games: bool = False,
) -> pd.DataFrame:
    """Web Scrape Function to scrape Schedule data by iterating through a list of months

    Args:
        month_list (list[str], optional): List of full month names to scrape.
            Defaults to season months.

        include_past_games (bool, optional): If True, includes games before today.
            Defaults to False.

    Returns:
        DataFrame of Schedule Data to be stored.

    """
    if month_list is None:
        month_list = [
            "october",
            "november",
            "december",
            "january",
            "february",
            "march",
            "april",
        ]

    current_date = datetime.now().date()
    schedule_df = pd.DataFrame()
    completed_months = []

    for month in month_list:
        try:
            url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}_games-{month}.html"

            # Use pd.read_html to get the table
            tables = pd.read_html(url)

            if not tables or len(tables) == 0:
                raise IndexError

            month_df = tables[0]

            if month_df.empty:
                raise IndexError

            # Select and rename columns
            month_df = month_df[
                ["Date", "Start (ET)", "Visitor/Neutral", "Home/Neutral"]
            ].rename(
                columns={
                    "Date": "date",
                    "Start (ET)": "start_time",
                    "Visitor/Neutral": "away_team",
                    "Home/Neutral": "home_team",
                }
            )

            schedule_df = pd.concat([schedule_df, month_df], ignore_index=True)
            completed_months.append(month)

            logging.info(
                f"Schedule scrape completed for {month}, {len(month_df)} rows retrieved"
            )

        except (IndexError, ValueError):
            logging.info(f"{month} has no data, skipping.")

    if schedule_df.empty:
        return pd.DataFrame()

    # Format and clean columns
    schedule_df["proper_date"] = pd.to_datetime(
        schedule_df["date"], format="%a, %b %d, %Y"
    ).dt.date
    schedule_df.columns = schedule_df.columns.str.lower()

    if not include_past_games:
        schedule_df = schedule_df[schedule_df["proper_date"] >= current_date]

    return schedule_df
