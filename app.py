import os
import logging
from urllib.request import urlopen
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import praw
from bs4 import BeautifulSoup
from sqlalchemy import exc, create_engine
import boto3
from botocore.exceptions import ClientError

print("Loading Python ELT Script Version: 0.1.10-dev0")
# GENERAL NOTES
# ValueError should capture any read_html failures
# logging is for identifying failures and sending an email out documenting them
# Cloudwatch logs are also enabled in ECS so all the print statements will get recorded there too.
# python:slim-3.8 provides everything i need and has the sql drivers and
# compilers to install pandas/numpy/sqlalchemy etc.

logging.basicConfig(
    filename="example.log",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
)
logging.info("Starting Logging Function")

print("LOADED FUNCTIONS")
logging.info("LOADED FUNCTIONS")

today = datetime.now().date()
todaytime = datetime.now()
yesterday = today - timedelta(1)
day = (datetime.now() - timedelta(1)).day
month = (datetime.now() - timedelta(1)).month
year = (datetime.now() - timedelta(1)).year
season_type = "Regular Season"


def get_player_stats():
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
        logging.info(
            f"General Stats Function Successful, retrieving {len(stats)} updated rows"
        )
        print(
            f"General Stats Function Successful, retrieving {len(stats)} updated rows"
        )
        return stats
    except IndexError:
        logging.info("General Stats Function Failed for Today's Games")
        print("General Stats Function Failed for Today's Games")
        df = []
        return df


def get_boxscores(month=month, day=day, year=year):
    url = "https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={}&day={}&year={}&type=all".format(
        month, day, year
    )
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")

    try:
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
        logging.info(
            f"Box Score Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        print(
            f"Box Score Function Successful, retrieving {len(df)} rows for {year}-{month}-{day}"
        )
        return df
    except IndexError:
        logging.info(
            f"Box Score Function Failed, no data available for {year}-{month}-{day}"
        )
        print(f"Box Score Function Failed, no data available for {year}-{month}-{day}")
        df = []
        return df


def get_injuries():
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
        df = df.rename(columns={"Update": "Date"})
        df.columns = df.columns.str.lower()
        logging.info(f"Injury Function Successful, retrieving {len(df)} rows")
        print(f"Injury Function Successful, retrieving {len(df)} rows")
        return df
    except ValueError:
        logging.info("Injury Function Failed for Today's Games")
        print("Injury Function Failed for Today's Games")
        df = []
        return df


def get_transactions():
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
        "?", "Jan 1, 2021", regex=True  # bad data 10-14-21
    )
    transactions["Date"] = pd.to_datetime(transactions["Date"])
    transactions.columns = transactions.columns.str.lower()
    transactions = transactions[['date', 'transaction']]
    logging.info(
        f"Transactions Function Successful, retrieving {len(transactions)} rows"
    )
    print(f"Transactions Function Successful, retrieving {len(transactions)} rows")
    return transactions


def get_advanced_stats():
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
        logging.info(
            f"Advanced Stats Function Successful, retrieving updated data for 30 Teams"
        )
        print(
            f"Advanced Stats Function Successful, retrieving updated data for 30 Teams"
        )
        return df
    except ValueError:
        logging.info("Advanced Stats Function Failed for Today's Games")
        print("Advanced Stats Function Failed for Today's Games")
        df = []
        return df


def get_odds():
    """
    Web Scrape function w/ pandas read_html that grabs current day's nba odds

    Args:
        None

    Returns:
        Pandas DataFrame of NBA moneyline + spread odds for upcoming games for that day
    """
    try:
        url = "https://sportsbook.draftkings.com/leagues/basketball/88670846?category=game-lines&subcategory=game"
        df = pd.read_html(url)

        data1 = df[0].copy()
        date_try = str(year) + " " + data1.columns[0]
        data1["date"] = np.where(
            date_try == "2021 Today",
            datetime.now().date(),  # if the above is true, then return this
            str(year) + " " + data1.columns[0],  # if false then return this
        )
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
        logging.info(f"Odds Function Successful, retrieving {len(data)} rows")
        print(f"Odds Function Successful, retrieving {len(data)} rows")
        return data
    except ValueError:
        logging.info("Odds Function Failed for Today's Games")
        print("Odds Function Failed for Today's Games")
        data = []
        return data


# NOTES ON ODD FUNCTION
# read_html scrapes the website and returns UTC data which makes that day's games come in 2 different date / time formats.
# grabbing both data frames and manually subtracting 5 hrs to get into CST time and making them the right day, then appending them together.
# also stripping out some text in the team name column, formatting is
# fully complete in DBT (GS -> GSW)


def scrape_subreddit(sub):
    """
    Web Scrape function w/ PRAW that grabs top ~27 top posts from r/nba

    Args:
        None

    Returns:
        Pandas DataFrame of all current top posts on r/nba
    """
    subreddit = reddit.subreddit(sub)
    posts = []
    for post in subreddit.hot(limit=27):
        posts.append(
            [
                post.title,
                post.score,
                post.id,
                post.url,
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
            "num_comments",
            "body",
            "scrape_date",
            "scrape_time",
        ],
    )
    posts.columns = posts.columns.str.lower()
    print("Grabbing 27 Recent popular posts from r/" + sub + " subreddit")
    logging.info("Grabbing 27 Recent popular posts from r/" + sub + " subreddit")
    return posts


def get_pbp_data(df):
    """
    Web Scrape function w/ pandas read_html that uses boxscore team aliases to scrape the pbp data interactively for each game played the previous day

    Args:
        None

    Returns:
        All PBP Data for the games returned in the Box Scores functions

    """
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
            df.query('location == "A"')[["team", "opponent"]].drop_duplicates().dropna()
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
                    (df["HomeScore"].str.contains("Start of 2nd quarter", na=False)),
                    (df["HomeScore"].str.contains("Start of 3rd quarter", na=False)),
                    (df["HomeScore"].str.contains("Start of 4th quarter", na=False)),
                    (df["HomeScore"].str.contains("Start of 1st overtime", na=False)),
                    (df["HomeScore"].str.contains("Start of 2nd overtime", na=False)),
                    (df["HomeScore"].str.contains("Start of 3rd overtime", na=False)),
                    (
                        df["HomeScore"].str.contains("Start of 4th overtime", na=False)
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
                df[["scoreAway", "scoreHome"]] = df["Score"].str.split("-", expand=True)
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
            # filtering only scoring plays here, keep other all other rows in future for lineups stuff etc.
            return pbp_list
        except ValueError:
            logging.info("PBP Function Failed for Yesterday's Games")
            print("PBP Function Failed for Yesterday's Games")
            df = []
            return df
    else:
        df = []
        logging.info("PBP Function No Data Yesterday")
        print("PBP Function No Data Yesterday")
        return df


# PBP NOTES
# Grabbing all current games from the Box Scores function, and then it uses the home teams to generate urls to the endpoints that hold the pbp data.
# Also creating custom 1st/2nd/3rd quarter objects here, renaming some
# columns, and doing some basic filtering to get the data in a manageable
# form for SQL


def sql_connection():
    """
    SQL Connection function connecting to my postgres db with schema = nba_source where initial data in ELT lands

    Args:
        None

    Returns:
        SQL Connection variable to schema: nba_source in my PostgreSQL DB
    """
    try:
        connection = create_engine(
            "postgresql+psycopg2://"
            + os.environ.get("RDS_USER")
            + ":"
            + os.environ.get("RDS_PW")
            + "@"
            + os.environ.get("IP")
            + ":"
            + "5432"
            + "/"
            + os.environ.get("RDS_DB"),
            connect_args={"options": "-csearch_path=nba_source"},
            # defining schema to connect to
            echo=False,
        )
        logging.info("SQL Connection Successful")
        print("SQL Connection Successful")
        return connection
    except exc.SQLAlchemyError as e:
        logging.info("SQL Connection Failed, Error:", e)
        print("SQL Connection Failed, Error:", e)
        return e


def write_to_sql(data, table_type):
    """
    SQL Table function to write a pandas data frame in aws_dfname_table format

    Args:
        data: The Pandas DataFrame to store in SQL

        table_type: Whether the table should replace or append to an existing SQL Table under that name

    Returns:
        Writes the Pandas DataFrame to a Table in Snowflake in the {nba_source} Schema we connected to.

    """
    data_name = [k for k, v in globals().items() if v is data][0]
    # ^ this disgusting monstrosity is to get the name of the -fucking- dataframe lmfao
    if len(data) == 0:
        print(data_name + " is empty, not writing to SQL")
        logging.info(data_name + " is empty, not writing to SQL")
    else:
        data.to_sql(
            con=conn,
            name=("aws_" + data_name + "_source"),
            index=False,
            if_exists=table_type,
        )
        print("Writing aws_" + data_name + "_source to SQL")
        logging.info("Writing aws_" + data_name + "_source to SQL")


def send_aws_email():
    """
    Email function utilizing boto3, has to be set up with SES in AWS and env variables passed in via Terraform.

    Args:
        None

    Returns:
        Sends an email out if errors for the run were greater than 0.
    """
    sender = os.environ.get("USER_EMAIL")
    recipient = os.environ.get("USER_EMAIL")
    aws_region = "us-east-1"
    subject = (
        str(len(logs)) + " Alert Fails for " + str(today) + " Python NBA Web Scrape"
    )
    body_html = message = """\
<h3>sup hoe here are the errors.</h3>
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


def send_email_function():
    """
    Email function that only executes & sends an email if there were errors for the run.

    Args:
        None

    Returns:
        Holds the actual send_email logic and executes if 1) there were errors and 2) if invoked as a script (aka on ECS)
    """
    try:
        if len(logs) > 0:
            print("Sending Email")
            send_aws_email()
        elif len(logs) == 0:
            print("No Errors!")
            # DONT SEND EMAIL
    except ValueError:
        print("oof")


print("STARTING WEB SCRAPE")
logging.info("STARTING WEB SCRAPE")

reddit = praw.Reddit(
    client_id=os.environ.get("reddit_accesskey"),
    client_secret=os.environ.get("reddit_secretkey"),
    user_agent="praw-app",
    username=os.environ.get("reddit_user"),
    password=os.environ.get("reddit_pw"),
)

# stats = get_player_stats()
boxscores = get_boxscores()
injury_data = get_injuries()
transactions = get_transactions()
adv_stats = get_advanced_stats()
odds = get_odds()
reddit_data = scrape_subreddit("nba")
pbp_data = get_pbp_data(boxscores)

print("FINISHED WEB SCRAPE")
logging.info("FINISHED WEB SCRAPE")

print("STARTING SQL STORING")
logging.info("STARTING SQL STORING")
# storing all data to SQL
conn = sql_connection()
# write_to_sql(stats, "replace")
write_to_sql(boxscores, "append")
write_to_sql(injury_data, "append")
write_to_sql(transactions, "append")  # fix this replace thing 
write_to_sql(adv_stats, "append")
write_to_sql(odds, "append")
write_to_sql(reddit_data, "append")
write_to_sql(pbp_data, "append")

logs = pd.read_csv("example.log", sep=r"\\t", engine="python", header=None)
logs = logs.rename(columns={0: "errors"})
logs = logs.query("errors.str.contains('Failed')", engine="python")

if __name__ == "__main__":
    send_email_function()

print("WOOT FINISHED")
