import os
import logging
from urllib.request import urlopen
from datetime import datetime, timezone, timedelta
import pandas as pd
import praw
from bs4 import BeautifulSoup
from sqlalchemy import exc, create_engine
import pymysql

logging.basicConfig(filename='example.log', level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info('Starting Logging Function')

print('LOADED FUNCTIONS')
logging.info('LOADED FUNCTIONS')

today = datetime.now().date()
yesterday = today - timedelta(1)
day = (datetime.now() - timedelta(1)).day
month = (datetime.now() - timedelta(1)).month
year = (datetime.now() - timedelta(1)).year

def get_player_stats():
    try:
        year = 2021
        url = "https://www.basketball-reference.com/leagues/NBA_{}_per_game.html".format(year)
        html = urlopen(url)
        soup = BeautifulSoup(html)

        headers = [th.getText() for th in soup.findAll('tr', limit=2)[0].findAll('th')]
        headers = headers[1:]

        rows = soup.findAll('tr')[1:]
        player_stats = [[td.getText() for td in rows[i].findAll('td')]
            for i in range(len(rows))]

        stats = pd.DataFrame(player_stats, columns = headers)
        stats['PTS'] = pd.to_numeric(stats['PTS'])
        logging.info(f'General Stats Function Successful, retrieving {len(stats)} updated rows')
        print(f'General Stats Function Successful, retrieving {len(stats)} updated rows')
        return(stats)
    except ValueError:
        logging.info("General Stats Function Failed for Today's Games")
        print("General Stats Function Failed for Today's Games")
        df = []
        return(df)

def get_boxscores(month = month, day = day, year = year):
    url = "https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={}&day={}&year={}&type=all".format(month, day, year)
    html = urlopen(url)
    soup = BeautifulSoup(html)

    try: 
        headers = [th.getText() for th in soup.findAll('tr', limit=2)[0].findAll('th')]
        headers = headers[1:]
        headers[2] = "Location"
        headers[4] = "Outcome"

        rows = soup.findAll('tr')[1:]
        player_stats = [[td.getText() for td in rows[i].findAll('td')]
            for i in range(len(rows))]

        df = pd.DataFrame(player_stats, columns = headers)
        df[['FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc']] = df[['FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc']].apply(pd.to_numeric)

        df.sort_values('PTS', ascending = False)
        logging.info(f'Box Score Function Successful, retrieving {len(df)} rows for {yesterday}')
        print(f'Box Score Function Successful, retrieving {len(df)} rows for {yesterday}')
        return(df)
    except IndexError:
        logging.info(f"Box Score Function Failed, no data available for {yesterday}")
        print(f"Box Score Function Failed, no data available for {yesterday}")
        df = []
        return(df)

def get_injuries():
    try:
        url = "https://www.basketball-reference.com/friv/injuries.fcgi"
        df = pd.read_html(url)[0]
        df = df.rename(columns = {"Update": "Date"})
        logging.info(f'Injury Function Successful, retrieving {len(df)} rows')
        print(f'Injury Function Successful, retrieving {len(df)} rows')
        return(df)
    except ValueError:
        logging.info("Injury Function Failed for Today's Games")
        print("Injury Function Failed for Today's Games")
        df = []
        return(df)

def get_transactions():
    url = "https://www.basketball-reference.com/leagues/NBA_2021_transactions.html"
    html = urlopen(url)
    soup = BeautifulSoup(html)
    trs = soup.findAll('li')[71:] # theres a bunch of garbage in the first 71 rows - no matter what 
    rows = []
    mylist = []
    for tr in trs:
        date = tr.find('span')
        if date is not None: # needed bc span can be null (multi <p> elements per span)
            date = date.text
        data = tr.findAll('p')
        for p in data:
            mylist.append(p.text)
        data3 = [date] + [mylist]
        rows.append(data3)
        mylist = []

    transactions = pd.DataFrame(rows)
    transactions.columns = ['Date', 'Transaction']
    transactions = transactions.explode('Transaction')
    transactions['Date'] = pd.to_datetime(transactions['Date'])
    transactions = transactions.query('Date != "NaN"')
    transactions
    logging.info(f'Transactions Function Successful, retrieving {len(transactions)} rows')
    print(f'Transactions Function Successful, retrieving {len(transactions)} rows')
    return(transactions)

def get_advanced_stats():
    try:
        url = "https://www.basketball-reference.com/leagues/NBA_2021.html"
        df = pd.read_html(url)
        df = pd.DataFrame(df[10])
        df.drop(columns = df.columns[0], 
            axis=1, 
            inplace=True)

        df.columns = ['Team', 'Age', 'W', 'L', 'PW', 'PL', 'MOV', 'SOS', 'SRS', 'ORTG', 'DRTG', 'NRTG', 'Pace', 'FTr', '3PAr', 'TS%', 'bby1', 'eFG%', 'TOV%', 'ORB%', 'FT/FGA', 'bby2', 'eFG%_opp', 'TOV%_opp', 'DRB%_opp', 'FT/FGA_opp', 'bby3', 'Arena', 'Attendance', 'Att/Game']
        df.drop(['bby1', 'bby2', 'bby3'], axis = 1, inplace = True)
        df = df.query('Team != "League Average"')
        logging.info(f'Advanced Stats Function Successful, retrieving updated data for 30 Teams')
        print(f'Advanced Stats Function Successful, retrieving updated data for 30 Teams')
        return(df)
    except ValueError:
        logging.info("Advanced Stats Function Failed for Today's Games")
        print("Advanced Stats Function Failed for Today's Games")
        df = []
        return(df)

def get_odds():
    try:
        url = "https://sportsbook.draftkings.com/leagues/basketball/88673861?category=game-lines&subcategory=game"
        df = pd.read_html(url)
        data1 = df[0]
        data2 = df[1]
        data2 = data2.rename(columns = {"Tomorrow": "Today"})
        data = data1.append(data2)
        data
        data['SPREAD'] = data['SPREAD'].str[:-4]
        data['TOTAL'] = data['TOTAL'].str[:-4]
        data['TOTAL'] = data['TOTAL'].str[2:]
        data.reset_index(drop = True)
        data

        data['Today'] = data['Today'].str.replace("AM|PM", " ")
        data['Today'] = data['Today'].str.split().str[1:2]
        data['Today'] = pd.DataFrame([str(line).strip('[').strip(']').replace("'","") for line in data['Today']])
        data = data.rename(columns = {"Today": "team", "SPREAD": "spread", "TOTAL": "total_pts", "MONEYLINE": "moneyline"})
        logging.info(f'Odds Function Successful, retrieving {len(data)} rows')
        print(f'Odds Function Successful, retrieving {len(data)} rows')
        return(data)
    except ValueError:
        logging.info("Odds Function Failed for Today's Games")
        print("Odds Function Failed for Today's Games")
        data = []
        return(data)


def scrape_subreddit(sub):
    subreddit = reddit.subreddit(sub)
    posts = []
    for post in subreddit.hot(limit=27):
        posts.append([post.title, post.score, post.id, post.subreddit, post.url, post.num_comments, post.selftext,
                    today, todaytime
        ])
    posts = pd.DataFrame(posts,columns=['title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'scrape_date', 'scrape_time'])
    print('Grabbing 27 Recent popular posts from r/' + sub + ' subreddit')
    logging.info('Grabbing 27 Recent popular posts from r/' + sub + ' subreddit')
    return(posts)

def sql_connection():
    try:
        connection = create_engine('mysql+pymysql://' + os.environ.get('RDS_USER') + ':' + os.environ.get('RDS_PW') + '@' + os.environ.get('IP') + ':' + '3306' + '/' + os.environ.get('RDS_DB'),
                     echo = False)
        logging.info('SQL Connection Successful')
        print('SQL Connection Successful')
        return(connection)
    except exc.SQLAlchemyError as e:
        logging.info('SQL Connection Failed, Error:', e)
        print('SQL Connection Failed, Error:', e)
        return(e)

def write_to_sql(data, table_type):
    data_name = [ k for k,v in globals().items() if v is data][0]
    if len(data) == 0:
        print(data_name + " Failed, not writing to SQL")
        logging.info(data_name + " Failed, not writing to SQL")
    else:
        # ^ this disgusting monstrosity is to get the name of the -fucking- dataframe lmfao
        data.to_sql(con = conn, name = ("aws_" + data_name + "_table"), index = False, if_exists = table_type)
        print("Writing " + data_name + " table to SQL")
        logging.info("Writing " + data_name + " table to SQL")


print('STARTING WEB SCRAPE')
logging.info('STARTING WEB SCRAPE')

reddit = praw.Reddit(client_id = os.environ.get('reddit_accesskey'),
                     client_secret = os.environ.get('reddit_secretkey'),
                     user_agent = 'praw-app',
                     username = os.environ.get('reddit_user'),
                     password = os.environ.get('reddit_pw'))
            
stats = get_player_stats()
boxscores = get_boxscores(month = month, day = yesterday, year = year)
injury_data = get_injuries()
transactions = get_transactions()
adv_stats = get_advanced_stats()
odds = get_odds()
reddit_data = scrape_subreddit('nba')

print('FINISHED WEB SCRAPE')
logging.info('FINISHED WEB SCRAPE')

print('STARTING SQL STORING')
logging.info('STARTING SQL STORING')
# storing all data to SQL
conn = sql_connection()
write_to_sql(stats, "replace")
write_to_sql(boxscores, "append")
write_to_sql(injury_data, "append")
write_to_sql(transactions, "replace")
write_to_sql(adv_stats, "replace")
write_to_sql(odds, "append")
write_to_sql(reddit_data, "append")


print('WOOT FINISHED')