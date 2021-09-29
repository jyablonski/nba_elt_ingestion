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


def test_basketball_reference_responsive(bbref_url):
   response = requests.get(bbref_url)
   assert response.status_code == 200

def test_draftkings_responsive(draftkings_url):
   response = requests.get(draftkings_url)
   assert response.status_code == 200


def test_get_player_stats():
      year = 2021
      url = "https://www.basketball-reference.com/leagues/NBA_{}_per_game.html".format(year)
      html = urlopen(url)
      soup = BeautifulSoup(html, "html.parser")

      headers = [th.getText() for th in soup.findAll('tr', limit=2)[0].findAll('th')]
      headers = headers[1:]

      rows = soup.findAll('tr')[1:]
      player_stats = [[td.getText() for td in rows[i].findAll('td')]
         for i in range(len(rows))]

      stats = pd.DataFrame(player_stats, columns = headers)
      stats['PTS'] = pd.to_numeric(stats['PTS'])
      stats.columns = stats.columns.str.lower()
      assert len(stats) > 0


def test_get_boxscores(month = 12, day = 23, year = 2020):
      url = "https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={}&day={}&year={}&type=all".format(month, day, year)
      html = urlopen(url)
      soup = BeautifulSoup(html, "html.parser")


      headers = [th.getText() for th in soup.findAll('tr', limit=2)[0].findAll('th')]
      headers = headers[1:]
      headers[1] = 'Team'
      headers[2] = "Location"
      headers[3] = 'Opponent'
      headers[4] = "Outcome"
      headers[6] = "FGM"
      headers[8] = "FGPercent"
      headers[9] = "threePFGMade"
      headers[10] = "threePAttempted"
      headers[11] = "threePointPercent"
      headers[14] = "FTPercent"
      headers[15] = "OREB"
      headers[16] = "DREB"
      headers[24] = 'PlusMinus'

      rows = soup.findAll('tr')[1:]
      player_stats = [[td.getText() for td in rows[i].findAll('td')]
         for i in range(len(rows))]

      df = pd.DataFrame(player_stats, columns = headers)
      df[['FGM', 'FGA', 'FGPercent', 'threePFGMade', 'threePAttempted', 'threePointPercent', 'OREB', 'DREB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PlusMinus', 'GmSc']] = df[['FGM', 'FGA', 'FGPercent', 'threePFGMade', 'threePAttempted', 'threePointPercent','OREB', 'DREB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PlusMinus', 'GmSc']].apply(pd.to_numeric)
      df['Date'] = datetime.now()
      df['Type'] = 'Regular Season'
      df['Season'] = 2022
      df['Location'] = df['Location'].apply(lambda x: 'A' if x == '@' else 'H')
      df['Team'] = df['Team'].str.replace("PHO", "PHX")
      df['Team'] = df['Team'].str.replace("CHO", "CHA")
      df['Team'] = df['Team'].str.replace("BRK", "BKN")
      df['Opponent'] = df['Opponent'].str.replace("PHO", "PHX")
      df['Opponent'] = df['Opponent'].str.replace("CHO", "CHA")
      df['Opponent'] = df['Opponent'].str.replace("BRK", "BKN")
      df.columns = df.columns.str.lower()
      assert len(df) > 0


def test_get_injuries():
      url = "https://www.basketball-reference.com/friv/injuries.fcgi"
      df = pd.read_html(url)[0]
      df = df.rename(columns = {"Update": "Date"})
      df.columns = df.columns.str.lower()
      assert len(df) > 0

def test_get_transactions():
    url = "https://www.basketball-reference.com/leagues/NBA_2022_transactions.html"
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")
    trs = soup.findAll('li')[50:] # theres a bunch of garbage in the first 50 rows - no matter what 
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
    transactions.columns = transactions.columns.str.lower()
    assert len(transactions) > 0

def test_get_advanced_stats():
      url = "https://www.basketball-reference.com/leagues/NBA_2021.html"
      df = pd.read_html(url)
      df = pd.DataFrame(df[10])
      df.drop(columns = df.columns[0], 
         axis=1, 
         inplace=True)

      df.columns = ['Team', 'Age', 'W', 'L', 'PW', 'PL', 'MOV', 'SOS', 'SRS', 'ORTG', 'DRTG', 'NRTG', 'Pace', 'FTr', '3PAr', 'TS%', 'bby1', 'eFG%', 'TOV%', 'ORB%', 'FT/FGA', 'bby2', 'eFG%_opp', 'TOV%_opp', 'DRB%_opp', 'FT/FGA_opp', 'bby3', 'Arena', 'Attendance', 'Att/Game']
      df.drop(['bby1', 'bby2', 'bby3'], axis = 1, inplace = True)
      df = df.query('Team != "League Average"')
      df['Team'] = df['Team'].str.replace("*", "", regex = True)
      df.columns = df.columns.str.lower()
      assert len(df) > 0

# no pbp test, no odds test
