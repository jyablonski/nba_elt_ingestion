import os
from urllib.request import urlopen
import pandas as pd
from sqlalchemy import exc, create_engine
from bs4 import BeautifulSoup
import pymysql

print('LOADED FUNCTIONS')


def get_transactions():
    url = "https://www.basketball-reference.com/leagues/NBA_2021_transactions.html"
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")
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
    print(f'Transactions Function Successful, retrieving {len(transactions)} rows')
    return(transactions)
    
df = get_transactions()    
d = {'col1': [1, 2], 'col2': [3, 4]}
df_raw = pd.DataFrame(data=d)
df_raw

def sql_connection():
    try:
        connection = create_engine('mysql+pymysql://' + os.environ.get('RDS_USER') + ':' + os.environ.get('RDS_PW') + '@' + os.environ.get('IP') + ':' + '3306' + '/' + os.environ.get('RDS_DB'),
                     echo = False)
        print('SQL Connection Successful')
        return(connection)
    except exc.SQLAlchemyError as e:
        print('SQL Connection Failed, Error:', e)
        return e

print('STARTING HANDLER')
conn = sql_connection()
# df = get_transactions()
df.to_sql(con=conn, name="transactions", index=False, if_exists="replace")
df_raw.to_sql(con=conn, name="df_latest2", index=False, if_exists="replace")
print('WOOT FINISHED')