import zipfile, io, os, glob
from zipfile import ZipFile
from io import StringIO, BytesIO
from requests_pkcs12 import get
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, datetime, time
import requests
from bs4 import BeautifulSoup
from ercot_config import pfx_path, pfx_password
import sqlalchemy, urllib
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql import text as sa_text
from sqlalchemy.sql.expression import column
import pymsteams as tm
import pickle

#### SQL SECTION ####
# FUNCTIONS
#query db
def queryDB(database: str, sql_string: str):
    driver = '{ODBC Driver 17 for SQL Server}'
    server = "brptemp.database.windows.net"
    database = '{0}'.format(database)
    username = "brp_admin"
    password = "Bro@dRe@chP0wer"

    conn_string = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(conn_string), fast_executemany=True)
    conn = engine.connect()
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

#database connection function
def dbConnect():
    server = "brptemp.database.windows.net"
    database = "ErcotMarketData"
    username = "brp_admin"
    password = "Bro@dRe@chP0wer"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)
    return(engine)

#sql engine
engine = dbConnect()

# QUERY
sql = 'select top 1 * from [dbo].[hourly_ruc] order by RUCTimeStamp desc'
#pull latest entry and timestamp from sql
table = queryDB('ErcotMarketData', sql)
latest_ts = table.RUCTimeStamp[0]
latest_ts

#### MIS PULL ####
#mis scraper function
def pull_mis(reportTypeId):
    #get request to report
    r = get('https://mis.ercot.com/misapp/GetReports.do?reportTypeId={}&mimic_duns=1172089695000'.format(reportTypeId), pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #bytes to string
    html = r.content.decode('utf-8')
    #parse html string
    soup = BeautifulSoup(html, 'html.parser')
    #pull out friendly names and links
    names = [n.text for n in soup.find_all('td', class_='labelOptional_ind')]
    links = [l.get('href') for l in soup.find_all('a', href=True)]
    #combine to df and filter only csvs
    files = pd.DataFrame(list(zip(names, links)), columns=['Names', 'Links'])
    files = files[files['Names'].str.contains("_csv")].reset_index(drop = True)
    #create download url
    file_url = "https://mis.ercot.com/" + files.loc[0,'Links']
    #make request with certs
    cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #create empty dataframe
    master_data = pd.DataFrame()
    #extract from each daily zip file w/o downloading
    with ZipFile(BytesIO(cert_url.content)) as zfile:
        csvs = zfile.infolist()
        csv_list = []
        for csv in csvs:
            df = pd.read_csv(zfile.open(csv.filename))
            csv_list.append(df)
            data = pd.concat(csv_list)
    #append to empty dataframe created in for loop
    master_data = pd.concat([master_data, data])

    return master_data

df = pull_mis('10050')
df['DeliveryDate'] = pd.to_datetime(df['DeliveryDate'])
df['RUCTimeStamp'] = pd.to_datetime(df['RUCTimeStamp'])
#reorder
first = df.pop('RUCTimeStamp')
df.insert(0, 'RUCTimeStamp', first)
df = df.sort_values(['RUCTimeStamp', 'HourEnding'])

#ruc timestamp variable from latest mis posting
mis_ts = df.RUCTimeStamp[0]
mis_ts
#delivery date variable for message if a new posting
delivery_date = df.DeliveryDate[0].strftime('%m/%d/%Y')

#if statement to either write to database and send RT alert to teams or pass
if mis_ts == latest_ts:
    print('no new ruc posting')
    pass
else:
    print('new posting. updating database and sending teams alert')
    #update sql table
    df.to_sql('hourly_ruc',con=engine,index=False,method=None,if_exists='append')
    #send teams message
    #load dictionary via pickle file
    with open(r"C:\BRP_scrapers\capacity_dict.pkl", 'rb') as handle:
        capacity_dict = pickle.load(handle)

    df.columns = ['Timestamp', 'DeliveryDate', 'HourEnding','ResourceName', 'ClearedCommit', 'ClearedDecommit', 'Reason', 'StartType', 'overrideflag', 'idk', 'dst']
    df['ResourceName'] = df['ResourceName'].str.strip()
    df['Capacity'] = df['ResourceName'].map(capacity_dict)
    df = df[['HourEnding', 'ResourceName', 'ClearedCommit', 'Capacity', 'Reason']]
    ruc = df.pivot(index=['ResourceName', 'Capacity', 'Reason'], columns='HourEnding', values='ClearedCommit')

    #ruc dataframe to string
    ruc_html = ruc.to_html()

    #rt alert title string
    title_string = f'Hourly RUC for {delivery_date}'
    ### connector card and message ###
    webhook = "https://engie.webhook.office.com/webhookb2/a10dc229-a310-417e-88d8-7671d76b8a95@24139d14-c62c-4c47-8bdd-ce71ea1d50cf/IncomingWebhook/fe5437c230a94bf78891a7b37255f13f/00121146-f154-450c-87f0-cabfd6a4cbd9"
    connector = tm.connectorcard(webhook)
    connector.text(ruc_html)
    connector.title(title_string)
    connector.color('0000FF')
    connector.send()
