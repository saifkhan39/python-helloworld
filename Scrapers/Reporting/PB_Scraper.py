# %%
import pandas as pd
import numpy as np
from urllib.request import urlopen
from io import StringIO, BytesIO
import zipfile
import datetime as dt
import time
from datetime import timedelta, datetime
import pyodbc, io
#, psutil
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import requests, json
import urllib
from io import StringIO
import itertools
import os
date = (dt.date.today() - timedelta(days = 90)).strftime('%Y%m%d')
from galaxy_vault.factory import VaultFactory 

def bids(date, market):

    parameters = {
    'resultformat': '6',
    'version': '3',
    'groupid': 'PUB_' + market + '_GRP',
    'startdatetime': date + 'T08:00-0000'
    }
    factory = VaultFactory()
    vault = factory.get_vault()
    url = vault.get_secret('caiso-oasis-url')+'/oasisapi/GroupZip?'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
    query = requests.get(url, params=parameters, stream=True, headers=headers)
    ###Get zip file

    file_temp = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    with file_temp.open(file_temp.namelist()[0]) as csv_in_zip:
      df = pd.read_csv(csv_in_zip)
    #file_temp.close()
    #file_temp.__del__()
     ##Drop GMT colums
    #df = df.drop(columns=['STARTTIME_GMT', 'STOPTIME_GMT', 'TIMEINTERVALSTART_GMT', 'TIMEINTERVALEND_GMT', 'SCH_BID_TIMEINTERVALSTART_GMT', 'SCH_BID_TIMEINTERVALSTOP_GMT'])

    ##Convert datatypes to Datetime
    df.STARTTIME =  pd.to_datetime(df.STARTTIME, format='%Y-%m-%d %H:%M:%S')
    df.STOPTIME =  pd.to_datetime(df.STOPTIME, format='%Y-%m-%d %H:%M:%S')
    ###Subtract an hour from Stoptime to prevent double counting
    # df.STOPTIME = df.STOPTIME - timedelta(hours=1)

    df = df[['STARTTIME', 'STOPTIME', 'MARKET_RUN_ID', 'RESOURCE_TYPE', 'SCHEDULINGCOORDINATOR_SEQ', 'RESOURCEBID_SEQ', 'MARKETPRODUCTTYPE', 'SELFSCHEDMW', 'SCH_BID_XAXISDATA', 'SCH_BID_Y1AXISDATA', 'SCH_BID_Y2AXISDATA', 'SCH_BID_CURVETYPE', 'MINEOHSTATEOFCHARGE', 'MAXEOHSTATEOFCHARGE']]

    df['STARTTIME_HE'] = (df['STARTTIME'].dt.hour+1)

    df['STOPTIME_HE'] = np.where((df['STOPTIME'].dt.hour == 0), 24, (df['STOPTIME'].dt.hour))

    return df

def dbConnect():
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    database = "CAISOMarketData"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)

    return(engine)

test = dbConnect()

ts = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

##DAM
DA = bids(date, 'DAM')
DA['Update_time'] = ts
DA.to_sql('DA_90_BidData', con=test, index=False, method=None, if_exists='append', chunksize=100000)
print('DA Bids from '+date+' uploaded')
# %%
##RTM
RT = bids(date, 'RTM')
RT['Update_time'] = ts
RT.to_sql('RT_90_BidData', con=test, index=False, method=None, if_exists='append', chunksize=500000)
print('RT Bids from '+date+' uploaded')
# %%
