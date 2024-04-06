# %%
import pandas as pd
import numpy as np
from urllib.request import urlopen
from io import StringIO, BytesIO
import zipfile
import datetime as dt
import time
from datetime import timedelta, datetime
import pyodbc, io, psutil
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import requests, json
import urllib
from io import StringIO
import itertools
import os

import yesapi
from galaxy_vault.factory import VaultFactory 

#from yesapi.functions import *
os.chdir(r"C:\Users\JohnMcMahon\BroadReachPower\BRP - Documents\Cap and Trade\Analyst Folders\McMahon\Python\Dash Apps")
import other_functions
from other_functions import pull_vns

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
date = (dt.date.today() + dt.timedelta(days=-1)).strftime('%m/%d/%Y')

# start = (dt.date(2021,1,1)).strftime('%m/%d/%Y')
# end = (dt.date.today() + dt.timedelta(days=-1)).strftime('%m/%d/%Y')

vns_df = pull_vns('caiso', date, date)
vns_df = vns_df.drop(columns=['MONTH', 'YEAR'])
vns_df['Update_time'] = ts

vns_df.to_sql('temp_caiso_vns', con=test, index=False, method=None, if_exists='append', chunksize=500000)

###CB AWARDS
def cb_awards(date):
    #XXX
    parameters = {
    'resultformat': '6',
    'queryname': 'ENE_CB_CLR_AWARDS',
    'version': '1',
    'startdatetime': date + 'T08:00-0000',
    'enddatetime': date + 'T08:00-0000',
    'NODE_ID': 'ALL'
    }
    factory = VaultFactory()
    vault = factory.get_vault()
    query = requests.get(vault.get_secret('caiso-oasis-url')+'/oasisapi/SingleZip?', params=parameters)

    ###Get zip file
    file = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    df = pd.read_csv(file.open(zipfile.ZipFile.namelist(file)[0]))
    df = df.drop(columns=df.loc[:,df.columns.str.contains('GMT')])
    df.loc[:,df.columns.str.contains('GMT')]

    return df


date2 = (dt.date.today() + dt.timedelta(days=-1)).strftime('%Y%m%d')
awards_df = cb_awards(date2)

awards_df.OPR_DT = pd.to_datetime(awards_df.OPR_DT, format='%Y-%m-%d')

awards_df['Datetime'] = pd.to_datetime(awards_df.OPR_DT + (awards_df.OPR_HR - 1).astype('timedelta64[h]'), format='%m-%d-%Y%H:%M:%S')

awards_df['Update_time'] = ts

awards_df.to_sql('temp_net_cb_awards', con=test, index=False, method=None, if_exists='append', chunksize=400000)
# %%
