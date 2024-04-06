import zipfile, io, os, glob
from zipfile import ZipFile
from io import StringIO, BytesIO
from requests_pkcs12 import get
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, datetime, time
from lxml import etree
from pathlib import Path
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWRITE
from difflib import SequenceMatcher
import http.client, urllib.request, urllib.parse, urllib.error, base64
import requests
import json, gzip
from urllib.request import urlopen
import sqlalchemy, urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import pyodbc
from sqlalchemy.sql.expression import column
import pytz
import arrow
from time import sleep
from galaxy_vault.factory import VaultFactory 

##Dates
today = dt.date.today().strftime('%Y%m%d')

##DST
def dstCheck(date):
    utc = arrow.get(date)
    local = utc.to('US/Central')
    local_string = str(local)
    if local_string[-4] == '5':
        print('It is DST')
        return('DST')
    else:
        print('It is not DST')
        return('notDST')

factory = VaultFactory()
vault = factory.get_vault()
price_map_url = vault.get_secret('caiso-mobile-url')+'/Web.Service.Chart/api/v3/ChartService/PriceContourMap1'

r = requests.get(price_map_url)
json_obj = r.json()

node_entries = json_obj['l'][2]['m']
return_dict = [{'node_id': str(entry['n']),
                'latitude': entry['c'][0],
                'longitude': entry['c'][1],
                'area': str(entry['a']),
                'node_type': str(entry['p'])} for entry in node_entries]

df_lmp = pd.DataFrame.from_dict(return_dict)
df_lmp = df_lmp.rename(columns={"node_id": "NODE"})

def queryDB(database: str, sql_string: str):
    driver = '{ODBC Driver 17 for SQL Server}'
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    database = '{0}'.format(database)

    conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

sql = """SELECT *
  FROM [dbo].[temp_nodal_hist]
  WHERE [OPR_DT] = ( SELECT MAX([OPR_DT]) FROM [dbo].[temp_nodal_hist]);"""

df = queryDB('CAISOMarketData', sql)
df.OPR_HR = df.OPR_HR - 1
df['DATETIME'] = pd.to_datetime(df.OPR_DT) + df.OPR_HR.astype('timedelta64[h]')
df.OPR_DT = pd.to_datetime(df.OPR_DT)
df['avg_mw'] = df.groupby(['NODE', 'OPR_DT'])['MW'].transform('mean')
df['std_mw'] = df.groupby(['NODE', 'OPR_DT'])['MW'].transform('std')


tb4 = pd.DataFrame(df.groupby(['OPR_DT', 'NODE'])['MW'].apply(lambda grp: grp.nlargest(4).sum()) - df.groupby(['OPR_DT', 'NODE'])['MW'].apply(lambda grp: grp.nsmallest(4).sum())).reset_index()
tb4.columns = ['OPR_DT', 'NODE', 'tb4']

tb2 = pd.DataFrame(df.groupby(['OPR_DT', 'NODE'])['MW'].apply(lambda grp: grp.nlargest(2).sum()) - df.groupby(['OPR_DT', 'NODE'])['MW'].apply(lambda grp: grp.nsmallest(2).sum())).reset_index()
tb2.columns = ['OPR_DT', 'NODE', 'tb2']

frames = [df[['OPR_DT', 'NODE', 'avg_mw', 'std_mw', 'Update_time']],tb2,tb4]
import functools as ft
final_df = ft.reduce(lambda left, right: pd.merge(left, right, on=['OPR_DT', 'NODE']), frames).drop_duplicates()

combo = pd.merge(df_lmp,final_df, on='NODE')
combo = combo.round(2)

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

combo.to_sql('temp_node_mapping', con=test, index=False, method=None, if_exists='append', chunksize=500000)
