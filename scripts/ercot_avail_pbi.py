import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
from lxml import html
import requests
import time
import csv
import re
from bs4 import BeautifulSoup
from io import StringIO
import itertools
import urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import pyodbc


def queryDB(sql_string):
    driver = '{ODBC Driver 17 for SQL Server}'
    server = "brptemp.database.windows.net"
    database = 'CaisoMarketData'
    username = "brp_admin"
    password = "Bro@dRe@chP0wer"

    conn_string = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

sql = """SELECT * FROM [dbo].[temp_ercot_avail]"""

bt = queryDB(sql)

start = (bt.OperatingDate.max() + timedelta(days = 1)).strftime('%Y-%m-%d')

def queryDB(database: str, sql_string: str):
    driver = '{ODBC Driver 17 for SQL Server}'
    server = "tcp:10.128.2.11,1433"
    database = '{0}'.format(database)
    username = "brptrading"
    password = "Brptr8ding#"

    conn_string = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

sql = """SELECT * FROM [dbo].[ERCOT_Availability]"""

raw = queryDB('MarketData', sql)

df = raw[['UnitName', 'FromDateTime', 'SchedType', 'SchedValue', 'OperatingDate']]

df = df[df.OperatingDate >= start]


df['MAX SOC'] = df[df.SchedType == 'SOC_HIGH'].groupby(['FromDateTime', 'SchedType', 'UnitName'])['SchedValue'].transform('sum')
df['MIN SOC'] = df[df.SchedType == 'SOC_LOW'].groupby(['FromDateTime', 'SchedType', 'UnitName'])['SchedValue'].transform('sum')

df['GEN HSL'] = df[df.SchedType == 'HSL_GEN'].groupby(['FromDateTime', 'SchedType', 'UnitName'])['SchedValue'].transform('sum')
df['LOAD HSL'] = df[df.SchedType == 'HSL_LOAD'].groupby(['FromDateTime', 'SchedType', 'UnitName'])['SchedValue'].transform('sum')


df['RRS'] = df[df.SchedType.str.contains('RRS')].groupby(['FromDateTime', 'UnitName'])['SchedValue'].transform('sum')
df['REGUP'] = df[df.SchedType.str.contains('REGUP')].groupby(['FromDateTime', 'UnitName'])['SchedValue'].transform('sum')
df['REGDN'] = df[df.SchedType.str.contains('REGDN')].groupby(['FromDateTime', 'UnitName'])['SchedValue'].transform('sum')
df['NSPIN'] = df[df.SchedType.str.contains('NSPIN')].groupby(['FromDateTime', 'UnitName'])['SchedValue'].transform('sum')

df = pd.melt(df,id_vars=['UnitName', 'FromDateTime', 'SchedType', 'SchedValue','OperatingDate'], var_name='Metric2', value_name='Value2')

df = df.dropna()

df['FLEET MAX SOC'] = df[df.Metric2 == 'MAX SOC'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')
df['FLEET MIN SOC'] = df[df.Metric2 == 'MIN SOC'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')

df['FLEET GEN HSL'] = df[df.Metric2 == 'GEN HSL'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')
df['FLEET LOAD HSL'] = df[df.Metric2 == 'LOAD HSL'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')


df['FLEET RRS'] = df[df.Metric2 == 'RRS'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')
df['FLEET REGUP'] = df[df.Metric2 == 'REGUP'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')
df['FLEET REGDN'] = df[df.Metric2 == 'REGDN'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')
df['FLEET NSPIN'] = df[df.Metric2 == 'NSPIN'].groupby(['FromDateTime', 'SchedType'])['Value2'].transform('sum')


df = pd.melt(df,id_vars=['UnitName', 'FromDateTime', 'SchedType', 'SchedValue','OperatingDate', 'Metric2', 'Value2'], var_name='FleetMetric', value_name='FleetValue')

df = df.dropna()

df = df[['UnitName', 'FromDateTime', 'OperatingDate', 'Metric2', 'Value2', 'FleetMetric', 'FleetValue']]

df = df.drop_duplicates()


def dbConnect():
    server = "brptemp.database.windows.net"
    database = "CAISOMarketData"
    username = "brp_admin"
    password = "Bro@dRe@chP0wer"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)

    return(engine)

test = dbConnect()

df.to_sql('temp_ercot_avail', con=test, index=False, method=None, if_exists='append')
