# %%
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from io import StringIO, BytesIO
import pickle
import requests
import json
import sqlalchemy, urllib
from sqlalchemy import create_engine

from yesapi.functions import *
from other_functions import *
from galaxy_vault.factory import VaultFactory 


# %%
start = (dt.date.today() - relativedelta(years=2)).strftime('%m/%d/%Y')
end = (dt.date.today() - dt.timedelta(days=1)).strftime('%m/%d/%Y')

# start = '1/22/2023'

df = pd.concat([clean(pull_prices(ca_hubs, 'DART', start, end)), clean(pull_gas(start,end)).iloc[:, :2]], axis=1)
df = df.shift(freq='-1H')
df.columns = ['TH_NP15_GEN-APND_da', 'TH_NP15_GEN-APND', 'TH_ZP26_GEN-APND_da', 'TH_ZP26_GEN-APND', 'TH_SP15_GEN-APND_da', 'TH_SP15_GEN-APND', 'SCG', 'PGECG']

df['HOURENDING'] = df.index.hour + 1
df['MARKETDAY'] = df.index.date
df['MARKETDAY'] = pd.to_datetime(df['MARKETDAY'])
df = df.reset_index()
df = df.drop(columns=['DATETIME'])
update_time = datetime.now()
df.insert(0, 'update_time', update_time)
# df = df.set_index('update_time')

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

engine = dbConnect()

df.to_sql('prev_2yr_hub_prices',con=engine,index=False,method=None,if_exists='replace')

# %%
