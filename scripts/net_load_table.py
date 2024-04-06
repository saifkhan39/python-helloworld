# %%
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, date, timedelta
from yesapi.functions import *
import urllib
from sqlalchemy import create_engine

#yes api items
#items = 'ASM_DA_REGDOWN:10000756298,ASM_DA_REGUP:10000756298,ASM_DA_RRS:10000756298,ASM_DA_NONSPIN:10000756298,DALMP:10000698382,RTLMP:10000698382,NET_LOAD_FORECAST_BID_CLOSE:10000712973,NET_LOAD_RT:10000712973'

#w/ ecrs
items = 'ASM_DA_REGDOWN:10000756298,ASM_DA_REGUP:10000756298,ASM_DA_RRS:10000756298,ASM_DA_NONSPIN:10000756298,ASM_DA_ECRS:10000756298,DALMP:10000698382,RTLMP:10000698382,NET_LOAD_FORECAST_BID_CLOSE:10000712973,NET_LOAD_RT:10000712973'

#pull data
df = pull_yes_data(items, today(-1), today(-1))
#clean and rename columns
df = clean(df, keep_datetime=True)
df.columns = ['RegDn', 'RegUp', 'RRS', 'NSPIN', 'ECRS', 'HB_HUBAVG_DA', 'HB_HUBAVG_RT', 'NET_LOAD_RT', 'NET_LOAD_FORECAST_BID_CLOSE', 'HE', 'trade_date']
#perform manipulations
df['trade_date'] = pd.to_datetime(df['trade_date'])
df['netload_ramp_rate'] = df.NET_LOAD_RT.diff().round(2)
df = df.reset_index()

df = df[['DATETIME', 'trade_date', 'HE', 'RegDn', 'RegUp', 'RRS', 'NSPIN', 'ECRS', 'HB_HUBAVG_DA', 'HB_HUBAVG_RT', 'NET_LOAD_RT', 'NET_LOAD_FORECAST_BID_CLOSE', 'netload_ramp_rate']]

# %%
#### WRITE TO DATABASE ####
#function to connect to database
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

engine = dbConnect()

#write tables to sql
df.to_sql('netload_table',con=engine,index=False,method=None,if_exists='append')

# %%
