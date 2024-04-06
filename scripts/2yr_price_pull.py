import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from yesapi.functions import *
from io import StringIO, BytesIO
import pickle
import requests
import json
import sqlalchemy, urllib
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

#GLOBAL VARIABLES and FUNCTIONS
#list to filter out winter storm days, august 2019, and dst days
storm = [d.strftime('%m/%d/%Y') for d in pd.date_range('2/11/2021','2/25/2021')]
#dst_days = ['03/14/2021', '11/07/2021']
dst_days = ['11/03/2019', '03/08/2020', '11/01/2020', '03/14/2021', '11/07/2021', '03/13/2022']

#date variables
start = (date.today() - relativedelta(years=2)).strftime('%m/%d/%Y')
end = today(-1)

## pull historical price data
master_price = pull_prices(all_hubs, 'dart', start, end)
master_price = clean(master_price, keep_datetime=True)

gas = pull_gas(start, end)
gas = clean(gas)

master_price = master_price.merge(gas, how='left', on='DATETIME')

#create update time column
update_time = datetime.today()

#add to front of dataframes
master_price.insert(0, 'update_time', update_time)
master_price.rename(columns={'Houston Ship Channel (GASPRICE)':'HSC'}, inplace=True)
master_price.columns = [col.replace(' (RTLMP)', '') for col in master_price.columns]
master_price.columns = [col.replace(' (DALMP)', '_DA') for col in master_price.columns]
master_price = master_price[(~master_price.MARKETDAY.isin(storm))&(~master_price.MARKETDAY.isin(dst_days))]
master_price['MARKETDAY'] = pd.to_datetime(master_price['MARKETDAY'])

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
master_price.to_sql('prev_2yr_hub_prices',con=engine,index=False,method=None,if_exists='replace')
