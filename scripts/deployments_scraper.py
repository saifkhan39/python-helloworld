import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, datetime, timedelta
from yesapi.functions import *
import pyodbc
import sqlalchemy, urllib
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql import text as sa_text
from sqlalchemy.sql.expression import column

## functions
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

#pull AS deployments by hour
df = pull_as_deployments(today(-1), today(-1))
#clean it up
deployments = clean(df).reset_index()
#shift datetime column
deployments['DATETIME'] = deployments['DATETIME'] + timedelta(hours=-1)
#rename columns
deployments.columns = ['hb', 'DRU', 'Reg Up Exhaustion', 'DRD', 'Reg Down Exhaustion']
#write to db
deployments.to_sql('Regulation_Deployments',con=engine,index=False,method=None,if_exists='append')

#pull AS deployments by SPP
df = pull_as_deployments(today(-1), today(-1), 'raw')
#clean it up
deployments = clean(df).resample('15min').mean().round(4).reset_index()
#drop unnecessary columns
deployments = deployments.drop(['deployed_regup', 'deployed_regdn'], axis=1)
#add Date and HE columns
deployments['Date'] = pd.to_datetime(deployments.DATETIME.dt.date)
deployments['HourEnding'] = deployments.DATETIME.dt.hour + 1
#rename columns
deployments.columns = ['datetime', 'Exhaustion_RU', 'Exhaustion_RD', 'Date', 'HourEnding']
deployments = deployments[['Date', 'HourEnding', 'Exhaustion_RU', 'Exhaustion_RD', 'datetime']]
#write to db
deployments.to_sql('Reg_Deployment_SPP',con=engine,index=False,method=None,if_exists='append')

#calculate deployment forecast (rolling average of last 7 days)
rolling = pull_as_deployments(today(-7), today(-1))
#groupby hour ending and average
rolling = pd.DataFrame(rolling.groupby('HOURENDING')[['regup_deployment_rate', 'regdn_deployment_rate']].mean()).reset_index()
#add forecast date and datetime columns
rolling['Date'] = pd.to_datetime(date.today() + timedelta(days=1))
rolling['datetime'] = (rolling['Date'] + pd.to_timedelta(rolling['HOURENDING'], unit='H')) + timedelta(hours=-1)
#rename and reorganize columns
rolling.columns = ['HourEnding', 'Exhaustion_RU', 'Exhaustion_RD', 'Date', 'datetime']
rolling = rolling[['Date', 'HourEnding', 'Exhaustion_RU', 'Exhaustion_RD', 'datetime']]
#write to db
rolling.to_sql('Reg_Deployment_Forecast',con=engine,index=False,method=None,if_exists='append')
