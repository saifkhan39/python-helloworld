# %%
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, timedelta
from yesapi.functions import *
import plotly.graph_objs as go
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
import itertools
import pyodbc
import sqlalchemy, urllib
from sqlalchemy import create_engine

#### Functions and Global Variables ####
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

#houston/brazoria county assets
houston_braz = houston_assets + brazoria_assets
batcave = ['BATCAVE_RN']
hundreds = ['BATCAVE_RN', 'NF_BRP_RN']
pueblos = ['BRP_PBL1_RN']
zapatas = ['BRP_ZPT1_RN']
odessa = ['ODESW_RN']

#sql string
sql = 'select * from proxy_prices'
#query data
raw_df = queryDB('ErcotMarketData', sql)
#empty dataframe to append to
upload_df = pd.DataFrame()
#for loop through different strips
for strip in ['BD', 'ND', 'ND+1']:
    #select strip
    df = raw_df[raw_df.strip == strip]
    #clean up columns
    df = df.set_index(['HOURENDING']).drop(['update_time', 'strip', 'DATETIME', 'MARKETDAY'], axis=1)
    #calculate heat rate
    df = df.iloc[:,1:].div(df.hsc, axis=0).round(2)
    #select rt asset only columns
    df = df[[col for col in df.columns if '_rt' in col]].iloc[:, :17]
    #rename
    df.columns = all_assets
    #create empty list to append to
    proxy_heat_rates = []

    #for loop through assets and calcuate heat rate quantiles
    for i in all_assets:
        for p in np.arange(.05, 1.0, .05):
            heat_rates = pd.DataFrame(df.groupby(df.index)[i].quantile(p.round(2)))
            heat_rates.columns = ['heat_rate']
            heat_rates['asset'], heat_rates['percentile'] = [i, p.round(2)]
            proxy_heat_rates.append(heat_rates)
    #append to emtpy list
    proxy_heat_rates = pd.concat(proxy_heat_rates)
    #pull today's gas price
    gas = pull_gas('hsc', today(), today(), 'daily').iloc[0,1]
    #calculate price
    proxy_heat_rates['price'] = round(proxy_heat_rates['heat_rate'] * gas, 2)

    #houston/brazoria county grouping
    houston = proxy_heat_rates[proxy_heat_rates.asset.isin(houston_braz)]
    houston = pd.DataFrame(houston.groupby([houston.index, 'percentile'])['price'].mean().round(2)).reset_index()
    houston['grouping'] = 'houston_brazoria'
    #batcave
    bc = proxy_heat_rates[proxy_heat_rates.asset.isin(batcave)]
    bc = pd.DataFrame(bc.groupby([bc.index, 'percentile'])['price'].mean().round(2)).reset_index()
    bc['grouping'] = 'bc'
    #hundreds
    bcnf = proxy_heat_rates[proxy_heat_rates.asset.isin(hundreds)]
    bcnf = pd.DataFrame(bcnf.groupby([bcnf.index, 'percentile'])['price'].mean().round(2)).reset_index()
    bcnf['grouping'] = 'bcnf'
    #pueblos
    pbls = proxy_heat_rates[proxy_heat_rates.asset.isin(pueblos)]
    pbls = pd.DataFrame(pbls.groupby([pbls.index, 'percentile'])['price'].mean().round(2)).reset_index()
    pbls['grouping'] = 'pueblos'
    #zapatas
    zpts = proxy_heat_rates[proxy_heat_rates.asset.isin(zapatas)]
    zpts = pd.DataFrame(zpts.groupby([zpts.index, 'percentile'])['price'].mean().round(2)).reset_index()
    zpts['grouping'] = 'zapatas'
    #odessa
    odesw = proxy_heat_rates[proxy_heat_rates.asset.isin(odessa)]
    odesw = pd.DataFrame(odesw.groupby([odesw.index, 'percentile'])['price'].mean().round(2)).reset_index()
    odesw['grouping'] = 'odessa'

    final = pd.concat([houston, bc, bcnf, pbls, zpts, odesw]).reset_index(drop=True)
    final['strip'] = strip

    #upload_df.append(final)
    upload_df = pd.concat([upload_df, final])

#bd, nd, nd+1 constants
bd = date.today()
nd = date.today()+timedelta(days=1)
nd_1 = date.today()+timedelta(days=2)

#trade date column
upload_df['trade_date'] = np.where(upload_df['strip']=='BD', bd, np.where(upload_df['strip']=='ND', nd, nd_1))

#update time column
upload_df['update_time'] = datetime.now()

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
upload_df.to_sql('proxy_prices_distributions',con=engine,index=False,method=None,if_exists='append')

# %%
