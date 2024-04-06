import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, date, timedelta
from yesapi.functions import *
import plotly.express as px
from dateutil.relativedelta import relativedelta
import sqlalchemy, urllib
from sqlalchemy import create_engine

#pull yesterday's 5min rtlmp's
df = pull_prices(all_assets, 'RTLMP', today(-1), today(-1), '5min')
#cleanup
final = clean(df)
final.columns = ['alvin', 'angleton', 'batcave', 'brazoria', 'dickinson', 'heights', 'loop463', 'lopeno', 'magnolia', 'northfork', 'odessa', 'pueblo1', 'pueblo2', 'ranchtown', 'sweeny', 'zapata1', 'zapata2']

final = pd.melt(final.reset_index(), id_vars='DATETIME', var_name='Asset', value_name='LMP').sort_values(['DATETIME', 'Asset']).reset_index(drop=True)

#pull ordc data
ordc = pull_ordc_data(today(-1), today(-1), '5min')
ordc = clean(ordc)
ordc = ordc[['RT_OR_PRADDER', 'RT_ORD_PRADDER']]

#merge
final = final.merge(ordc, how='left', on='DATETIME')

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
final.to_sql('5min_asset_lmps',con=engine,index=False,method=None,if_exists='append')
