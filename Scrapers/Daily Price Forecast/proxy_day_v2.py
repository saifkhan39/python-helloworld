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

#GLOBAL VARIABLES and FUNCTIONS
#load dictionary via pickle file
with open(r"C:\Users\JohnMcMahon\BroadReachPower\BRP - Documents\Cap and Trade\Analyst Folders\McMahon\Projects\Dictionaries\models_dict.pkl", 'rb') as handle:
    models_dict = pickle.load(handle)

#create historical dictionaries
historical_dict = dict(RTLOAD=models_dict['load'], WIND_RTI=models_dict['wind'], GENERATION_SOLAR_RT=models_dict['solar'], REG_THERM_GEN_OFFLINE=models_dict['outages'], TRANS_USAGE_OTC_IMP=models_dict['paths'])
#create forecast dictionary
fx_dict = dict(LOAD_FORECAST=models_dict['load'], WIND_FORECAST=models_dict['wind'], SOLAR_FORECAST=models_dict['solar'], REG_THERM_GEN_OFFLINE=models_dict['outages'], TRANS_USAGE_OTC_IMP=models_dict['paths'])
#create function to pull yes energy data
def pull_yes_data(dictionary, type = 'historical'):
    #create empty list to append items to
    items_list = []
    headers_list = []
    #for loop through dictionary
    for i in dictionary.keys():
        for key, value in dictionary[i].items():
            item = i + ':' + str(value)
            header = str(key)
            items_list.append(item)
            headers_list.append(header)
    #join items in comma separated string
    items = ','.join(items_list)

    #define start and end date variables
    if type == 'historical':
        start_date = (datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d')
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    elif type == 'bd':
        start_date = (datetime.today()).strftime('%Y-%m-%d')
        end_date = start_date
    elif type == 'nd':
        start_date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = start_date
    elif type == 'nd1':
        start_date = (datetime.today() + timedelta(days=2)).strftime('%Y-%m-%d')
        end_date = start_date
    else:
        print("fx type criteria not met. try either 'nd' or 'nd1'")

    #define parameters
    parameters = {

        'agglevel': 'hour',
        'startdate': start_date,
        'enddate': end_date,
        'items': items,

    }
    #define query
    factory = VaultFactory()
    vault = factory.get_vault()
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    df = df.set_index('DATETIME')
    #rename columns
    df_front = pd.concat([df.pop(x) for x in df.iloc[:,:-5]], axis=1)
    df_front.columns = headers_list
    df = pd.concat([df_front, df], axis=1)

    return df

#list to filter out winter storm days
# storm = [d.strftime('%m/%d/%Y') for d in pd.date_range('2/11/2021','2/20/2021')]

## PULL DATA AND DETERMINE PROXY DAYS ##
#pull historical data
historical_data = pull_yes_data(historical_dict)
#trim off date/time related columns
historical = historical_data.iloc[:,:-3]
#filter out winter storm
# historical = historical[~historical.MARKETDAY.isin(storm)]
#calculate net load
historical['net_load'] = historical['CAISO_load'] - historical['CAISO_wind'] - historical['CAISO_solar']
# historical['thermal_outages'] = historical['SP15_outages'] + historical['NP15_outages']
#calculate ramp rates
historical['1hr_ramp'] = historical['net_load'].diff().round(2)
historical['2hr_ramp'] = historical['net_load'].diff(2).round(2)
historical['3hr_ramp'] = historical['net_load'].diff(3).round(2)
#calculate rolling averages
historical['1hr_avg'] = historical['net_load'].rolling(1).mean().round(2)
historical['2hr_avg'] = historical['net_load'].rolling(2).mean().round(2)
historical['3hr_avg'] = historical['net_load'].rolling(3).mean().round(2)
historical = historical.fillna(method='bfill')
#drop unnecesary columns
# historical = historical[['net_load', '1hr_ramp', '2hr_ramp', '3hr_ramp', 'thermal_outages', 'HOURENDING', 'MARKETDAY']]
historical = historical.drop(['CAISO_load', 'CAISO_wind', 'CAISO_solar'], axis=1)

##BD
#pull forward looking data
bd_data = pull_yes_data(fx_dict,'bd')
#trim the fat
bd = bd_data.iloc[:,:-3]
#calculate net load
bd['net_load'] = bd['CAISO_load'] - bd['CAISO_wind'] - bd['CAISO_solar']
#calculate ramp rates
bd['1hr_ramp'] = bd['net_load'].diff().round(2)
bd['2hr_ramp'] = bd['net_load'].diff(2).round(2)
bd['3hr_ramp'] = bd['net_load'].diff(3).round(2)
#calculate rolling averages
bd['1hr_avg'] = bd['net_load'].rolling(1).mean().round(2)
bd['2hr_avg'] = bd['net_load'].rolling(2).mean().round(2)
bd['3hr_avg'] = bd['net_load'].rolling(3).mean().round(2)
bd = bd.fillna(method='bfill')
#drop unnecesary columns
bd = bd.drop(['CAISO_load', 'CAISO_wind', 'CAISO_solar'], axis=1)

#empty df
bd_master = pd.DataFrame()
#for loop through hours of historical df to evaluate euclidean distance to fx df
#populate distance column of hourly filtered dfs wiht distance and append to empty df
for i in range(1,25):
    X = (historical[historical.HOURENDING == i]).drop(['HOURENDING', 'MARKETDAY'], axis=1)
    X = X.values

    y = (bd[bd.HOURENDING == i]).drop(['HOURENDING', 'MARKETDAY'], axis=1)
    y = y.values

    distances = (np.linalg.norm(X - y, axis=1)).round(2)

    df_hour = historical[historical.HOURENDING == i]
    df_hour['distance'] = distances

    bd_master = bd_master.append(df_hour)

#sort df by index values
bd_master = bd_master.sort_index()
#create proxy day list
bd_proxy_days = list(bd_master.groupby('MARKETDAY')['distance'].sum().sort_values(ascending=True).index)
bd_proxy_days = bd_proxy_days[0:25]

## ND+1
#pull forward looking data
nd_data = pull_yes_data(fx_dict,'nd')
#trim the fat
nd = nd_data.iloc[:,:-3]
#calculate net load
nd['net_load'] = nd['CAISO_load'] - nd['CAISO_wind'] - nd['CAISO_solar']
#calculate ramp rates
nd['1hr_ramp'] = nd['net_load'].diff().round(2)
nd['2hr_ramp'] = nd['net_load'].diff(2).round(2)
nd['3hr_ramp'] = nd['net_load'].diff(3).round(2)
#calculate rolling averages
nd['1hr_avg'] = nd['net_load'].rolling(1).mean().round(2)
nd['2hr_avg'] = nd['net_load'].rolling(2).mean().round(2)
nd['3hr_avg'] = nd['net_load'].rolling(3).mean().round(2)
nd = nd.fillna(method='bfill')
#drop unnecesary columns
nd = nd.drop(['CAISO_load', 'CAISO_wind', 'CAISO_solar'], axis=1)
#empty df
nd_master = pd.DataFrame()
#for loop through hours of historical df to evaluate euclidean distance to fx df
#populate distance column of hourly filtered dfs wiht distance and append to empty df
for i in range(1,25):
    X = (historical[historical.HOURENDING == i]).drop(['HOURENDING', 'MARKETDAY'], axis=1)
    X = X.values

    y = (nd[nd.HOURENDING == i]).drop(['HOURENDING', 'MARKETDAY'], axis=1)
    y = y.values

    distances = (np.linalg.norm(X - y, axis=1)).round(2)

    df_hour = historical[historical.HOURENDING == i]
    df_hour['distance'] = distances

    nd_master = nd_master.append(df_hour)

#sort df by index values
nd_master = nd_master.sort_index()
#create proxy day list
nd_proxy_days = list(nd_master.groupby('MARKETDAY')['distance'].sum().sort_values(ascending=True).index)
nd_proxy_days = nd_proxy_days[0:25]
## ND+1
#pull forward looking data
nd1_data = pull_yes_data(fx_dict,'nd1')
#trim the fat
nd1 = nd1_data.iloc[:,:-3]
#calculate net load
nd1['net_load'] = nd1['CAISO_load'] - nd1['CAISO_wind'] - nd1['CAISO_solar']
##calcilate ramp ratws
nd1['1hr_ramp'] = nd1['net_load'].diff().round(2)
nd1['2hr_ramp'] = nd1['net_load'].diff(2).round(2)
nd1['3hr_ramp'] = nd1['net_load'].diff(3).round(2)
#calculate rolling averages
nd1['1hr_avg'] = nd1['net_load'].rolling(1).mean().round(2)
nd1['2hr_avg'] = nd1['net_load'].rolling(2).mean().round(2)
nd1['3hr_avg'] = nd1['net_load'].rolling(3).mean().round(2)
nd1 = nd1.fillna(method='bfill')
#drop unnecesary columns
nd1 = nd1.drop(['CAISO_load', 'CAISO_wind', 'CAISO_solar'], axis=1)
#empty df
nd1_master = pd.DataFrame()
#for loop through hours of historical df to evaluate euclidean distance to fx df
#populate distance column of hourly filtered dfs wiht distance and append to empty df
for i in range(1,25):
    X = (historical[historical.HOURENDING == i]).drop(['HOURENDING', 'MARKETDAY'], axis=1)
    X = X.values

    y = (nd1[nd1.HOURENDING == i]).drop(['HOURENDING', 'MARKETDAY'], axis=1)
    y = y.values

    distances = (np.linalg.norm(X - y, axis=1)).round(2)

    df_hour = historical[historical.HOURENDING == i]
    df_hour['distance'] = distances

    nd1_master = nd1_master.append(df_hour)

#sort df by index values
nd1_master = nd1_master.sort_index()
#create proxy day list
nd1_proxy_days = list(nd1_master.groupby('MARKETDAY')['distance'].sum().sort_values(ascending=True).index)
nd1_proxy_days = nd1_proxy_days[0:25]

## pull historical price data to filter by proxy days
#node dictionary
node_dict = {
    'TH_SP15_GEN-APND': 20000004682,
    'TH_NP15_GEN-APND': 20000004677,
    'TH_ZP26_GEN-APND': 20000004670,
    'PEORIA_7_N001': 10002486799,
    'WEBER_6_N001': 20000003856,
    'RIPON_1_N001': 20000003085
}

#create function to pull historical darts and gas prices
def pull_historical_prices(dictionary):
    #create string to fulfill items parameter
    items_list = []
    headers_list = []
    for key, value in dictionary.items():
        item = 'DALMP:' + str(value) + ',LMP_RTPD:' + str(value)
        items_list.append(item)
        header = str(key) + '_da,' + str(key) + '_rt'
        headers_list.append(header)

    #append gas data type and header
    items_list.append('GASPRICE:10000002768')
    headers_list.append('SCG')
    items_list.append('GASPRICE:10000687008')
    headers_list.append('PGECG')

    items = ','.join(items_list)
    headers_list = ','.join(headers_list).split(",")

    #define parameters
    parameters1 = {

        'agglevel': 'hour',
        'startdate': (datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d'),
        'enddate': (datetime.today() - relativedelta(years=1)).strftime('%Y-%m-%d'),
        'items': items,

    }
    parameters2 = {

        'agglevel': 'hour',
        'startdate': (datetime.today() - relativedelta(years=1) + timedelta(days=1)).strftime('%Y-%m-%d'),
        'enddate': (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'items': items,

    }

    #define query
    factory = VaultFactory()
    vault = factory.get_vault()
    query1 = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?',params = parameters1, auth = auth)
    query2 = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?',params = parameters2, auth = auth)

    #pull and manipulate raw data
    raw1 = pd.read_csv(StringIO(query1.text)).round(2)
    raw2 = pd.read_csv(StringIO(query2.text)).round(2)

    #manipulate dataframe
    df = pd.concat([raw1, raw2], ignore_index=True)
    df = df.iloc[:,:-3]
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])
    df = df.set_index('DATETIME')

    #rename columns
    df_front = pd.concat([df.pop(x) for x in df.iloc[:,:-2]], axis=1)
    df_front.columns = headers_list
    df = pd.concat([df_front, df], axis=1)

    return df

#pull prices
master_price = pull_historical_prices(node_dict)
#create nd and nd1 proxy price df's
bd_proxy_prices = master_price[master_price.MARKETDAY.isin(bd_proxy_days)].reset_index()
bd_proxy_prices.insert(0, 'strip', 'BD')
nd_proxy_prices = master_price[master_price.MARKETDAY.isin(nd_proxy_days)].reset_index()
nd_proxy_prices.insert(0, 'strip', 'ND')
nd1_proxy_prices = master_price[master_price.MARKETDAY.isin(nd1_proxy_days)].reset_index()
nd1_proxy_prices.insert(0, 'strip', 'ND+1')

#pull as prices
start_date = (datetime.today() - relativedelta(years=2)).strftime('%m/%d/%Y')
end_date = (datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y')

ca_as = pull_as('caiso', 'all', start_date, end_date).set_index('DATETIME').iloc[:,:-3]
ca_as.columns = ['rd', 'ru', 'sp', 'ns', 'HOURENDING', 'MARKETDAY']
np_as = pull_np_as('caiso', 'all', start_date, end_date).set_index('DATETIME').iloc[:,:-3]
np_as.columns = ['rd', 'ru', 'sp', 'ns', 'HOURENDING', 'MARKETDAY']
sp_as = pull_sp_as('caiso', 'all', start_date, end_date).set_index('DATETIME').iloc[:,:-3]
sp_as.columns = ['rd', 'ru', 'sp', 'ns', 'HOURENDING', 'MARKETDAY']
np_master_as = ca_as.iloc[:, 0:4] + np_as.iloc[:, 0:4]
np_master_as = np_master_as.add_prefix('np_')
sp_master_as = ca_as.iloc[:, 0:4] + sp_as.iloc[:, 0:4]
sp_master_as = sp_master_as.add_prefix('sp_')
master_as = ca_as[['MARKETDAY', 'HOURENDING']]
master_as = pd.concat([master_as, np_master_as], axis=1)
master_as = pd.concat([master_as, sp_master_as], axis=1)

#create nd and nd1 proxy as prices df's
nd_proxy_as = master_as[master_as.MARKETDAY.isin(nd_proxy_days)].reset_index()
nd1_proxy_as = master_as[master_as.MARKETDAY.isin(nd1_proxy_days)].reset_index()

#stack price dataframes
proxy_prices = pd.concat([bd_proxy_prices, nd_proxy_prices, nd1_proxy_prices])

update_time = datetime.today()
#add to front of dataframes
bd_proxy_prices.insert(0, 'update_time', update_time)
nd_proxy_prices.insert(0, 'update_time', update_time)
nd1_proxy_prices.insert(0, 'update_time', update_time)
proxy_prices.insert(0, 'update_time', update_time)
nd_proxy_as.insert(0, 'update_time', update_time)
nd1_proxy_as.insert(0, 'update_time', update_time)

#### WRITE TO DATABASE ####
# function to connect to database
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

#write tables to sql
proxy_prices.to_sql('proxy_prices',con=engine,index=False,method=None,if_exists='replace')

nd_proxy_prices.to_sql('nd_proxy_prices',con=engine,index=False,method=None,if_exists='replace')
nd1_proxy_prices.to_sql('nd1_proxy_prices',con=engine,index=False,method=None,if_exists='replace')

nd_proxy_as.to_sql('nd_proxy_as',con=engine,index=False,method=None,if_exists='replace')
nd1_proxy_as.to_sql('nd1_proxy_as',con=engine,index=False,method=None,if_exists='replace')

ranking = pd.DataFrame(list(zip(bd_proxy_days, nd_proxy_days, nd1_proxy_days)), columns = ['BD', 'ND', 'ND+1'])
ranking.insert(0, 'update_time', update_time)
ranking.to_sql('proxy_date_ranking',con=engine,index=False,method=None,if_exists='replace')
print(ranking)
# %%
