import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from yesapi.functions import *
from io import StringIO, BytesIO
import pickle
import requests
import json
import sqlalchemy, urllib
from sqlalchemy import create_engine

#GLOBAL VARIABLES and FUNCTIONS
#load dictionary via pickle file
with open(r"C:\BRP_scrapers\models_dict_v2.pkl", 'rb') as handle:
    models_dict = pickle.load(handle)

# with open(r"C:\Users\MattSindler\BroadReachPower\BRP - Documents\Cap and Trade\Analyst Folders\Sindler\Projects\Dictionaries\models_dict.pkl", 'wb') as handle:
#     pickle.dump(models_dict, handle)

#create historical dictionaries
historical_dict = dict(RTLOAD=models_dict['load'], WIND_RTI=models_dict['wind'], GENERATION_SOLAR_RT=models_dict['solar'], TOTAL_RESOURCE_CAP_OUT=models_dict['outages'])
#create forecast dictionary
fx_dict = dict(LOAD_FORECAST=models_dict['load'], WIND_STWPF=models_dict['wind'], SOLAR_STPPF=models_dict['solar'], TOTAL_RESOURCE_CAP_OUT=models_dict['outages'])

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
    query = requests.get('https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
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
storm = [d.strftime('%m/%d/%Y') for d in pd.date_range('2/11/2021','2/25/2021')]
august19 = [d.strftime('%m/%d/%Y') for d in pd.date_range('8/12/2019','8/21/2019')]
#dst_days = ['03/14/2021', '11/07/2021']
dst_days = ['03/10/2019', '11/03/2019', '03/08/2020', '11/01/2020', '03/14/2021', '11/07/2021', '3/13/2022', '11/06/2022']

## PULL DATA AND DETERMINE PROXY DAYS ##
#pull historical data
historical_data = pull_yes_data(historical_dict)
#trim off date/time related columns
historical = historical_data.iloc[:,:-3]
#filter out winter storm
historical = historical[(~historical.MARKETDAY.isin(storm))&(~historical.MARKETDAY.isin(august19))&(~historical.MARKETDAY.isin(dst_days))]
#calculate net load
historical['net_load'] = historical['ercot_load'] - historical['ercot_wind'] - historical['solar']
#calculate ramp rates
historical['1hr_ramp'] = historical.net_load.diff()
historical['2hr_ramp'] = historical.net_load.diff(2)
historical['3hr_ramp'] = historical.net_load.diff(3)
#calculate rolling averages
historical['1hr_avg'] = historical['net_load'].rolling(1).mean().round(2)
historical['2hr_avg'] = historical['net_load'].rolling(2).mean().round(2)
historical['3hr_avg'] = historical['net_load'].rolling(3).mean().round(2)
historical = historical.fillna(method='bfill')
#drop unnecesary columns
historical = historical.drop(['ercot_load', 'ercot_wind'], axis=1)

## BD
#pull forward looking data
bd_data = pull_yes_data(fx_dict,'bd')
#trim the fat
bd = bd_data.iloc[:,:-3]
#calculate net load
bd['net_load'] = bd['ercot_load'] - bd['ercot_wind'] - bd['solar']
#calculate ramp rates
bd['1hr_ramp'] = bd.net_load.diff()
bd['2hr_ramp'] = bd.net_load.diff(2)
bd['3hr_ramp'] = bd.net_load.diff(3)
#calculate rolling averages
bd['1hr_avg'] = bd['net_load'].rolling(1).mean().round(2)
bd['2hr_avg'] = bd['net_load'].rolling(2).mean().round(2)
bd['3hr_avg'] = bd['net_load'].rolling(3).mean().round(2)
bd = bd.fillna(method='bfill')
#drop unnecesary columns
bd = bd.drop(['ercot_load', 'ercot_wind'], axis=1)
##take out after long day
#bd = bd.reset_index().drop([1,1]).set_index('DATETIME')

#empty df
bd_master = pd.DataFrame()
#for loop through hours of historical df to evaluate euclidean distance to fx df
#populate distance column of hourly filtered dfs wiht distance and append to empty df
for i in range(1,25):
    if i == 3:
        pass
    else:
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

## ND
#pull forward looking data
nd_data = pull_yes_data(fx_dict,'nd')
#trim the fat
nd = nd_data.iloc[:,:-3]
#calculate net load
nd['net_load'] = nd['ercot_load'] - nd['ercot_wind'] - nd['solar']
#calculate ramp rates
nd['1hr_ramp'] = nd.net_load.diff()
nd['2hr_ramp'] = nd.net_load.diff(2)
nd['3hr_ramp'] = nd.net_load.diff(3)
#calculate rolling averages
nd['1hr_avg'] = nd['net_load'].rolling(1).mean().round(2)
nd['2hr_avg'] = nd['net_load'].rolling(2).mean().round(2)
nd['3hr_avg'] = nd['net_load'].rolling(3).mean().round(2)
nd = nd.fillna(method='bfill')
#drop unnecesary columns
nd = nd.drop(['ercot_load', 'ercot_wind'], axis=1)
##take out after long day
#nd = nd.reset_index().drop([1,1]).set_index('DATETIME')

#empty df
nd_master = pd.DataFrame()
#for loop through hours of historical df to evaluate euclidean distance to fx df
#populate distance column of hourly filtered dfs wiht distance and append to empty df
for i in range(1,25):
    if i == 3:
        pass
    else:
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
nd1['net_load'] = nd1['ercot_load'] - nd1['ercot_wind'] - nd1['solar']
#calculate ramp rates
nd1['1hr_ramp'] = nd1.net_load.diff()
nd1['2hr_ramp'] = nd1.net_load.diff(2)
nd1['3hr_ramp'] = nd1.net_load.diff(3)
#calculate rolling averages
nd1['1hr_avg'] = nd1['net_load'].rolling(1).mean().round(2)
nd1['2hr_avg'] = nd1['net_load'].rolling(2).mean().round(2)
nd1['3hr_avg'] = nd1['net_load'].rolling(3).mean().round(2)
nd1 = nd1.fillna(method='bfill')
#drop unnecesary columns
nd1 = nd1.drop(['ercot_load', 'ercot_wind'], axis=1)

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
    'ALVIN_RN': 10016433362,
    'BRPANGLE_RN': 10016437282,
    'BATCAVE_RN': 10016670894,
    'BRP_BRAZ_RN': 10016437281,
    'BRP_DIKN_RN': 10016437279,
    'BRHEIGHT_RN': 10016437277,
    'BRP_LOOP_RN': 10016473683,
    'BRP_LOP1_RN': 10016761589,
    'BRPMAGNO_RN': 10016437280,
    'NF_BRP_RN': 10016498292,
    'ODESW_RN': 10016433361,
    'BRP_PBL1_RN': 10016483004,
    'BRP_PBL2_RN': 10016483001,
    'BRP_RN_UNIT1': 10016473685,
    'BRP_SWNY_RN': 10016473684,
    'BRP_ZPT1_RN': 10016483003,
    'BRP_ZPT2_RN': 10016483009,
    'HB_HUBAVG': 10000698382,
    'HB_HOUSTON': 10000697077,
    'HB_NORTH': 10000697078,
    'HB_SOUTH': 10000697079,
    'HB_PAN': 10015999590,
    'HB_WEST': 10000697080,
    'LZ_AEN': 10000698388,
    'LZ_CPS': 10000698389,
    'LZ_HOUSTON': 10000698390,
    'LZ_LCRA': 10000698391,
    'LZ_NORTH': 10000698392,
    'LZ_RAYBN': 10000698393,
    'LZ_SOUTH': 10000698394,
    'LZ_WEST': 10000698395,
}
dictionary = node_dict
#create function to pull historical darts and gas prices
def pull_historical_prices(dictionary):
    #create string to fulfill items parameter
    items_list = []
    headers_list = []
    for key, value in dictionary.items():
        item = 'DALMP:' + str(value) + ',RTLMP:' + str(value)
        items_list.append(item)
        header = str(key) + '_da,' + str(key) + '_rt'
        headers_list.append(header)

    #append gas data type and header
    items_list.append('SYSTEM_LAMBDA_DA:10000756298,SYSTEM_LAMBDA:10000756298,GASPRICE:10000002639')
    headers_list.append('SYSTEM_LAMBDA_da,SYSTEM_LAMBDA_rt,hsc')

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
    query1 = requests.get('https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?',params = parameters1, auth = auth)
    query2 = requests.get('https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?',params = parameters2, auth = auth)

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

#create bd, nd, and nd1 proxy price df's
bd_proxy_prices = master_price[master_price.MARKETDAY.isin(bd_proxy_days)].reset_index()
bd_proxy_prices.insert(0, 'strip', 'BD')
nd_proxy_prices = master_price[master_price.MARKETDAY.isin(nd_proxy_days)].reset_index()
nd_proxy_prices.insert(0, 'strip', 'ND')
nd1_proxy_prices = master_price[master_price.MARKETDAY.isin(nd1_proxy_days)].reset_index()
nd1_proxy_prices.insert(0, 'strip', 'ND+1')

#pull as prices
start_date = (datetime.today() - relativedelta(years=2)).strftime('%m/%d/%Y')
end_date = (datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y')

master_as = pull_as('ercot', 'all', start_date, end_date).set_index('DATETIME').iloc[:,:-3]

#create nd and nd1 proxy as prices df's
nd_proxy_as = master_as[master_as.MARKETDAY.isin(nd_proxy_days)].reset_index()
nd1_proxy_as = master_as[master_as.MARKETDAY.isin(nd1_proxy_days)].reset_index()

#stack price dataframes
proxy_prices = pd.concat([bd_proxy_prices, nd_proxy_prices, nd1_proxy_prices])

#create update time column
update_time = datetime.today()

#add to front of dataframes
bd_proxy_prices.insert(0, 'update_time', update_time)
nd_proxy_prices.insert(0, 'update_time', update_time)
nd1_proxy_prices.insert(0, 'update_time', update_time)
proxy_prices.insert(0, 'update_time', update_time)
nd_proxy_as.insert(0, 'update_time', update_time)
nd1_proxy_as.insert(0, 'update_time', update_time)

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
proxy_prices.to_sql('proxy_prices',con=engine,index=False,method=None,if_exists='replace')

nd_proxy_prices.to_sql('nd_proxy_prices',con=engine,index=False,method=None,if_exists='replace')
nd1_proxy_prices.to_sql('nd1_proxy_prices',con=engine,index=False,method=None,if_exists='replace')

nd_proxy_as.to_sql('nd_proxy_as',con=engine,index=False,method=None,if_exists='replace')
nd1_proxy_as.to_sql('nd1_proxy_as',con=engine,index=False,method=None,if_exists='replace')

#additional dataframe ranking dates
ranking = pd.DataFrame(list(zip(bd_proxy_days, nd_proxy_days, nd1_proxy_days)), columns = ['BD', 'ND', 'ND+1'])
ranking.insert(0, 'update_time', update_time)
ranking.to_sql('proxy_date_ranking',con=engine,index=False,method=None,if_exists='replace')
