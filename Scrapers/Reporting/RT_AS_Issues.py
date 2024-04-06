import pandas as pd
from urllib.request import urlopen
from io import StringIO, BytesIO
import zipfile
import datetime as dt
import time
from datetime import timedelta, datetime
import pyodbc, io, psutil
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import requests, json
import urllib
from io import StringIO
from yesapi.functions import *
import seaborn as sns
import os
import sklearn.linear_model
from sklearn.linear_model import LinearRegression
from galaxy_vault.factory import VaultFactory 


AS_dict = {
    'AS_CAISO_EXP': 10002484315,
    'AS_NP26_EXP': 10002539052,
    'AS_SP26_EXP': 10002484317,
}

AS_dict2 = {
    'AS_CAISO': 10002484314,
    'AS_NP26': 10002863700,
    'AS_SP26': 10002484316,
}

start = (dt.date.today() - dt.timedelta(days=365*5)).strftime('%Y-%m-%d')
end = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')


factory = VaultFactory()
vault = factory.get_vault()
yes_energy_username = vault.get_secret_individual_account('yes-energy-username')
yes_energy_password = vault.get_secret_individual_account('yes-energy-password')
auth = (yes_energy_username, yes_energy_password)
def data_pull(dictionary, type: str):
    items_list = []
    if dictionary == AS_dict and type == 'Price':
        for value in dictionary.values():
            item = ('RTM SP_CLR_PRC:' + str(value) +
                    ',RTM NS_CLR_PRC:' + str(value) + ',RTM RU_CLR_PRC:' + str(value) + ',RTM RD_CLR_PRC:' + str(value))
            items_list.append(item)
    elif dictionary == AS_dict2 and type == 'Price':
        for value in dictionary.values():
            item = ('RTM SP_CLR_PRC:' + str(value) +
                    ',RTM NS_CLR_PRC:' + str(value) + ',RTM RU_CLR_PRC:' + str(value) + ',RTM RD_CLR_PRC:' + str(value))
            items_list.append(item)
    elif dictionary == AS_dict and type == 'Procurements':
        for value in dictionary.values():
            item = ('RTM SP_PROC_MW:' + str(value) +
                    ',RTM NS_PROC_MW:' + str(value) + ',RTM RU_PROC_MW:' + str(value) + ',RTM RD_PROC_MW:' + str(value))
            items_list.append(item)
    items = ','.join(items_list)
    #DST flag
    if time.localtime().tm_isdst == 1:
        tz = 'PPT'
    else:
        tz = 'PST'
    #define parameters
    parameters = {
        'agglevel': 'hour',
        'startdate': start,
        'enddate': end,
        'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?',params = parameters, auth = auth)
    #pull and manipulate raw data
    raw = pd.read_csv(StringIO(query.text),on_bad_lines='skip').round(2)
    #delete unnecesary columns
    df = raw
    df = df.set_index('DATETIME')
    df.index = pd.to_datetime(df.index)
    ##Shift df to align HE and Marketday with Datetime from yes
    df = df.shift(freq='-1H')
    ##Chnage month values to integers
    # df['MONTH'] = df['MONTH'].str.upper().map(month_dict)
    # df['MONTH'] = df['MONTH'].astype(str)
    # df['YEAR'] = df['YEAR'].astype(str)

    return df


rt_AS1 = data_pull(AS_dict, 'Price')
rt_AS2 = data_pull(AS_dict2, 'Price')

rt_data = pd.concat([rt_AS1,rt_AS2], axis=1)
rt_data = rt_data.loc[:,~rt_data.columns.duplicated()]

rt_data = rt_data.fillna(0)

rt_procurments = data_pull(AS_dict, 'Procurements')

rt_data2=rt_data
# rt_procurments.to_csv('rt_procurments.csv')
os.chdir(r"C:\BRP_scrapers\CAISO")
