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
from galaxy_vault.factory import VaultFactory 


##Find what dates we need from master datebase

def queryDB(sql_string):
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    driver = '{ODBC Driver 17 for SQL Server}'
    database = 'CaisoMarketData'
    conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df


sql = """SELECT * FROM [dbo].[ciso_daily_storage]"""
#query sql data
storage = queryDB(sql)
storage = storage.sort_values(by='datetime')
storage = storage.drop_duplicates()

start = (storage.datetime.dt.date.max() + timedelta(days = 1))
end = (start + timedelta(days = 5))
dt_list = list(pd.date_range(start,end,freq='d').strftime('%b%d-%Y'))

print("Master Table up-to-date through "+storage.datetime.dt.date.max().strftime('%b%d-%Y'))
##Ping html and scrape text data
for date in dt_list:
    factory = VaultFactory()
    vault = factory.get_vault()
    r = requests.get(vault.get_secret('caiso-base-url')+'/Documents/DailyEnergyStorageReport'+date+'.html')
    soup = BeautifulSoup(r.text, 'html.parser')
    data = soup.body
    data = str(data)
    ####Set up timeseries for mapping
    five_dt = (pd.DataFrame(columns=['NULL'],index=pd.date_range(pd.to_datetime(date), pd.to_datetime(date) + timedelta(days = 1),freq='5T')).index.strftime('%Y-%m-%d %H:%M:%S'))
    five_dt = five_dt[:-1]

    fmm_dt = (pd.DataFrame(columns=['NULL'],index=pd.date_range(pd.to_datetime(date), pd.to_datetime(date) + timedelta(days = 1),freq='15T')).index.strftime('%Y-%m-%d %H:%M:%S'))
    fmm_dt = fmm_dt[:-1]

    h_dt = (pd.DataFrame(columns=['NULL'],index=pd.date_range(pd.to_datetime(date), pd.to_datetime(date) + timedelta(days = 1),freq='1H')).index.strftime('%Y-%m-%d %H:%M:%S'))
    h_dt = h_dt[:-1]
    ##Functions to format data from text to pandas timeseries
    def data_mapping(start, stop, timeseries):
        if start in data:
            df=pd.DataFrame()
            strt = data.index(start)
            stp = data.index(stop)

            sub_data = data[strt:stp]
            sub_data = sub_data[sub_data.index('[')+1:sub_data.index(']')]

            df['x'] = pd.DataFrame([x.split(';') for x in sub_data.split('\n')])
            df = df['x'].str.split(',', expand=True).transpose()
            df.columns = [start]
            df[start] = np.where(df[start] == ' "NA"', np.nan, df[start])
            df[start] = np.where(df[start] == '"NA"', np.nan, df[start])
            df[start] = df[start].astype('float')
            df.index = timeseries
        else:
            print('Data not yet posted for '+date+' from CAISO')
            exit()

        return df

    five_strt = (['tot_energy_ifm', 'tot_energy_ruc',
                'tot_energy_rtpd', 'tot_energy_rtd',
                'tot_charge_ifm', 'tot_charge_ruc',
                'tot_charge_rtpd', 'tot_charge_rtd',
                'tot_energy_hybrid_ifm', 'tot_energy_hybrid_ruc',
                'tot_energy_hybrid_rtpd', 'tot_energy_hybrid_rtd',
                'tot_charge_hybrid_ifm', 'tot_charge_hybrid_ruc',
                'tot_charge_hybrid_rtpd', 'tot_charge_hybrid_rtd'])

    five_stp = (['tot_energy_ruc', 'tot_energy_rtpd',
                'tot_energy_rtd', 'tot_charge_ifm',
                'tot_charge_ruc', 'tot_charge_rtpd',
                'tot_charge_rtd', 'as_ru_ifm',
                'tot_energy_hybrid_ruc', 'tot_energy_hybrid_rtpd',
                'tot_energy_hybrid_rtd', 'tot_charge_hybrid_ifm',
                'tot_charge_hybrid_ruc', 'tot_charge_hybrid_rtpd',
                'tot_charge_hybrid_rtd', 'as_ru_hybrid_ifm'])

    h_strt = (['as_ru_ifm', 'as_rd_ifm',
            'as_sr_ifm', 'as_nr_ifm',
            'as_ru_hybrid_ifm', 'as_rd_hybrid_ifm',
            'as_sr_hybrid_ifm', 'as_nr_hybrid_ifm',
            'bid_ifm_pos_ss', 'bid_ifm_pos_1',
            'bid_ifm_pos_2', 'bid_ifm_pos_3',
            'bid_ifm_pos_4', 'bid_ifm_pos_5',
            'bid_ifm_pos_6', 'bid_ifm_pos_7',
            'bid_ifm_pos_8', 'bid_ifm_pos_9',
            'bid_ifm_pos_10', 'bid_ifm_pos_11',
            'bid_ifm_neg_ss', 'bid_ifm_neg_1',
            'bid_ifm_neg_2', 'bid_ifm_neg_3',
            'bid_ifm_neg_4', 'bid_ifm_neg_5',
            'bid_ifm_neg_6', 'bid_ifm_neg_7',
            'bid_ifm_neg_8', 'bid_ifm_neg_9',
            'bid_ifm_neg_10', 'bid_ifm_neg_11',
            'bid_ifm_pos_hybrid_ss', 'bid_ifm_pos_hybrid_1',
            'bid_ifm_pos_hybrid_2', 'bid_ifm_pos_hybrid_3',
            'bid_ifm_pos_hybrid_4', 'bid_ifm_pos_hybrid_5',
            'bid_ifm_pos_hybrid_6', 'bid_ifm_pos_hybrid_7',
            'bid_ifm_pos_hybrid_8', 'bid_ifm_pos_hybrid_9',
            'bid_ifm_pos_hybrid_10', 'bid_ifm_pos_hybrid_11',
            'bid_ifm_neg_hybrid_ss', 'bid_ifm_neg_hybrid_1',
            'bid_ifm_neg_hybrid_2', 'bid_ifm_neg_hybrid_3',
            'bid_ifm_neg_hybrid_4', 'bid_ifm_neg_hybrid_5',
            'bid_ifm_neg_hybrid_6', 'bid_ifm_neg_hybrid_7',
            'bid_ifm_neg_hybrid_8', 'bid_ifm_neg_hybrid_9',
            'bid_ifm_neg_hybrid_10', 'bid_ifm_neg_hybrid_11'])

    h_stp = (['as_rd_ifm',
            'as_sr_ifm', 'as_nr_ifm',
            'as_ru_rtpd', 'as_rd_hybrid_ifm',
            'as_sr_hybrid_ifm', 'as_nr_hybrid_ifm',
            'as_ru_hybrid_rtpd', 'bid_ifm_pos_1',
            'bid_ifm_pos_2', 'bid_ifm_pos_3',
            'bid_ifm_pos_4', 'bid_ifm_pos_5',
            'bid_ifm_pos_6', 'bid_ifm_pos_7',
            'bid_ifm_pos_8', 'bid_ifm_pos_9',
            'bid_ifm_pos_10', 'bid_ifm_pos_11',
            'bid_ifm_neg_ss', 'bid_ifm_neg_1',
            'bid_ifm_neg_2', 'bid_ifm_neg_3',
            'bid_ifm_neg_4', 'bid_ifm_neg_5',
            'bid_ifm_neg_6', 'bid_ifm_neg_7',
            'bid_ifm_neg_8', 'bid_ifm_neg_9',
            'bid_ifm_neg_10', 'bid_ifm_neg_11',
            'bid_rtpd_pos_ss', 'bid_ifm_pos_hybrid_1',
            'bid_ifm_pos_hybrid_2', 'bid_ifm_pos_hybrid_3',
            'bid_ifm_pos_hybrid_4', 'bid_ifm_pos_hybrid_5',
            'bid_ifm_pos_hybrid_6', 'bid_ifm_pos_hybrid_7',
            'bid_ifm_pos_hybrid_8', 'bid_ifm_pos_hybrid_9',
            'bid_ifm_pos_hybrid_10', 'bid_ifm_pos_hybrid_11',
            'bid_ifm_neg_hybrid_ss', 'bid_ifm_neg_hybrid_1',
            'bid_ifm_neg_hybrid_2', 'bid_ifm_neg_hybrid_3',
            'bid_ifm_neg_hybrid_4', 'bid_ifm_neg_hybrid_5',
            'bid_ifm_neg_hybrid_6', 'bid_ifm_neg_hybrid_7',
            'bid_ifm_neg_hybrid_8', 'bid_ifm_neg_hybrid_9',
            'bid_ifm_neg_hybrid_10', 'bid_ifm_neg_hybrid_11',
            'bid_rtpd_pos_hybrid_ss'])

    fmm_strt = (['as_ru_rtpd', 'as_rd_rtpd',
            'as_sr_rtpd', 'as_nr_rtpd',
            'as_ru_hybrid_rtpd', 'as_rd_hybrid_rtpd',
            'as_sr_hybrid_rtpd', 'as_nr_hybrid_rtpd',
            'bid_rtpd_pos_ss', 'bid_rtpd_pos_1',
            'bid_rtpd_pos_2', 'bid_rtpd_pos_3',
            'bid_rtpd_pos_4', 'bid_rtpd_pos_5',
            'bid_rtpd_pos_6', 'bid_rtpd_pos_7',
            'bid_rtpd_pos_8', 'bid_rtpd_pos_9',
            'bid_rtpd_pos_10', 'bid_rtpd_pos_11',
            'bid_rtpd_neg_ss', 'bid_rtpd_neg_1',
            'bid_rtpd_neg_2', 'bid_rtpd_neg_3',
            'bid_rtpd_neg_4', 'bid_rtpd_neg_5',
            'bid_rtpd_neg_6', 'bid_rtpd_neg_7',
            'bid_rtpd_neg_8', 'bid_rtpd_neg_9',
            'bid_rtpd_neg_10', 'bid_rtpd_neg_11',
            'bid_rtpd_pos_hybrid_ss', 'bid_rtpd_pos_hybrid_1',
            'bid_rtpd_pos_hybrid_2', 'bid_rtpd_pos_hybrid_3',
            'bid_rtpd_pos_hybrid_4', 'bid_rtpd_pos_hybrid_5',
            'bid_rtpd_pos_hybrid_6', 'bid_rtpd_pos_hybrid_7',
            'bid_rtpd_pos_hybrid_8', 'bid_rtpd_pos_hybrid_9',
            'bid_rtpd_pos_hybrid_10', 'bid_rtpd_pos_hybrid_11',
            'bid_rtpd_neg_hybrid_ss', 'bid_rtpd_neg_hybrid_1',
            'bid_rtpd_neg_hybrid_2', 'bid_rtpd_neg_hybrid_3',
            'bid_rtpd_neg_hybrid_4', 'bid_rtpd_neg_hybrid_5',
            'bid_rtpd_neg_hybrid_6', 'bid_rtpd_neg_hybrid_7',
            'bid_rtpd_neg_hybrid_8', 'bid_rtpd_neg_hybrid_9',
            'bid_rtpd_neg_hybrid_10', 'bid_rtpd_neg_hybrid_11'])

    fmm_stp = (['as_rd_rtpd',
            'as_sr_rtpd', 'as_nr_rtpd',
            'tot_energy_hybrid_ifm', 'as_rd_hybrid_rtpd',
            'as_sr_hybrid_rtpd', 'as_nr_hybrid_rtpd',
            'bid_ifm_pos_ss', 'bid_rtpd_pos_1',
            'bid_rtpd_pos_2', 'bid_rtpd_pos_3',
            'bid_rtpd_pos_4', 'bid_rtpd_pos_5',
            'bid_rtpd_pos_6', 'bid_rtpd_pos_7',
            'bid_rtpd_pos_8', 'bid_rtpd_pos_9',
            'bid_rtpd_pos_10', 'bid_rtpd_pos_11',
            'bid_rtpd_neg_ss', 'bid_rtpd_neg_1',
            'bid_rtpd_neg_2', 'bid_rtpd_neg_3',
            'bid_rtpd_neg_4', 'bid_rtpd_neg_5',
            'bid_rtpd_neg_6', 'bid_rtpd_neg_7',
            'bid_rtpd_neg_8', 'bid_rtpd_neg_9',
            'bid_rtpd_neg_10', 'bid_rtpd_neg_11',
            'bid_ifm_pos_hybrid_ss', 'bid_rtpd_pos_hybrid_1',
            'bid_rtpd_pos_hybrid_2', 'bid_rtpd_pos_hybrid_3',
            'bid_rtpd_pos_hybrid_4', 'bid_rtpd_pos_hybrid_5',
            'bid_rtpd_pos_hybrid_6', 'bid_rtpd_pos_hybrid_7',
            'bid_rtpd_pos_hybrid_8', 'bid_rtpd_pos_hybrid_9',
            'bid_rtpd_pos_hybrid_10', 'bid_rtpd_pos_hybrid_11',
            'bid_rtpd_neg_hybrid_ss', 'bid_rtpd_neg_hybrid_1',
            'bid_rtpd_neg_hybrid_2', 'bid_rtpd_neg_hybrid_3',
            'bid_rtpd_neg_hybrid_4', 'bid_rtpd_neg_hybrid_5',
            'bid_rtpd_neg_hybrid_6', 'bid_rtpd_neg_hybrid_7',
            'bid_rtpd_neg_hybrid_8', 'bid_rtpd_neg_hybrid_9',
            'bid_rtpd_neg_hybrid_10', 'bid_rtpd_neg_hybrid_11',
            'price_bin_colors_invert'])


    five_df = pd.DataFrame()
    for (a,b) in zip(five_strt,five_stp):
        df = data_mapping(a, b, five_dt)
        five_df = pd.concat([five_df, df], axis=1)


    h_df = pd.DataFrame()
    for (a,b) in zip(h_strt,h_stp):
        df = data_mapping(a, b, h_dt)
        h_df = pd.concat([h_df, df], axis=1)


    fmm_df = pd.DataFrame()
    for (a,b) in zip(fmm_strt,fmm_stp):
        df = data_mapping(a, b, fmm_dt)
        fmm_df = pd.concat([fmm_df, df], axis=1)


    combo = pd.concat([five_df,fmm_df,h_df], axis=1).ffill()
    combo.index = pd.to_datetime(combo.index)
    combo = combo.reset_index()
    combo = combo.rename(columns={"index": "datetime"})

    def dbConnect():
        database = "CAISOMarketData"
        factory = VaultFactory()
        vault = factory.get_vault()
        db_credentials = vault.get_db_credentials()
        driver = '{ODBC Driver 17 for SQL Server}'
        odbc_str = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
        connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
        engine = create_engine(connect_str,fast_executemany=True)

        return(engine)

    test = dbConnect()

    ts = (dt.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    combo['update_time'] = ts
    combo = combo.drop_duplicates()
    print('Uploading Data from '+date)
    combo.to_sql('ciso_daily_storage', con=test, index=False, method=None, if_exists='append')



###Update in bulk
# def queryDB(sql_string):
#     driver = '{ODBC Driver 17 for SQL Server}'
#     server = "brptemp.database.windows.net"
#     database = 'CaisoMarketData'
#     username = 
#     password = 
#
#     conn_string = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
#     conn = pyodbc.connect(conn_string)
#     df = pd.read_sql(sql_string, conn)
#     conn.close()
#
#     return df
#
#
# sql = """SELECT * FROM [dbo].[ciso_daily_storage]"""
# #query sql data
# storage = queryDB(sql)
#
# storage = storage.drop_duplicates()
#
# storage = storage.sort_values(by='datetime')
#
# storage.to_sql('ciso_daily_storage', con=test, index=False, method=None, if_exists='replace')
