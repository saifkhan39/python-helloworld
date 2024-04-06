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

def queryDB(database: str, sql_string: str):
    driver = '{ODBC Driver 17 for SQL Server}'
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    database = '{0}'.format(database)

    conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

sql = """SELECT * FROM [dbo].[ice_forwards] WHERE [contract_code] IN ('SPM', 'NPM', 'OFP', 'ONP')"""
forwards_df = queryDB('ErcotMarketData', sql).drop(columns=['contract_type', 'strike','net_change','expiration_date', 'product_id'])
forwards_df['MONTH'] = pd.DatetimeIndex(forwards_df['strip']).month
forwards_df['YEAR'] = pd.DatetimeIndex(forwards_df['strip']).year
forwards_df = forwards_df[['trade_date', 'hub', 'strip', 'contract_code', 'settlement_price']]
forwards_df = forwards_df.rename(columns={'trade_date': 'Trade Date'})
forwards_df = forwards_df.rename(columns={'contract_code': 'HUB'})
forwards_df = forwards_df[['Trade Date', 'strip', 'HUB', 'settlement_price']]

#Breakout - POWER
##_________________________________________________________________
SPM = forwards_df.loc[forwards_df.HUB == 'SPM']
NPM = forwards_df.loc[forwards_df.HUB == 'NPM']
onpk = pd.concat([SPM,NPM])

OFP = forwards_df.loc[forwards_df.HUB == 'OFP']
ONP = forwards_df.loc[forwards_df.HUB == 'ONP']
offpk = pd.concat([OFP,ONP])

##Gas
def queryDB(database: str, sql_string: str):
    driver = '{ODBC Driver 17 for SQL Server}'
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()    
    database = '{0}'.format(database)
    conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

sql = """SELECT * FROM [dbo].[ice_gas_forwards] WHERE [CONTRACT] IN ('H', 'PGE', 'EIS', 'SCL', 'SIS')"""
# sql = """SELECT * FROM [dbo].[ice_gas_forwards]"""
gas_df = queryDB('ErcotMarketData', sql)

gas_df = gas_df.groupby(['HUB', 'TRADE DATE', 'STRIP'])['SETTLEMENT PRICE'].sum()
gas_df = pd.DataFrame(gas_df).reset_index()
henry = gas_df[gas_df.HUB == 'Henry'].drop(columns='HUB')
henry.columns = ['TRADE DATE','STRIP','HENRY PRICE']
basis = gas_df[gas_df.HUB != 'Henry']
gas = pd.merge(basis,henry,on=['TRADE DATE', 'STRIP'])
gas['Close'] = gas['SETTLEMENT PRICE'] + gas['HENRY PRICE']
gas = gas[['TRADE DATE', 'STRIP', 'HUB', 'Close']]
gas.columns = ['Trade Date', 'strip', 'HUB', 'Close']

gas2 = gas.copy()
gas['HUB'] = gas['HUB'].replace('PG&E-Citygate', 'NPM')
gas['HUB'] = gas['HUB'].replace('Socal-Border', 'SPM')
gas2['HUB'] = gas2['HUB'].replace('PG&E-Citygate', 'ONP')
gas2['HUB'] = gas2['HUB'].replace('Socal-Border', 'OFP')


df1 = pd.merge(gas,onpk,how='left',left_on=['Trade Date', 'strip', 'HUB'], right_on=['Trade Date', 'strip', 'HUB'])

df2 = pd.merge(gas2,offpk,how='left',left_on=['Trade Date', 'strip', 'HUB'], right_on=['Trade Date', 'strip', 'HUB'])

result = pd.concat([df1,df2])
result = result.rename(columns={'settlement_price': 'Power Price'})
result = result.rename(columns={'Close': 'Gas Price'})

df = result
df = df.dropna()
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

test = dbConnect()

df.to_sql('temp_gas_forwards', con=test, index=False, method=None, if_exists='replace')





#
# ####NEW
#
#
# os.chdir(r"C:\Users\JohnMcMahon\BroadReachPower\BRP - Documents\Cap and Trade\Analyst Folders\McMahon\Python\Scrapers")
# import Prices
# from Prices import gas_dict, data_pull, da_lmps, rt_lmps, result_export
#
# power_df = result_export.loc[result_export.Product == 'Energy']
# power_df = power_df.drop(columns=['Product'])
# power_df = power_df.rename(columns={'Value': 'Power_Price'})
# power_df = power_df.rename(columns={'Node': 'Hub'})
# power_df = power_df.reset_index()
#
# gas_df = data_pull(gas_dict)
# gas_df = gas_df[['Socal-Citygate (GASPRICE)', 'PG&E - Citygate (GASPRICE)']]
# gas_df = gas_df.rename(columns={'Socal-Citygate (GASPRICE)': 'SP15'})
# gas_df = gas_df.rename(columns={'PG&E - Citygate (GASPRICE)': 'NP15'})
# gas_df = gas_df.reset_index()
# gas_df = pd.melt(gas_df, id_vars=['DATETIME'], value_vars=['SP15', 'NP15'])
#
# gas_df = gas_df.rename(columns={'variable': 'Hub'})
# gas_df = gas_df.rename(columns={'value': 'Gas_Price'})
#
# hr_df = pd.merge(power_df, gas_df, how='left', left_on=['DATETIME', 'Hub'], right_on=['DATETIME', 'Hub'])
#
# df = hr_df
# df['HE'] = (df['DATETIME'].dt.hour + 1)
#
#
# def queryDB(database: str, sql_string: str):
#     driver = '{ODBC Driver 17 for SQL Server}'
#     factory = VaultFactory()
#     vault = factory.get_vault()
#     db_credentials = vault.get_db_credentials()
#     database = '{0}'.format(database)
#
#     conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
#     conn = pyodbc.connect(conn_string)
#     df = pd.read_sql(sql_string, conn)
#     conn.close()
#
#     return df
#
# sql = """SELECT * FROM [dbo].[ice_forwards] WHERE [contract_code] IN ('SPM', 'NPM', 'OFP', 'ONP') AND [trade_date] = (SELECT MAX([trade_date]) FROM [dbo].[ice_forwards])"""
# forwards_df = queryDB('ErcotMarketData', sql).drop(columns=['contract_type', 'strike','net_change','expiration_date', 'product_id'])
# forwards_df['MONTH'] = pd.DatetimeIndex(forwards_df['strip']).month
# forwards_df['YEAR'] = pd.DatetimeIndex(forwards_df['strip']).year
#
# import GasPiv
# from GasPiv import fixed_df, fixed_table
#
# fixed_df = fixed_df.loc[(fixed_df['Trade Date'] == fixed_df['Trade Date'].max())]
# fixed_df = fixed_df[['Ticker', 'Trade Date', 'Expiry Month', 'Close']]
# fixed_df = fixed_df.rename(columns={'Expiry Month': 'strip'})
# fixed_df['strip'] = pd.DatetimeIndex(fixed_df['strip'])
# fixed_df['Trade Date'] = pd.DatetimeIndex(fixed_df['Trade Date'])
# forwards_df = forwards_df[['trade_date', 'hub', 'strip', 'contract_code', 'settlement_price']]
# forwards_df = forwards_df.rename(columns={'trade_date': 'Trade Date'})
# forwards_df = forwards_df[['Trade Date', 'strip', 'contract_code', 'settlement_price']]
#
# #Breakout - POWER
# ##_________________________________________________________________
# SPM = forwards_df.loc[forwards_df.contract_code == 'SPM']
# OFP = forwards_df.loc[forwards_df.contract_code == 'OFP']
#
# NPM = forwards_df.loc[forwards_df.contract_code == 'NPM']
# ONP = forwards_df.loc[forwards_df.contract_code == 'ONP']
#
# #Breakout - Gas
# ##____________________________________________________________
# SCG_df = fixed_df.loc[fixed_df.Ticker == 'SCG']
# SCG_df = SCG_df.drop(columns=['Ticker'])
# SCG_df['contract_code'] = 'SPM'
#
# SCG_df2 = fixed_df.loc[fixed_df.Ticker == 'SCG']
# SCG_df2 = SCG_df2.drop(columns=['Ticker'])
# SCG_df2['contract_code'] = 'OFP'
#
# PGE_df = fixed_df.loc[fixed_df.Ticker == 'PGECITY']
# PGE_df = PGE_df.drop(columns=['Ticker'])
# PGE_df['contract_code'] = 'NPM'
#
# PGE_df2 = fixed_df.loc[fixed_df.Ticker == 'PGECITY']
# PGE_df2 = PGE_df2.drop(columns=['Ticker'])
# PGE_df2['contract_code'] = 'ONP'
#
# df1 = pd.merge(SPM,SCG_df,how='left',left_on=['Trade Date', 'strip', 'contract_code'], right_on=['Trade Date', 'strip', 'contract_code'])
# df2 = pd.merge(OFP,SCG_df2,how='left',left_on=['Trade Date', 'strip', 'contract_code'], right_on=['Trade Date', 'strip', 'contract_code'])
# df3 = pd.merge(NPM,PGE_df,how='left',left_on=['Trade Date', 'strip', 'contract_code'], right_on=['Trade Date', 'strip', 'contract_code'])
# df4 = pd.merge(ONP,PGE_df2,how='left',left_on=['Trade Date', 'strip', 'contract_code'], right_on=['Trade Date', 'strip', 'contract_code'])
#
# frames = [df1,df2,df3,df4]
# result = pd.concat(frames)
# result = result.rename(columns={'settlement_price': 'Power Price'})
# result = result.rename(columns={'Close': 'Gas Price'})
