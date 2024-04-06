import zipfile, io, os, glob
from zipfile import ZipFile
from io import StringIO, BytesIO
from requests_pkcs12 import get
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, datetime, time
from lxml import etree
from pathlib import Path
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWRITE
from difflib import SequenceMatcher
import http.client, urllib.request, urllib.parse, urllib.error, base64
import requests
import json, gzip
from urllib.request import urlopen
import sqlalchemy, urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import pyodbc
from sqlalchemy.sql.expression import column
from galaxy_vault.factory import VaultFactory 


#### CALCULATE WIND AND SOLAR FORECASTS BY UNIT WITH IMPLIED CAPACITY FACTORS ####
## PROCESS DOCUMENTATION ##
# 1. query resource names from 60dDAMGenResourceData table in database using
#     SELECT [Resource Name]
# 	   ,[Settlement Point Name]
# 	   ,[Resource Type]
#     FROM [dbo].[60dDAMGenResourceData]
#     WHERE [Resource Type] IN ('WIND', 'PVGR')
#     GROUP BY [Resource Name]
# 	   ,[Settlement Point Name]
# 	   ,[Resource Type]
#
# 2. download unit capacities for wind/solar for particular season from ercot
# 3. create table with UnitName, ResourceName, ResourceType, Zone, Capacity
#     ResourceName column originally titled 'UNIT CODE'
# 4. index Settlement Point Name from resource names QUERIED and match on resource_names to
#     add a settlement point name column to the unit capacities dataframe/csv

unit_details = pd.read_csv(r"C:\BRP_scrapers\flows forecast dependencies\unit_details.csv")
#clean and filter
unit_details.columns = ['UnitName', 'ResourceName', 'SettlementPointName', 'ResourceType', 'Zone', 'Year', 'Capacity']
unit_details = unit_details[['ResourceName', 'SettlementPointName', 'ResourceType', 'Zone', 'Capacity']]
renewables = ['SOLAR', 'WIND-C', 'WIND-P', 'WIND-O']
solar = ['SOLAR']
wind = ['WIND-C', 'WIND-P', 'WIND-O']
units = unit_details[unit_details['ResourceType'].isin(renewables)].dropna()

#datetime variable
today = datetime.combine(dt.date.today(), time())

## WIND
#calculate wind capacity by zone
wind_cap = pd.DataFrame(units[units.ResourceType.isin(wind)].groupby(units.Zone)['Capacity'].sum().round()).T
#get wind forecast by zone
#funciton
def getwind():
    reportID = 14787
    factory = VaultFactory()
    vault = factory.get_vault()
    ercotQuery = pd.read_json(vault.get_secret('ercot-sa-url')+"/misapp/servlets/IceDocListJsonWS?reportTypeId=" + str(reportID))
    ercotQuery = pd.json_normalize(ercotQuery['ListDocsByRptTypeRes'][0])
    ercotQuery['type'] = ercotQuery['Document.FriendlyName'].str.slice(-3)
    ercotQuery['PublishDate'] = pd.to_datetime(ercotQuery['Document.PublishDate'], format='%Y-%m-%dT%H:%M:%S')
    csvList = ercotQuery[ercotQuery['type'] == 'csv']
    csvList = csvList[['Document.DocID', 'Document.ConstructedName', 'PublishDate']]
    csvList['zipFile'] = vault.get_secret('ercot-sa-url')+"/misdownload/servlets/mirDownload?doclookupId=" + csvList[
        'Document.DocID']



    # dest = 'C:\\Users\\CaseyKopp\\Documents\\Python_Scripts\\ERCOT Downloads'
    file = csvList['zipFile'][0]

    df = pd.read_csv(file, compression='zip')
    df['HB'] = df['HOUR_ENDING'] - 1
    df['datetime'] = df['DELIVERY_DATE'].astype(str) + ' ' + df['HB'].astype(str) + ':00'
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = pd.melt(df, id_vars=['datetime', 'DELIVERY_DATE'],
                 value_vars=['ACTUAL_SYSTEM_WIDE', 'COP_HSL_SYSTEM_WIDE', 'STWPF_SYSTEM_WIDE', 'WGRPP_SYSTEM_WIDE',
                             'ACTUAL_PANHANDLE', 'COP_HSL_PANHANDLE', 'STWPF_PANHANDLE', 'WGRPP_PANHANDLE',
                             'ACTUAL_COASTAL', 'COP_HSL_COASTAL', 'STWPF_COASTAL', 'WGRPP_COASTAL', 'ACTUAL_SOUTH',
                             'COP_HSL_SOUTH', 'STWPF_SOUTH', 'WGRPP_SOUTH', 'ACTUAL_WEST', 'COP_HSL_WEST', 'STWPF_WEST',
                             'WGRPP_WEST', 'ACTUAL_NORTH', 'COP_HSL_NORTH', 'STWPF_NORTH', 'WGRPP_NORTH'],
                 value_name='value', var_name='type')
    df['region'] = np.where(df['type'].str.slice(-4) == "WIDE", "ISO", "")
    df['region'] = np.where(df['type'].str.slice(-4) == "NDLE", "PANHANDLE", df['region'])
    df['region'] = np.where(df['type'].str.slice(-4) == "STAL", "COASTAL", df['region'])
    df['region'] = np.where(df['type'].str.slice(-4) == "OUTH", "SOUTH", df['region'])
    df['region'] = np.where(df['type'].str.slice(-4) == "WEST", "WEST", df['region'])
    df['region'] = np.where(df['type'].str.slice(-4) == "ORTH", "NORTH", df['region'])
    return df
#pull data
wind_fx = getwind()
wind_fx = wind_fx[wind_fx['datetime'] >= today]
#filter for STWPF
wind_fx = wind_fx[wind_fx.type.str.contains('STWPF')]
#groupby timestamp and zone
wind_fx = wind_fx.groupby(['datetime', 'region'])['value'].sum().round().reset_index()
wind_fx = wind_fx.pivot(index='datetime', columns='region', values='value').drop('ISO', axis=1)
#calculate zonal wind capacity factors by hour
wind_cf = (wind_fx/wind_cap.values).round(3)
#aggreate wind_units and their capacity by settlement point name
wind_units = units[units.ResourceType.isin(wind)].groupby(['SettlementPointName', 'Zone']).sum('Capacity').reset_index().set_index('SettlementPointName')
#create empty dataframe
derated_wind_fx = pd.DataFrame(columns=['datetime'])
#for loop through
for i, j in wind_units.iterrows():
    zone = j.Zone
    capacity = j.Capacity

    unit_fx = wind_cf[[zone]]*capacity

    unit_fx = unit_fx.reset_index()
    unit_fx.columns = ['datetime', i]

    derated_wind_fx = derated_wind_fx.merge(unit_fx, on='datetime', how='outer')
#reset index
derated_wind_fx = derated_wind_fx.set_index('datetime')

## SOLAR
#calculate solar capacity
solar_cap = pd.DataFrame(units[units.ResourceType == 'SOLAR'].groupby(units.ResourceType)['Capacity'].sum().round()).T
#get solar forecast by zone
#funciton
def getsolar():
    reportID = 13483
    factory = VaultFactory()
    vault = factory.get_vault()
    ercotQuery = pd.read_json(vault.get_secret('ercot-sa-url')+"/misapp/servlets/IceDocListJsonWS?reportTypeId=" + str(reportID))
    ercotQuery = pd.json_normalize(ercotQuery['ListDocsByRptTypeRes'][0])
    ercotQuery['type'] = ercotQuery['Document.FriendlyName'].str.slice(-3)
    ercotQuery['PublishDate'] = pd.to_datetime(ercotQuery['Document.PublishDate'], format='%Y-%m-%dT%H:%M:%S')
    csvList = ercotQuery[ercotQuery['type'] == 'csv']
    csvList = csvList[['Document.DocID', 'Document.ConstructedName', 'PublishDate']]
    csvList['zipFile'] = vault.get_secret('ercot-sa-url')+"/misdownload/servlets/mirDownload?doclookupId=" + csvList[
        'Document.DocID']
    # dest = 'C:\\Users\\CaseyKopp\\Documents\\Python_Scripts\\ERCOT Downloads'
    file = csvList['zipFile'][0]
    df = pd.read_csv(file, compression='zip')
    df['HB'] = df['HOUR_ENDING'] - 1
    df['datetime'] = df['DELIVERY_DATE'].astype(str) + ' ' + df['HB'].astype(str) + ':00'
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = pd.melt(df, id_vars=['datetime', 'DELIVERY_DATE'],
                 value_vars=['ACTUAL_SYSTEM_WIDE', 'COP_HSL_SYSTEM_WIDE', 'STPPF_SYSTEM_WIDE', 'PVGRPP_SYSTEM_WIDE'],
                 value_name='value', var_name='type')
    return df
#pull data
solar_fx = getsolar()
solar_fx = solar_fx[solar_fx['datetime'] >= today]
#filter for STWPF
solar_fx = solar_fx[solar_fx.type.str.contains('STPPF')]
#groupby timestamp
solar_fx = solar_fx.groupby('datetime').sum('value').round()
#calculate zonal solar capacity factors by hour
solar_cf = solar_fx/solar_cap.values
#replace values greater than 1
solar_cf.value = np.where(solar_cf.value > 1, 1, solar_cf.value).round(3)
#aggreagate solar units and their capacities by settlemeent point name
solar_units = units[units.ResourceType == 'SOLAR'].groupby('SettlementPointName').sum('Capacity')
#create empty dataframe
derated_solar_fx = pd.DataFrame(columns=['datetime'])
#for loop through
for i, j in solar_units.iterrows():
    capacity = j.Capacity

    unit_fx = (solar_cf*capacity)
    unit_fx.columns = [i]

    derated_solar_fx = derated_solar_fx.merge(unit_fx, on='datetime', how='outer')
#reset index
derated_solar_fx = derated_solar_fx.set_index('datetime')

## LOAD
#funciton
def get7daydemand():
    reportID = 12311
    ercotQuery = pd.read_json(vault.get_secret('ercot-sa-url')+"/misapp/servlets/IceDocListJsonWS?reportTypeId=" + str(reportID))
    ercotQuery = pd.json_normalize(ercotQuery['ListDocsByRptTypeRes'][0])
    ercotQuery['type'] = ercotQuery['Document.FriendlyName'].str.slice(-3)
    ercotQuery['PublishDate'] = pd.to_datetime(ercotQuery['Document.PublishDate'], format='%Y-%m-%dT%H:%M:%S')
    csvList = ercotQuery[ercotQuery['type'] == 'csv']
    csvList = csvList[['Document.DocID', 'Document.ConstructedName', 'PublishDate']]
    factory = VaultFactory()
    vault = factory.get_vault()
    csvList['zipFile'] = vault.get_secret('ercot-sa-url')+"/misdownload/servlets/mirDownload?doclookupId=" + csvList[
        'Document.DocID']
    # dest = 'C:\\Users\\CaseyKopp\\Documents\\Python_Scripts\\ERCOT Downloads'
    file = csvList['zipFile'][0]
    df = pd.read_csv(file, compression='zip')

    df['HB'] = df['HourEnding'].str.split(':').str[0]
    df['HB'] = pd.to_numeric(df['HB']) - 1

    df['datetime'] = df['DeliveryDate'].astype(str) + ' ' + df['HB'].astype(str) + ':00'
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = pd.melt(df, id_vars=['datetime', 'DeliveryDate'],
                 value_vars=['North', 'South', 'West', 'Houston', 'SystemTotal'], value_name='value', var_name='type')
    return df
#pull data
load_fx = get7daydemand()
#groupby timestamp and zone
load_fx = load_fx.groupby(['datetime', 'type'])['value'].sum().round().reset_index()
load_fx = load_fx.pivot(index='datetime', columns='type', values='value').drop('SystemTotal', axis=1)


#### PULL WIND/SOLAR ASSET & LOAD SHIFT FACTORS ####
#function to query database
def queryDB(database: str, sql_string: str):
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    driver = '{ODBC Driver 17 for SQL Server}'
    database = '{0}'.format(database)
    conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

sql = 'select * from [dbo].[master_shift_factors]'

## pull everything first, before filtering for wind, solar, and load specific settlement point names
master_shifts = queryDB('ErcotMarketData', sql).drop('Direction', axis=1).set_index(['ConstraintName', 'ContingencyName'])

## WIND
#pull average shift factors for all wind units from master shift factor database
wind_shifts = master_shifts[list(wind_units.index)]
#groupby and average while keeping in long format
wind_shifts = wind_shifts.stack().reset_index(name='ShiftFactor').rename(columns={'level_2':'SettlementPoint'}).set_index('SettlementPoint').sort_index()

## SOLAR
#pull average shift factors for all solar units from master shift factor database
solar_shifts = master_shifts[list(solar_units.index)]
#groupby and average while keeping in long format
solar_shifts = solar_shifts.stack().reset_index(name='ShiftFactor').rename(columns={'level_2':'SettlementPoint'}).set_index('SettlementPoint').sort_index()

## LOAD
#create load zone list
lz = ['LZ_HOUSTON', 'LZ_NORTH', 'LZ_SOUTH', 'LZ_WEST']
#rename load_fx columns while list is readily available
load_fx.columns = lz
#pull average shift factors for all load regions
load_shifts = master_shifts[lz]
#groupby and average while keeping in long format
load_shifts = load_shifts.stack().reset_index(name='ShiftFactor').rename(columns={'level_2':'SettlementPoint'}).set_index('SettlementPoint').sort_index()


#### CALCULATE POTENTIAL CONGESTION IMPACT ####
## Wind
#make derated_wind_fx long
derated_wind_fx = pd.melt(derated_wind_fx, var_name='SettlementPoint', value_name='output', ignore_index=False).reset_index().set_index('SettlementPoint')
#merge wind_shifts and derated_wind_fx on settlement point (unit name)
wind_merged = pd.merge(wind_shifts, derated_wind_fx, how='left', on='SettlementPoint')
#calculate flows column
wind_merged['flow_delta'] = (wind_merged.ShiftFactor*wind_merged.output).round(3)
#group by and sum flows by constraint, contingency, time
wind_flows = pd.DataFrame((wind_merged.reset_index()).groupby(['ConstraintName', 'ContingencyName', 'datetime'])['flow_delta'].sum())

## Solar
#make derated_solar_fx long
derated_solar_fx = pd.melt(derated_solar_fx, var_name='SettlementPoint', value_name='output', ignore_index=False).reset_index().set_index('SettlementPoint')
#merge solar_shifts and derated_solar_fx on settlement point (unit name)
solar_merged = pd.merge(solar_shifts, derated_solar_fx, how='left', on='SettlementPoint')
#calculate flows column
solar_merged['flow_delta'] = (solar_merged.ShiftFactor*solar_merged.output).round(3)
#group by and sum flows by constraint, contingency, time
solar_flows = pd.DataFrame((solar_merged.reset_index()).groupby(['ConstraintName', 'ContingencyName', 'datetime'])['flow_delta'].sum())

## Load
#make load_fx long
load_fx = pd.melt(load_fx, var_name='SettlementPoint', value_name='output', ignore_index=False).reset_index().set_index('SettlementPoint')
#merge load_shifts and derated_load_fx on settlement point (unit name)
load_merged = pd.merge(load_shifts, load_fx, how='left', on='SettlementPoint')
#calculate flows column
load_merged['flow_delta'] = (-(load_merged.ShiftFactor*load_merged.output)).round(3)
#group by and sum flows by constraint, contingency, time
load_flows = pd.DataFrame((load_merged.reset_index()).groupby(['ConstraintName', 'ContingencyName', 'datetime'])['flow_delta'].sum())


#### SUM ALL FLOW DELTAS ####
#make dataframe list
flow_dfs = [wind_flows, solar_flows, load_flows]
#concatenate dfs and add
total_flows = pd.DataFrame((pd.concat(flow_dfs, join='outer', axis=1).fillna(0)).sum(1))
total_flows.columns = ['flow_delta']
#reset_index, add update time columns and reposition
total_flows = total_flows.reset_index()
total_flows['UpdateTime'] = pd.to_datetime(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
total_flows.columns = ['ConstraintName', 'ContingencyName', 'ForecastTimeStamp', 'Flows', 'UpdateTime']
total_flows = total_flows[['UpdateTime', 'ConstraintName', 'ContingencyName', 'ForecastTimeStamp', 'Flows']]


#### WRITE TO DATABASE ####
#function to connect to database
def dbConnect():
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    database = "ErcotMarketData"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)
    return(engine)

engine = dbConnect()

#write to daily flows forecast temp table
total_flows.to_sql('flows_forecast_daily',con=engine,index=False,method=None,if_exists="replace")
#append to flows_forecast table
total_flows.to_sql('flows_forecast',con=engine,index=False,method=None,if_exists="append")



# #filter for tomorrow
# tomorrow = total_flows[total_flows.ForecastTimeStamp.dt.date == (dt.date.today() + dt.timedelta(1))]
# x = (dt.date.today() + dt.timedelta(1)).strftime('%m%d')
# #export
# tomorrow.to_csv(r"C:\Users\MattSindler\Projects\asset_dash\Congestion Analysis\flows_impact_{}.csv".format(x))
