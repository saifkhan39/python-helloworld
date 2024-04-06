import zipfile, io, os, glob
from zipfile import ZipFile
from io import StringIO, BytesIO
from requests_pkcs12 import get
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, datetime, time
from lxml import etree
import requests
import json, gzip
from urllib.request import urlopen
import sqlalchemy, urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import pyodbc
from sqlalchemy.sql.expression import column
from yesapi.functions import *

### CALCULATE WIND AND SOLAR FORECASTS BY UNIT WITH IMPLIED CAPACITY FACTORS ####
## PROCESS DOCUMENTATION ##
# 2. download unit capacities for wind/solar for particular season from ercot
# 3. create table with UnitName, ResourceName, ResourceType, Zone, Capacity
#     ResourceName column originally titled 'UNIT CODE'
# 4. index Settlement Point Name from resource names QUERIED and match on resource_names to
#     add a settlement point name column to the unit capacities dataframe/csv

resource_names = pd.read_csv(r"C:\BRP_scrapers\flows forecast dependencies\resource_names.csv")
unit_capacities = pd.read_csv(r"C:\BRP_scrapers\flows forecast dependencies\unit_capacities.csv")

units = pd.merge(unit_capacities, resource_names[['ResourceName', 'SettlementPointName']], how='left', on='ResourceName').dropna()
units = units[['ResourceName', 'SettlementPointName', 'ResourceType', 'Zone', 'Capacity']]

#datetime variable
today = datetime.today().strftime('%m/%d/%Y')
end = (datetime.today() + timedelta(days=7)).strftime('%m/%d/%Y')

## WIND
#calculate wind capacity by zone
wind_cap = pd.DataFrame(units[units.ResourceType == 'WIND'].groupby(units.Zone)['Capacity'].sum().round()).T
#get wind forecast by zone
wind_fx = pull_wind(['GR_COASTAL', 'GR_NORTH', 'GR_PANHANDLE', 'GR_SOUTH', 'GR_WEST'], 'fx', today, end).set_index('DATETIME').iloc[:,:-5]
wind_fx.columns = ['COASTAL', 'NORTH', 'PANHANDLE', 'SOUTH', 'WEST']
wind_fx.index.names = ['datetime']
#calculate zonal wind capacity factors by hour
wind_cf = (wind_fx/wind_cap.values).round(3)
#aggreate wind_units and their capacity by settlement point name
wind_units = units[units.ResourceType == 'WIND'].groupby(['SettlementPointName', 'Zone']).sum('Capacity').reset_index().set_index('SettlementPointName')
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
solar_cap = pd.DataFrame(units[units.ResourceType == 'PVGR'].groupby(units.ResourceType)['Capacity'].sum().round()).T
#get solar forecast by zone
solar_fx = pull_solar(['ERCOT'], 'fx', today, end).set_index('DATETIME').iloc[:,:-5]
solar_fx.columns = ['value']
solar_fx.index.names = ['datetime']
#calculate zonal solar capacity factors by hour
solar_cf = solar_fx/solar_cap.values
#replace values greater than 1
solar_cf.value = np.where(solar_cf.value > 1, 1, solar_cf.value).round(3)
#aggreagate solar units and their capacities by settlemeent point name
solar_units = units[units.ResourceType == 'PVGR'].groupby('SettlementPointName').sum('Capacity')
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
#pull data
load_fx = pull_demand(['HOUSTON', 'NORTH (ERCOT)', 'SOUTH', 'WEST (ERCOT)'], 'fx', today, end).set_index('DATETIME').iloc[:,:-5]
load_fx.columns = ['Houston', 'North', 'South', 'West']
load_fx.index.names = ['datetime']

#### PULL WIND/SOLAR ASSET & LOAD SHIFT FACTORS ####
#mis certs
pfx_path = r'C:\BRP_scrapers\1172089695000$msindler.pfx'
pfx_password = 'XIX7f!tmzWUv4'

#create date variables to choose last few days
day = dt.date.today().strftime('%Y%m%d.')
day1 = (dt.date.today()+timedelta(days=-1)).strftime('%Y%m%d.')
day2 = (dt.date.today()+timedelta(days=-2)).strftime('%Y%m%d.')
day3 = (dt.date.today()+timedelta(days=-3)).strftime('%Y%m%d.')

#pull mis functions
def pull_mis(reportTypeId):
    #scrape daily shift factor files
    parser = etree.HTMLParser() # object for HTML text
    page = get('https://mis.ercot.com/misapp/GetReports.do?reportTypeId={}&mimic_duns=1172089695000'.format(reportTypeId), pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    html = page.content.decode("utf-8")
    tree = etree.parse(StringIO(html), parser=parser)
    #obtain <a href='url'> links in html
    refs = tree.xpath("//a")
    hrefs = [link.get('href', '') for link in refs]
    links = [l for l in hrefs]
    html_tag = tree.getroot()
    #create empty list to append filenames to
    filenames = []
    #for loop through and append
    for j in html_tag.iter("td"):# class='labelOptional_ind'"):
        text = j.text
        if text != None:
            filenames.append(text)

    #create filename daataframe with filename and link columns
    files = pd.DataFrame(list(zip(filenames,links)), columns = ["Names","Links"])
    #csv only
    files = files[files['Names'].str.contains("_csv")].reset_index(drop = True)
    #create empty dataframe to append daily dataframe to
    big_data = pd.DataFrame()
    #for loop through lastest 9 files (should be today+1 to today-7 aka tomorrow to last week)
    for i in range(0, len(files)):
        if (day in files.loc[i, 'Names']) or (day1 in files.loc[i, 'Names']) or (day2 in files.loc[i, 'Names']) or (day3 in files.loc[i, 'Names']):
            #create download url
            file_url = "https://mis.ercot.com/" + files.loc[i,'Links']
            print(file_url)
            #make request with certs
            cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
            #create empty dataframe
            master_data = pd.DataFrame()
            #extract from each daily zip file w/o downloading
            with ZipFile(BytesIO(cert_url.content)) as zfile:
                csvs = zfile.infolist()
                csv_list = []
                for csv in csvs:
                    df = pd.read_csv(zfile.open(csv.filename))
                    csv_list.append(df)
                    data = pd.concat(csv_list)
            #append to empty dataframe created in for loop
            master_data = master_data.append(data)
            #append to big_data
            big_data = big_data.append(master_data)

    return big_data

## pull everything first, before filtering for wind, solar, and load specific settlement point names
master_shifts = pull_mis('16013')
master_shifts.columns = ['timestamp', 'flag', 'id', 'ConstraintName', 'ContingencyName', 'SettlementPoint', 'ShiftFactor']

## WIND
#pull average shift factors for all wind units over the past 7 days of DA congestion
wind_shifts = master_shifts[master_shifts.SettlementPoint.isin(list(wind_units.index))]
#groupby and average while keeping in long format
wind_shifts = wind_shifts.groupby(['SettlementPoint','ConstraintName', 'ContingencyName'])['ShiftFactor'].mean().reset_index().set_index('SettlementPoint')

## SOLAR
#pull average shift factors for all solar units over the past 7 days of DA congestion
solar_shifts = master_shifts[master_shifts.SettlementPoint.isin(list(solar_units.index))]
#groupby and average while keeping in long format
solar_shifts = solar_shifts.groupby(['SettlementPoint','ConstraintName', 'ContingencyName'])['ShiftFactor'].mean().reset_index()

## LOAD
#create load zone list
lz = ['LZ_HOUSTON', 'LZ_NORTH', 'LZ_SOUTH', 'LZ_WEST']
#rename load_fx columns while list is readily available
load_fx.columns = lz
#pull average shift factors for all load regions over the past 7 days of DA congestion
load_shifts = master_shifts[master_shifts.SettlementPoint.isin(lz)]
#groupby and average while keeping in long format
load_shifts = load_shifts.groupby(['SettlementPoint','ConstraintName', 'ContingencyName'])['ShiftFactor'].mean().reset_index().set_index('SettlementPoint')

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

#append to flows_forecast table
total_flows.to_sql('flows_forecast_rt',con=engine,index=False,method=None,if_exists="append")

print('flows uploaded')
