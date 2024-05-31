import os
import requests
import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, timedelta, datetime
import time
from io import StringIO, BytesIO
import pickle
import csv
from typing import Union
from galaxy_vault.factory import VaultFactory 

# GLOBAL VARIABLES
#authentication variable
factory = VaultFactory()
vault = factory.get_vault()
yes_energy_username = vault.get_secret_individual_account('yes-energy-username')
yes_energy_password = vault.get_secret_individual_account('yes-energy-password')
auth = (yes_energy_username, yes_energy_password)
#list comprehension logic check for ISO choice
accepted_ercot = ['ERCOT', 'ercot', 'Ercot', 'tx', 'TX']
accepted_caiso = ['CAISO', 'caiso', 'Caiso', 'ca', 'CA']

#load dictionary via pickle file
this_dir, this_filename = os.path.split(__file__)
data_path = os.path.join(this_dir, 'yes_api_dict.pkl')
with open(data_path, 'rb') as handle:
    yes_api_dict = pickle.load(handle)

#### FUNCTIONS ####
## Pricing Data
def pull_prices(nodes: list, price_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for dart choice
    dart = ['DART', 'dart', 'da/rt', 'DA/RT']
    #list comprehension logic check for dart choice
    fifteen = ['15min', '15', 'fifteen', '15MIN']
    #declare datatype
    datatype = price_type
    #create empty string to fulfill items poriton of api params
    items_list = []
    #for loop through node list to create items_list
    if datatype in dart:
        #logic check if CAISO 15min DART requested
        if period in fifteen:
            for i in nodes:
                objectid = yes_api_dict['pricing'][i]
                item = 'DALMP:' + str(objectid) + ',LMP_15MIN:' + str(objectid)
                items_list.append(item)
                #redefine period variable
                period = 'raw'
        else:
            for i in nodes:
                objectid = yes_api_dict['pricing'][i]
                item = 'DALMP:' + str(objectid) + ',RTLMP:' + str(objectid)
                items_list.append(item)
    elif datatype == '15v5':
        for i in nodes:
            objectid = yes_api_dict['pricing'][i]
            item = 'DALMP:' + str(objectid) + ',LMP_15MIN:' + str(objectid) + ',RTLMP:' + str(objectid)
            items_list.append(item)
    elif datatype == 'ha':
        for i in nodes:
            objectid = yes_api_dict['pricing'][i]
            item = 'DALMP:' + str(objectid) + ',HALMP:' + str(objectid) + ',LMP_15MIN:' + str(objectid) + ',RTLMP:' + str(objectid)
            items_list.append(item)
    else:
        for i in nodes:
            objectid = yes_api_dict['pricing'][i]
            item = datatype + ':' + str(objectid)
            items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Proxy Pricing Data
def pull_proxy_prices(nodes: list, price_type: str, period = 'hour'):
    #list comprehension logic check for dart choice
    dart = ['DART', 'dart', 'da/rt', 'DA/RT']
    #declare datatype
    datatype = price_type
    #create empty string to fulfill items poriton of api params
    items_list = []
    #for loop through node list to create items_list
    if datatype in dart:
        for i in nodes:
            objectid = yes_api_dict['pricing'][i]
            item = 'DALMP:' + str(objectid) + ',RTLMP:' + str(objectid)
            items_list.append(item)
    else:
        for i in nodes:
            objectid = yes_api_dict['pricing'][i]
            item = datatype + ':' + str(objectid)
            items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'datecollections': 9258275,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## System Lambda Data
def pull_sys_lambda(type: str, start: str, end: str, period = 'hour'):
    #define objectid variable
    objectid = 10000756298
    #list comprehension logic check type choice
    da = ['DA', 'da']
    rt = ['RT', 'rt']
    indicative = ['indicative', 'fx']
    #list comprehension logic check for dart choice
    dart = ['DART', 'dart', 'da/rt', 'DA/RT']
    #declare datatype
    if type in da:
        datatype = 'SYSTEM_LAMBDA_DA'
        #create items string
        items = datatype + ':' + str(objectid)
    elif type in rt:
        datatype = 'SYSTEM_LAMBDA'
        #create items string
        items = datatype + ':' + str(objectid)
    elif type in indicative:
        datatype = 'SYSTEM_LAMBDA_INDC'
        #create items string
        items = datatype + ':' + str(objectid)
    elif type in dart:
        #create items string
        items = 'SYSTEM_LAMBDA_DA:' + str(objectid) + ',SYSTEM_LAMBDA:' + str(objectid)
    else:
        print("type argument criteria not met: try either 'da', 'rt', or 'indicative'")
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Pull Calculated DART Spreads
def pull_dart(nodes: list, start: str, end: str, period = 'hour'):
    #declare datatype
    datatype = 'DARTLMP'
    #create empty string to fulfill items poriton of api params
    items_list = []
    #for loop through node list to create items_list
    for i in nodes:
        objectid = yes_api_dict['pricing'][i]
        item = datatype + ':' + str(objectid)
        items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    #multiply by -1 to reaturn proper dart
    dart_columns = list(df.columns[1:len(nodes)+1])
    for i in dart_columns:
        df[i] = df[i]*-1

    return df

## Ancillary Price Data
def pull_as(iso: str, product_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    reg_up = ['reg up', 'regup', 'reg_up', 'Reg Up', 'Regup', 'Reg_Up', 'REGUP', 'REG_UP', 'ASM_DA_REGUP']
    reg_down = ['reg down', 'regdown', 'reg_down', 'Reg Down', 'Regdown', 'Reg_Down', 'REGDOWN', 'REG_DOWN', 'ASM_DA_REGDOWN']
    spin = ['spin', 'Spin', 'RRS', 'rrs', 'ASM_DA_RRS']
    non_spin = ['nonspin', 'non_spin', 'non spin', 'NON SPIN', 'NON_SPIN', 'ASM_DA_NONSPIN']
    all = ['all', 'All', 'ALL']

    if iso in accepted_ercot:
        objectid = 10000756298
        if product_type in reg_up:
            datatype = 'ASM_DA_REGUP'
        if product_type in reg_down:
            datatype = 'ASM_DA_REGDOWN'
        if product_type in spin:
            datatype = 'ASM_DA_RRS'
        if product_type in non_spin:
            datatype = 'ASM_DA_NONSPIN'
        if product_type in all:
            datatype = ['ASM_DA_REGDOWN', 'ASM_DA_REGUP', 'ASM_DA_RRS', 'ASM_DA_NONSPIN']
    elif iso in accepted_caiso:
        objectid = 10002484315 #only have AS_CAISO_EXP at the moment
        if product_type in reg_up:
            datatype = 'DAM RU_CLR_PRC'
        if product_type in reg_down:
            datatype = 'DAM RD_CLR_PRC'
        if product_type in spin:
            datatype = 'DAM SP_CLR_PRC'
        if product_type in non_spin:
            datatype = 'DAM NS_CLR_PRC'
        if product_type in all:
            datatype = ['DAM RD_CLR_PRC', 'DAM RU_CLR_PRC', 'DAM SP_CLR_PRC', 'DAM NS_CLR_PRC']
    else:
        print("ISO argument criteria not met: try either 'ercot' or 'caiso'")
    #create item requirement for query parameters
    if product_type in all:
        items_list = []
        for i in datatype:
            item = i + ':' + str(objectid)
            items_list.append(item)
        items = ','.join(items_list)
    else:
        items = datatype + ':' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Demand Data
def pull_demand(nodes: list, type: str, start: str, end: str, period = 'hour'):
    """
    type: actuals, current forecasts, bid close forecasts, and 'all'.
    
    bid close forecasts only exists for 'WZ_' objectid's.
    """
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']
    accepted_bidclose = ['bidclose', 'bid close', 'bid_close']

    if type in accepted_historical:
        datatype = 'RTLOAD'
    elif type in accepted_forecast:
        datatype = 'LOAD_FORECAST'
    elif type in accepted_bidclose:
        datatype = 'BIDCLOSE_LOAD_FORECAST'
    elif type == 'all':
        datatype = ['RTLOAD', 'LOAD_FORECAST', 'BIDCLOSE_LOAD_FORECAST']
    else:
        print("different 1st argument needed: try 'RT' or 'forecast' or 'bidclose'")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #logic check if datatype list is used or not
    if type == 'all':
        #for loop through node list to create items_list
        for d in datatype:
            for i in nodes:
                objectid = yes_api_dict['load'][i]
                item = d + ':' + str(objectid)
                items_list.append(item)
    else:
        for i in nodes:
            objectid = yes_api_dict['load'][i]
            item = datatype + ':' + str(objectid)
            items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## ERCOT Net Load Data
def pull_netload(type: str, start: str, end: str, period = 'hour'):
    """
    type: actuals, current forecasts, bid close forecasts, and 'all'.
      
    """
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']
    accepted_bidclose = ['bidclose', 'bid close', 'bid_close']

    if type in accepted_historical:
        datatype = 'NET_LOAD_RT'
    elif type in accepted_forecast:
        datatype = 'NET_LOAD_FORECAST_CURRENT'
    elif type in accepted_bidclose:
        datatype = 'NET_LOAD_FORECAST_BID_CLOSE'
    elif type == 'all':
        datatype = ['NET_LOAD_RT', 'NET_LOAD_FORECAST_CURRENT', 'NET_LOAD_FORECAST_BID_CLOSE']
    else:
        print("different 1st argument needed: try 'RT' or 'forecast' or 'bidclose'")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #logic check if type is 'all'
    if type == 'all':
        #create items_list
        for d in datatype:
            objectid = yes_api_dict['load']['ERCOT']
            item = d + ':' + str(objectid)
            items_list.append(item)
    else:
        objectid = yes_api_dict['load']['ERCOT']
        item = datatype + ':' + str(objectid)
        items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Wind Data
def pull_wind(nodes: list, type: str, start: str, end: str, period = 'hour'):
    """
    type: actuals, current forecasts, bid close forecasts, and 'all'.
      
    """
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']
    accepted_bidclose = ['bidclose', 'bid close', 'bid_close']
    #list comprehension logic check for either ercot or caiso data
    ercot = ['ERCOT', 'GR_COASTAL', 'GR_ERCOT', 'GR_NORTH', 'GR_PANHANDLE', 'GR_SOUTH', 'GR_WEST', 'NORTH (ERCOT)', 'SOUTH_HOUSTON', 'WEST (ERCOT)', 'WEST_NORTH']
    caiso = ['CAISO', 'NP15', 'SP15']

    #logic check for ISO
    check_ercot = all(item in ercot for item in nodes)
    check_caiso = all(item in caiso for item in nodes)

    if type in accepted_historical:
        if check_ercot is True:
            datatype = 'WINDDATA'
        elif check_caiso is True:
            datatype = 'OUTLOOK_WIND_5MIN'
        else:
            print("error with actuals node list: must be comprised of only ercot or caiso nodes")
    elif type in accepted_forecast:
        if check_ercot is True:
            datatype = 'WIND_STWPF'
        elif check_caiso is True:
            datatype = 'WIND_FORECAST'
        else:
            print("error with forecast node list: must be comprised of only ercot or caiso nodes")
    elif type in accepted_bidclose:
        if check_ercot is True:
            datatype = 'WIND_STWPF_BIDCLOSE'
        elif check_caiso is True:
            datatype = 'WIND_FORECAST_BIDCLOSE'
        else:
            print("error with forecast node list: must be comprised of only ercot or caiso nodes")
    elif type == 'all':
        if check_ercot is True:
            datatype = ['WINDDATA', 'WIND_STWPF', 'WIND_STWPF_BIDCLOSE']
        elif check_caiso is True:
            datatype = ['OUTLOOK_WIND_5MIN', 'WIND_FORECAST', 'WIND_FORECAST_BIDCLOSE']
        else:
            print("error with forecast node list: must be comprised of only ercot or caiso nodes")
    else:
        print("different 1st argument needed: try 'RT' or 'forecast'")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #logic check if type is 'all'
    if type == 'all':
        #for loop through node list to create items_list
        for d in datatype:
            for i in nodes:
                objectid = yes_api_dict['wind'][i]
                item = d + ':' + str(objectid)
                items_list.append(item)
    else:
        #for loop through node list to create items_list
        for i in nodes:
            objectid = yes_api_dict['wind'][i]
            item = datatype + ':' + str(objectid)
            items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Solar Data
def pull_solar(nodes: list, type: str, start: str, end: str, period = 'hour'):
    """
    type: actuals, current forecasts, bid close forecasts, and 'all'.
    
    There are no bid close forecasts available for texas solar regions.
      
    """
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']
    accepted_bidclose = ['bidclose', 'bid close', 'bid_close']
    #list comprehension logic check for either ercot or caiso data
    ercot = ['ERCOT', 'CenterEast', 'CenterWest', 'FarEast', 'FarWest', 'NorthWest', 'SouthEast']
    caiso = ['CAISO', "NO' HUB", 'NP15', 'SP15', 'ZP26']

    #logic check for ISO
    check_ercot = all(item in ercot for item in nodes)
    check_caiso = all(item in caiso for item in nodes)

    if type in accepted_historical:
        if check_ercot is True:
            datatype = 'GENERATION_SOLAR_RT'
        elif check_caiso is True:
            datatype = 'OUTLOOK_SOLAR_5MIN'
        else:
            print("error with forecast node list: must be comprised of only ercot or caiso nodes")
    elif type in accepted_forecast:
        if check_ercot is True:
            datatype = 'SOLAR_STPPF'
        elif check_caiso is True:
            datatype = 'SOLAR_FORECAST'
        else:
            print("error with forecast node list: must be comprised of only ercot or caiso nodes")
    elif type in accepted_bidclose:
        if check_ercot is True:
            datatype = 'SOLAR_STPPF_BIDCLOSE'
        elif check_caiso is True:
            datatype = 'BIDCLOSE_SOLAR_FORECAST'
        else:
            print("error with forecast node list: must be comprised of only ercot or caiso nodes")
    elif type == 'all':
        if check_ercot is True:
            datatype = ['GENERATION_SOLAR_RT', 'SOLAR_STPPF', 'SOLAR_STPPF_BIDCLOSE']
        elif check_caiso is True:
            datatype = ['OUTLOOK_SOLAR_5MIN', 'SOLAR_FORECAST', 'BIDCLOSE_SOLAR_FORECAST']
        else:
            print("error with forecast node list: must be comprised of only ercot or caiso nodes")
    else:
        print("different 1st argument needed: try 'RT' or 'forecast'")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #logic check if type is 'all'
    if type == 'all':
        #for loop through node list to create items_list
        for d in datatype:
            for i in nodes:
                objectid = yes_api_dict['solar'][i]
                item = d + ':' + str(objectid)
                items_list.append(item)
    else:
        #for loop through node list to create items_list
        for i in nodes:
            objectid = yes_api_dict['solar'][i]
            item = datatype + ':' + str(objectid)
            items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Wind Curtailment Data
def pull_wind_curtailment(nodes: list, start: str, end: str, period = 'hour'):
    #create empty string to fulfill items poriton of api params
    items_list = []
    #for loop through node list to create items_list
    for i in nodes:
        objectid = yes_api_dict['wind'][i]
        item = 'WIND_COPHSL:' + str(objectid) + ',WINDDATA:' + str(objectid)
        items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Solar Curtailment Data
def pull_solar_curtailment(nodes: list, start: str, end: str, period = 'hour'):
    #create empty string to fulfill items poriton of api params
    items_list = []
    #for loop through node list to create items_list
    for i in nodes:
        objectid = yes_api_dict['solar'][i]
        item = 'SOLAR_COPHSL:' + str(objectid) + ',GENERATION_SOLAR_RT:' + str(objectid)
        items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Gen Outage Data
def pull_gen_outages(iso: str, nodes: list, outage_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for type of outage data
    accepted_total = ['total', 'Total', 'all', 'All', 'system']
    accepted_renew = ['renew', 'Renew', 'renewable', 'Renewable', 'renewables']
    accepted_hydro = ['hydro', 'Hydro']
    accepted_thermal = ['thermal', 'Thermal']

    if iso in accepted_ercot:
        if outage_type in accepted_total:
            datatype = 'TOTAL_RESOURCE_CAP_OUT'
        elif outage_type in accepted_renew:
            datatype = 'RENEW_RESOURCE_CAP_OUT'
        else:
            print("ERCOT only has 'total' and 'renewable' generation outage data")
    elif iso in accepted_caiso:
        if outage_type in accepted_total:
            print('CAISO total aggregate outage data unavailable')
        elif outage_type in accepted_renew:
            datatype = 'REG_REN_GEN_OFFLINE'
        elif outage_type in accepted_hydro:
            datatype = 'REG_HYDRO_GEN_OFFLINE'
        elif outage_type in accepted_thermal:
            datatype = 'REG_THERM_GEN_OFFLINE'
        else:
            print("outage_type argument criteria not met: try 'total', 'renew', 'hydro', or 'thermal'")
    else:
        print("Issue with regions supplied in first argument. Must be acceptable generation outage datatypes")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #for loop through node list to create items_list
    for i in nodes:
        objectid = yes_api_dict['gen_outages'][i]
        item = datatype + ':' + str(objectid)
        items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Temperature Data
def pull_temps(location: Union[str, list[str]], type: str, start: str, end: str, period = 'hour'):
    """
    type: actuals, current forecasts, 'vs', and 'all'.
    
    """
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']

    if type in accepted_historical:
        datatype = 'TEMP_F'
    elif type in accepted_forecast:
        datatype = 'NWS7FC_TMP_LATEST'
    elif type in ['v', 'vs', 'versus']:
        datatype = ['TEMP_F', 'NWS7FC_TMP_LATEST']
    elif type == 'all':
        datatype = ['TEMP_F', 'NWS7FC_TMP_LATEST', 'TEMP_NORM', 'TEMP_NORM_05', 'TEMP_NORM_95']
    else:
        print("different 1st argument needed: try 'RT' or 'forecast' or 'bidclose'")
    
    #create empty string to fulfill items poriton of api params
    items_list = []
    #check type of location argument input
    if isinstance(location, str) is True:
        #logic check if type is 'all'
        if type in ['v', 'vs', 'versus', 'all']:
            #create items_list
            for d in datatype:
                objectid = yes_api_dict['temperature'][location]
                item = d + ':' + str(objectid)
                items_list.append(item)
        else:
            objectid = yes_api_dict['temperature'][location]
            item = datatype + ':' + str(objectid)
            items_list.append(item)
    else:
        #logic check if type is 'all'
        if type in ['v', 'vs', 'versus', 'all']:
            #for loop through node list to create items_list
            for d in datatype:
                for i in location:
                    objectid = yes_api_dict['temperature'][i]
                    item = d + ':' + str(objectid)
                    items_list.append(item)
        else:
            #for loop through node list to create items_list
            for i in location:
                objectid = yes_api_dict['temperature'][i]
                item = datatype + ':' + str(objectid)
                items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start, "%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end, "%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(
        vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    
    return df

## Weather Data
def pull_weather_data(location: Union[str, list[str]], type: Union[str, list[str]], start: str, end: str, period = 'hour'):
    """
    Try one of the following: 'TEMP_F', 'DEWPOINT_F', 'HEATINDEX', 'CLOUD_COVER_PCT', 'WIND_MPH', 'HDD', 'CDD'.
    Or try 'all'.
     
    """
    
    if type == 'all':
        datatype = ['TEMP_F', 'DEWPOINT_F', 'HEATINDEX', 'CLOUD_COVER_PCT', 'WIND_MPH', 'HDD', 'CDD']
    else:
        datatype = type
    
    #create empty string to fulfill items poriton of api params
    items_list = []
    #check type of location argument input
    if isinstance(location, str) is True:
        #logic check if type is 'all'
        if isinstance(type, list):
            #create items_list
            for d in datatype:
                objectid = yes_api_dict['temperature'][location]
                item = d + ':' + str(objectid)
                items_list.append(item)
        else:
            objectid = yes_api_dict['temperature'][location]
            item = datatype + ':' + str(objectid)
            items_list.append(item)
    else:
        #logic check if type is 'all'
        if isinstance(type, list):
            #for loop through node list to create items_list
            for d in datatype:
                for i in location:
                    objectid = yes_api_dict['temperature'][i]
                    item = d + ':' + str(objectid)
                    items_list.append(item)
        else:
            #for loop through node list to create items_list
            for i in location:
                objectid = yes_api_dict['temperature'][i]
                item = datatype + ':' + str(objectid)
                items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Gas Data (HSC)
def pull_gas(start: str, end: str, period = 'hour'):
    #define datatype variable
    datatype = 'GASPRICE'
    #define objectid variable
    objectid = 10000002639
    #create items string
    items = datatype + ':' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## ORDC Data
def pull_ordc_data(start: str, end: str, period = 'hour'):
    #define objectid variable
    objectid = 10000756298
    #datatypes
    datatypes = ['SYSTEM_LAMBDA', 'RT_OR_PRADDER', 'RT_OFF_PRADDER', 'RT_ORD_PRADDER', 'RT_ON_CAP', 'RT_OFF_CAP']
    #create items_list
    items_list = []
    for i in datatypes:
        item = i + ':' + str(objectid)
        items_list.append(item)
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    #rename columns
    df = df.rename(columns={df.columns[1]: 'SYSTEM_LAMBDA', df.columns[2]: 'RT_OR_PRADDER', df.columns[3]: 'RT_OFF_PRADDER', df.columns[4]: 'RT_ORD_PRADDER', df.columns[5]: 'RT_ON_CAP', df.columns[6]: 'RT_OFF_CAP'})
    #calculate ordc
    df['ordc_adders'] = df.RT_OR_PRADDER + df.RT_ORD_PRADDER

    #reorder columns
    first_cols = list(df.columns[0:7]) + ['ordc_adders']
    last_cols = [col for col in df.columns if col not in first_cols]

    df = df[first_cols + last_cols]

    return df

## ORDC and Fundamental Data
def pull_ordc_and_fundies(start: str, end: str, period = 'hour'):
    #define objectid variable
    objectid = 10000756298
    #datatypes
    datatypes = ['SYSTEM_LAMBDA', 'RT_OR_PRADDER', 'RT_OFF_PRADDER', 'RT_ORD_PRADDER', 'RT_ON_CAP', 'RT_OFF_CAP', 'TOTAL_RESOURCE_CAP_OUT', 'HDL', 'PHYS_RESP_CAP']
    #create items_list
    items_list = []
    for i in datatypes:
        item = i + ':' + str(objectid)
        items_list.append(item)
    #add wind and solar data to the list
    demand_solar_wind = ['GENERATION_SOLAR_RT:10000712973', 'WINDDATA:10000712973', 'RTLOAD:10000712973']
    items_list = items_list + demand_solar_wind
    #join items in items_list to create item paramenter string
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    #rename columns
    df = df.rename(columns={df.columns[1]: 'SYSTEM_LAMBDA', df.columns[2]: 'RT_OR_PRADDER', df.columns[3]: 'RT_OFF_PRADDER',
        df.columns[4]: 'RT_ORD_PRADDER', df.columns[5]: 'RT_ON_CAP', df.columns[6]: 'RT_OFF_CAP',
        df.columns[7]: 'GEN_OUTAGES', df.columns[8]: 'HDL', df.columns[9]: 'PRC', df.columns[10]: 'SOLAR', df.columns[11]: 'WIND', df.columns[12]: 'LOAD'})

    df['NETLOAD'] = df.LOAD - df.WIND - df.SOLAR
    df['NETLOAD_OUTAGES'] = df.LOAD - df.WIND - df.SOLAR + df.GEN_OUTAGES
    df['thermals_plus_reserves'] = df.NETLOAD + df.RT_ON_CAP
    df['thermals_outages_reserves'] = df.NETLOAD_OUTAGES + df.RT_ON_CAP
    df['netload_prc'] = df.NETLOAD_OUTAGES + df.PRC

    #calculate lambda + adders
    df['lambda_adders'] = df.SYSTEM_LAMBDA + df.RT_OR_PRADDER + df.RT_ORD_PRADDER + df.RT_OFF_PRADDER

    #reorder columns
    first_cols = list(df.columns[0:13]) + ['NETLOAD', 'NETLOAD_OUTAGES', 'thermals_plus_reserves', 'thermals_outages_reserves', 'netload_prc', 'lambda_adders']
    last_cols = [col for col in df.columns if col not in first_cols]

    df = df[first_cols + last_cols]

    return df

## Online/Offline Reserve Capacity Data
def pull_reserve_cap(start: str, end: str, period = 'hour'):
    #define objectid variable
    objectid = 10000756298
    #create items string
    items = 'RT_ON_CAP:' + str(objectid) + ',RT_OFF_CAP:' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    #rename columns
    df = df.rename(columns={df.columns[1]: 'on_reserve_cap', df.columns[2]: 'off_reserve_cap'})
    #calculate total reserve cap
    total_cap = df.on_reserve_cap + df.off_reserve_cap
    df.insert(3, 'total_reserve_cap', total_cap)

    return df

## Non-Spin Reserve Capacity Data
def pull_npsin_reserve_cap(start: str, end: str, period = 'hour'):
    #define objectid variable
    objectid = 10000756298
    #create items string
    items = 'NONSPIN_ON_GEN_OFFER:' + str(objectid) + ',NONSPIN_OFF_GEN_RSCS:' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    #rename columns
    df = df.rename(columns={df.columns[1]: 'on_npsin_resources', df.columns[2]: 'off_nspin_resources'})

    return df

## PRC Data
def pull_prc(start: str, end: str, period = 'hour'):
    #define objectid variable
    objectid = 10000756298
    #create items string
    items = 'PHYS_RESP_CAP:' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Frequency Data
def pull_frequency(start: str, end: str, period = 'hour'):
    #define datatype variable
    datatype = 'FREQUENCY'
    #define objectid variable
    objectid = 10000756298
    #create items string
    items = datatype + ':' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Inertia Data
def pull_inertia(start: str, end: str, period = 'hour'):
    #define datatype variable
    datatype = 'SYSTEM_INERTIA'
    #define objectid variable
    objectid = 10000756298
    #create items string
    items = datatype + ':' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Pull AS Procurement Data
def pull_as_volumes(start: str, end: str, period = 'hour'):
    #define datatype variable
    datatypes = ['DA_ASMVOL_REG_D', 'DA_ASMVOL_REG_U', 'DA_ASMVOL_RRS', 'DA_ASMVOL_NOSPN']
    #define objectid variable
    objectid = 10000756298
    #create emtpy items list
    items_list = []
    #for loop through datatypes
    for i in datatypes:
        item = i + ':' + str(objectid)
        items_list.append(item)
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Pull AS Deployment Data
def pull_as_deployments(start: str, end: str, period = 'hour'):
    #define datatype variable
    datatypes = ['DEP_REGUP', 'UNDEP_REGUP', 'DEP_REGDOWN', 'UNDEP_REGDOWN']
    #define objectid variable
    objectid = 10000756298
    #create emtpy items list
    items_list = []
    #for loop through datatypes
    for i in datatypes:
        item = i + ':' + str(objectid)
        items_list.append(item)
    items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    #calculate deployment rate
    df['regup_deployment_rate'] = df.iloc[:,1]/(df.iloc[:,1] + df.iloc[:,2])
    df['regdn_deployment_rate'] = df.iloc[:,3]/(df.iloc[:,3] + df.iloc[:,4])
    #drop undeployed columns
    cols = [2,4]
    df.drop(df.columns[cols], axis=1, inplace=True)
    #rename columns
    df = df.rename(columns={df.columns[1]: 'deployed_regup', df.columns[2]: 'deployed_regdn'})
    #reorder columns
    front = ['DATETIME', 'deployed_regup', 'regup_deployment_rate', 'deployed_regdn', 'regdn_deployment_rate']
    df = df[front + [col for col in df.columns if col not in front]]

    return df

## Stock Yes Energy API Pull (only items, start, and end parameters required)
def pull_yes_data(items: str, start: str, end: str, period = 'hour'):
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

## Stock Yes Energy API Pull given html link (typically for metadata)
def pull_yes_html(link):
    #define query
    query = requests.get(link, auth=auth)
    #pull and manipulate raw data
    df = pd.read_html(query.text)

    return pd.concat(df)

## Additional Functions
#Filter to only on-peak hours
def onpk(df):
    if 'HOURENDING' in df.columns:
        df = df[df.HOURENDING.isin(list(range(7,23)))]
    else:
        df['he'] = np.where(df.index.hour == 0, 24, df.index.hour)
        df = df[df.he.isin(list(range(7,23)))].drop('he', axis=1)
    return df

#Filter and calculate opa
def opa(df):
    if 'HOURENDING' in df.columns:
        df = df[df.HOURENDING.isin(list(range(7,23)))]
        columns_to_drop = ['HOURENDING', 'MARKETDAY', 'PEAKTYPE', 'MONTH', 'YEAR']
        df = df.drop(columns=[col for col in df if col in columns_to_drop]).mean(numeric_only=True).round(2)
    else:
        df = df[df.index.hour.isin(list(range(7,23)))].mean().round(2)
    return df

#Filter for morning hours
def morning(df):
    if 'HOURENDING' in df.columns:
        df = df[df.HOURENDING.isin(list(range(7, 10)))]
    else:
        df['he'] = np.where(df.index.hour == 0, 24, df.index.hour)
        df = df[df.he.isin(list(range(7, 10)))].drop('he', axis=1)
    return df

#Filter for evening hours
def evening(df):
    if 'HOURENDING' in df.columns:
        df = df[df.HOURENDING.isin(list(range(17, 20)))]
    else:
        df['he'] = np.where(df.index.hour == 0, 24, df.index.hour)
        df = df[df.he.isin(list(range(17, 20)))].drop('he', axis=1)
    return df

#Set index to 'DATETIME' and chop off date/time related columns
def clean(df, keep_datetime = False):
    if keep_datetime == True:
        new_df = df.set_index('DATETIME').iloc[:,:-3]
    else:
        new_df = df.set_index('DATETIME').iloc[:,:-5]
    return new_df

#add hour ending columns after cleaning
def add_he(df):
    if 'DATETIME' in df.columns:
        df['he'] = np.where(df.DATETIME.dt.hour == 0, 24, df.DATETIME.dt.hour)
    else:
        df['he'] = np.where(df.index.hour == 0, 24, df.index.hour)
    return df

#function to quickly create dynamic date variables
def today(days_input = 0):
    date_string = (date.today() + timedelta(days=days_input)).strftime('%m/%d/%Y')
    return date_string

def pull_np_as(iso: str, product_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    reg_up = ['reg up', 'regup', 'reg_up', 'Reg Up', 'Regup', 'Reg_Up', 'REGUP', 'REG_UP', 'ASM_DA_REGUP']
    reg_down = ['reg down', 'regdown', 'reg_down', 'Reg Down', 'Regdown', 'Reg_Down', 'REGDOWN', 'REG_DOWN', 'ASM_DA_REGDOWN']
    spin = ['spin', 'Spin', 'RRS', 'rrs', 'ASM_DA_RRS']
    non_spin = ['nonspin', 'non_spin', 'non spin', 'NON SPIN', 'NON_SPIN', 'ASM_DA_NONSPIN']
    all = ['all', 'All', 'ALL']

    if iso in accepted_ercot:
        objectid = 10002539052
        if product_type in reg_up:
            datatype = 'ASM_DA_REGUP'
        if product_type in reg_down:
            datatype = 'ASM_DA_REGDOWN'
        if product_type in spin:
            datatype = 'ASM_DA_RRS'
        if product_type in non_spin:
            datatype = 'ASM_DA_NONSPIN'
        if product_type in all:
            datatype = ['ASM_DA_REGDOWN', 'ASM_DA_REGUP', 'ASM_DA_RRS', 'ASM_DA_NONSPIN']
    elif iso in accepted_caiso:
        objectid = 10002539052 #only have AS_CAISO_EXP at the moment
        if product_type in reg_up:
            datatype = 'DAM RU_CLR_PRC'
        if product_type in reg_down:
            datatype = 'DAM RD_CLR_PRC'
        if product_type in spin:
            datatype = 'DAM SP_CLR_PRC'
        if product_type in non_spin:
            datatype = 'DAM NS_CLR_PRC'
        if product_type in all:
            datatype = ['DAM RD_CLR_PRC', 'DAM RU_CLR_PRC', 'DAM SP_CLR_PRC', 'DAM NS_CLR_PRC']
    else:
        print("ISO argument criteria not met: try either 'ercot' or 'caiso'")
    #create item requirement for query parameters
    if product_type in all:
        items_list = []
        for i in datatype:
            item = i + ':' + str(objectid)
            items_list.append(item)
        items = ','.join(items_list)
    else:
        items = datatype + ':' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

def pull_sp_as(iso: str, product_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    reg_up = ['reg up', 'regup', 'reg_up', 'Reg Up', 'Regup', 'Reg_Up', 'REGUP', 'REG_UP', 'ASM_DA_REGUP']
    reg_down = ['reg down', 'regdown', 'reg_down', 'Reg Down', 'Regdown', 'Reg_Down', 'REGDOWN', 'REG_DOWN', 'ASM_DA_REGDOWN']
    spin = ['spin', 'Spin', 'RRS', 'rrs', 'ASM_DA_RRS']
    non_spin = ['nonspin', 'non_spin', 'non spin', 'NON SPIN', 'NON_SPIN', 'ASM_DA_NONSPIN']
    all = ['all', 'All', 'ALL']

    if iso in accepted_ercot:
        objectid = 10002484317
        if product_type in reg_up:
            datatype = 'ASM_DA_REGUP'
        if product_type in reg_down:
            datatype = 'ASM_DA_REGDOWN'
        if product_type in spin:
            datatype = 'ASM_DA_RRS'
        if product_type in non_spin:
            datatype = 'ASM_DA_NONSPIN'
        if product_type in all:
            datatype = ['ASM_DA_REGDOWN', 'ASM_DA_REGUP', 'ASM_DA_RRS', 'ASM_DA_NONSPIN']
    elif iso in accepted_caiso:
        objectid = 10002484317 #only have AS_CAISO_EXP at the moment
        if product_type in reg_up:
            datatype = 'DAM RU_CLR_PRC'
        if product_type in reg_down:
            datatype = 'DAM RD_CLR_PRC'
        if product_type in spin:
            datatype = 'DAM SP_CLR_PRC'
        if product_type in non_spin:
            datatype = 'DAM NS_CLR_PRC'
        if product_type in all:
            datatype = ['DAM RD_CLR_PRC', 'DAM RU_CLR_PRC', 'DAM SP_CLR_PRC', 'DAM NS_CLR_PRC']
    else:
        print("ISO argument criteria not met: try either 'ercot' or 'caiso'")
    #create item requirement for query parameters
    if product_type in all:
        items_list = []
        for i in datatype:
            item = i + ':' + str(objectid)
            items_list.append(item)
        items = ','.join(items_list)
    else:
        items = datatype + ':' + str(objectid)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': items,
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df


## Useful Lists
#ERCOT Price Nodes
all_assets = ['ALVIN_RN','BRPANGLE_RN','BATCAVE_RN','BRP_BRAZ_RN','BRP_DIKN_RN','BRHEIGHT_RN','BRP_LOOP_RN','BRP_LOP1_RN','BRPMAGNO_RN','NF_BRP_RN','ODESW_RN','BRP_PBL1_RN','BRP_PBL2_RN','BRP_RN_UNIT1','BRP_SWNY_RN','BRP_ZPT1_RN','BRP_ZPT2_RN']

houston_assets = ['ALVIN_RN','BRP_DIKN_RN','BRHEIGHT_RN','BRPMAGNO_RN']

brazoria_assets = ['BRPANGLE_RN','BRP_BRAZ_RN','BRP_SWNY_RN']

south_assets = ['BRP_LOOP_RN','BRP_PBL1_RN','BRP_PBL2_RN','BRP_ZPT1_RN','BRP_ZPT2_RN']

central_assets = ['BATCAVE_RN','NF_BRP_RN','BRP_RN_UNIT1']

load_zones = ['LZ_AEN', 'LZ_CPS', 'LZ_HOUSTON', 'LZ_LCRA', 'LZ_NORTH', 'LZ_RAYBN', 'LZ_SOUTH', 'LZ_WEST']

all_hubs =  ['HB_HUBAVG','HB_HOUSTON','HB_NORTH','HB_SOUTH','HB_PAN','HB_WEST']

#ERCOT Demand Regions
tx_demand_zones = ['ERCOT', 'HOUSTON', 'NORTH (ERCOT)', 'SOUTH', 'WEST (ERCOT)']

tx_weather_zones = ['WZ_Coast', 'WZ_East', 'WZ_FarWest', 'WZ_North', 'WZ_NorthCentral', 'WZ_SouthCentral', 'WZ_Southern', 'WZ_West']

#ERCOT Wind Regions
tx_wind_regions = ['ERCOT', 'GR_COASTAL', 'GR_NORTH', 'GR_PANHANDLE', 'GR_SOUTH', 'GR_WEST']

#ERCOT Solar Regions
tx_solar_regions = ['ERCOT', 'CenterEast', 'CenterWest', 'FarEast', 'FarWest', 'NorthWest', 'SouthEast']

#ERCOT Weather Locations
tx_temp_locations = ['Houston', 'Dallas', 'Austin', 'San Antonio', 'Corpus Christi', 'Midland-Odessa', 'San Angelo', 'Lubbock', 'Abilene', 'Waco-Madison']

major_cities = ['Houston', 'Dallas', 'San Antonio', 'Austin']

#Items Strings
fundamentals_rt = 'RTLMP:10000698382,RTLOAD:10000712973,WINDDATA:10000712973,GENERATION_SOLAR_RT:10000712973,TOTAL_RESOURCE_CAP_OUT:10000756298'
