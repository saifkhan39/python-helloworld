import os
import requests
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, datetime
import time
from io import StringIO, BytesIO
import pickle
import csv
from yesapi.functions import *
import zipfile
from galaxy_vault.factory import VaultFactory 

# with open(r"C:\Users\JohnMcMahon\BroadReachPower\BRP - Documents\Cap and Trade\Analyst Folders\McMahon\Projects\Dictionaries\rolling_dict.pkl", 'rb') as handle:
#     rolling_dict = pickle.load(handle)
# rolling_dict
#
# models_dict = {'nodes': {
#     'TH_SP15_GEN-APND': 20000004682,
#     'TH_NP15_GEN-APND': 20000004677,
#     'TH_ZP26_GEN-APND': 20000004670,
#     'PEORIA_7_N001': 10002486799,
#     'WEBER_6_N001': 20000003856,
#     'GWFPWR_1_UNITS-APND': 20000001395,
#     'BORDER_6_B1': 20000004319,
#     'GWFGT1_7_B1': 20000001393,
#     'RIPON_1_N001': 20000003085},
#     'load': {
#     'CAISO_load': 10000328798,
#     'SCE_load': 10000328795,
#     'SDGE_load': 10000328797,
#     'PG&E_load': 10000328796},
#     'solar': {
#     'CAISO_solar': 10000328798,
#     'NP15_solar': 10002494909,
#     'SP15_solar': 10002494908,
#     'ZP26_solar': 10002494910},
#     'wind': {
#     'CAISO_wind': 10000328798,
#     'NP15_wind': 10002494909,
#     'SP15_wind': 10002494908},
#     'hydro': {
#     'CAISO_hydro':10000328798},
#     'nuke': {
#     'CAISO_nuke':10000328798},
#     'imports': {
#     'CAISO_imports': 10000328798},
#     'paths': {
#     'Malin': 10015935826,
#     'Mead': 10002899805,
#     'Nob': 10002899822,
#     'PV': 10002899823},
#     'gas': {
#     'SoCal_gas_price': 10000002768,
#     'PG&E_gas_price': 10000687008},
#     'outages': {
#     'SP15_outages':10002494908,
#     'NP15_outages': 10002494909},
# }
#
# os.chdir(r"C:\Users\JohnMcMahon\BroadReachPower\BRP - Documents\Cap and Trade\Analyst Folders\McMahon\Projects\Dictionaries")
#
#
# filename = 'models_dict.pkl'
# outfile = open(filename, 'wb')
# pickle.dump(models_dict,outfile)
# outfile.close()

#
# GLOBAL VARIABLES
# authentication variable

factory = VaultFactory()
vault = factory.get_vault()
yes_energy_username = vault.get_secret_individual_account('yes-energy-username')
yes_energy_password = vault.get_secret_individual_account('yes-energy-password')
auth = (yes_energy_username, yes_energy_password)

accepted_caiso = ['CAISO', 'caiso', 'Caiso', 'ca', 'CA']
accepted_ercot = ['ERCOT', 'ercot', 'Ercot', 'tx', 'TX']

start = (dt.date.today() + dt.timedelta(days=-7)).strftime('%m/%d/%Y')
end = (dt.date.today() + dt.timedelta(days=1)).strftime('%m/%d/%Y')

def tb_calc(df, duration: int, unit = 'mw'):
    df = df.melt(ignore_index=False)
    df = pd.DataFrame(df.groupby([df.index.date, 'variable'])['value'].apply(lambda x: x.nlargest(duration).sum()) - df.groupby([df.index.date, 'variable'])['value'].apply(lambda x: x.nsmallest(duration).sum())).reset_index()
    df = df.pivot_table(index='level_0', columns='variable')
    df.columns = df.columns.droplevel(0)
    df.columns.name = None
    df = df.reset_index()
    df = df.rename(columns={'level_0': 'DATETIME'})
    df = df.set_index('DATETIME')
    df = df.add_suffix('_tb'+str(duration))
    if 'kw' in unit:
        df = (df*365)/12000

    return df

def pull_ca_netload(type: str, start: str, end: str, period = '5min'):
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']

    if type in accepted_historical:
        datatypes = ['RTLOAD', 'OUTLOOK_WIND_5MIN', 'OUTLOOK_SOLAR_5MIN']
    elif type in accepted_forecast:
        datatypes = ['LOAD_FORECAST', 'WIND_FORECAST', 'SOLAR_FORECAST']
    else:
        print("different 1st argument needed: try 'RT' or 'forecast'")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #create items_list
    for i in datatypes:
        objectid = yes_api_dict['load']['CAISO']
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

    #calculate net load
    net_load = df.iloc[:,1] - df.iloc[:,2] - df.iloc[:,3]
    df.insert(1, 'net_load', net_load)
    # df = df.drop(df.iloc[:,2:5], axis=1)

    return df


def pull_np_netload(type: str, start: str, end: str, period = '5min'):
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']

    if type in accepted_historical:
        datatypes = ['WIND_RTI', 'GENERATION_SOLAR_RT']
        ldt = ['RTLOAD']
        # zp = ['GENERATION_SOLAR_RT']
    elif type in accepted_forecast:
        datatypes = ['WIND_FORECAST', 'SOLAR_FORECAST']
        ldt = ['LOAD_FORECAST']
        # zp = ['SOLAR_FORECAST']
    else:
        print("different 1st argument needed: try 'RT' or 'forecast'")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #create items_list
    for i in ldt:
        objectid = 10000328796
        item = i + ':' + str(objectid)
        items_list.append(item)
    for i in datatypes:
        objectid = 10002494909
        item = i + ':' + str(objectid)
        items_list.append(item)
    # for i in zp:
    #     objectid = 10002494910
    #     item = i + ':' + str(objectid)
    #     items_list.append(item)
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
    #calculate net load
    net_load = df.iloc[:,1] - df.iloc[:,2] - df.iloc[:,3]
    df.insert(1, 'np_net_load', net_load)
    df = df.drop(df.iloc[:,2:5], axis=1)

    return df


def pull_sp_netload(type: str, start: str, end: str, period = '5min'):
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']

    if type in accepted_historical:
        datatypes = ['WIND_RTI', 'GENERATION_SOLAR_RT']
        ldt = ['RTLOAD']
        zp = ['GENERATION_SOLAR_RT']
    elif type in accepted_forecast:
        datatypes = ['WIND_FORECAST', 'SOLAR_FORECAST']
        ldt = ['LOAD_FORECAST']
        zp = ['SOLAR_FORECAST']
    else:
        print("different 1st argument needed: try 'RT' or 'forecast'")

    #create empty string to fulfill items poriton of api params
    items_list = []
    #create items_list
    for i in ldt:
        objectid = 10000328795
        item = i + ':' + str(objectid)
        items_list.append(item)
    for i in ldt:
        objectid = 10000328797
        item = i + ':' + str(objectid)
        items_list.append(item)
    for i in datatypes:
        objectid = 10002494908
        item = i + ':' + str(objectid)
        items_list.append(item)
    for i in zp:
        objectid = 10002494910
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
    #calculate net load
    net_load = df.iloc[:,1] + df.iloc[:,2] - df.iloc[:,3] - df.iloc[:,4] - df.iloc[:,5]
    df.insert(1, 'sp_net_load', net_load)
    df = df.drop(df.iloc[:,2:7], axis=1)

    return df


def load_data(dictionary, start: str, end: str):
    #create string to fulfill items parameter
    items_list = []
    for value in dictionary.values():
        item = 'RTLOAD:' + str(value) + ',DALOAD:' + str(value) + ',LOAD_FORECAST:' + str(value)
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
    raw = pd.read_csv(StringIO(query.text)).round(2)
    #delete unnecesary columns
    df = raw.iloc[:,:-5]
    #strip '(GENERATION_SOLAR_RT)' from column names
    # df = df.rename(columns = lambda x : str(x).replace(' (RTLOAD)','').replace(' (ERCOT)',''))
    #set index ot datetime
    df = df.set_index('DATETIME')
    #change to datetime format
    df.index = pd.to_datetime(df.index)

    return df

def pull_5min_prices(nodes: list, price_type: str, start: str, end: str, period = '5min'):
    #list comprehension logic check for dart choice
    dart = ['DART', 'dart', 'da/rt', 'DA/RT']
    #declare datatype
    datatype = price_type
    #create empty string to fulfill items poriton of api params
    items_list = []
    #for loop through node list to create items_list
    for i in nodes:
        objectid = yes_api_dict['pricing'][i]
        item = 'DALMP:' + str(objectid) + ',RTLMP:' + str(objectid) + ',LMP_15MIN:' + str(objectid)
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


def pull_as_proc(market: str, product_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    reg_up = ['reg up', 'regup', 'reg_up', 'Reg Up', 'Regup', 'Reg_Up', 'REGUP', 'REG_UP', 'ASM_DA_REGUP']
    reg_down = ['reg down', 'regdown', 'reg_down', 'Reg Down', 'Regdown', 'Reg_Down', 'REGDOWN', 'REG_DOWN', 'ASM_DA_REGDOWN']
    spin = ['spin', 'Spin', 'RRS', 'rrs', 'ASM_DA_RRS']
    non_spin = ['nonspin', 'non_spin', 'non spin', 'NON SPIN', 'NON_SPIN', 'ASM_DA_NONSPIN']
    all = ['all', 'All', 'ALL']

    if market == 'da':
        objectid = 10002484315 #only have AS_CAISO_EXP at the moment
        if product_type in reg_up:
            datatype = 'DAM RU_PROC_MW'
        if product_type in reg_down:
            datatype = 'DAM RD_PROC_MW'
        if product_type in spin:
            datatype = 'DAM SP_PROC_MW'
        if product_type in non_spin:
            datatype = 'DAM NS_PROC_MW'
        if product_type in all:
            datatype = ['DAM RU_PROC_MW', 'DAM RD_PROC_MW', 'DAM SP_PROC_MW', 'DAM NS_PROC_MW']
    elif market == 'rt':
        objectid = 10002484315 #only have AS_CAISO_EXP at the moment
        if product_type in reg_up:
            datatype = 'RTM RU_PROC_MW'
        if product_type in reg_down:
            datatype = 'RTM RD_PROC_MW'
        if product_type in spin:
            datatype = 'RTM SP_PROC_MW'
        if product_type in non_spin:
            datatype = 'RTM NS_PROC_MW'
        if product_type in all:
            datatype = ['RTM RU_PROC_MW', 'RTM RD_PROC_MW', 'RTM SP_PROC_MW', 'RTM NS_PROC_MW']
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

def alt_np_as_proc(iso: str, product_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    reg_up = ['reg up', 'regup', 'reg_up', 'Reg Up', 'Regup', 'Reg_Up', 'REGUP', 'REG_UP', 'ASM_DA_REGUP']
    reg_down = ['reg down', 'regdown', 'reg_down', 'Reg Down', 'Regdown', 'Reg_Down', 'REGDOWN', 'REG_DOWN', 'ASM_DA_REGDOWN']
    spin = ['spin', 'Spin', 'RRS', 'rrs', 'ASM_DA_RRS']
    non_spin = ['nonspin', 'non_spin', 'non spin', 'NON SPIN', 'NON_SPIN', 'ASM_DA_NONSPIN']
    all = ['all', 'All', 'ALL']

    if iso in accepted_caiso:
        objectid = 10002539052 #only have AS_CAISO_EXP at the moment
        if product_type in reg_up:
            datatype = 'DAM RU_PROC_MW'
        if product_type in reg_down:
            datatype = 'DAM RD_PROC_MW'
        if product_type in spin:
            datatype = 'DAM SP_PROC_MW'
        if product_type in non_spin:
            datatype = 'DAM NS_PROC_MW'
        if product_type in all:
            datatype = ['DAM RU_PROC_MW', 'DAM RD_PROC_MW', 'DAM SP_PROC_MW', 'DAM NS_PROC_MW']
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

def alt_sp_as_proc(iso: str, product_type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    reg_up = ['reg up', 'regup', 'reg_up', 'Reg Up', 'Regup', 'Reg_Up', 'REGUP', 'REG_UP', 'ASM_DA_REGUP']
    reg_down = ['reg down', 'regdown', 'reg_down', 'Reg Down', 'Regdown', 'Reg_Down', 'REGDOWN', 'REG_DOWN', 'ASM_DA_REGDOWN']
    spin = ['spin', 'Spin', 'RRS', 'rrs', 'ASM_DA_RRS']
    non_spin = ['nonspin', 'non_spin', 'non spin', 'NON SPIN', 'NON_SPIN', 'ASM_DA_NONSPIN']
    all = ['all', 'All', 'ALL']

    if iso in accepted_caiso:
        objectid = 10002484317 #only have AS_CAISO_EXP at the moment
        if product_type in reg_up:
            datatype = 'DAM RU_PROC_MW'
        if product_type in reg_down:
            datatype = 'DAM RD_PROC_MW'
        if product_type in spin:
            datatype = 'DAM SP_PROC_MW'
        if product_type in non_spin:
            datatype = 'DAM NS_PROC_MW'
        if product_type in all:
            datatype = ['DAM RU_PROC_MW', 'DAM RD_PROC_MW', 'DAM SP_PROC_MW', 'DAM NS_PROC_MW']
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

def pull_as_req(iso: str, product_type: str, start: str, end: str, period = 'hour'):
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
        objectid = 10002484315
        if product_type in reg_up:
            datatype = 'DAM RU_REQ_MIN_MW'
        if product_type in reg_down:
            datatype = 'DAM RD_REQ_MIN_MW'
        if product_type in spin:
            datatype = 'DAM SP_REQ_MIN_MW'
        if product_type in non_spin:
            datatype = 'DAM NS_REQ_MIN_MW'
        if product_type in all:
            datatype = ['DAM RU_REQ_MIN_MW', 'DAM RD_REQ_MIN_MW', 'DAM SP_REQ_MIN_MW', 'DAM NS_REQ_MIN_MW']
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


def pull_as_2dareq(iso: str, product_type: str, start: str, end: str, period = 'hour'):
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
            datatype = '2DA RU_REQ_MIN_MW'
        if product_type in reg_down:
            datatype = '2DA RD_REQ_MIN_MW'
        if product_type in spin:
            datatype = '2DA SP_REQ_MIN_MW'
        if product_type in non_spin:
            datatype = '2DA NS_REQ_MIN_MW'
        if product_type in all:
            datatype = ['2DA RU_REQ_MIN_MW', '2DA RD_REQ_MIN_MW', '2DA SP_REQ_MIN_MW', '2DA NS_REQ_MIN_MW']
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

def alt_np_as_2dareq(iso: str, product_type: str, start: str, end: str, period = 'hour'):
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
        objectid = 10002539052
        if product_type in reg_up:
            datatype = '2DA RU_REQ_MIN_MW'
        if product_type in reg_down:
            datatype = '2DA RD_REQ_MIN_MW'
        if product_type in spin:
            datatype = '2DA SP_REQ_MIN_MW'
        if product_type in non_spin:
            datatype = '2DA NS_REQ_MIN_MW'
        if product_type in all:
            datatype = ['2DA RU_REQ_MIN_MW', '2DA RD_REQ_MIN_MW', '2DA SP_REQ_MIN_MW', '2DA NS_REQ_MIN_MW']
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

def alt_sp_as_2dareq(iso: str, product_type: str, start: str, end: str, period = 'hour'):
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
        objectid = 10002484317
        if product_type in reg_up:
            datatype = '2DA RU_REQ_MIN_MW'
        if product_type in reg_down:
            datatype = '2DA RD_REQ_MIN_MW'
        if product_type in spin:
            datatype = '2DA SP_REQ_MIN_MW'
        if product_type in non_spin:
            datatype = '2DA NS_REQ_MIN_MW'
        if product_type in all:
            datatype = ['2DA RU_REQ_MIN_MW', '2DA RD_REQ_MIN_MW', '2DA SP_REQ_MIN_MW', '2DA NS_REQ_MIN_MW']
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

def alt_np_as_req(iso: str, product_type: str, start: str, end: str, period = 'hour'):
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
        objectid = 10002539052
        if product_type in reg_up:
            datatype = 'DAM RU_REQ_MIN_MW'
        if product_type in reg_down:
            datatype = 'DAM RD_REQ_MIN_MW'
        if product_type in spin:
            datatype = 'DAM SP_REQ_MIN_MW'
        if product_type in non_spin:
            datatype = 'DAM NS_REQ_MIN_MW'
        if product_type in all:
            datatype = ['DAM RU_REQ_MIN_MW', 'DAM RD_REQ_MIN_MW', 'DAM SP_REQ_MIN_MW', 'DAM NS_REQ_MIN_MW']
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

def alt_sp_as_req(iso: str, product_type: str, start: str, end: str, period = 'hour'):
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
        objectid = 10002484317
        if product_type in reg_up:
            datatype = 'DAM RU_REQ_MIN_MW'
        if product_type in reg_down:
            datatype = 'DAM RD_REQ_MIN_MW'
        if product_type in spin:
            datatype = 'DAM SP_REQ_MIN_MW'
        if product_type in non_spin:
            datatype = 'DAM NS_REQ_MIN_MW'
        if product_type in all:
            datatype = ['DAM RU_REQ_MIN_MW', 'DAM RD_REQ_MIN_MW', 'DAM SP_REQ_MIN_MW', 'DAM NS_REQ_MIN_MW']
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

#AS = pd.concat([pull_as('caiso', 'all', start, end), pull_as_proc('caiso', 'all', start, end), pull_as_req('caiso', 'all', start, end), pull_as_2dareq('caiso', 'all', start, end)], axis=1)


##BPA WIND
def pull_bpa_wind(nodes: list, type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']
    #list comprehension logic check for either ercot or caiso data
    caiso = ['BPA','CAISO', 'NP15', 'SP15']

    if type in accepted_historical:
        datatype = 'WIND_RTI',
        objectid = '10001845403'
    else:
        print("different 1st argument needed: try 'RT' or 'forecast'")

    # #create empty string to fulfill items poriton of api params
    # items_list = []
    # #for loop through node list to create items_list
    # for i in nodes:
    #     item = datatype + ':' + str(objectid)
    #     items_list.append(item)
    # #join items in items_list to create item paramenter string
    # items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'WIND_RTI:10001845403',
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

def pull_bpa_hydro(nodes: list, type: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for either historical or forecast data
    accepted_historical = ['RT', 'rt', 'Actuals', 'Actual', 'actuals', 'historic', 'historical']
    accepted_forecast = ['Forecast', 'forecast', 'fx']
    #list comprehension logic check for either ercot or caiso data
    caiso = ['BPA','CAISO', 'NP15', 'SP15']

    if type in accepted_historical:
        datatype = 'HYDRO_RT',
        objectid = '10001845403'
    else:
        print("different 1st argument needed: try 'RT' or 'forecast'")

    # #create empty string to fulfill items poriton of api params
    # items_list = []
    # #for loop through node list to create items_list
    # for i in nodes:
    #     item = datatype + ':' + str(objectid)
    #     items_list.append(item)
    # #join items in items_list to create item paramenter string
    # items = ','.join(items_list)
    #define parameters
    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'HYDRO_RT:10001845403',
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

def pull_vns(nodes: list, start: str, end: str, period = 'hour'):

    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'CAISO_TOTAL_VIRTUAL_SUPPLY:10000328798,CAISO_TOTAL_VIRTUAL_DEMAND:10000328798',
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

def pull_imports(start: str, end: str, period='5min'):

    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'IMPORTS_5MIN:10000328798',
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

def pull_stack(start: str, end: str, period='hour'):

    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'IMPORTS_5MIN:10000328798,COALGEN_5MIN:10000328798,LARGE_HYDRO_5MIN:10000328798,NUCLEARGEN_5MIN:10000328798,GASGEN_5MIN:10000328798,BATTERY_TREND:10000328798,RENEWGEN_HOURLY:10000328798',
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

def pull_gen_outages(start: str, end: str, period='hour'):

    parameters = {

        'agglevel': period,
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'REG_THERM_GEN_OFFLINE:10002494909,REG_THERM_GEN_OFFLINE:10002494908,REG_REN_GEN_OFFLINE:10002494909,REG_REN_GEN_OFFLINE:10002494908,REG_HYDRO_GEN_OFFLINE:10002494909,REG_HYDRO_GEN_OFFLINE:10002494908',
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df


def pull_ra_cap(start: str, end: str):

    parameters = {

        'agglevel': 'hour',
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'AVAIL RA CAP FC:10000328798:today 48hours,AVAIL NET RA CAP FC:10000328798:today 48hours'
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)
    df = df.iloc[1: , :]

    return df

def pull_da_commitments(start: str, end: str):

    parameters = {

        'agglevel': 'hour',
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'DA_EXP_MW:10002496439,DA_EXP_MW:10002496442,DA_EXP_MW:10002496441,DA_EXP_MW:10002496443,DA_IMP_MW:10002496439,DA_IMP_MW:10002496442,DA_IMP_MW:10002496441,DA_IMP_MW:10002496443,DA_GEN_MW:10000328798,DA_GEN_MW:10000328796,DA_GEN_MW:10000328795,DA_GEN_MW:10000328797'
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

def pull_gas(start: str, end: str):

    parameters = {

        'agglevel': 'hour',
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'GASPRICE:10000002768,GASPRICE:10000687008,GASPRICE:10000002667,GHG_ALLOWANCE_INDEX:10000756297'
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df

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


def frp_req(region: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    ciso = ['CISO', 'ciso', 'CAISO', 'caiso', 'ca', 'CA']
    eim = ['EIM', 'eim']

    if region in ciso:
        objectid = 10002737739
    elif region in eim:
        objectid = 10004065392 #only have AS_CAISO_EXP at the moment
    else:
        print("ISO argument criteria not met: try either 'ercot' or 'caiso'")

    datatype = ['FLEX_RTPD_UP_REQ', 'FLEX_RT_UP_REQ', 'FLEX_RTPD_DOWN_REQ', 'FLEX_RT_DOWN_REQ']
    #create item requirement for query parameters
    items_list = []
    for i in datatype:
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

def curtailments(type: str, gen_type: str, report: str, start: str, end: str, period = 'hour'):
    #list comprehension logic check for ancillary product choice
    system = ['System', 'system', 'sys', 's']
    local = ['Local', 'local', 'loc', 'l']
    all = ['all', 'All', 'ALL']

    objectid = 10000756297
    if type in system and gen_type == 'solar':
        datatype = 'SOL_ECOSYS_CURT_'+report
    elif type in system and gen_type == 'wind':
        datatype = 'WIND_ECOSYS_CURT_'+report
    elif type in local and gen_type == 'solar':
        datatype = 'SOL_ECOLOC_CURT_'+report
    elif type in local and gen_type == 'wind':
        datatype = 'WIND_ECOLOC_CURT_'+report
    elif type in all and gen_type in all:
        datatype = ['SOL_ECOSYS_CURT_'+report, 'SOL_ECOLOC_CURT_'+report, 'WIND_ECOSYS_CURT_'+report, 'WIND_ECOLOC_CURT_'+report]
    else:
        print("error")
    #create item requirement for query parameters
    if type in all:
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


def pull_unit_outages(start: str, end: str):

    parameters = {

        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'source': 'CAISO',
        'datetype': 'ACTIVE',
        'outagetype': 'Planned,Unplanned',
        'outagestates': 'Cancelled,Future,Ongoing,Past'
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/generationoutage/CAISO?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_html(StringIO(query.text))
    df = pd.concat(df)
    #datetime column to datetime format
    df.STARTDATE = pd.to_datetime(df.STARTDATE)
    df.ENDDATE = pd.to_datetime(df.ENDDATE)

    df.PREV_STARTDATE = pd.to_datetime(df.PREV_STARTDATE)
    df.PREV_ENDDATE = pd.to_datetime(df.PREV_ENDDATE)

    df.DELIVERY_DATE = pd.to_datetime(df.DELIVERY_DATE)
    df.DELIVERY_TIME = pd.to_datetime(df.DELIVERY_TIME)

    return df

def pull_ca_reserves(start: str, end: str, period = 'hour'):
    #define objectid variable
    objectid = 10000328798
    #create items string
    items = 'RESERVE REQS:' + str(objectid) + ',RESERVE REQS FC:' + str(objectid) + ',CAISO OPERATING RESERVES RATIO:' + str(objectid)
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
    # df = df.rename(columns={df.columns[1]: 'on_reserve_cap', df.columns[2]: 'off_reserve_cap'})
    # #calculate total reserve cap
    # total_cap = df.on_reserve_cap + df.off_reserve_cap
    # df.insert(3, 'total_reserve_cap', total_cap)

    return df

def congestion(market: str, start: str, end: str):
    parameters = {
    'resultformat': '6',
    'queryname': 'PRC_NOMOGRAM',
    'version': '1',
    'market_run_id': market,
    'nomogram_id': 'ALL',
    'startdatetime': start + 'T07:00-0000',
    'enddatetime': end + 'T07:00-0000',
    }
    ###: encoding issue req formatting into str before query
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    query = requests.get(vault.get_secret('caiso-oasis-url')+'/oasisapi/SingleZip?', params=parameters_str)
    ###Get zip file
    file = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    df = pd.read_csv(file.open(zipfile.ZipFile.namelist(file)[0]))
    df = df.drop(columns=['INTERVALSTARTTIME_GMT', 'INTERVALENDTIME_GMT', 'GROUP', 'NOMOGRAM_ID_XML'])

    return df

def pull_temps(start: str, end: str):

    parameters = {

        'agglevel': 'hour',
        'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
        'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
        #'timezone': tz,
        'items': 'TEMP_F:10000355297,TEMP_F:10000357063,TEMP_F:10000356773,TEMP_F:10000357073,TEMP_F:10000357022,TEMP_F:10000356751,TEMP_NORM:10000355297,TEMP_NORM:10000357063,TEMP_NORM:10000356773,TEMP_NORM:10000357073,TEMP_NORM:10000357022,TEMP_NORM:10000356751,TEMP_NORM_05:10000355297,TEMP_NORM_05:10000357063,TEMP_NORM_05:10000356773,TEMP_NORM_05:10000357073,TEMP_NORM_05:10000357022,TEMP_NORM_05:10000356751,TEMP_NORM_95:10000355297,TEMP_NORM_95:10000357063,TEMP_NORM_95:10000356773,TEMP_NORM_95:10000357073,TEMP_NORM_95:10000357022,TEMP_NORM_95:10000356751,FORCTEMP_F:10000355297,FORCTEMP_F:10000357063,FORCTEMP_F:10000356773,FORCTEMP_F:10000357073,FORCTEMP_F:10000357022,FORCTEMP_F:10000356751'
    }
    #define query
    query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
    #pull and manipulate raw data
    df = pd.read_csv(StringIO(query.text)).round(2)
    #datetime column to datetime format
    df.DATETIME = pd.to_datetime(df.DATETIME)

    return df


# def constraint_summary(start: str, end: str, market: str, collection: str):
#
#
# parameters = {
#
#     'market': market,
#     'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
#     'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
#     'collections': collection,
# }
# #define query
# query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/constraint/summary/CAISO?', params=parameters, auth=auth)
# #pull and manipulate raw data
# df = pd.read_html(StringIO(query.text))
# df = pd.concat(df)
# #datetime column to datetime format
# df.STARTDATE = pd.to_datetime(df.STARTDATE)
# df.ENDDATE = pd.to_datetime(df.ENDDATE)
#
# df.PREV_STARTDATE = pd.to_datetime(df.PREV_STARTDATE)
# df.PREV_ENDDATE = pd.to_datetime(df.PREV_ENDDATE)
#
# df.DELIVERY_DATE = pd.to_datetime(df.DELIVERY_DATE)
# df.DELIVERY_TIME = pd.to_datetime(df.DELIVERY_TIME)
#
#     return df

ca_old = ['PEORIA_7_N001', 'WEBER_6_N001']
ca_assets = ['CH.STN._7_N001', 'WEBER_6_N001']

ca_hubs = ['TH_NP15_GEN-APND', 'TH_ZP26_GEN-APND', 'TH_SP15_GEN-APND']

ca_ties = ['PALOVRDE_ASR-APND', 'MEAD_5_N501', 'MALIN_5_N101', 'SYLMARDC_2_N501']

more_issues = ['OUTLOOK_SOLAR_5MIN:10000328798','OUTLOOK_WIND_5MIN:10000328798','RTLOAD:10000328798','DALMP:20000004682','DALMP:20000004677','RTLMP:20000004682','RTLMP:20000004677']



# def pull_hr_sparks(power: list, region: str, start: str, end: str, period = 'hour'):
#     #list comprehension logic check for dart choice
#     ercot = ['ERCOT', 'ercot', 'Ercot', 'tx', 'TX']
#     #list comprehension logic check for dart choice
#     caiso = ['CAISO', 'caiso', 'Caiso', 'ca', 'CA']
#     #declare datatype
#     fifteen = ['15min', '15', 'fifteen', '15MIN']
#     # datatype = price_type
#     #create empty string to fulfill items poriton of api params
#     items_list = []
#     #for loop through node list to create items_list
#     if region in caiso:
#         #logic check if CAISO 15min DART requested
#         for i in power:
#             objectid = yes_api_dict['pricing'][i]
#             item = 'DALMP:' + str(objectid) + ',LMP_15MIN:' + str(objectid) + ',CAISO_5MIN_LMP:' + str(objectid)
#             items_list.append(item)
#         gas_item = 'GASPRICE:' + str(10000002768) + ',GASPRICE:' + str(10000687008)
#         items_list.append(gas_item)
#     else:
#         for i in power:
#             objectid = yes_api_dict['pricing'][i]
#             item = 'DALMP:' + str(objectid) + ',RTLMP:' + str(objectid)
#             items_list.append(item)
#         gas_item = 'GASPRICE:' + str(10000002639)
#         items_list.append(gas_item)
#     #join items in items_list to create item paramenter string
#     items = ','.join(items_list)
#     #define parameters
#     parameters = {
#
#         'agglevel': period,
#         'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
#         'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
#         #'timezone': tz,
#         'items': items,
#     }
#     #define query
#     query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
#     #pull and manipulate raw data
#     df = pd.read_csv(StringIO(query.text)).round(2)
#     #datetime column to datetime format
#     df.DATETIME = pd.to_datetime(df.DATETIME)
#
#     if region in caiso:
#         for i in df.columns[0:7]:
#             d
#
#     return df
# hub_dict = {
#     'TH_SP15_GEN-APND': 20000004682,
#     'TH_NP15_GEN-APND': 20000004677,
# }
# AS = AS.loc[:,AS.columns.str.contains('AS_CAISO_EXP')]
# t.columns[0:7]
# t=pull_hr_sparks(hub_dict.keys(), 'ca', start, end)

# def pull_tie_flows(start: str, end: str):
#
#     parameters = {
#
#         'agglevel': 'hour',
#         'startdate': datetime.strptime(start,"%m/%d/%Y").strftime('%Y-%m-%d'),
#         'enddate': datetime.strptime(end,"%m/%d/%Y").strftime('%Y-%m-%d'),
#         #'timezone': tz,
#         'items': 'AVAIL RA CAP FC:10000328798:today 48hours,AVAIL NET RA CAP FC:10000328798:today 48hours'
#     }
#     #define query
#     query = requests.get(vault.get_secret('yes-energy-base-url')+'/PS/rest/timeseries/multiple.csv?', params=parameters, auth=auth)
#     #pull and manipulate raw data
#     df = pd.read_csv(StringIO(query.text)).round(2)
#     #datetime column to datetime format
#     df.DATETIME = pd.to_datetime(df.DATETIME)
#     df = df.iloc[1: , :]
#
#     return df
