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
import zipfile
from zipfile import ZipFile
from time import sleep
import pytz
import arrow

accepted_all = ['ALL', 'all']
accepted_all_apnd = ['ALL_APND', 'all_apnd', 'ALL_AP', 'all_ap', 'ALL_APNODES', 'all_apnodes', 'AP', 'ap', 'APND', 'apnd']

accepted_da = ['DA', 'DAM', 'dam', 'da']
accepted_rtpd = ['RT', 'RTM', 'rt', 'rtm', 'RTPD', 'rtpd', '15']
accepted_rtd = ['RTD', 'rtd', '5']
accepted_ha = ['HASP', 'hasp', 'HA', 'ha']
accepted_ruc = ['ruc', 'RUC']

today = dt.date.today().strftime('%Y%m%d')
tomorrow = (dt.date.today() + timedelta(days=1)).strftime('%Y%m%d')
## Pull in additional paramters to be appended to base paramaters
this_dir, this_filename = os.path.split(__file__)
data_path = os.path.join(this_dir, 'ciso_api_list.csv')
addn_params = pd.read_csv(data_path)

##DST
def dstCheck(date):
    utc = arrow.get(date)
    local = utc.to('US/Central')
    local_string = str(local)
    if local_string[-4] == '5':
        # print('It is DST')
        return('DST')
    else:
        # print('It is not DST')
        return('notDST')

##Global base urls
single_url = 'http://oasis.caiso.com/oasisapi/SingleZip?'
group_url = 'http://oasis.caiso.com/oasisapi/GroupZip?'

###: create unique parameters for report of interest: append unique key/values to base parameters
def create_paramters(type: str, start, end = None):
    start = datetime.strptime(str(start),"%m/%d/%Y").strftime('%Y%m%d')
    if end == None:
        end = (pd.to_datetime(start) + timedelta(days=1)).strftime('%Y%m%d')
    else:
        end = datetime.strptime(str(end),"%m/%d/%Y").strftime('%Y%m%d')
        end = (pd.to_datetime(end) + timedelta(days=1)).strftime('%Y%m%d')
    if dstCheck(pd.to_datetime(start)) == 'DST':
        x='7'
    else:
        x='8'
    if dstCheck(pd.to_datetime(end)) == 'DST':
        y='7'
    else:
        y='8'
    parameters = {
    'resultformat': '6',
    'startdatetime': str(start) + 'T0'+x+':00-0000',
    'enddatetime': str(end) + 'T0'+y+':00-0000',
    }
    ###APPEND ADDITIONAL PARAMS
    ap = addn_params[addn_params['type'] == type]
    ap = dict(zip(ap['parameter'], ap['value']))
    parameters.update(ap)

    return parameters
###: open zi[file and read csv to pandas
def single_csv(file):
    df = pd.read_csv(file.open(zipfile.ZipFile.namelist(file)[0]))
    df = df.drop(columns=df.loc[:,df.columns.str.contains('GMT')])

    return df
###: main caiso api function
def pull_ciso_api(type: str, start, end = None, url = single_url):
    ###: Pull & format parmeters
    parameters = create_paramters(type, start, end)
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    ###: query url & get zip file
    query = requests.get(url, params=parameters_str)
    file = zipfile.ZipFile(BytesIO(query.content))
    ###: Read CSV to Dataframe
    # print(query.url)
    # print(query)
    df = single_csv(file)

    return df

#####___________________________________________________________
###Ad Hoc Functions
def pull_gen_changes(start, end, type='changes'):
    ###Date 1
    df1 = pull_ciso_api('master_gen_list', end)
    df1_p = pull_ciso_api('res_listing', end)[['RESOURCE_ID', 'NODE_ID']]
    df1_p.columns = ['PARENT_RESOURCE_ID', 'NODE_ID']
    df1['PARENT_RESOURCE_ID'] = np.where((df1['PARENT_RESOURCE_ID'].isna()), df1['RESOURCE_ID'],  df1['PARENT_RESOURCE_ID'])
    df1 = pd.merge(df1, df1_p, on='PARENT_RESOURCE_ID')
    df1 = df1[['GEN_UNIT_NAME', 'NODE_ID', 'RESOURCE_TYPE', 'ENERGY_SOURCE', 'ZONE', 'NET_DEPENDABLE_CAPACITY','NAMEPLATE_CAPACITY', 'BAA_ID', 'UDC', 'COD']]
    df1.COD = pd.to_datetime(df1.COD)
    df1 = df1[(df1.COD >= start) & (df1.COD <= end)]
    if type == 'changes':
        df1_piv = pd.pivot_table(df1, columns='ZONE', index='ENERGY_SOURCE', values='NET_DEPENDABLE_CAPACITY', aggfunc='sum', margins=True, margins_name = 'Total')
        df = df1_piv
    else:
        df = df1

    return df

#####__________________________________________________________
###Special One off Functions

def ciso_constraints(market: str, start, end = None):
    if market in accepted_da:
        market = 'DAM'
        qname = 'PRC_NOMOGRAM'
        vsion = '12'
    elif market in accepted_ha:
        market = 'HASP'
        qname = 'PRC_NOMOGRAM'
        vsion = '12'
    elif market in accepted_rtpd:
        market = 'RTM'
        qname = 'PRC_NOMOGRAM'
        vsion = '12'
    elif market in accepted_rtd:
        market = 'RTM'
        qname = 'PRC_RTM_NOMOGRAM'
        vsion = '1'
    else:
        print("different 1st argument needed: try 'da' or 'rt'")
    if end == None:
        end = (pd.to_datetime(start) + timedelta(days=1)).strftime('%Y%m%d')
    else:
        end = (pd.to_datetime(end) + timedelta(days=1)).strftime('%Y%m%d')
    if dstCheck(pd.to_datetime(start)) == 'DST':
        x='7'
    else:
        x='8'
    if dstCheck(pd.to_datetime(end)) == 'DST':
        y='7'
    else:
        y='8'
    parameters = {
    'resultformat': '6',
    'queryname': qname,
    'version': vsion,
    'startdatetime': start + 'T0'+x+':00-0000',
    'enddatetime': end + 'T0'+y+':00-0000',
    'market_run_id': market,
    'nomogram_id': 'ALL'
    }
    ###: encoding issue req formatting into str before query
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    query = requests.get('http://oasis.caiso.com/oasisapi/SingleZip?', params=parameters_str)
    ##Get zip file
    file = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    df = single_csv(file)

    return df

def ciso_intertie_constraints(market: str, start, end = None):
    if market in accepted_da:
        market = 'DAM'
        qname = 'PRC_CNSTR'
        vsion = '12'
    elif market in accepted_ha:
        market = 'HASP'
        qname = 'PRC_CNSTR'
        vsion = '12'
    elif market in accepted_rtpd:
        market = 'RTM'
        qname = 'PRC_CNSTR'
        vsion = '12'
    elif market in accepted_rtd:
        market = 'RTM'
        qname = 'PRC_RTM_FLOWGATE'
        vsion = '12'
    else:
        print("different 1st argument needed: try 'da' or 'rt'")
    if end == None:
        end = (pd.to_datetime(start) + timedelta(days=1)).strftime('%Y%m%d')
    else:
        end = (pd.to_datetime(end) + timedelta(days=1)).strftime('%Y%m%d')
    if dstCheck(pd.to_datetime(start)) == 'DST':
        x='7'
    else:
        x='8'
    if dstCheck(pd.to_datetime(end)) == 'DST':
        y='7'
    else:
        y='8'
    parameters = {
    'resultformat': '6',
    'queryname': qname,
    'version': vsion,
    'startdatetime': start + 'T0'+x+':00-0000',
    'enddatetime': end + 'T0'+y+':00-0000',
    'market_run_id': market,
    'ti_id': 'ALL',
    'ti_direction': 'ALL'
    }
    ###: encoding issue req formatting into str before query
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    query = requests.get('http://oasis.caiso.com/oasisapi/SingleZip?', params=parameters_str)
    ##Get zip file
    file = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    df = single_csv(file)

    return df

def ciso_lmps(market: str, node: str, start, end = None):
    if market in accepted_da:
        market = 'DAM'
        qname = 'PRC_LMP'
        vsion = '12'
    elif market in accepted_ruc:
        market = 'RUC'
        qname = 'PRC_LMP'
        vsion = '12'
    elif market in accepted_ha:
        market = 'HASP'
        qname = 'PRC_HASP_LMP'
        vsion = '3'
    elif market in accepted_rtd:
        market = 'INTVL'
        qname = 'PRC_INTVL_LMP'
        vsion = '3'
    elif market in accepted_rtpd:
        market = 'RTPD'
        qname = 'PRC_RTPD_LMP'
        vsion = '3'
    else:
        print("different 1st argument needed: try 'da' or 'rt'")
    if end == None:
        end = (pd.to_datetime(start) + timedelta(days=1)).strftime('%Y%m%d')
    else:
        end = (pd.to_datetime(end) + timedelta(days=1)).strftime('%Y%m%d')
    if dstCheck(pd.to_datetime(start)) == 'DST':
        x='7'
    else:
        x='8'
    if dstCheck(pd.to_datetime(end)) == 'DST':
        y='7'
    else:
        y='8'
    parameters = {
    'resultformat': '6',
    'queryname': qname,
    'version': vsion,
    'startdatetime': start + 'T0'+x+':00-0000',
    'enddatetime': end + 'T0'+y+':00-0000',
    'market_run_id': market,
    'node': node
    }
    ###: encoding issue req formatting into str before query
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    query = requests.get('http://oasis.caiso.com/oasisapi/SingleZip?', params=parameters_str)
    ##Get zip file
    file = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    df = single_csv(file)

    return df

def ciso_lmps_grp(market: str, group: str, start, end = None):
    if market in accepted_da:
        market = 'DAM'
        qname = 'PRC_LMP'
        vsion = '12'
    elif market in accepted_ruc:
        market = 'RUC'
        qname = 'PRC_LMP'
        vsion = '12'
    elif market in accepted_ha:
        market = 'HASP'
        qname = 'PRC_HASP_LMP'
        vsion = '3'
    elif market in accepted_rtd:
        market = 'INTVL'
        qname = 'PRC_INTVL_LMP'
        vsion = '3'
    elif market in accepted_rtpd:
        market = 'RTPD'
        qname = 'PRC_RTPD_LMP'
        vsion = '3'
    else:
        print("different 1st argument needed: try 'da' or 'rt'")
    if group in accepted_all:
        group = 'ALL'
    elif group in accepted_all_apnd:
        group = 'ALL_APNODES'
    else:
        print("different 2nd argument needed: try 'all' or 'apnd'")
    if end == None:
        end = (pd.to_datetime(start) + timedelta(days=1)).strftime('%Y%m%d')
    else:
        end = (pd.to_datetime(end) + timedelta(days=1)).strftime('%Y%m%d')
    if dstCheck(pd.to_datetime(start)) == 'DST':
        x='7'
    else:
        x='8'
    if dstCheck(pd.to_datetime(end)) == 'DST':
        y='7'
    else:
        y='8'
    parameters = {
    'resultformat': '6',
    'queryname': qname,
    'version': vsion,
    'startdatetime': start + 'T0'+x+':00-0000',
    'enddatetime': end + 'T0'+y+':00-0000',
    'market_run_id': market,
    'grp_type': group
    }
    ###: encoding issue req formatting into str before query
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    query = requests.get('http://oasis.caiso.com/oasisapi/SingleZip?', params=parameters_str)
    ##Get zip file
    file = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    if group in accepted_all:
        df = pd.DataFrame()
        for i in range(len(file.namelist())):
            filename = file.namelist()[i]
            if market+'_LMP' in filename:
                df = pd.read_csv(file.open(zipfile.ZipFile.namelist(file)[i]))
                df = df.drop(columns=df.loc[:,df.columns.str.contains('GMT')])
                break
    elif group in accepted_all_apnd:
        df = pd.read_csv(file.open(zipfile.ZipFile.namelist(file)[0]))
        df = df.drop(columns=df.loc[:,df.columns.str.contains('GMT')])
    df = pd.read_csv(file.open(zipfile.ZipFile.namelist(file)[0]))
    df = df.drop(columns=df.loc[:,df.columns.str.contains('GMT')])
    df.loc[:,df.columns.str.contains('GMT')]

    return df

def fnm_mapping(fnm_version: str, area = 'CISO', map_type = 'GEN_RES'):

    parameters = {
    'resultformat': '6',
    'groupid': 'ATL_FNM_MAPPING_DATA_GRP',
    'version': '5',
    'fnmversion': fnm_version,
    # 'startdatetime': start + 'T0'+x+':00-0000',
    # 'enddatetime': end + 'T0'+y+':00-0000',
    'baa_grp_id': area,
    'mappingtype': map_type,
    }
    ###: encoding issue req formatting into str before query
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    query = requests.get('http://oasis.caiso.com/oasisapi/GroupZip?', params=parameters_str)

    ###Get zip file
    file = zipfile.ZipFile(BytesIO(query.content))
    ###Pull in CSV file as DataFrame
    df = pd.read_csv(file.open(zipfile.ZipFile.namelist(file)[0]))
    df = df.drop(columns=df.loc[:,df.columns.str.contains('GMT')])
    # df.loc[:,df.columns.str.contains('GMT')]

    return df
