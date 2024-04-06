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
import pytz
import arrow
from time import sleep
from galaxy_vault.factory import VaultFactory 

date = (dt.date.today() - timedelta(days=1)).strftime('%Y%m%d')

# end_date = (dt.date(2022,6,13) + timedelta(days=0))
# numdays = 365
# date_list = [(end_date - dt.timedelta(days=x)).strftime('%Y%m%d') for x in range(numdays)]

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

accepted_all = ['ALL', 'all']
accepted_all_apnd = ['ALL_APND', 'all_apnd', 'ALL_AP', 'all_ap', 'ALL_APNODES', 'all_apnodes', 'AP', 'ap', 'APND', 'apnd']

accepted_da = ['DA', 'DAM', 'dam', 'da']
accepted_rtpd = ['RT', 'RTM', 'rt', 'rtm', 'RTPD', 'rtpd', '15']
accepted_rtd = ['RTD', 'rtd', '15']
accepted_ha = ['HASP', 'hasp', 'HA', 'ha']
accepted_ruc = ['ruc', 'RUC']
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
    factory = VaultFactory()
    vault = factory.get_vault()
    query = requests.get(vault.get_secret('caiso-oasis-url')+'/oasisapi/SingleZip?', params=parameters_str)
    # print(query.status_code)
    # print(start)
    # print(query)
    # print(query.url)
    # print(query.url)
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

def ciso_lmps_rtpd(market: str, group: str, start, hb: int):
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
    if dstCheck(pd.to_datetime(start)) == 'DST':
        x = start + 'T0'+str(7)+':00-0000'
        x = (pd.to_datetime(x) + timedelta(hours=hb)).strftime('%Y%m%dT%H:%M-0000')
        y = (pd.to_datetime(x) + timedelta(hours=hb+1)).strftime('%Y%m%dT%H:%M-0000')
    else:
        x = start + 'T0'+str(8)+':00-0000'
        x = (pd.to_datetime(x) + timedelta(hours=hb)).strftime('%Y%m%dT%H:%M-0000')
        y = (pd.to_datetime(x) + timedelta(hours=hb+1)).strftime('%Y%m%dT%H:%M-0000')
    parameters = {
    'resultformat': '6',
    'queryname': qname,
    'version': vsion,
    'startdatetime': x,
    'enddatetime': y,
    'market_run_id': market,
    'grp_type': group
    }
    ###: encoding issue req formatting into str before query
    parameters_str = "&".join("%s=%s" % (k,v) for k,v in parameters.items())
    factory = VaultFactory()
    vault = factory.get_vault()
    query = requests.get(vault.get_secret('caiso-oasis-url')+'/oasisapi/SingleZip?', params=parameters_str)
    # print(query.status_code)
    # print(start)
    # print(query)
    # print(query.url)
    # print(query.url)
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

##DA
df = ciso_lmps_grp('da', 'all', date)
df = df[['OPR_DT', 'OPR_HR', 'MW', 'NODE']]
df = df.sort_values(by=['OPR_DT', 'OPR_HR'])

##RTPD
df_rtpd = pd.DataFrame()
for i in list(range(0,24)):
    df_rt = ciso_lmps_rtpd('rt', 'all', date, i)
    df_rt = df_rt[['OPR_DT', 'OPR_HR', 'OPR_INTERVAL', 'PRC', 'NODE']]
    df_rt['PRC'] = df_rt.groupby(['NODE', 'OPR_DT', 'OPR_HR'])['PRC'].transform('mean')
    df_rt = df_rt.drop(columns=['OPR_INTERVAL'])
    df_rt = df_rt.drop_duplicates()
    df_rtpd = df_rtpd.append(df_rt)
    sleep(5)


lmps = pd.merge(df, df_rtpd, on=['OPR_DT', 'OPR_HR', 'NODE'])
lmps.columns = ['opr_dt', 'opr_hr', 'daprice', 'node', 'rtpdprice']
lmps['dart'] = lmps['daprice'] - lmps['rtpdprice']
lmps = lmps[['opr_dt', 'opr_hr', 'daprice', 'rtpdprice', 'dart', 'node']]
lmps = lmps.round(4)

lmps['upload_time'] = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

def dbConnect():
    server = "tcp:10.128.2.11,1433"
    database = "caisomarketdata"
    factory = VaultFactory()
    vault = factory.get_vault()
    username = vault.get_secret("caiso-username")
    password = vault.get_secret("caiso-password")
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)

    return(engine)

test = dbConnect()

lmps.to_sql('iso_prices', con=test, index=False, method=None, if_exists='append', chunksize=500000)
