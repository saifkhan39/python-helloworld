# %%
import sys
from zipfile import ZipFile, BadZipFile
import os
import pandas as pd
import datetime as dt
from datetime import date, datetime, timedelta
import sqlalchemy
import urllib
from sqlalchemy import create_engine
import pyodbc
import requests
from requests.auth import HTTPBasicAuth
import json
from io import BytesIO
import pandas as pd
import time
from time import sleep

##define non function constants
# start = (date.today() + timedelta(days=-31)).strftime('%m/%d/%Y')
trade_date = (date.today() + timedelta(days=-1)).strftime('%m/%d/%Y')


settlement_url = "https://pmfileapi.apx.com/reporting/getReport?reportPath=/Settlement/Settlement By Trade or Publish Date&reportFormat=CSV"


#APX API Functions
#function to retrieve api token
def get_token():
    #define API request constants
    APX_CLIENT_ID='MkSte-ISO-BroadReachPower'
    APX_USERNAME="QBROAD_ws_access"
    APX_CLIENT_SECRET="EzY(NRZuZ9J@AtE2rKbC"
    APX_PASSWORD="69IjZIdYRchd*s"
    grant_type = 'password'
    #headers parameters
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
            }
    #auth parameters
    authinfo = HTTPBasicAuth(APX_CLIENT_ID,APX_CLIENT_SECRET)
    #data paramenters
    data = {'grant_type': grant_type,
            'username': APX_USERNAME,
            'password': APX_PASSWORD}
    #post request
    response = requests.post('https://apxjwtauthprod.apx.com/oauth/token', auth=authinfo, data=data, headers=headers)
    #read response text
    result = json.loads(response.text)
    #define token constant
    token = result['access_token']
    # print(response.url)

    return token

#function to get request headers


def get_request_headers(token):
    #headers
    request_headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    return request_headers


#function to create payload
def get_payload(qse: str, trade_date_start: str, trade_date_end: str, type: str):
    """
    qse: 'QBRD11'
    trade_date_start: 'm/d/yyyy'
    trade_date_end: 'm/d/yyyy'
    market: 'DAM', 'RTM'
    type: 'Initial', 'Final', 'TrueUp'
    """
    #define common report paramaters
    common_params = [
        {"key": 'region', 'value': 'TX'},
        {"key": 'market_participant', 'value': f'{qse}'},
        {"key": 'business_days_in_past', 'value': '1'},
        {"key": 'begin_trade_date', 'value': trade_date_start},
        {"key": 'end_trade_date', 'value': trade_date_end},
        {"key": 'source_group', 'value': 'Statements'},
        {"key": 'source_version', 'value': f'{type}'},
        {"key": 'AggregateIntervals', 'value': 'No'},
        {"key": 'UseAPXAllocatedAmts', 'value': 'No'}
    ]

    #payload
    payload = json.dumps(common_params)

    return payload

#function to pull raw report data
def pull_report_data(url: str, request_headers: dict, payload: str):
    """
    Function to pull raw apx report data. Define report specific url, request headers, and payload parameters first.
    url: imported from apx_utils
    request_headers: {'Authorization': f'Bearer {token}'}
    payload: use get_payload() function
    """
    #put request
    response = requests.request("PUT", url, headers=request_headers, data=payload)

    print(response.status_code)
    # print(response.headers['Content-Type'])
    # print(len(response.content))
    # print(response.content[:100])
    print(response.url)
    # print(response.content)
    #open bytes zipfile and convert to dataframe
    with ZipFile(BytesIO(response.content)) as zfile:
        csv = zfile.namelist()[0]
        data = pd.read_csv(zfile.open(csv))
        #csvs = zfile.infolist()
    return data


types = ['DAM_Initial_Statement', 'RTM_Initial_Statement',
         'RTM_Final_Statement', 'RTM_TrueUp_Statement',
         'RTM_Restlmt_Statement', 'RTM_Restlmt2_Statement', 'RTM_Restlmt3_Statement']

qses = ['QBROAD', 'QBRD11']


def bulk_grab(trade_date):
    print(trade_date)
    all_df = pd.DataFrame()
    for x in qses:
        print(x)
        for i in types:
            print(i)
            #get data
            token = get_token()
            headers = get_request_headers(token)
            # print(headers)
            payload = get_payload(x, trade_date, trade_date, i)
            # print(payload)
            try:
                temp = pull_report_data(settlement_url, headers, payload)
            except BadZipFile:
                # print("Report Type DNE. Skipping to next report.")
                continue
            all_df = all_df.append(temp)
            sleep(5)
            
    return all_df

master = bulk_grab(trade_date)

if master.empty:
    print('No unique APX ERCOT Settlement Data Published in the last 1 Business Days')
    sys.exit()
else:
    pass

#set new column names to match SQL db names
new_column_names = {
    'Region': 'region',
    'Market_Participant': 'market_participant',
    'Source_Group': 'source_group',
    'Report_TimeStamp': 'report_timestamp',
    'Aggregate_Intervals': 'aggregate_intervals',
    'Charge_Code2': 'charge_code',
    'Charge_Description2': 'charge_description',
    'comment': 'comment',
    'Location': 'location',
    'Public_Location2': 'public_location',
    'Source_Location2': 'source_location',
    'Sink_Location2': 'sink_location',
    'Trade_Date': 'trade_date',
    'Interval_Begin': 'interval_begin',
    'Interval_End': 'interval_end',
    'Version_Prefix2': 'version_prefix',
    'Textbox27': 'bq_market',
    'textbox3': 'bq_value',
    'Textbox28': 'price_market',
    'textbox4': 'price_value',
    'Textbox29': 'amount_market',
    'textbox5': 'amount_value',
    'Allocation_Flag1': 'allocation_flag',
    'TwentyFifth_Hour1': 'twentyfifth_hour'
}

master = master.rename(columns=new_column_names)

#create and merge dtm_updated column
dtm_updated = [datetime.now()]*len(master)
#insert dtm_updated column
master.insert(0, 'dtm_updated', dtm_updated)

#formatting for SQL upload
master['dtm_updated'] = pd.to_datetime(master['dtm_updated'])
master['region'] = master['region'].astype(str)
master['market_participant'] = master['market_participant'].astype(
    str)
master['source_group'] = master['source_group'].astype(str)
master['report_timestamp'] = pd.to_datetime(
    master['report_timestamp'])
master['aggregate_intervals'] = master['aggregate_intervals'].astype(
    str)
master['charge_code'] = master['charge_code'].astype(str)
master['charge_description'] = master['charge_description'].astype(
    str)
master['comment'] = master['comment'].astype(str)
master['location'] = master['location'].astype(str)
master['public_location'] = master['public_location'].astype(str)
master['source_location'] = master['source_location'].astype(str)
master['sink_location'] = master['sink_location'].astype(str)
master['trade_date'] = pd.to_datetime(master['trade_date']).dt.date
master['interval_begin'] = pd.to_datetime(master['interval_begin'])
master['interval_end'] = pd.to_datetime(master['interval_end'])
master['version_prefix'] = master['version_prefix'].astype(str)
master['bq_market'] = master['bq_market'].astype(str)
master['bq_value'] = master['bq_value'].astype(float)
master['price_market'] = master['price_market'].astype(str)
master['price_value'] = master['price_value'].astype(float)
master['amount_market'] = master['amount_market'].astype(str)
master['amount_value'] = master['amount_value'].astype(float)
master['allocation_flag'] = master['allocation_flag'].astype(str)
master['twentyfifth_hour'] = master['twentyfifth_hour'].astype(str)

#Connect to SQL DB for upload
def dbConnect(database: str):
    """
    SQLAlchemy engine to write to brptemp DB
    
    Example: df.to_sql('name_of_table', con=dbConnect, method=None, if_exists='replace')
    """
    server = "brptemp.database.windows.net"
    username = "brp_admin"
    password = "Bro@dRe@chP0wer"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = f"DRIVER={driver};SERVER={server};PORT=1433;UID={username};DATABASE={database};PWD={password}"
    connect_str = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"
    engine = create_engine(connect_str, fast_executemany=True)
    return engine


if not master.empty:
    #Upload table to SQL
    master.to_sql('apx_settlement_statements', con=dbConnect(
            'APX'), index=False, method=None, if_exists='append', chunksize=200000)
    print('ERCOT APX Settlement Report Uploaded to SQL')
else:
    print('No unique APX ERCOT Settlement Data Published in the last 1 Business Days')

# %%
