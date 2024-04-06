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
from galaxy_vault.factory import VaultFactory 


#define trade date parameter constants
start = (date.today() + timedelta(days=-29)).strftime('%m/%d/%Y')
trade_date = (date.today() + timedelta(days=1)).strftime('%m/%d/%Y')
factory = VaultFactory()
vault = factory.get_vault()
market_volumes_url = vault.get_secret('apx-base-url')+"/reporting/getReport?reportPath=/Scheduling/ISO Market Volumes&reportFormat=CSV"

#SQL DB Functions
#query existing SQL table
def queryDB(sql_string, date_filter):
    driver = '{ODBC Driver 17 for SQL Server}'
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()    
    database = 'apx_caiso'

    conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID=' + \
        db_credentials.username+';DATABASE=' + database + ';PWD=' + db_credentials.password
    conn = pyodbc.connect(conn_string)

    # Add date filter to SQL query string
    sql_string += " WHERE trade_date >= '{}'".format(date_filter)

    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

#Connect to SQL DB for upload
def dbConnect(database: str):
    """
    SQLAlchemy engine to write to brptemp DB
    
    Example: df.to_sql('name_of_table', con=dbConnect, method=None, if_exists='replace')
    """
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = f"DRIVER={driver};SERVER={db_credentials.server};PORT=1433;UID={db_credentials.username};DATABASE={database};PWD={db_credentials.password}"
    connect_str = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"
    engine = create_engine(connect_str, fast_executemany=True)
    return engine

#APX API Functions
#function to retrieve api token
def get_token():
    factory = VaultFactory()
    vault = factory.get_vault()
    apx_credentials = vault.get_apx_credentials()
    grant_type = 'password'
    #headers parameters
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    #auth parameters
    authinfo = HTTPBasicAuth(apx_credentials.client_id, apx_credentials.client_secret)
    #data paramenters
    data = {'grant_type': grant_type,
            'username': apx_credentials.username,
            'password': apx_credentials.password}
    #post request
    response = requests.post(
        vault.get_secret('apx-auth-url'), auth=authinfo, data=data, headers=headers)
    #read response text
    result = json.loads(response.text)
    #define token constant
    token = result['access_token']
    print(response.url)

    return token

#function to get request headers
def get_request_headers(token):
    #headers
    request_headers = {
        'Authorization': f'Bearer {token}',
    }

    return request_headers

#function to create payload
def get_payload(trade_date_start: str, trade_date_end: str, report: str):
    """
    trade_date_start: 'm/d/yyyy'
    trade_date_end: 'm/d/yyyy'
    report: 'dispatched_energy', 'market_volumes', 'meter_vs_sched', or 'meter_data'
    """
    #logic to determine interval_type parameter
    if report == 'market_volumes':
        interval_type = 'Hourly'
    elif report == 'meter_data':
        interval_type = '5 min'
    else:
        print("report argument not fulfilled. please try either 'market_volumes' or 'meter_data'")

    #define common report paramaters
    common_params = [
        {"key": 'Region', 'value': 'MRTU'},
        {"key": 'Market_Participant', 'value': 'BRP1'},
        {"key": 'Begin_Trade_Date', 'value': trade_date_start},
        {"key": 'End_Trade_Date', 'value': trade_date_end}
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
    # print(response.url)
    #open bytes zipfile and convert to dataframe
    with ZipFile(BytesIO(response.content)) as zfile:
        csv = zfile.namelist()[0]
        data = pd.read_csv(zfile.open(csv))
        #csvs = zfile.infolist()
    return data

#START
#Pull in latest 31 trade dates from master table
sql = "SELECT * FROM [apx_caiso].[dbo].[apx_iso_market_volumes]"
df = queryDB(sql, start)

#get apx api access token
token = get_token()
#define request headers and data/payload parameters
headers = get_request_headers(token)
payload = get_payload(start, trade_date, 'market_volumes')

#pull raw report data into temp table
try:
    temp = pull_report_data(market_volumes_url, headers, payload)
except BadZipFile:
    print(f'No Market Volume from {start} to {trade_date}')
    sys.exit()

## clean up temp dataframe
#make column headers lowercase
lowercase_header = [x.lower() for x in temp.columns]
temp.columns = lowercase_header
#strip excess white space from strings
cols_to_strip = ['product_type', 'schedule_type', 'tx_type']
temp[cols_to_strip] = temp[cols_to_strip].apply(lambda x: x.str.strip())
#create and merge dtm_updated column
dtm_updated = [datetime.now()]*len(temp)
#insert dtm_updated column
temp.insert(0, 'dtm_updated', dtm_updated)
temp['trade_date'] = pd.to_datetime(temp['trade_date'])

##Filter out duplicate values from temp dataframe
cols = ['region', 'interval_begin',
        'interval_end', 'market_participant_code', 'market_stage',
        'from_location', 'to_location', 'product_type', 'schedule_type',
        'tx_type', 'cp_id', 'mwh', 'price', 'reference',
        'trade_date', 'is_25th_hour']

data = pd.concat([df, temp]).drop_duplicates(subset=cols, keep=False)

##If not empty, append to SQL table
if not data.empty:
    data.to_sql('apx_iso_market_volumes', con=dbConnect('apx_caiso'), index=False, method=None, if_exists='append')
    print(
        f'Market Volume Data from {pd.to_datetime(data.trade_date.dt.date.unique())} Added to Market Volumes Table')
else:
    print(f'No Unique Market Volume Data to Add from {start} to {trade_date}')


# %%
