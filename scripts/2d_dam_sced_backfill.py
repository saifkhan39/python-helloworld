# %%
import pandas as pd
import numpy as np
from requests_pkcs12 import get
from bs4 import BeautifulSoup
from zipfile import ZipFile
from io import BytesIO
from ercot_config import pfx_path, pfx_password
import urllib
from sqlalchemy import create_engine
import time

# %%
#function to parse name of csv and insert column in position 1 with data type
def read_transform_csv(zfile, filename, market):
    fn = filename.rsplit('-')[0].rsplit('2d_Agg_')[1]
    df = pd.read_csv(zfile.open(filename))
    df['Type'] = fn
    df = df.rename(columns=lambda x: x.strip().replace(' ', '_').lower())
    #DAM check
    if market == 'RT':
        pass
    else:
        df.columns = ['delivery_date', 'hour_ending', 'price', 'mw', 'type']
        
    df['price'] = df['price'].astype(float)
        
    return df.round(2)

#function read particular file categary and concatenate csvs
def compile_files(zfile, including, excluding, market = 'RT'):
    csvs = zfile.infolist()
    df = pd.concat([read_transform_csv(zfile, csv.filename, market) 
                    for csv in csvs if any(sub in csv.filename for sub in including) 
                    and not any(sub in csv.filename for sub in excluding)])    
    return df
    
#mis scraper function to pull 2-day DAM and SCED files
def pull_2day_data(i, reportTypeId):
    #define url
    url = f'https://mis.ercot.com/misapp/GetReports.do?reportTypeId={reportTypeId}&mimic_duns=1172089695000'
    #get request to report
    r = get(url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #bytes to string
    html = r.content.decode('utf-8')
    #parse html string
    soup = BeautifulSoup(html, 'html.parser')
    #pull out friendly names and links
    names = [n.text for n in soup.find_all('td', class_='labelOptional_ind')]
    links = [l.get('href') for l in soup.find_all('a', href=True)]
    #combine to df and filter only csvs
    files = pd.DataFrame(list(zip(names, links)), columns=['Names', 'Links'])
    #create download url
    file_url = "https://mis.ercot.com/" + files.loc[i, 'Links']
    print(file_url)
    #make request with certs
    cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #extract from each daily zip file w/o downloading
    with ZipFile(BytesIO(cert_url.content)) as zfile:
        #SCED data (broken out into parts to maintain order)
        rt_data = compile_files(zfile, ['IRR', 'PVGR', 'Wind', 'CLR'], ['zzz'])

        #DAM data
        dam_data = compile_files(zfile, ['Agg'], ['IRR', 'PVGR', 'Wind', 'CLR'], market='DAM')
        
        #change dtypes
        rt_data['sced_time_stamp'] = pd.to_datetime(rt_data['sced_time_stamp'])
        dam_data['delivery_date'] = pd.to_datetime(dam_data['delivery_date'])

    return rt_data, dam_data

#function to create sql alchemy engine and connect to db
def dbConnect(database: str, server_str: str = 'brptemp'):
    """
    SQLAlchemy engine to write to brptemp DB or Dallas DB
    
    Example: df.to_sql('name_of_table', con=dbConnect, method=None, if_exists='replace')
    """
    if server_str == 'brptemp':
        server = "brptemp.database.windows.net"
        username = "brp_admin"
        password = "Bro@dRe@chP0wer"
    else:
        server = "tcp:10.128.2.11,1433"
        username = "brptrading"
        password = "Brptr8ding#"
        
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = f"DRIVER={driver};SERVER={server};PORT=1433;UID={username};DATABASE={database};PWD={password}"
    connect_str = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"
    engine = create_engine(connect_str, fast_executemany=True)
    return engine

# # %%
# for i in reversed(range(0,1)):
#     print()
#     print(i)   
#     print('pulling data')
#     rt, dam = pull_2day_data(i, 13054)
#     print(f"data pulled for {dam.iloc[0,0].strftime('%m/%d/%Y')}")
    
#     print('\nappending rt data')
#     rt_start = time.time()
#     rt.to_sql('2D_SCED_data', con=dbConnect('ErcotMarketData'), if_exists='append', chunksize=50000, index=False)
#     rt_end = time.time()
#     rt_elapsed = rt_end - rt_start
#     print(f'rt data written in {rt_elapsed:.2f} seconds')

#     print('\nappending dam data')
#     dam_start = time.time()
#     dam.to_sql('2D_DAM_data', con=dbConnect('ErcotMarketData'), if_exists='append', chunksize=50000, index=False)
#     dam_end = time.time()
#     dam_elapsed = dam_end - dam_start
#     print(f'dam data written in {dam_elapsed:.2f} seconds')
