import zipfile, io, os, glob
from zipfile import ZipFile
from io import StringIO, BytesIO
from requests_pkcs12 import get
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, datetime, time
import requests
import pyodbc
from bs4 import BeautifulSoup
from ercot_config import pfx_path, pfx_password
import sqlalchemy, urllib
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql import text as sa_text
from sqlalchemy.sql.expression import column

#### PULL SHIFT FACTORS ####
#node dicitonary
node_dict = {
    'ALVIN_RN': 10016433362,
    'BATCAVAE_RN': 10016670894,
    'BRHEIGHT_RN': 10016437277,
    'BRP_BRAZ_RN': 10016437281,
    'BRP_DIKN_RN': 10016437279,
    'BRP_LOOP_RN': 10016473683,
    'BRP_PBL1_RN': 10016483004,
    'BRP_SWNY_RN': 10016473684,
    'BRP_ZPT1_RN': 10016483003,
    'BRPANGLE_RN': 10016437282,
    'BRPMAGNO_RN': 10016437280,
    'NF_BRP_RN': 10016498292,
    'ODESW_RN': 10016433361,
    'HB_HOUSTON': 10000697077,
    'HB_HUBAVG': 10000698382,
    'HB_NORTH': 10000697078,
    'HB_PAN': 10015999590,
    'HB_SOUTH': 10000697079,
    'HB_WEST': 10000697080,
}

## functions
#database connection function
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

#sql engine
engine = dbConnect()

#pull da shift factor function
def da_shifts():
    #get request to report
    r = get('https://mis.ercot.com/misapp/GetReports.do?reportTypeId={}&mimic_duns=1172089695000'.format(reportTypeId), pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #bytes to string
    html = r.content.decode('utf-8')
    #parse html string
    soup = BeautifulSoup(html, 'html.parser')
    #pull out friendly names and links
    names = [n.text for n in soup.find_all('td', class_='labelOptional_ind')]
    links = [l.get('href') for l in soup.find_all('a', href=True)]
    #combine to df and filter only csvs
    files = pd.DataFrame(list(zip(names, links)), columns=['Names', 'Links'])
    files = files[files['Names'].str.contains("_csv")].reset_index(drop = True)
    #create download url for latest da clear zip file
    file_url = "https://mis.ercot.com/" + files.loc[0,'Links']
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
    #change date column to datetime format
    master_data['DeliveryDate'] = pd.to_datetime(master_data['DeliveryDate'])
    #change format of hour ending column
    master_data['HourEnding'] = pd.to_numeric(df.HourEnding.apply(lambda x: str(x)[1:2] if int(str(x)[0:2]) < 10 else str(x)[0:2]))
    #upload to database
    master_data.to_sql('DA_Shift_Factors',con=engine,index=False,method=None,if_exists='append')

    return 'DAY AHEAD SHIFT FACTORS UPLOADED'


    # #create filename daataframe with filename and link columns
    # files = pd.DataFrame(list(zip(filenames,links)), columns = ["Names","Links"])
    # #csv only
    # files = files[files['Names'].str.contains("_csv")].reset_index(drop = True)
    # # #create empty dataframe to append daily dataframe to
    # big_data = pd.DataFrame()
    # #for loop through lastest 9 files (should be today+1 to today-7 aka tomorrow to last week)
    # for i in range(0, len(files)):
    #     #create download url
    #     file_url = "https://mis.ercot.com/" + files.loc[i,'Links']
    #     print(file_url)
    #     #make request with certs
    #     cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #     #create empty dataframe
    #     master_data = pd.DataFrame()
    #     #extract from each daily zip file w/o downloading
    #     with ZipFile(BytesIO(cert_url.content)) as zfile:
    #         csvs = zfile.infolist()
    #         csv_list = []
    #         for csv in csvs:
    #             df = pd.read_csv(zfile.open(csv.filename))
    #             csv_list.append(df)
    #             data = pd.concat(csv_list)
    #     #append to empty dataframe created in for loop
    #     master_data = master_data.append(data)
    #     #change date column to datetime format
    #     master_data['DeliveryDate'] = pd.to_datetime(master_data['DeliveryDate'])
    #     #change format of hour ending column
    #     master_data['HourEnding'] = pd.to_numeric(master_data.HourEnding.apply(lambda x: str(x)[1:2] if int(str(x)[0:2]) < 10 else str(x)[0:2]))
    #     #append to big_data
    #     big_data = big_data.append(master_data)
    #
    # #upload to database
    # big_data.to_sql('DA_Shift_Factors_test',con=engine,index=False,chunksize=10000,method=None,if_exists='replace')
    #
    # return 'DA SHIFT FACTORS ALL UPLOADED'




## pull shift factors
da_shifts()
