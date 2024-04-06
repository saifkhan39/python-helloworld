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
#date variables
yesterday = (dt.date.today() + timedelta(days=-1)).strftime('%Y%m%d.')
extra_hour = (dt.date.today()).strftime('%Y%m%d.00')

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

#pull rt shift factors
def pull_mis(reportTypeId):
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
    #create empty dataframe to append daily dataframe to
    big_data = pd.DataFrame()
    #for loop through last 14 days
    for i in range(0, len(files)):
        if (yesterday in files.loc[i, 'Names']) or (extra_hour in files.loc[i, 'Names']):
            #create download url
            file_url = "https://mis.ercot.com/" + files.loc[i,'Links']
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
            master_data = pd.concat([master_data, data])
            #append to big_data
            big_data = pd.concat([big_data, master_data])

    #format datetime columns
    big_data['SCED_Time_Stamp'] = pd.to_datetime(big_data['SCED_Time_Stamp'])

    return big_data

#pull shifts
raw_shifts = pull_mis('16013')
#clean up
raw_shifts = raw_shifts[raw_shifts.SCED_Time_Stamp.dt.date != (dt.date.today() + timedelta(days=-2))]
raw_shifts['Date'] = raw_shifts.SCED_Time_Stamp.dt.date
#average
rt_sf = pd.DataFrame(raw_shifts.groupby(['Date', 'Constraint_Name', 'Contingency_Name', 'Settlement_Point'])['Shift_Factor'].mean().round(2)).reset_index()
rt_sf.info()
#upload to database
rt_sf.to_sql('RT_Shift_Factors',con=engine,index=False,method=None,if_exists='append')
