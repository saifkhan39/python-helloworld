# %%
import zipfile, io, os, glob
from zipfile import ZipFile
from io import StringIO, BytesIO
from requests_pkcs12 import get
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, datetime, time
import requests
from bs4 import BeautifulSoup
from ercot_config import pfx_path, pfx_password
import pyodbc
import sqlalchemy, urllib
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql import text as sa_text
from sqlalchemy.sql.expression import column
import pymsteams as tm
import time

#mis scraper function
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
    for i in range(0,1):
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
        big_data = pd.concat([big_data,master_data])

    #format datetime columns
    big_data['RTDTimestamp'] = pd.to_datetime(big_data['RTDTimestamp'])
    big_data['IntervalEnding'] = pd.to_datetime(big_data['IntervalEnding'])

    return big_data
# %%
#pull data
df = pull_mis('13222')

#create list of RTORPA
rtorpa = list(df.RTORPA)
# %%
if any(x < 10 for x in rtorpa):
    print('yes')
else:
    print('no')
# %%
#start if statement
if any(x > 10 for x in rtorpa):
    #latest timestamp variable
    timestamp = str(df.RTDTimestamp[0].strftime('%H:%M:%S'))
    #drop irrelevant columns
    data = df[['IntervalEnding', 'SystemLambda', 'RTORPA', 'RTOFFPA', 'RTORDPA', 'RTOLCAP', 'RTOFFCAP']].round(2)
    #clean IntervalEnding
    data['IntervalEnding'] = data['IntervalEnding'].dt.strftime('%H:%M')
    #rename columns
    data.columns = ['Interval Ending', 'System Lambda ($)', 'RTORPA ($)', 'RTOFFPA ($)', 'RTORDPA ($)', 'RTOLCAP (MW)', 'RTOFFCAP (MW)']
    data = data.set_index('Interval Ending')
    data_html = data.to_html()
    #create message
    message_string = 'Latest Indicative ORDC and Reliability Deployment Pirce Adders/Reserves for {0}:\n {1}\n {2}'
    if any(x < 5900 for x in list(data['RTOLCAP (MW)'])):
        cap_string = 'Online reserve capacity forecasted below 5,900MW!'
    else:
        cap_string = 'Online reserve capacity NOT forecasted below 5,900MW. Low risk for price spike'
    #rt alert title string
    title_message = f'Latest Indicative ORDC and Reliability Deployment Pirce Adders/Reserves for {timestamp}:'
    ### connector card and message ###
    webhook = "https://engie.webhook.office.com/webhookb2/a10dc229-a310-417e-88d8-7671d76b8a95@24139d14-c62c-4c47-8bdd-ce71ea1d50cf/IncomingWebhook/fe5437c230a94bf78891a7b37255f13f/00121146-f154-450c-87f0-cabfd6a4cbd9"
    connector = tm.connectorcard(webhook)
    connector.text(data_html)
    connector.title(title_message)
    connector.send()
    
    time.sleep(5)
else:
    print('no risk. goodbye')

# %%
