import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, datetime, timedelta
import zipfile, io, os, glob
from zipfile import ZipFile
from io import StringIO, BytesIO
from requests_pkcs12 import get
import requests
from bs4 import BeautifulSoup
import plotly.express as px
from ercot_config import pfx_path, pfx_password

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
        master_data = master_data.append(data)
        #append to big_data
        big_data = big_data.append(master_data)

    #format datetime columns
    big_data['DeliveryDate'] = pd.to_datetime(big_data['DeliveryDate'])
    big_data['HourEnding'] = big_data['HourEnding'].apply(lambda x: x[:-3]).astype(int)

    return big_data

raw = pull_mis(12341)

date = raw.SASMID[0]

df = raw.pivot(index='HourEnding', columns='ASType', values='MCPC')

df = df[['REGDN', 'REGUP', 'RRS', 'NSPIN']]
fig = px.line(df).update_layout(title_text=date, title_x=0.5).update_xaxes(title_text='HE')
fig.show()
print(df.to_string())
