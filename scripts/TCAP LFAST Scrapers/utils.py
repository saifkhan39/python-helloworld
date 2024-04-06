#######################################################################################################
# the purposes of this file is to download and extract the necessary xml files from the ercot website #
#######################################################################################################
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
from lxml import etree
from sqlalchemy.sql.expression import column
import sqlalchemy, urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
from sqlalchemy.exc import SQLAlchemyError
import pytz 


pfx_path = r'Cert\Casey KoppFPLSQ38.pfx'
pfx_password = 'newpass1'

reportTypeId = 11113
#options are RTM_INITIAL; RTM_FINAL; RTM_TRUEUP
reportType = 'RTM_INITIAL'

def list_files(reportTypeId, reportType):
    r = get('https://mis.ercot.com/misapp/GetReports.do?reportTypeId={}&mimic_duns=1980232982000'.format(reportTypeId), pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    html = r.content.decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    names = [n.text for n in soup.find_all('td', class_='labelOptional_ind')]
    links = [l.get('href') for l in soup.find_all('a', href=True)]
    table = {'name': names, 
            'links': links}
    t = pd.DataFrame(table)
    print(t)
    initial_files = t[t['name'].str.contains(reportType)].reset_index(drop = True)
    initial_files['date'] = initial_files.name.str.slice(59, 63) + '-' + initial_files.name.str.slice(63, 65) + '-' + initial_files.name.str.slice(65, 67)
    initial_files['date'] = pd.to_datetime(initial_files['date'])
    return(initial_files)

# def download_files(files, start_date, header_name):

#options for header name are LTOTQSE for 15 minute load and LRSLSE for hourly load ratio share

def download_files(file_list, header_name, start_date):
    #try downloading the first file
    
    output = pd.DataFrame()
    links = file_list[file_list['date'] > pd.to_datetime(start_date)]
    links = links['links'].tolist()
    print(links)
    if len(links) > 0:
        for l in links:
            #create the url to download the zip file
            file_url = "https://mis.ercot.com/" + l

            #download the zip files
            cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
            zfile = ZipFile(BytesIO(cert_url.content))

            #read the xml file names
            # %%
            xmlnames = pd.DataFrame({'name': zfile.namelist()})
            # %%

            #get the proper header file and download it
            # %%
            header_file = xmlnames[xmlnames['name'].str.contains("DAIOUTPUTHEADER")].reset_index(drop = True).loc[0, 'name']
            header = pd.read_xml(zfile.open(header_file))
            header = header[['UIDDAIOUTPUTHEADER', 'SAVERECORDER']]
            #get the proper interval data file and downlaod it
            interval_file = xmlnames[xmlnames['name'].str.contains("DAIOUTPUTINTERVAL")].reset_index(drop = True).loc[0, 'name']
            interval = pd.read_xml(zfile.open(interval_file))

            #merge the interval data and the header data together and get the proper datetime formatting
            df = pd.merge(interval, header, how = 'left', on = 'UIDDAIOUTPUTHEADER')
            df['STARTTIME'] = pd.to_datetime(df['STARTTIME'], format='%Y-%m-%dT%H:%M:%S')
            df['STOPTIME'] = pd.to_datetime(df['STARTTIME'], format='%Y-%m-%dT%H:%M:%S')
            # %%

            #get the right header name from the dataset
            header_lookup = header[header['SAVERECORDER'].str.contains(header_name)].reset_index().loc[0,['SAVERECORDER']][0]

            #pull the wide interval data
            wide_table = df[df['SAVERECORDER'] == header_lookup].reset_index()
            start_time = wide_table.loc[0, 'STARTTIME']
            intervals = wide_table.loc[0, 'INTERVALCOUNT']
            long = pd.melt(wide_table, value_vars=wide_table.columns)

            interval_data = long[long['variable'].str.contains('INT')].reset_index(drop = True)[2:].reset_index(drop=True).dropna()


            if intervals > 25:
                timeframe = pd.date_range(start = start_time, end = start_time + timedelta(days=1), freq = '15min', closed = 'left', tz = 'America/Chicago')
            else:
                timeframe = pd.date_range(start = start_time, end = start_time + timedelta(days=1), freq = '60min', closed = 'left', tz = 'America/Chicago')
                

            temp = {'time': timeframe,
                    'value': interval_data['value']}


            

            temp = pd.DataFrame(temp)
            output = output.append(temp)
            print(l)
            output = output.reset_index(drop = True)
        output = output.resample('H', on = 'time').value.sum().reset_index()

        # create the necessary adjustments for utc time and local time
        output['utc_adj'] = pd.to_timedelta(output['time'].astype(str).str.slice(-4,-3).astype(int), unit='h')
        output['datetime_local'] = output['time'].astype(str).str.slice(0,16)
        output['datetime_local'] = pd.to_datetime(output['datetime_local'])
        output['datetime_utc'] = output['utc_adj'] + output['datetime_local']

        output = output[['datetime_utc','datetime_local','value']]
        return output
    else:
        print("There's nothing to upload") 
        return(pd.DataFrame())  



#mis scraper function
def pull_mis(reportTypeId):
    parser = etree.HTMLParser() # object for HTML text
    page = get('https://mis.ercot.com/misapp/GetReports.do?reportTypeId={}&mimic_duns=1172089695000'.format(reportTypeId), pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    html = page.content.decode("utf-8")
    tree = etree.parse(StringIO(html), parser=parser)
    #obtain <a href='url'> links in html
    refs = tree.xpath("//a")
    hrefs = [link.get('href', '') for link in refs]
    links = [l for l in hrefs]
    html_tag = tree.getroot()
    #create empty list to append filenames to
    filenames = []
    #for loop through and append
    for j in html_tag.iter("td"):# class='labelOptional_ind'"):
        text = j.text
        if text != None:
            filenames.append(text)

    #create filename daataframe with filename and link columns
    files = pd.DataFrame(list(zip(filenames,links)), columns = ["Names","Links"])
    #csv only
    files = files[files['Names'].str.contains("_csv")].reset_index(drop = True)
    #create empty dataframe to append daily dataframe to
    big_data = pd.DataFrame()
    #for loop through last 24 prints
    for i in range(0,len(files)):
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
    return big_data

def dbConnect():
    server = "tcp:10.128.2.11,1433"
    database = "MarketData"
    username = "brptrading"
    password = "Brptr8ding#"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)
    return(engine)

def pull_lrs_forecast(start_date):
    nd_lrs = pull_mis(12335)
    nd_lrs['datetime'] = nd_lrs['DeliveryDate'] + ' ' + nd_lrs['HourEnding']
    nd_lrs['date'] = pd.to_datetime(nd_lrs['DeliveryDate'])
    nd_lrs['HE'] = pd.to_timedelta(nd_lrs.HourEnding.str.slice(0,2).astype(int)-1, unit = 'h')
    nd_lrs['datetime'] = nd_lrs['date'] + nd_lrs['HE']
    nd_lrs = nd_lrs.rename(columns = {'datetime': 'datetime_local','LRS':'value'})
    nd_lrs['datetime_utc'] = nd_lrs['datetime_local'].dt.tz_localize('US/Central', ambiguous='infer').dt.tz_convert(pytz.utc).dt.strftime('%Y-%m-%d %H:%M')
    nd_lrs['datetime_utc'] = pd.to_datetime(nd_lrs['datetime_utc'])
    nd_lrs['name'] = 'LRS_Forecast'
    nd_lrs = nd_lrs[['datetime_local','datetime_utc','name','value']]
    nd_lrs = nd_lrs[nd_lrs['datetime_local'] > start_date]
    return(nd_lrs)