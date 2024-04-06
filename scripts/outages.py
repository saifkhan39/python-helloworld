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

#### global variables and functions ####
#mis certs
pfx_path = r'C:\BRP_scrapers\1172089695000$msindler.pfx'
pfx_password = 'XIX7f!tmzWUv4'

#mis scraper function
def pull_mis(reportTypeId):
    parser = etree.HTMLParser() # object for HTML text
    page = get('https://mis.ercot.com/misapp/GetReports.do?reportTypeId={}&mimic_duns=1172089695000'.format(reportTypeId), pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    html = page.content.decode("utf-8")
    tree = etree.parse(StringIO(html), parser=parser)
    #obtain <a href='url'> links in html
    a = tree.xpath("//a")
    hrefs = [link.get('href', '') for link in a]
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
    #create download url
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

    return master_data

#### pull and filter latest outage data ####
#pull latest outage file
df = pull_mis(13446)
## filter ##
#filter out cancelled, rejected, or withdrawn outages
statuses = ['Cancl', 'Rejct', 'Withd']
df = df[~df['OutageStatus'].isin(statuses)]
#select only line and xf outages
equipment = ['LN', 'XF']
df = df[df['EquipmentType'].isin(equipment)]
#select only 69, 138, and 345 kV outages
voltages = [69, 138, 345]
df = df[df['VoltageLevel'].isin(voltages)]
#convert planned/actual start and end columns to datetime format
df[['PlannedStartDate', 'PlannedEndDate', 'ActualStartDate', 'ActualEndDate', 'SubmitTime']] = df[['PlannedStartDate', 'PlannedEndDate', 'ActualStartDate', 'ActualEndDate', 'SubmitTime']].apply(pd.to_datetime)
#reorg column order
front = ['EquipmentName', 'EquipmentFromStationName', 'EquipmentToStationName', 'EquipmentType', 'VoltageLevel', 'PlannedStartDate', 'PlannedEndDate', 'ActualStartDate', 'ActualEndDate', 'NatureOfWork', 'SubmitTime']
df = df[front]
df.columns = ['Outage', 'From', 'To', 'Type', 'kV', 'PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd', 'Nature', 'Submitted']
#create three different dataframes: current outages, upcoming outages, ended outages
today = dt.date.today()
tomorrow = dt.date.today() + timedelta(days=1)
two_days = dt.date.today() + timedelta(days=2)
last_week = dt.date.today() + timedelta(days=-7)
#current outages
current = df[(df['ActualStart'].dt.date <= today) & (df['ActualEnd'].isnull())].sort_values('ActualStart', ascending=False)
#upcoming outages (includes ongoing + nd outages)
upcoming = df[(df['ActualEnd'].isnull()) & (df['PlannedStart'].dt.date <= tomorrow)].sort_values('PlannedStart', ascending=False)
#nd1
nd1 = df[(df['ActualEnd'].isnull()) & (df['PlannedStart'].dt.date <= two_days)].sort_values('PlannedStart', ascending=False)
#concluded outages (past 7 days)
ended = df[(df['ActualEnd'].dt.date <= today) & (df['ActualEnd'].dt.date >= last_week)].sort_values('ActualEnd', ascending=False)

# WRITE TO EXCEL #
#file path
fp = r"C:\Users\MattSindler\OneDrive - BroadReachPower\Shared Documents\Cap and Trade\DailyOperations\Congestion Analysis\outage scraper\outages.xlsx"
folder = r"C:\Users\MattSindler\OneDrive - BroadReachPower\Shared Documents\Cap and Trade\DailyOperations\Congestion Analysis\outage scraper"

#write csvs
current.to_csv(folder+r"\currentoutages.csv", index=False)
upcoming.to_csv(folder+r"\ndoutages.csv", index=False)
nd1.to_csv(folder+r"\nd1outages.csv", index=False)
ended.to_csv(folder+r"\concludedoutages.csv", index=False)
