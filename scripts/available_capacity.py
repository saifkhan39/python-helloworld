# %%
import os
from os import listdir, walk
from os.path import isfile, join
from os.path import exists
import requests
from bs4 import BeautifulSoup as bsp
import time
import pandas as pd
from datetime import datetime, timedelta, date
import plotly.express as px
import numpy as np
import plotly.graph_objects as go
from pandas import json_normalize
import pymsteams as tm
import matplotlib.pyplot as plt
from io import BytesIO
import base64

def ErcotQuery(Report_ID):
    reportID = Report_ID
    ercotQuery = pd.read_json("https://sa.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId="+str(reportID))
    ercotQuery = pd.json_normalize(ercotQuery['ListDocsByRptTypeRes'][0])
    ercotQuery['type'] = ercotQuery['Document.FriendlyName'].str.slice(-3)
    ercotQuery['PublishDate'] = pd.to_datetime(ercotQuery['Document.PublishDate'], format = '%Y-%m-%dT%H:%M:%S')
    csvList = ercotQuery[ercotQuery['type']=='csv']
    csvList = csvList[['Document.DocID','Document.ConstructedName','PublishDate']]
    csvList['zipFile'] = "https://sa.ercot.com/misdownload/servlets/mirDownload?doclookupId=" + csvList['Document.DocID']
    zipFiles = csvList['zipFile'].tolist()
    df = pd.concat(pd.read_csv(zips, compression='zip') for zips in zipFiles)
    return(df)

def RecentData(Report_ID,call_number):
    reportID = Report_ID
    ercotQuery = pd.read_json("https://sa.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId="+str(reportID))
    ercotQuery = pd.json_normalize(ercotQuery['ListDocsByRptTypeRes'][0])
    ercotQuery['type'] = ercotQuery['Document.FriendlyName'].str.slice(-3)
    ercotQuery['PublishDate'] = pd.to_datetime(ercotQuery['Document.PublishDate'], format = '%Y-%m-%dT%H:%M:%S')
    csvList = ercotQuery[ercotQuery['type']=='csv']
    csvList = csvList[['Document.DocID','Document.ConstructedName','PublishDate']]
    csvList['zipFile'] = "https://sa.ercot.com/misdownload/servlets/mirDownload?doclookupId=" + csvList['Document.DocID']
    zipFiles = csvList['zipFile'].tolist()
    file = zipFiles[call_number]
    df = pd.read_csv(file, compression = 'zip')
    df['report_time'] = 'Hours Ago: ' + str(call_number)
    return(df)

def convertdate_1(df):
    df['HB'] = df['HourEnding'].str.split(':').str[0]
    df['HB'] = pd.to_numeric(df['HB']) - 1
    df['datetime'] = df['DeliveryDate'].astype(str) + ' ' + df['HB'].astype(str) + ':00'
    df['datetime'] = pd.to_datetime(df['datetime'])
    return(df)

def convertdate_2(df):
    df['HB'] = df['HOUR_ENDING'] - 1
    df['datetime'] = df['DELIVERY_DATE'].astype(str) + ' ' + df['HB'].astype(str) + ':00'
    df['datetime'] = pd.to_datetime(df['datetime'])
    return(df)


def pull_data(previous_hours):
    #### ERCOT availble capacity Forecast ####
    available_capacity = RecentData(12315,previous_hours)
    available_capacity = convertdate_1(available_capacity)
    available_capacity = available_capacity[['datetime', 'AvailCapGen','report_time']]

    #### ERCOT Load Forecast ####
    load = RecentData(12311,previous_hours)
    load = convertdate_1(load)
    load = load[['datetime', 'SystemTotal', 'report_time']]
    load = load.rename(columns = {
        'SystemTotal': 'load'
        })

    df = pd.merge(available_capacity, load, how = 'inner', on = ['datetime', 'report_time'])
    df['capacity_margin'] = df['AvailCapGen'] - df['load']
    return(df)

def create_graph(previous_hours):
    df = pull_data(previous_hours)
    df = df[df['datetime'] <= df['datetime'][0] + timedelta(days = 3)]
    fig, ax = plt.subplots()
    ax.plot(df['datetime'],df['capacity_margin'])
    fig.savefig('test.png')

def create_message():
    buf = BytesIO()
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    encoded_fig = f"data:image/png;base64,{data}"
    connector = tm.connectorcard("https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/9f50725834ee43ffa09d67e58c7dcd8e/384edb55-72b2-4eb3-9600-385bcd68c8b8")
    connector.text("This is the available capacity")
    Section1 = tm.cardsection()
    Section1.title('Graph of Available Capacity minus Load')
    Section1.addImage(encoded_fig)
    connector.addSection(Section1)
    connector.send()

def create_message_text():
    df = pull_data(0)
    now = datetime.now()
    hour = df[(df['capacity_margin']<6000) & (df['datetime'] > now)]
    d = hour['datetime'].dt.date.unique()[0]
    date_string = hour['datetime'].dt.date.unique()[0].strftime('%m/%d/%Y')
    he = hour[hour.datetime.dt.date == d]['datetime'].dt.hour.to_list()
    margin = hour[hour.datetime.dt.date == d]['capacity_margin'].round(0).to_list()
    return d, date_string, he, margin

def send_message(date_string, he, margin):
    #connector = tm.connectorcard("https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/9f50725834ee43ffa09d67e58c7dcd8e/384edb55-72b2-4eb3-9600-385bcd68c8b8")
    connector = tm.connectorcard('https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/5dc4f7b87e6f4ae5b04a39820d1c6a13/47c07d63-9bdd-4d99-9f71-ac7e7e4b52e9')
    connector.text(f'The highest risk times for energy prices on {date_string} are HE: {he}, where the capacity margin is: {margin}, respectively.')
    connector.send()

def send_message2():
    #connector = tm.connectorcard("https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/9f50725834ee43ffa09d67e58c7dcd8e/384edb55-72b2-4eb3-9600-385bcd68c8b8")
    connector = tm.connectorcard('https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/5dc4f7b87e6f4ae5b04a39820d1c6a13/47c07d63-9bdd-4d99-9f71-ac7e7e4b52e9')
    connector.text('Capacity shortage risk detected more than 2 days out.')
    connector.send()


# %%
#loop through days there is risk and send if risk detected
df = pull_data(0)
now = datetime.now()
hour = df[(df['capacity_margin']<6000) & (df['datetime'] > now)]
days = hour['datetime'].dt.date.unique()

for d in days:
    date_string = d.strftime('%m/%d/%Y')
    he = hour[hour.datetime.dt.date == d]['datetime'].dt.hour.to_list()
    he = [x + 1 for x in he]
    margin = hour[hour.datetime.dt.date == d]['capacity_margin'].round(0).to_list()

    if len(he) == 0:
        print('no capacity risk, retrying in 5mins')
    else:
        if d <= (date.today() + timedelta(days=2)):
            send_message(date_string, he, margin)
        else:
            print('too far out to send')

# %%
