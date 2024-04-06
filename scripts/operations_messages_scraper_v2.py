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
from ercot_config import pfx_path, pfx_password, pfx_path2, pfx_password2
import pyodbc
import sqlalchemy, urllib
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql import text as sa_text
from sqlalchemy.sql.expression import column
import pymsteams as tm

#pull notices
def pull_public_notices():
    #get request to report
    r = get('https://mis.ercot.com/secure/widgets/common/notices?_=1644243121878', pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #bytes to string
    html = r.content.decode("utf-8")
    df = pd.concat(pd.read_html(html))
    return df

#pull operations messages
df = pull_public_notices()
#rename columns
df.columns = ['Datetime', 'Notice', 'Type', 'Priority', 'Classification']
#datetime column
df['Datetime'] = pd.to_datetime(df['Datetime'])
df
#pull last message
last_message = pd.read_csv(r'C:\BRP_scrapers\scripts\last_message.csv')
last_message
last_message_time = last_message.Datetime[0]
last_message_time
#filter scraped messages to determine if any are new
new_messages = df[df.Datetime > last_message_time]
#create empty list to append messages to
messages_list = []
for i, j in new_messages.iterrows():
    time = j.Datetime
    notice = j.Notice
    message = "({}) {}".format(str(time), notice)
    messages_list.append(message)
####
if len(messages_list) > 0:
    final_messages = '\n\n'.join(messages_list)
    ### connector card and message ###
    connector = tm.connectorcard("https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/9f50725834ee43ffa09d67e58c7dcd8e/384edb55-72b2-4eb3-9600-385bcd68c8b8")
    connector.title('v2')
    connector.text(final_messages)
    connector.send()

    #create new 'last message' csv to write and reference later
    #last notice pulled
    last_message = df.loc[0:0].reset_index(drop=True)
    #export to csv to reference upon next pull
    last_message.to_csv(r'C:\BRP_scrapers\scripts\last_message.csv', index=False)
else:
    pass
    print('NO NEW OPS MESSAGE MF')

print('new version')
# #pull certified notices
# def pull_certified_notices():
#     #get request to report
#     r = get('https://mis.ercot.com/secure/widgets/common/notices', pkcs12_filename=pfx_path2, pkcs12_password=pfx_password2)
#     #bytes to string
#     html = r.content.decode("utf-8")
#     df = pd.concat(pd.read_html(html))
#     return df
#
# #pull operations messages
# df = pull_certified_notices()
# #rename columns
# df.columns = ['Datetime', 'Notice', 'Type', 'Priority', 'Classification']
# #datetime column
# df['Datetime'] = pd.to_datetime(df['Datetime'])
# #pull last message
# last_message = pd.read_csv(r'C:\BRP_scrapers\scripts\last_certified_message.csv')
# last_message_time = last_message.Datetime[0]
# #filter scraped messages to determine if any are new
# new_messages = df[df.Datetime > last_message_time]
# new_messages
# #check if is COP Warning
# if new_messages['Type'].str.contains('COP').any():
#     #filter to only COP Warning rows
#     new_messages = new_messages[new_messages['Type'] == 'COP Warning']
#     #create empty list to append messages to
#     messages_list = []
#     for i, j in new_messages.iterrows():
#         time = j.Datetime
#         notice = j.Notice
#         message = "({}) {}".format(str(time), notice)
#         messages_list.append(message)
#
#     if len(messages_list) > 0:
#         final_messages = '\n\n'.join(messages_list)
#         ### connector card and message ###
#         connector = tm.connectorcard("https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/9f50725834ee43ffa09d67e58c7dcd8e/384edb55-72b2-4eb3-9600-385bcd68c8b8")
#         connector.text(final_messages)
#         connector.send()
#
#         #create new 'last message' csv to write and reference later
#         #last notice pulled
#         last_message = df.loc[0:0].reset_index(drop=True)
#         #export to csv to reference upon next pull
#         last_message.to_csv(r'C:\BRP_scrapers\scripts\last_certified_message.csv', index=False)
#     else:
#         pass
#         print('NO NEW OPS MESSAGE MF')
# else:
#     print('NO QSE WARNING')
