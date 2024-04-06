# %%
import pandas as pd
from datetime import time
import requests
from bs4 import BeautifulSoup
import pymsteams as tm

#pull notices
def pull_public_notices():
    #define url
    url = 'https://www.ercot.com/services/comm/mkt_notices/opsmessages'
    #make get request and decode
    r = requests.get(url)
    html = r.content.decode('utf-8')
    #parse html string
    soup = BeautifulSoup(html, 'html.parser')
    #get table div html
    table_div = soup.find('table', class_='table table-condensed notices-table')
    #empty list
    df = []
    #for loop through table_div and define ops message postings
    for row in table_div.find_all('tr'):
        values = row.find_all('td')
        if len(values) == 4:
            new_row = [i.text for i in values]
            df.append(new_row)
    #list to df
    df = pd.DataFrame(df, columns=['Datetime', 'Notice', 'Type', 'Status'])

    return df

#pull operations messages
df = pull_public_notices()
#datetime column
df['Datetime'] = pd.to_datetime(df['Datetime'])

#pull last message
last_message = pd.read_csv(r'C:\BRP_scrapers\scripts\last_message.csv')
last_message
last_message_time = last_message.Datetime[0]
last_message_time
#filter scraped messages to determine if any are new
new_messages = df[df.Datetime > last_message_time]
new_messages

#create empty list to append messages to
messages_list = []
for i, j in new_messages.iterrows():
    time = j.Datetime
    notice = j.Notice
    message = "[{}] {}".format(str(time), notice)
    messages_list.append(message)

if len(messages_list) > 0:
    #join messages into string
    final_messages = '\n\n'.join(messages_list)
    final_messages
    ### connector card and message ###
    webhook = "https://engie.webhook.office.com/webhookb2/a10dc229-a310-417e-88d8-7671d76b8a95@24139d14-c62c-4c47-8bdd-ce71ea1d50cf/IncomingWebhook/fe5437c230a94bf78891a7b37255f13f/00121146-f154-450c-87f0-cabfd6a4cbd9"
    connector = tm.connectorcard(webhook)
    connector.title('Ops Messages')
    connector.text(final_messages)
    connector.color('FF0000')
    connector.send()

    #create new 'last message' csv to write and reference later
    #last notice pulled
    last_message = df.loc[0:0].reset_index(drop=True)
    #export to csv to reference upon next pull
    last_message.to_csv(r'C:\BRP_scrapers\scripts\last_message.csv', index=False)
else:
    pass
    print('NO NEW OPS MESSAGE MF')

# %%
