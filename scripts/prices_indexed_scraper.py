# %%
from zipfile import ZipFile
from io import BytesIO
from requests_pkcs12 import get
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
from ercot_config import pfx_path, pfx_password
from db_tools import queryDB, dbConnect
from scraper_failure_handling import send_failure_message

# %%
#### Functions ####
#mis scraper functions
def pull_mis_da(reportTypeId, date):
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
        if (date in files.loc[i, 'Names']):
            #create download url
            file_url = "https://mis.ercot.com/" + files.loc[i,'Links']
            #make request with certs
            cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
            #extract from each daily zip file w/o downloading
            with ZipFile(BytesIO(cert_url.content)) as zfile:
                csvs = zfile.infolist()
                csv_list = []
                for csv in csvs:
                    df = pd.read_csv(zfile.open(csv.filename))
                    csv_list.append(df)
                    data = pd.concat(csv_list)
            #concat to big_data
            big_data = pd.concat([big_data, data])

    #format datetime columns
    big_data['DeliveryDate'] = pd.to_datetime(big_data['DeliveryDate'])

    return big_data

def pull_mis_rt(reportTypeId, date):
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
        if (date in files.loc[i, 'Names']):
            #create download url
            file_url = "https://mis.ercot.com/" + files.loc[i, 'Links']
            #make request with certs
            cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
            #extract from each daily zip file w/o downloading
            with ZipFile(BytesIO(cert_url.content)) as zfile:
                csvs = zfile.infolist()
                csv_list = []
                for csv in csvs:
                    df = pd.read_csv(zfile.open(csv.filename))
                    csv_list.append(df)
                    data = pd.concat(csv_list)
            #concat to big_data
            big_data = pd.concat([big_data, data])

    #format datetime columns
    big_data['DeliveryDate'] = pd.to_datetime(big_data['DeliveryDate'])

    return big_data

# %%
#### Begin Scraper ####
try:
    #check latest data appended to prices_indexed table
    sql = ("""

        select top 1 *
        from dbo.prices_indexed
        order by flowdate desc, flowhour desc

    """)

    latest = queryDB('ErcotMarketData', sql)

    #define a few variables
    last_date = latest.flowdate[0]
    last_hour = latest.flowhour[0].hour

    if last_hour == 23:
        rt_date = (last_date + timedelta(days=1)).strftime('%Y%m%d.')
        rt_date_filter = last_date.strftime('%Y-%m-%d')
        da_date = (last_date - timedelta(days=1)).strftime('%Y%m%d.')
    else:
        rt_date = last_date.strftime('%Y%m%d.')
        rt_date_filter = last_date.strftime('%Y-%m-%d')
        da_date = (last_date - timedelta(days=1)).strftime('%Y%m%d.')

    #print date param variables
    scraper_date_params = f"""
        Last Date: {last_date}
        Last Hour: {last_hour}
        DA Report Date Variable: {da_date}
        RT Date Variable: {rt_date}
        RT Date Filter: {rt_date_filter}

    """
    print(scraper_date_params)

    #pull da prices
    da_raw = pull_mis_da(12331, da_date)
    da = da_raw.drop('DSTFlag', axis=1)
    da.columns = ['DeliveryDate', 'HourEnding', 'SettlementPoint', 'daprice']
    da['HourEnding'] = (da['HourEnding'].apply(lambda x: str(x)[:-3])).astype(int)

    #pull rt prices
    rt_raw = pull_mis_rt(12301, rt_date)
    rt = rt_raw
    rt = pd.DataFrame(rt.groupby(['DeliveryDate','DeliveryHour', 'SettlementPointName'])['SettlementPointPrice'].mean().round(2)).reset_index()
    current_hourending = datetime.now().hour + 1

    if last_date != date.today():
        rt = rt[(rt.DeliveryDate == rt_date_filter)]
    elif last_hour == 23:
        rt = rt[(rt.DeliveryDate == rt_date_filter) & (rt.DeliveryHour == 24)]
    else:
        rt = rt[(rt.DeliveryDate == rt_date_filter) & (rt.DeliveryHour < current_hourending)]

    #rename columns
    rt.columns = ['DeliveryDate', 'HourEnding', 'SettlementPoint', 'rtprice']

    #merge into final df to be appended to sql dataframe
    final = da.merge(rt, how='left', on=['DeliveryDate', 'HourEnding', 'SettlementPoint']).dropna()
    final.columns = ['flowdate', 'flowhour', 'node', 'daprice', 'rtprice']

    #calculate darts
    final['dart'] = final.daprice - final.rtprice

    #filter out data that has already been appended earlier today
    if last_hour < 23:
        final = final[final.flowhour > last_hour]
    else:
        final = final[final.flowhour == 24]

    #deal with date & time manipulation
    final['flowhour'] = np.where(final.flowhour == 24, 0, final.flowhour)
    final['flowdate'] = np.where(final.flowhour == 0, final.flowdate + timedelta(days=1), final.flowdate)
    final['flowhour'] = np.where(final.flowhour < 10, final['flowhour'].astype(str).str.zfill(2), final.flowhour)
    final['flowhour'] = final['flowhour'].apply(lambda x: str(x)+':00:00')
    final['flowdate'] = final['flowdate'].astype(str)
    final = final[['flowdate', 'flowhour', 'daprice', 'rtprice', 'dart', 'node']]
    final

    #write tables to sql
    final.to_sql('prices_indexed', con=dbConnect('ErcotMarketData'), index=False, method=None, if_exists='append')
    
except Exception as e:
    import os
    fn = os.path.basename(__file__)
    send_failure_message(e, fn)  
# %%
