import pandas as pd
from urllib.request import urlopen
from io import StringIO, BytesIO
import zipfile
import datetime as dt
import time
from datetime import timedelta, datetime
import pyodbc, io, psutil
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
import requests, json
import urllib
from io import StringIO
from yesapi.functions import *
import seaborn as sns
import os
import sklearn.linear_model
from sklearn.linear_model import LinearRegression
from galaxy_vault.factory import VaultFactory 

os.chdir(r"C:\BRP_scrapers\CAISO")

hublmp_dict = {
    'TH_SP15_GEN-APND': 20000004682,
    'TH_NP15_GEN-APND': 20000004677,
}

gas_dict = {
    'Socal-Citygate': 10000002768,
    'PG&E - Citygate': 10000687008
}

AS_dict = {
    'AS_CAISO_EXP': 10002484315,
    'AS_NP26_EXP': 10002539052,
    'AS_SP26_EXP': 10002484317,
}

AS_dict2 = {
    'AS_CAISO': 10002484314,
    'AS_NP26': 10002863700,
    'AS_SP26': 10002484316,
}

rt_AS = {
    'AS_CAISO_EXP': 10002484315,
    'AS_NP26_EXP': 10002539052,
    'AS_SP26_EXP': 10002484317,
}

rt_AS2 = {
    'AS_CAISO': 10002484314,
    'AS_NP26': 10002863700,
    'AS_SP26': 10002484316,
}

start = (dt.date.today() - dt.timedelta(days=365*5)).strftime('%Y-%m-%d')
end = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')

factory = VaultFactory()
vault = factory.get_vault()
yes_energy_username = vault.get_secret_individual_account('yes-energy-username')
yes_energy_password = vault.get_secret_individual_account('yes-energy-password')
auth = (yes_energy_username, yes_energy_password)
def data_pull(dictionary):
    items_list = []
    if dictionary == hublmp_dict:
        for value in dictionary.values():
            item = 'DALMP:' + str(value) + ',RTLMP:' + str(value)
            items_list.append(item)
    elif dictionary == gas_dict:
        for value in dictionary.values():
            item = 'GASPRICE:' + str(value) + ',GASPRICE:' + str(value)
            items_list.append(item)
    elif dictionary == AS_dict:
        for value in dictionary.values():
            item = ('DAM SP_CLR_PRC:' + str(value) + ',DAM NS_CLR_PRC:' + str(value) + ',DAM RU_CLR_PRC:' + str(value) +
                    ',DAM RD_CLR_PRC:' + str(value))
            items_list.append(item)
    elif dictionary == rt_AS:
        for value in dictionary.values():
            item = ('RTM SP_CLR_PRC:' + str(value) +
                    ',RTM NS_CLR_PRC:' + str(value) + ',RTM RU_CLR_PRC:' + str(value) + ',RTM RD_CLR_PRC:' + str(value))
            items_list.append(item)
    elif dictionary == AS_dict2:
        for value in dictionary.values():
            item = ('DAM SP_CLR_PRC:' + str(value) + ',DAM NS_CLR_PRC:' + str(value) + ',DAM RU_CLR_PRC:' + str(value) +
                    ',DAM RD_CLR_PRC:' + str(value))
            items_list.append(item)
    elif dictionary == rt_AS2:
        for value in dictionary.values():
            item = ('RTM SP_CLR_PRC:' + str(value) +
                    ',RTM NS_CLR_PRC:' + str(value) + ',RTM RU_CLR_PRC:' + str(value) + ',RTM RD_CLR_PRC:' + str(value))
            items_list.append(item)
    items = ','.join(items_list)
    #DST flag
    if time.localtime().tm_isdst == 1:
        tz = 'PPT'
    else:
        tz = 'PST'
    #define parameters
    parameters = {
        'agglevel': 'hour',
        'startdate': start,
        'enddate': end,
        'timezone': tz,
        'items': items,
    }
    #define query
    factory = VaultFactory()
    vault = factory.get_vault()
    yes_energy_base_url = vault.get_secret('yes-energy-base-url')
    query = requests.get(yes_energy_base_url,params = parameters, auth = auth)
    #pull and manipulate raw data
    raw = pd.read_csv(StringIO(query.text),on_bad_lines='skip').round(2)
    #delete unnecesary columns
    df = raw
    df = df.set_index('DATETIME')
    df.index = pd.to_datetime(df.index)
    ##Shift df to align HE and Marketday with Datetime from yes
    df = df.shift(freq='-1H')
    ##Chnage month values to integers
    # df['MONTH'] = df['MONTH'].str.upper().map(month_dict)
    # df['MONTH'] = df['MONTH'].astype(str)
    # df['YEAR'] = df['YEAR'].astype(str)

    return df

###AS ---- DA

DA_AS1 = data_pull(AS_dict)
DA_AS2 = data_pull(AS_dict2)

da_data = pd.concat([DA_AS1,DA_AS2], axis=1)
da_data = da_data.loc[:,~da_data.columns.duplicated()]

da_data = da_data.fillna(0)

da_data['SP15 RegUp'] = (da_data['AS_CAISO_EXP (DAM RU_CLR_PRC)']+da_data['AS_CAISO (DAM RU_CLR_PRC)']+
                        da_data['AS_SP26_EXP (DAM RU_CLR_PRC)']+da_data['AS_SP26 (DAM RU_CLR_PRC)'])

da_data['SP15 RegDown'] = (da_data['AS_CAISO_EXP (DAM RD_CLR_PRC)']+da_data['AS_CAISO (DAM RD_CLR_PRC)']+
                        da_data['AS_SP26_EXP (DAM RD_CLR_PRC)']+da_data['AS_SP26 (DAM RD_CLR_PRC)'])

da_data['SP15 Spin'] = (da_data['AS_CAISO_EXP (DAM SP_CLR_PRC)']+da_data['AS_CAISO (DAM SP_CLR_PRC)']+
                        da_data['AS_SP26_EXP (DAM SP_CLR_PRC)']+da_data['AS_SP26 (DAM SP_CLR_PRC)'])

da_data['SP15 Nonspin'] = (da_data['AS_CAISO_EXP (DAM NS_CLR_PRC)']+da_data['AS_CAISO (DAM NS_CLR_PRC)']+
                        da_data['AS_SP26_EXP (DAM NS_CLR_PRC)']+da_data['AS_SP26 (DAM NS_CLR_PRC)'])

##NP AS Prices
da_data['NP15 RegUp'] = (da_data['AS_CAISO_EXP (DAM RU_CLR_PRC)']+da_data['AS_CAISO (DAM RU_CLR_PRC)']+
                        da_data['AS_NP26_EXP (DAM RU_CLR_PRC)']+da_data['AS_NP26 (DAM RU_CLR_PRC)'])

da_data['NP15 RegDown'] = (da_data['AS_CAISO_EXP (DAM RD_CLR_PRC)']+da_data['AS_CAISO (DAM RD_CLR_PRC)']+
                        da_data['AS_NP26_EXP (DAM RD_CLR_PRC)']+da_data['AS_NP26 (DAM RD_CLR_PRC)'])

da_data['NP15 Spin'] = (da_data['AS_CAISO_EXP (DAM SP_CLR_PRC)']+da_data['AS_CAISO (DAM SP_CLR_PRC)']+
                        da_data['AS_NP26_EXP (DAM SP_CLR_PRC)']+da_data['AS_NP26 (DAM SP_CLR_PRC)'])

da_data['NP15 Nonspin'] = (da_data['AS_CAISO_EXP (DAM NS_CLR_PRC)']+da_data['AS_CAISO (DAM NS_CLR_PRC)']+
                        da_data['AS_NP26_EXP (DAM NS_CLR_PRC)']+da_data['AS_NP26 (DAM NS_CLR_PRC)'])

da_data = da_data[['HOURENDING', 'MARKETDAY', 'PEAKTYPE', 'MONTH', 'YEAR', 'SP15 RegUp','SP15 RegDown', 'SP15 Spin', 'SP15 Nonspin', 'NP15 RegUp','NP15 RegDown', 'NP15 Spin', 'NP15 Nonspin']]

###AS ---- RT
import RT_AS_Issues
from RT_AS_Issues import rt_data2, rt_data


rt_data['SP15 RegUp'] = (rt_data['AS_CAISO_EXP (RTM RU_CLR_PRC)']+rt_data['AS_CAISO (RTM RU_CLR_PRC)']+
                        rt_data['AS_SP26_EXP (RTM RU_CLR_PRC)']+rt_data['AS_SP26 (RTM RU_CLR_PRC)'])

rt_data['SP15 RegDown'] = (rt_data['AS_CAISO_EXP (RTM RD_CLR_PRC)']+rt_data['AS_CAISO (RTM RD_CLR_PRC)']+
                        rt_data['AS_SP26_EXP (RTM RD_CLR_PRC)']+rt_data['AS_SP26 (RTM RD_CLR_PRC)'])

rt_data['SP15 Spin'] = (rt_data['AS_CAISO_EXP (RTM SP_CLR_PRC)']+rt_data['AS_CAISO (RTM SP_CLR_PRC)']+
                        rt_data['AS_SP26_EXP (RTM SP_CLR_PRC)']+rt_data['AS_SP26 (RTM SP_CLR_PRC)'])

rt_data['SP15 Nonspin'] = (rt_data['AS_CAISO_EXP (RTM NS_CLR_PRC)']+rt_data['AS_CAISO (RTM NS_CLR_PRC)']+
                        rt_data['AS_SP26_EXP (RTM NS_CLR_PRC)']+rt_data['AS_SP26 (RTM NS_CLR_PRC)'])

##NP AS Prices
rt_data['NP15 RegUp'] = (rt_data['AS_CAISO_EXP (RTM RU_CLR_PRC)']+rt_data['AS_CAISO (RTM RU_CLR_PRC)']+
                        rt_data['AS_NP26_EXP (RTM RU_CLR_PRC)']+rt_data['AS_NP26 (RTM RU_CLR_PRC)'])

rt_data['NP15 RegDown'] = (rt_data['AS_CAISO_EXP (RTM RD_CLR_PRC)']+rt_data['AS_CAISO (RTM RD_CLR_PRC)']+
                        rt_data['AS_NP26_EXP (RTM RD_CLR_PRC)']+rt_data['AS_NP26 (RTM RD_CLR_PRC)'])

rt_data['NP15 Spin'] = (rt_data['AS_CAISO_EXP (RTM SP_CLR_PRC)']+rt_data['AS_CAISO (RTM SP_CLR_PRC)']+
                        rt_data['AS_NP26_EXP (RTM SP_CLR_PRC)']+rt_data['AS_NP26 (RTM SP_CLR_PRC)'])

rt_data['NP15 Nonspin'] = (rt_data['AS_CAISO_EXP (RTM NS_CLR_PRC)']+rt_data['AS_CAISO (RTM NS_CLR_PRC)']+
                        rt_data['AS_NP26_EXP (RTM NS_CLR_PRC)']+rt_data['AS_NP26 (RTM NS_CLR_PRC)'])

rt_data = rt_data[['HOURENDING', 'MARKETDAY', 'PEAKTYPE', 'MONTH', 'YEAR', 'SP15 RegUp','SP15 RegDown', 'SP15 Spin', 'SP15 Nonspin', 'NP15 RegUp','NP15 RegDown', 'NP15 Spin', 'NP15 Nonspin']]

##Energy Prices
hub_prices = data_pull(hublmp_dict)
gas_prices = data_pull(gas_dict)

da_lmps = hub_prices[['HOURENDING', 'TH_SP15_GEN-APND (DALMP)', 'TH_NP15_GEN-APND (DALMP)']]
rt_lmps = hub_prices[['TH_SP15_GEN-APND (RTLMP)', 'TH_NP15_GEN-APND (RTLMP)']]

da_gas = gas_prices[['Socal-Citygate (GASPRICE)', 'PG&E - Citygate (GASPRICE)']]
rt_gas = gas_prices[['Socal-Citygate (GASPRICE)', 'PG&E - Citygate (GASPRICE)']]

###Combine
da_df = pd.concat([da_data,da_lmps], axis=1)
da_df = da_df.rename(columns={'TH_SP15_GEN-APND (DALMP)': 'SP15 Energy'})
da_df = da_df.rename(columns={'TH_NP15_GEN-APND (DALMP)': 'NP15 Energy'})
da_df = da_df.reset_index()

rt_df = pd.concat([rt_lmps,rt_data], axis=1)
rt_df = rt_df.rename(columns={'TH_SP15_GEN-APND (RTLMP)': 'SP15 Energy'})
rt_df = rt_df.rename(columns={'TH_NP15_GEN-APND (RTLMP)': 'NP15 Energy'})
rt_df = rt_df.reset_index()

df = pd.melt(da_df, id_vars=['DATETIME'], value_vars=['SP15 RegUp', 'SP15 RegDown', 'SP15 Spin', 'SP15 Nonspin', 'NP15 RegUp', 'NP15 RegDown', 'NP15 Spin', 'NP15 Nonspin', 'SP15 Energy', 'NP15 Energy'])
df['Market'] = 'DA'

rt_df2 = pd.melt(rt_df, id_vars=['DATETIME'], value_vars=['SP15 RegUp', 'SP15 RegDown', 'SP15 Spin', 'SP15 Nonspin', 'NP15 RegUp', 'NP15 RegDown', 'NP15 Spin', 'NP15 Nonspin', 'SP15 Energy', 'NP15 Energy'])
rt_df2['Market'] = 'RT'

###Combining
df = df.set_index('DATETIME')
rt_df2 = rt_df2.set_index('DATETIME')

result = pd.concat([df,rt_df2])

labels = result['variable'].str.split(' ', expand=True)
labels = labels.rename(columns={0: 'Node'})
labels = labels.rename(columns={1: 'Product'})

result_export = pd.concat([result, labels], axis=1)
result_export = result_export.drop(columns=['variable'])
result_export = result_export.rename(columns={'value': 'Value'})

ts = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
result_export['Update_time'] = ts


df = result_export.reset_index()
df['HE'] = (df['DATETIME'].dt.hour + 1)
# df = df.reset_index()

###sql
def dbConnect():
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    database = "CAISOMarketData"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)

    return(engine)

test = dbConnect()

df.to_sql('temp_prices', con=test, index=False, method=None, if_exists='replace', chunksize=50000)

###____temp_gas
###DA
da_lmps = da_lmps.rename(columns={'TH_SP15_GEN-APND (DALMP)': 'SP15'})
da_lmps = da_lmps.rename(columns={'TH_NP15_GEN-APND (DALMP)': 'NP15'})
da_lmps = da_lmps.reset_index()
da_lmps = pd.melt(da_lmps, id_vars=['DATETIME'], value_vars=['SP15', 'NP15'])
da_lmps = da_lmps.rename(columns={'variable': 'Hub'})
da_lmps = da_lmps.rename(columns={'value': 'Power_Price'})

da_gas = da_gas.rename(columns={'Socal-Citygate (GASPRICE)': 'SP15'})
da_gas = da_gas.rename(columns={'PG&E - Citygate (GASPRICE)': 'NP15'})
da_gas = da_gas.reset_index()
da_gas = pd.melt(da_gas, id_vars=['DATETIME'], value_vars=['SP15', 'NP15'])
da_gas = da_gas.rename(columns={'variable': 'Hub'})
da_gas = da_gas.rename(columns={'value': 'Gas_Price'})

da_hr = pd.concat([da_lmps, da_gas], axis=1)
da_hr['Market'] = 'DA'
###RT
rt_lmps = rt_lmps.rename(columns={'TH_SP15_GEN-APND (RTLMP)': 'SP15'})
rt_lmps = rt_lmps.rename(columns={'TH_NP15_GEN-APND (RTLMP)': 'NP15'})
rt_lmps = rt_lmps.reset_index()
rt_lmps = pd.melt(rt_lmps, id_vars=['DATETIME'], value_vars=['SP15', 'NP15'])
rt_lmps = rt_lmps.rename(columns={'variable': 'Hub'})
rt_lmps = rt_lmps.rename(columns={'value': 'Power_Price'})

rt_gas = rt_gas.rename(columns={'Socal-Citygate (GASPRICE)': 'SP15'})
rt_gas = rt_gas.rename(columns={'PG&E - Citygate (GASPRICE)': 'NP15'})
rt_gas = rt_gas.reset_index()
rt_gas = pd.melt(rt_gas, id_vars=['DATETIME'], value_vars=['SP15', 'NP15'])
rt_gas = rt_gas.rename(columns={'variable': 'Hub'})
rt_gas = rt_gas.rename(columns={'value': 'Gas_Price'})

rt_hr = pd.concat([rt_lmps, rt_gas], axis=1)
rt_hr['Market'] = 'RT'

final_hr = pd.concat([da_hr, rt_hr])
final_hr = final_hr.loc[:,~final_hr.columns.duplicated()]
final_hr['HE'] = final_hr['DATETIME'].dt.hour + 1
final_hr['Update_time'] = ts


final_hr.to_sql('temp_gas', con=test, index=False, method=None, if_exists='replace', chunksize=100000)

###Stroage Spreads
hilo_df = df

hilo_df = hilo_df.dropna()
hilo_df = hilo_df.loc[hilo_df.Product == 'Energy']
hilo_df = hilo_df[['DATETIME', 'Value', 'Node', 'Market', 'HE']]
hilo_df['Date'] = hilo_df['DATETIME'].dt.date


hilo_df['Max'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform('max')
#SECONDMAX

hilo_df['2nd Max'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform(lambda x: x.nlargest(2).min())
#THIRDMAX

hilo_df['3rd Max'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform(lambda x: x.nlargest(3).min())
#FOURTHMAX

hilo_df['4th Max'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform(lambda x: x.nlargest(4).min())

#MIN
hilo_df['Min'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform('min')
#SECONDMIN

hilo_df['2nd Min'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform(lambda x: x.nsmallest(2).max())
#THIRDMIN

hilo_df['3rd Min'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform(lambda x: x.nsmallest(3).max())
#FOURTHMIN

hilo_df['4th Min'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform(lambda x: x.nsmallest(4).max())

hilo_df['5th Min'] = hilo_df.groupby(['Date', 'Market', 'Node'])['Value'].transform(lambda x: x.nsmallest(5).max())


hilo_df['1-Hour Spread'] = (hilo_df['Max'] - hilo_df['Min'])
### 2 Hour Spread
hilo_df['2-Hour Spread'] = ((hilo_df['Max'] + hilo_df['2nd Max']) - (hilo_df['Min'] + hilo_df['2nd Min']))
### 4 Hour Spread
hilo_df['4-Hour Spread'] = ((hilo_df['Max'] + hilo_df['2nd Max'] + hilo_df['3rd Max'] + hilo_df['4th Max']) - (hilo_df['Min'] + hilo_df['2nd Min'] + hilo_df['3rd Min'] + hilo_df['4th Min']))
### 4 High | 5 Low Spread
hilo_df['4-High_5-Low Spread'] = ((hilo_df['Max'] + hilo_df['2nd Max'] + hilo_df['3rd Max'] + hilo_df['4th Max']) - (hilo_df['Min'] + hilo_df['2nd Min'] + hilo_df['3rd Min'] + hilo_df['4th Min'] + hilo_df['5th Min']))


############################

hilo_df = hilo_df[['Date', 'Node', 'Market', '1-Hour Spread', '2-Hour Spread', '4-Hour Spread', '4-High_5-Low Spread']]

hilo_df = hilo_df.drop_duplicates()

spreads = hilo_df

spreads.to_sql('temp_spreads', con=test, index=False, method=None, if_exists='replace', chunksize=100000)



# da_lmps.columns
# da_lmps = da_lmps.reset_index()
# test = da_lmps[['HOURENDING', 'TH_SP15_GEN-APND (DALMP)']]
#
# import seaborn as sns
# import matplotlib.pyplot as plt
# sns.boxplot(x=test['HOURENDING'], y=test['TH_SP15_GEN-APND (DALMP)'])
# plt.rcParams['figure.figsize'] = [1.5, 10]
# plt.show()
#
# import plotly.express as px
# # import plotly.graph_objects as go
# fig = px.box(test, x="HOURENDING", y="TH_SP15_GEN-APND (DALMP)", points='suspectedoutliers')
# # fig = fig.add_trace(go.Box(boxpoints='all'))
# fig.show()
#
# import matplotlib.pyplot as plt
# data=test.set_index('HOURENDING')
# data = data.transpose()
# data
# # fig1, ax1 = plt.subplots()
# # ax1.set_title('Basic Plot')
# plt.boxplot(data)


# ##____________________________________________________________________
##Gas, energy, HR, and Spark Spread data
# gas_df = data_pull(gas_dict)
#
# gas_df = gas_df[['Socal-Citygate (GASPRICE)', 'PG&E - Citygate (GASPRICE)', 'HOURENDING', 'MARKETDAY', 'PEAKTYPE', 'MONTH', 'YEAR']]
#
# hr_df = pd.concat([da_lmps,rt_lmps], axis=1)
# hr_df = pd.concat([hr_df,gas_df], axis=1)
#
# hr_df = hr_df.rename(columns={'HOURENDING': 'HE'})
# hr_df = hr_df[['TH_SP15_GEN-APND (DALMP)', 'TH_NP15_GEN-APND (DALMP)', 'TH_SP15_GEN-APND (RTLMP)', 'TH_NP15_GEN-APND (RTLMP)', 'Socal-Citygate (GASPRICE)', 'PG&E - Citygate (GASPRICE)', 'HE']]
