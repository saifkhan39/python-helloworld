# %%
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from yesapi.functions import *
from db_tools import queryDB, dbConnect

# %%
#determine latest timestamp
last_ts_q = 'select max(dtm_central) from dbo.rt_asset_prices_5min'
last_ts = queryDB('ErcotMarketData', last_ts_q).loc[0][0]

#define start and end constants
start = last_ts.strftime('%m/%d/%Y')
end = today()

#define asset list
engie_assets = ['SUNVASLR_ALL', 'LBRA_BES1']
assets = all_assets + engie_assets
#pull 5min RTLMPs
prices = pull_prices(assets, 'RTLMP', start, end, '5min')
#clean up and rename columns
prices = clean(prices)
prices.columns = assets
#melt dataframe
prices = pd.melt(prices.reset_index(), id_vars='DATETIME', value_vars=assets, value_name='rtlmp', var_name='sp')
prices['td'] = np.where((prices.DATETIME.dt.hour == 0)&(prices.DATETIME.dt.minute == 0), prices.DATETIME.dt.date + timedelta(days=-1), prices.DATETIME.dt.date)

#read in settlement point to resource mapping
fp = r"C:\BRP_scrapers\script dependencies\sp_gen_load_mapping.csv"
mapping = pd.read_csv(fp)
mapping.columns = ['sp', 'gen_resource', 'load_resource', 'last_date']
mapping['last_date'] = pd.to_datetime(mapping['last_date']).dt.date.fillna(date.today())

#merge mapping onto long prices df
prices = prices.merge(mapping, how='left', on='sp')

#name change
#check for name change
prices['name_change'] = np.where(prices['td'] <= prices['last_date'], 1, np.nan)
#dropna associated with name change
prices = prices.dropna().reset_index(drop=True)

""" 2/7/2024: removing below line for now because it seems unnecessary and need to account for resources with shared sp """ 
# #keep first record per datetime, sp
# prices = prices.groupby(['DATETIME', 'sp']).first().reset_index().drop(['td', 'last_date', 'name_change'], axis=1)
prices = prices.drop(['td', 'last_date', 'name_change'], axis=1)

#re-melt
prices = prices.melt(id_vars=['DATETIME', 'sp', 'rtlmp'], value_name='resource').drop('variable', axis=1)
prices = prices.sort_values(['DATETIME', 'sp', 'resource'], ignore_index=True)

#pull 5min price adders
ordc = pull_ordc_data(start, end, '5min')
ordc = clean(ordc)[['RT_ORD_PRADDER', 'RT_OR_PRADDER', 'RT_OFF_PRADDER']]

#merge ordc onto prices df
prices = prices.merge(ordc, how='left', on='DATETIME')

#rename columns
prices.columns = prices.columns.str.lower()
prices = prices.rename(columns={'datetime':'dtm_central'})

#create dataframe of unique time range in utc and add cst column which we will use in merge later
dtm_utc = pd.DataFrame(pd.date_range(last_ts, today(2), freq='5T', tz='UTC'), columns=['dtm_utc'])
dtm_utc['dtm_central'] = dtm_utc['dtm_utc'].dt.tz_convert('US/Central').dt.tz_localize(None)

#merge onto prices df
prices = prices.merge(dtm_utc, how='left', on='dtm_central')
#make sure data is new
prices = prices[prices['dtm_central'] > last_ts]

#check for NaT's associated with spring DST change
if prices['dtm_utc'].isna().sum() > 0:
    na_index = prices[(prices.dtm_utc.isna())].index[0]
    datetime_na_fill = prices.loc[na_index-1].dtm_utc + timedelta(minutes=5)
    prices['dtm_utc'] = prices['dtm_utc'].fillna(datetime_na_fill).dt.tz_localize(None)
else:
    prices['dtm_utc'] = prices['dtm_utc'].dt.tz_localize(None)

# %%
#write to db
prices.to_sql('rt_asset_prices_5min', dbConnect('ErcotMarketData'), if_exists='append', chunksize=10000, index=False)
# %%
