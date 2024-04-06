# %%
import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, timedelta, time
from db_tools import queryDB, dbConnect

# %%
#determine last td to be calculated
last_entry_sql = "select top 1 * from dbo.rt_pnl_5min order by dtm_central desc"
last_entry = queryDB('ErcotMarketData', last_entry_sql)
last_ts = last_entry['dtm_central'][0]
last_td = last_entry['td'][0]

#determine td variable value
yesterday = date.today() + timedelta(days=-1)

if last_td == yesterday:
    td_start = yesterday
    td_end = yesterday
else:
    td_start = last_td + timedelta(days=1)
    td_end = yesterday

meter_q = f"""

select interval_end, location, location_type, mwh, trade_date
from dbo.apx_meter_data
where trade_date >= '{td_start}' and trade_date <= '{td_end}'

"""

meter_data = queryDB('APX', meter_q).drop('trade_date', axis=1)
meter_data.columns = ['dtm_central', 'resource', 'location_type', 'mwh']

#query prices
prices_q = f"""

select *
from dbo.rt_asset_prices_5min
where dtm_central > '{td_start}'
order by dtm_central

"""
prices = queryDB('ErcotMarketData', prices_q)

#merge dfs
df = pd.merge(meter_data, prices[['dtm_central', 'resource', 'rtlmp']], on=['dtm_central', 'resource'])
#make load side throughput negative
df['mwh'] = np.where(df.location_type.str.contains('L'), df.mwh*-1, df.mwh)
#calculate revenue
df['rev'] = round(df.mwh*df.rtlmp, 2)
#add td and hour ending columns
df['td'] = np.where(df.dtm_central.dt.time > time(0,0), df.dtm_central.dt.date, df.dtm_central.dt.date + timedelta(days=-1))
df['he'] = np.where((df.dtm_central.dt.minute == 0), np.where(df.dtm_central.dt.hour == 0, 24, df.dtm_central.dt.hour), df.dtm_central.dt.hour + 1)

#filter to rows > latest_ts
df = df[df['dtm_central'] > last_ts]
# %%
#write to db
df.to_sql('rt_pnl_5min', con=dbConnect('ErcotMarketData'), if_exists='append', index=False)
# %%
