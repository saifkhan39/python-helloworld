# %%
import pandas as pd
from datetime import date, timedelta
from yesapi.functions import *
from db_tools import queryDB, dbConnect

# %%
if date.today().day > 8:
    start = today(-8)
elif date.today().day < 2:
    start = today(-3)
else:
    start = (date.today() - timedelta(days=(date.today().day)-1)).strftime('%m/%d/%Y')

end = today(-1)

#pull dalmp/rtlmp and calculate opa
df = pull_prices(['HB_NORTH'], 'DALMP', start, end)
df = clean(df)
df2 = onpk(df)
df2 = df2.groupby(df2.index.date).mean().round(2)
df2.columns = ['rtlmp']
df2 = df2.loc[df2.index.repeat(24)]
df2.index = df.index

#pull AS and calculate scalars
as_ = pull_as('tx', 'all', start, end)
as_ = clean(as_)
as_.columns = ['REGDN', 'REGUP', 'RRS', 'NSPIN', 'ECRS']

scalars = as_.div(df2.rtlmp, axis=0).round(2)
scalars = add_he(scalars)
scalars = scalars.groupby('he').mean().round(2)

#pull ice data
#define sql query
sql_yesterday = (date.today()+timedelta(days=-1)).strftime('%Y-%m-%d')

sql = f"""

select top 10 *
from dbo.ice_forwards
where trade_date = (
    select max(trade_date)
    from dbo.ice_forwards
)
and strip > '{sql_yesterday}' 
and contract_code = 'END' and contract_type = 'D'
order by strip

"""

ice = queryDB('ErcotMarketData', sql)

#create empty df to concat/append to
fx = pd.DataFrame()

#for loop through rows and calculate hourly AS forecast
for i, j in ice.iterrows():
    hrly = pd.date_range(j.strip, periods=24, freq='H')
    prices = (scalars*j.settlement_price).round(2)
    prices.index = hrly
    fx = pd.concat([fx, prices])

#rename index
fx.index.names = ['datetime']

#fix any values > price cap
fx = fx.clip(upper=4950).reset_index()

#insert update time
update_time = datetime.now()
fx.insert(0, 'update_time', update_time)

# %%
#write to db
fx.to_sql('implied_forward_AS_fx', dbConnect('ErcotMarketData'), index=False, if_exists='replace')
# %%
