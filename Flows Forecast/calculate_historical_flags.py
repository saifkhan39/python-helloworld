import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, timedelta
import pyodbc
import plotly.express as px
from yesapi.functions import *
from functools import reduce
from galaxy_vault.factory import VaultFactory 

#### GLOBAL VARIABLES AND FUNCTIONS ####
#query db
def queryDB(database: str, sql_string: str):
    factory = VaultFactory()
    vault = factory.get_vault()
    db_credentials = vault.get_db_credentials()
    driver = '{ODBC Driver 17 for SQL Server}'
    database = '{0}'.format(database)
    conn_string = 'DRIVER='+driver+';SERVER='+db_credentials.server+';PORT=1433;UID='+db_credentials.username+';DATABASE='+ database + ';PWD='+ db_credentials.password
    conn = pyodbc.connect(conn_string)
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

#pull historical congestion from 9/28/2021 on
#start and end dates
start_date = '09/28/2021'
end_date = '04/03/2022'
#pull the data
factory = VaultFactory()
vault = factory.get_vault()
yes_url = vault.get_secret('yes-energy-base-url')
df = pull_yes_html(yes_url+'/PS/rest/constraint/hourly/RT/ERCOT?startdate={}&enddate={}'.format(start_date, end_date))
#revert to hour beginning timestamp
df['DATETIME'] = pd.to_datetime(df['DATETIME']) - timedelta(hours=1)
#filter down shadow prices >= 25
df = df[df.SHADOWPRICE >= 25].reset_index(drop=True)
df = df[['FACILITYNAME', 'CONTINGENCY', 'DATETIME', 'SHADOWPRICE']]
#create separator
sep = 'KV '
#ERCOT constraint names
df['CONSTRAINT'] = df['FACILITYNAME'].apply(lambda x: x.split(sep, 1)[-1])
#reorg
df = df[['DATETIME', 'CONSTRAINT', 'CONTINGENCY', 'SHADOWPRICE']]
df['SHADOWPRICE'] = 1

#for loop through unique constraint/contingency pairs and create list of timestamps bound
constraints = df.CONSTRAINT.unique()
#empty dataframe to append to
pairs = []

for i in constraints:
    constraint = i
    contingencies = df[df.CONSTRAINT == constraint].CONTINGENCY.unique()
    for j in contingencies:
        timestamps = df[(df.CONSTRAINT == constraint) & (df.CONTINGENCY == j)]
        timestamps = list(timestamps.DATETIME.astype(str))
        pair = [constraint, j, timestamps]
        pairs.append(pair)

pairs = pd.DataFrame(pairs, columns = ['Constraint', 'Contingency', 'Timestamps'])
#check if has bound more than once. remove if it hasn't
pairs['prints'] = [len(x) for x in pairs.Timestamps]
pairs = pairs[pairs.prints >= 5].reset_index(drop=True)
#create stat columns to append to later
pairs['Average'] = np.nan
pairs['Min'] = np.nan
pairs['Max'] = np.nan

#query in list format
constraints = tuple(pairs.Constraint)
contingencies = tuple(pairs.Contingency)
stamps = tuple(list(set([stamp for list_ in pairs.Timestamps for stamp in list_])))

#create sql string
sql = ("""

select *
from [dbo].[flows_forecast]
where ConstraintName IN {}
and ContingencyName IN {}
and ForecastTimeStamp IN {}

""")

flows = queryDB('ErcotMarketData', sql.format(constraints, contingencies, stamps))

mean = flows.groupby(['ConstraintName', 'ContingencyName'])['Flows'].mean().round(2)
mean = pd.DataFrame(mean)

max = flows.groupby(['ConstraintName', 'ContingencyName'])['Flows'].max().round(2)
max = pd.DataFrame(max)

min = pd.DataFrame(flows.groupby(['ConstraintName', 'ContingencyName'])['Flows'].min().round(2))

#left join
flags = reduce(lambda x,y: pd.merge(x, y, on=['ConstraintName', 'ContingencyName'], how='outer'), [mean, max, min])
flags.columns = ['Avg', 'Max', 'Min']
flags = flags.reset_index()

flags.to_csv(r"C:\Users\MattSindler\BroadReachPower\BRP - Documents\Cap and Trade\Analyst Folders\Sindler\Projects\Flows Forecast\historical_binding_flags.csv", index=False)
print('all done')
