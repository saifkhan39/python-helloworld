# %%

import pandas as pd
import utils as ut
import numpy as np
import DSTCheck as dst
from datetime import timedelta
import datetime as dt
import Teams_Connect as tc

# open a connection to the database
engine = ut.dbConnect()

# function to pull the latest date from the SQL database to make sure we aren't creating dups 
def latest_date(series_name):
    d = pd.read_sql(f"SELECT max(datetime_local) from dbo.TCAP_Historian where name = '{series_name}'",engine).iloc[0][0]
    return(d)


# %%
############################################################
### this section is for the daily LRS forecast download ####
############################################################

# %%

try:
    LRS_Forecast_latest = latest_date('LRS_Forecast')

    lrs_forecast = ut.pull_lrs_forecast(LRS_Forecast_latest)

    if lrs_forecast.empty == False:
        lrs_forecast.to_sql('TCAP_Historian',con=engine,index=False,method=None,if_exists="append")
    else:
        print('Data is up to date')
except:
    tc.failure_notice('TCAP Historian (LRS_Forecast)')


#this will get the files required for the next two sections
files = ut.list_files(11113, 'RTM_INITIAL')

#######################################################
### this section is for the initial load  download ####
#######################################################

#options for header name are LTOTQSE for 15 minute load and LRSLSE for hourly load ratio share

# %%

try:
    load_latest = latest_date('Load_Initial')
    load = ut.download_files(file_list=files, header_name='LTOTQSE_1623',
                             start_date=load_latest.strftime('%Y-%m-%d'))
    if load.empty == True:
        print('Still nothing to upload')
    else:
        load['name'] = 'Load_Initial'
        load = load[['datetime_utc','datetime_local','name','value']]
        load.to_sql('TCAP_Historian',con=engine,index=False,method=None,if_exists="append")
        print('Data is updated')
except:
    tc.failure_notice('TCAP Historian (Load Intitial)')

##################################################################
### this section is for the initial load ratio share download ####
##################################################################

try:
    lrs_latest = latest_date('LRS_Initial')
    lrs = ut.download_files(file_list=files, header_name='LRSLSE_436_1623',
                            start_date=lrs_latest.strftime('%Y-%m-%d'))
    if lrs.empty == True:
        print('Still nothing to upload')
    else:
        lrs['name'] = 'LRS_Initial'
        lrs = lrs[['datetime_utc','datetime_local','name','value']]
        lrs.to_sql('TCAP_Historian',con=engine,index=False,method=None,if_exists="append")
        print('Data is updated')
except:
    tc.failure_notice('TCAP Historian (LRS Intitial)')
# %%
