# %%
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, date, timedelta
from yesapi.functions import *
import plotly.express as px

link =  vault.get_secret('yes-energy-base-url')+'/PS/rest/objects/genunits/ercot?'

## pull genunits object data
def pull_genunits(link):
    #define query
    query = requests.get(link, auth=auth)
    #pull and manipulate raw data
    df = pd.read_html(query.text)

    return pd.concat(df)

raw_data = pull_genunits(link)
df = raw_data
df = df[(df.STATUS == 'Operating')&(df.NODENAME.isna())]
df = df[['PLANTNAME', 'PLANTNUMUNITS', 'NAMEPLATECAPACITY', 'NODENAME', 'PRIMARYFUEL', 'PRIMEMOVER', 'UNITNAME']].reset_index(drop=True)
df.to_csv(r"C:\Users\msindler\Broad Reach Power LLC\BRP - Documents\Cap and Trade\Analyst Folders\Sindler\Projects\Flows Forecast\flows forecast dependencies\genunits_nodename_na.csv", index=False)

# %%
