import os
from pathlib import Path
import pandas as pd
from sqlalchemy.sql.expression import column
import sqlalchemy, urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
from sqlalchemy.exc import SQLAlchemyError


def dbConnect():
    server = "tcp:10.128.2.11,1433"
    database = "MarketData"
    username = "brptrading"
    password = "Brptr8ding#"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)
    return(engine)






