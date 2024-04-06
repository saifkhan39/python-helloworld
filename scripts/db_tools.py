import pandas as pd
import sqlalchemy, urllib
from sqlalchemy import create_engine

#function to query sql db
def queryDB(database: str, sql_string: str, server: str = 'brptemp'):
    """
    Query any DB in the brptemp or brptrading (10.128.2.11) server    
    """
    credentials = {
        'brptemp': {
            'driver': '{ODBC Driver 17 for SQL Server}',
            'server': "brptemp.database.windows.net",
            'database': database,
            'username': "brp_admin",
            'password': "Bro@dRe@chP0wer"
        },
        'brptrading': {
            'driver': '{ODBC Driver 17 for SQL Server}',
            'server': "tcp:10.128.2.11,1433",
            'database': database,
            'username': "brptrading",
            'password': "Brptr8ding#"
        },
    }
    cred = credentials[server]

    conn_string = (f"DRIVER={cred['driver']};"
                   f"SERVER={cred['server']};"
                   f"PORT=1433;"
                   f"UID={cred['username']};"
                   f"DATABASE={cred['database']};"
                   f"PWD={cred['password']}")

    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_string)}',
                                      fast_executemany=True)
    conn = engine.connect()
    df = pd.read_sql(sql_string, conn)
    conn.close()

    return df

#function to create sql alchemy engine and connect to db
def dbConnect(database: str, server_str: str = 'brptemp'):
    """
    SQLAlchemy engine to write to brptemp DB or Dallas DB
    
    Example: df.to_sql('name_of_table', con=dbConnect, method=None, if_exists='replace')
    """
    if server_str == 'brptemp':
        server = "brptemp.database.windows.net"
        username = "brp_admin"
        password = "Bro@dRe@chP0wer"
    else:
        server = "tcp:10.128.2.11,1433"
        username = "brptrading"
        password = "Brptr8ding#"
        
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = f"DRIVER={driver};SERVER={server};PORT=1433;UID={username};DATABASE={database};PWD={password}"
    connect_str = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"
    engine = create_engine(connect_str, fast_executemany=True)
    return engine
