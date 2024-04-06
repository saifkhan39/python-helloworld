import sys
import time
import urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime, timedelta

## functions and constants
#function to connect to database
def dbConnect():
    server = "brptemp.database.windows.net"
    database = "ErcotMarketData"
    username = "brp_admin"
    password = "Bro@dRe@chP0wer"
    driver = '{ODBC Driver 17 for SQL Server}'
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine = create_engine(connect_str,fast_executemany=True)
    return(engine)

#copy data
def copy_table_in_chunks(source_table, target_table, chunk_size_days):
    engine = dbConnect()

    with engine.connect() as connection:
        start_time = time.time()

        # Replace with your actual timestamp column name
        timestamp_column = "sced_time_stamp"
        chunk_duration = timedelta(days=chunk_size_days)

        start_date = datetime(2023,5,24,0,0,0)
        end_date = start_date + chunk_duration

        while True:
            # Fetch a chunk of rows within the date range
            fetch_query = text(f"SELECT * FROM {source_table} WHERE {timestamp_column} >= :start_date AND {timestamp_column} < :end_date")
            rows = connection.execute(
                fetch_query, start_date=start_date, end_date=end_date).fetchall()

            if not rows:
                break  # No more rows to copy

            # Insert the fetched rows into the target table
            insert_query = f"INSERT INTO {target_table} SELECT * FROM {source_table} WHERE {timestamp_column} IN ({','.join([str(row[timestamp_column]) for row in rows])})"
            connection.execute(insert_query)

            start_date = end_date
            end_date = start_date + chunk_duration

        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"Copy process completed at: {time.ctime(end_time)}")
        print(f"Time elapsed: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script_name.py source_table target_table chunk_size_days")
    else:
        source_table = sys.argv[1]
        target_table = sys.argv[2]
        chunk_size_days = int(sys.argv[3])
        copy_table_in_chunks(source_table, target_table, chunk_size_days)
