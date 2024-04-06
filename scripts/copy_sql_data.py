import sys
import time
import urllib
from sqlalchemy import create_engine

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
def main(source_table, target_table):
    engine = dbConnect()

    # Connect to the database and copy data to the target table
    with engine.connect() as connection:
        start_time = time.time()
        
        copy_query = f"INSERT INTO {target_table} SELECT * FROM {source_table}"
        
        print(f"Copy query will be executed:")
        print(f"Source Table: {source_table}")
        print(f"Target Table: {target_table}")
        
        confirmation = input("Is this correct? (y/n): ")
        
        if confirmation.lower() == "y":
            connection.execute(copy_query)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            print(f"Copy query started at: {time.ctime(start_time)}")
            print(f"Copy query completed at: {time.ctime(end_time)}")
            print(f"Time elapsed: {elapsed_time:.2f} seconds")
        else:
            print("Copy query execution cancelled.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py source_table target_table")
    else:
        source_table = sys.argv[1]
        target_table = sys.argv[2]
        main(source_table, target_table)