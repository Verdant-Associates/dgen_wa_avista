import os
from pathlib import Path
import json
import sys 
#* Import packages for Postgres connections:
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
import pandas.io.sql as sqlio
import pandas as pd 
import re 
from pathlib import Path 
from pathlib import PurePath
import datetime 
    
file_yymmdd = datetime.datetime.today().strftime('%Y%m%d')
print('The file suffix is:', file_yymmdd)


#* Read the Postgres credentials from a json file:
pg_creds_json = Path('pg_params_connect.json')
#* Convert the json file to a dictionary:
with open(pg_creds_json) as f:
    pg_creds = json.loads(f.read())    
  
print('The pr_creds dict is:', pg_creds)

#* Define a function to create a SQLAlchemy connection:
def sqla_pg_con(creds, db='postgres'):
    #* Create a URL for the SQLAlchemy connection type:
    url = f"""postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{db}"""
    #* Create the SQLAlchemy engine:
    sqla_pg_con = sqlalchemy.create_engine(url, client_encoding='utf8')
    return sqla_pg_con

#* Define a function to create a psycopg2 connection:
def psyc_pg_con(pg_creds, db='postgres'):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(f'''dbname='{db}' user='{pg_creds["user"]}' host='{pg_creds["host"]}' password='{pg_creds["password"]}' ''')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn



# Function to clean all the column names...
def column_cleaner(col_list, lc=True):
    print('Column Cleaner Executing...')
    clean_cols = []
    for col in col_list:
        clean_col = re.sub('[^0-9a-zA-Z]+', '_', col.strip())
        clean_col = re.sub('_{2,}', '_', clean_col.strip())
        clean_col = re.sub('_$', '', clean_col.strip())
        if lc:
            clean_cols.append(clean_col.lower())
        else:
            clean_cols.append(clean_col)
    return clean_cols

'''
#* Testing the SQL Academy connection type:
#* Reading with context manager:
with sqla_pg_con(pg_creds, 'sgip2022').connect() as conn:
    query = "SELECT * FROM sgip2022.ctl.meter_data_sources;"
    df = sqlio.read_sql_query(query, conn)
    print('Data read using context manager:')
    print(df.head())

#* Reading without context manager:
conn = sqla_pg_con(pg_creds, 'sgip2022')
query = "SELECT * FROM sgip2022.ctl.meter_data_sources;"
df = sqlio.read_sql_query(query, conn)
print('Data read without using context manager:')

print(df.head())

#* Writing to the database with context manager:
with sqla_pg_con(pg_creds, 'sgip2022').connect() as conn:
    df.to_sql('garbage_table', con=conn, index=False, schema='sandbox', if_exists='replace')


#* Writing to the database without context manager:
df.to_sql('garbage_table', con=sqla_pg_con(pg_creds, 'sgip2022'), index=False, schema='sandbox', if_exists='replace')


#* Testing the psycopg2 connection types:
with psyc_pg_con(pg_creds, db='rates') as conn:
    print('Drop the tables with the fit statistics and parameter estimates..')
    cur = conn.cursor()
    query = f""" DROP TABLE IF EXISTS sandbox.garbage_table; """
    print(query)
    cur.execute(query)

    query = f""" CREATE TABLE sandbox.garbage_table (xxx text); """
    print(query)
    cur.execute(query)
    
'''

# #* Create a URL for the SQLAlchemy connection type:
# url = f"""postgresql://{pg_creds['user']}:{pg_creds['password']}@{pg_creds['host']}:{pg_creds['port']}/{pg_creds['dbname']}"""

# #* Create the SQLAlchemy engine:
# sqla_pg_con = sqlalchemy.create_engine(url, client_encoding='utf8')

#* For psycopg2, define a connection function for the psycopg2 connection:
#* Why? Only so you can use a in a context manager, which as far as I know
#* is not possible if you just create the connection.
# def pg_connect(pg_creds, db='postgres'):
#     """ Connect to the PostgreSQL database server """
#     conn = None
#     try:
#         print('Connecting to the PostgreSQL database...')
#         conn = psycopg2.connect(f'''dbname='{db}' user='{pg_creds["user"]}' host='{pg_creds["host"]}' password='{pg_creds["password"]}' ''')
#         # conn = psycopg2.connect(**params_dic)
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#         sys.exit(1)
#     print("Connection successful")
#     return conn

# print('Test the connection...')
# with pg_connect(pg_creds) as conn:
#     print('Connection good...')
#     print(type(conn))

# print(type(sqla_pg_con))


#* How to use the connection types:
'''
#* SQLAlchemy:
with sqla_pg_con.connect() as conn:
    query = "SELECT * FROM sgip2022.ctl.meter_data_sources;"
    df = sqlio.read_sql_query(query, conn)

    print(df.head())


df.to_sql('garbage_table', con=sqla_pg_con, index=False, schema='sandbox', if_exists='replace')

with pg_connect(pg_creds) as conn:
    print('Drop the tables with the fit statistics and parameter estimates..')
    cur = conn.cursor()
    query = f""" DROP TABLE IF EXISTS sandbox.garbage_table; """
    print(query)
    cur.execute(query)

    query = f""" CREATE TABLE sandbox.garbage_table (xxx text); """
    print(query)
    cur.execute(query)
'''