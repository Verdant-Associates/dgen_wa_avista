from pathlib import Path
import json 
import sqlalchemy
import psycopg2 
import sys 
import pandas.io.sql as sqlio
from pathlib import Path 
from pathlib import PurePath
import datetime 
import re
#* Read the Postgres credentials from a json file:
pg_creds_json = Path('pg_params_connect.json')

with open(pg_creds_json) as f:
    pg_creds = json.loads(f.read())    

print(pg_creds)
print('Done')

project_root = Path('V:\Projects\Avista DER Forecast') # Everything else should be stored on the I drive.
raw_data_root = project_root / 'Data' 
output_root = project_root / 'Output'

path_var_dict = {k: v for k, v in locals().items() if isinstance(v, PurePath)} # This could be more robust...

for var, pathobj in path_var_dict.items():
    print(var, pathobj, f'Checking: {pathobj.is_dir()}')
    
file_yymmdd = datetime.datetime.today().strftime('%Y%m%d')
print('The file suffix is:', file_yymmdd)


# {'dbname': 'dgen_db', 'host': '127.0.0.1', 'port': '5432', 'user': 'postgres', 'password': 'postgres'}
#* Define a function to create a SQLAlchemy connection:
def sqla_pg_con(creds, db='postgres'):
    #* Create a URL for the SQLAlchemy connection type:
    url = f"""postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{db}"""

    # url = f"""postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{db}/{creds['dbname']}"""
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

# Function to clean all the column names...
def polars_clean_col_dict(col_list, lc=True):
    print('Column Cleaner Executing...')
    clean_cols = {}
    for col in col_list:
        clean_col = re.sub('[^0-9a-zA-Z]+', '_', col.strip())
        clean_col = re.sub('_{2,}', '_', clean_col.strip())
        clean_col = re.sub('_$', '', clean_col.strip())
        if lc:
            clean_cols[col]=clean_col.lower()
        else:
            clean_cols[col]=clean_col
    return clean_cols

#* Create county ID map for use in the diffusion results:
with sqla_pg_con(pg_creds, 'dgen_db').connect() as conn:
    county_map = sqlio.read_sql_query(f'''SELECT county_id, county FROM diffusion_shared.county_geoms;''', conn)
