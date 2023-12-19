from dgen_000_setup import * 
from pathlib import Path
import json 
import sqlalchemy
import psycopg2 
import sys 
import pandas.io.sql as sqlio
import settings
import pandas as pd #* Dgen is Pandas, so for now we'll use Pandas for everything.
print(sys.executable)

eia_ids = pd.read_csv(Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\python\eia_id_mapping.csv'), dtype={'Utility Number': str})

eia_id_dict = eia_ids.set_index('Utility Number')['Utility Name'].to_dict()


with sqla_pg_con(pg_creds, db='dgen_db').connect() as pg_con:
    query = f"""SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name like '%diffusion_results_20231218%';;"""
    schema_list_df = (sqlio.read_sql_query(sqlalchemy.text(query), pg_con))

schema_list = schema_list_df['schema_name'].unique().tolist()
print(schema_list)

adopter_sum = pd.DataFrame()

for schema in schema_list:
    input_info= []
    with sqla_pg_con(pg_creds, 'dgen_db').connect() as conn:
        for tbl in ['input_value_of_resiliency_user_defined', 
                    'input_batt_prices_user_defined',
                    'input_pv_plus_batt_prices_user_defined']:
            query = f'''SELECT * FROM {schema}.{tbl};'''
            inval = sqlio.read_sql_query(query, conn)
            input_info.append(inval['val'].values[0])
        input_info.append(schema)

        query = f'''SELECT * FROM {schema}.agent_outputs;'''
        df = (sqlio.read_sql_query(query, conn)
                   .assign(utility=lambda x: x['eia_id'].map(eia_id_dict))
                   .merge(county_map, on='county_id', how='left'))
        print(', '.join(input_info))
        dfsum = (df.groupby(['year'])['number_of_adopters'].sum().reset_index()
                   .assign(scenario=', '.join(input_info)))
        adopter_sum = pd.concat([adopter_sum, dfsum])
        print(df.groupby(['year'])['number_of_adopters'].sum())
        # print(df.groupby(['utility', 'county'])['number_of_adopters'].sum())


print(adopter_sum.groupby('scenario')['number_of_adopters'].sum())
(adopter_sum.groupby(['scenario', 'year'])['number_of_adopters'].sum().reset_index().to_clipboard(sep='~', index=False))

print(pd.pivot_table(adopter_sum, index='year', columns='scenario', values='number_of_adopters'))
(pd.pivot_table(adopter_sum, index='year', columns='scenario', values='number_of_adopters')
   .reset_index()
   .to_clipboard(sep='~', index=False))

#* Import data from Fred:

# William –
print([c for c in df.columns if 'trac' in c])
df['tract_id_alias'].value_counts()
df['tract_id_alias'].nunique()


# I just sent a download link the Avista customer data that I’ve put together. It’s a relatively large CSV file 
# that has feeder assignments and census tract information (both the 2020 and 2010 varieties). You can use this 
# in your work takes you down a feeder-level path. A mostly complete data dictionary is attached – some 
# English descriptions of column names.
feederdf = pd.read_csv(raw_data_root / 'spid_merged.csv', dtype={'GEOID10_tract': str,
                                                                 'GEOID20_tract': str})
feederdf.columns = column_cleaner(feederdf.columns)
print(feederdf.info())

print(feederdf['geoid20_tract'].nunique())
print(feederdf['geoid20_tract'].nunique())
print(feederdf.groupby('countyname')['geoid20_tract'].nunique())
print(feederdf.groupby(['countyname', 'sector'])['prem_id'].nunique())

#* Tracts map cleantly to counties. No cross county tracts.
print(feederdf.groupby('geoid20_tract')['countyname'].nunique().to_frame().reset_index().groupby('countyname')['geoid20_tract'].nunique())


print(feederdf['geoid20_tract'].value_counts().sort_index())


print(df[['utility', 'county']].drop_duplicates()
                    .merge(feederdf[['countyname']].drop_duplicates().rename(columns={'countyname': 'county'}), 
                           on='county', 
                           how='left', 
                           indicator=True)
                    .sort_values(['utility', '_merge', 'county']))



#         print('Data read using context manager:')
#         print(df.head())
#         print(df.columns)

# df.to_clipboard(sep='~', index=False)


# df[df.duplicated(['year', 'county_id', 'bldg_id'], keep=False)].sort_values(['county_id', 'year']).head(20)
# df[df.duplicated(['year', 'county_id'], keep=False)].sort_values(['county_id', 'year']).head(20)

# [c for c in df.columns if 'bldg' in c]
# [c for c in df.columns if 'county' in c]


# df.groupby(['county_id', 'bldg_id'])['year'].agg(['min', 'max', 'count'])

# for col in ['county_id', 'year', 'bldg_id']:
#         print(col)
#         print(df[col].value_counts())

# #* This is a unique key, but what's in the bin?
# df[df.duplicated(['year', 'bin_id'], keep=False)].sort_values(['county_id', 'year']).head(20)


# df.groupby(['county_id', 'year']).size()

# df['year'].value_counts().sort_index()
# df['year'].value_counts().sort_index()

# for col in df.columns:
#     print(col)
#     if df[col].nunique() < 30:
#         print(col)
#         print(df[col].value_counts())