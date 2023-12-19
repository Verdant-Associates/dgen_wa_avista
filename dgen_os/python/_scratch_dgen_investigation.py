from pathlib import Path
import json 
import sqlalchemy
import psycopg2 
import sys 
import pandas.io.sql as sqlio
import settings
from dgen_000_setup import * 
import pandas as pd #* Dgen is Pandas, so for now we'll use Pandas for everything.
print(sys.executable)

eia_ids = pd.read_csv(Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\python\eia_id_mapping.csv'), dtype={'Utility Number': str})

eia_id_dict = eia_ids.set_index('Utility Number')['Utility Name'].to_dict()

# for schema in ['diffusion_results_20231103_093344288267_fina', 'diffusion_results_20231018_184159268553_fina']:
# for schema in ['diffusion_results_20231103_093344288267_fina', 'diffusion_results_20231018_184159268553_fina', 'dgen_db.diffusion_results_20231103_130028205855_fina']:
for schema in ['dgen_db.diffusion_results_20231106_082945746529_fina']:
    with sqla_pg_con(pg_creds, 'dgen_db').connect() as conn:
        county_map = sqlio.read_sql_query(f'''SELECT county_id, county FROM diffusion_shared.county_geoms;''', conn)
        query = f'''SELECT * FROM {schema}.agent_outputs;'''
        df = (sqlio.read_sql_query(query, conn)
                   .assign(utility=lambda x: x['eia_id'].map(eia_id_dict))
                   .merge(county_map, on='county_id', how='left'))
        
        # df[['eia_id']].info()
        # print(df.groupby(['utility'])['number_of_adopters'].sum())
        print(df.groupby(['year'])['number_of_adopters'].sum())
        # print(df.groupby(['utility', 'county'])['number_of_adopters'].sum())

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