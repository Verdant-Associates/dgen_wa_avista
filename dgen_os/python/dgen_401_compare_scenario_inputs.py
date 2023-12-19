from pathlib import Path
import json 
import sqlalchemy
import psycopg2 
import sys 
import os
import pandas.io.sql as sqlio
import settings
import pandas as pd
from dgen_000_setup import * 
import pandas as pd #* Dgen is Pandas, so for now we'll use Pandas for everything.
print(sys.executable)

batt_price_dir = Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\input_data\batt_prices')
print(batt_price_dir.is_dir())

price_files = batt_price_dir.glob('*.csv')

all_prices = pd.DataFrame()
for pf in price_files:
    print(pf)
    df = pd.read_csv(pf)
    df['filename'] = pf.name
    all_prices = pd.concat([all_prices, df])

print(all_prices.info())

for col in [c for c in all_prices.columns if 'batt_capex' in c]:
    print(all_prices.groupby('filename')[col].mean())

'''

#* List of inputs... 
input_dict = {  'Retail Electricity Price Escalation Scenario': 'ATB19_Mid_Case_retail',
                'Wholesale Electricity Price Scenario': 'ATB19_Mid_Case_wholesale',
                'PV Price Scenario': 'pv_price_atb19_mid',
                'PV Technical Performance Scenario': 'pv_tech_performance_defaultFY19',
                'Storage Cost Scenario': 'batt_prices_FY20_mid',
                'Storage Technical Performance Scenario': 'batt_tech_performance_SunLamp17',
                'PV + Storage Cost Scenario': 'pv_plus_batt_prices_FY20_mid',
                'Financing Scenario': 'financing_atb_FY19',
                'Depreciation Scenario': 'deprec_sch_FY19',
                'Value of Resiliency Scenario': 'vor_FY20_mid',
                'Carbon Intensity Scenario': 'carbon_intensities_FY19'}

#* Can we find where they are?

dgen_dir = Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os')

all_files = []
for root, directories, filenames in os.walk(dgen_dir):
    all_files.extend([os.path.join(root, x) for x in filenames])


input_files = []
for f in all_files:
    for i in input_dict.values():
        if i in f:
            print(i, f)
            input_files.append(f)





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

'''