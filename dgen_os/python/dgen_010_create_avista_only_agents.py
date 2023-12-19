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

source_agent_dir = Path(r'V:\Projects\Avista DER Forecast\DGen\agent_files')

#* utility      eia_id
#* Avista Corp  20169     39

#* Pull the county mapping table from the database:
with sqla_pg_con(pg_creds, 'dgen_db').connect() as conn:
    county_map = sqlio.read_sql_query(f'''SELECT county_id, county FROM diffusion_shared.county_geoms;''', conn)

# print(county_map.head())
#* EIA IDs come from Greg's source:
eia_ids = pd.read_csv(Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\python\eia_id_mapping.csv'), dtype={'Utility Number': str})

eia_id_dict = eia_ids.set_index('Utility Number')['Utility Name'].to_dict()

res_agents = (pd.read_pickle(source_agent_dir / r'ALL_UTILS_agent_df_base_res_wa_revised.pkl'))
res_agents = res_agents.query('eia_id == "20169"')

print('After Subsetting:')
print(res_agents.head())

'''
res_agents = (res_agents.assign(utility=lambda x: x['eia_id'].map(eia_id_dict))
                .merge(county_map, on='county_id', how='left')
                #* Remove all non-Avista agents:
                .query('utility == "Avista Corp"'))
                # .drop(columns=['utility', 'county']))
print(res_agents.info())
print(res_agents.groupby(['utility', 'eia_id']).size())
'''
            
res_agents.to_pickle(source_agent_dir / r'agent_df_base_res_wa_avista_revised.pkl')#* Drop the utility column, since it's all Avista now.
# print(res_agents.utility.unique())



res_agents = (pd.read_pickle(source_agent_dir / r'ALL_UTILS_agent_df_base_res_wa_revised.pkl'))
res_agents = res_agents.query('eia_id == "20169"')

print('After Subsetting:')
print(res_agents.head())

'''
res_agents = (res_agents.assign(utility=lambda x: x['eia_id'].map(eia_id_dict))
                .merge(county_map, on='county_id', how='left')
                #* Remove all non-Avista agents:
                .query('utility == "Avista Corp"'))
                # .drop(columns=['utility', 'county']))
print(res_agents.info())
print(res_agents.groupby(['utility', 'eia_id']).size())
'''
            
res_agents.to_pickle(source_agent_dir / r'agent_df_base_res_wa_avista_revised.pkl')#* Drop the utility column, since it's all Avista now.
# print(res_agents.utility.unique())



# df = pd.read_pickle(source_agent_dir / r'agent_df_base_res_wa_avista_revised.pkl')
# # print(df.info())
# print('After re-import of pickle:')
# print(df.head())

res_agents = (pd.read_pickle(source_agent_dir / r'ALL_UTILS_agent_df_base_res_wa_revised.pkl'))
res_agents = res_agents.query('eia_id == "20169"')

print('After Subsetting:')
print(res_agents.head())

'''
res_agents = (res_agents.assign(utility=lambda x: x['eia_id'].map(eia_id_dict))
                .merge(county_map, on='county_id', how='left')
                #* Remove all non-Avista agents:
                .query('utility == "Avista Corp"'))
                # .drop(columns=['utility', 'county']))
print(res_agents.info())
print(res_agents.groupby(['utility', 'eia_id']).size())
'''
            
res_agents.to_pickle(source_agent_dir / r'agent_df_base_res_wa_avista_revised.pkl')#* Drop the utility column, since it's all Avista now.
# print(res_agents.utility.unique())



com_agents = (pd.read_pickle(source_agent_dir / r'ALL_UTILS_agent_df_base_com_wa_revised.pkl'))
com_agents = res_agents.query('eia_id == "20169"')

print('After Subsetting:')
print(com_agents.head())

            
com_agents.to_pickle(source_agent_dir / r'agent_df_base_com_wa_avista_revised.pkl')#* Drop the utility column, since it's all Avista now.

