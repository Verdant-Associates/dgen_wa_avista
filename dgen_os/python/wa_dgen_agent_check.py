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

#* Pull the county mapping table from the database:
with sqla_pg_con(pg_creds, 'dgen_db').connect() as conn:
    county_map = sqlio.read_sql_query(f'''SELECT county_id, county FROM diffusion_shared.county_geoms;''', conn)

#* EIA IDs come from Greg's source:
eia_ids = pd.read_csv(Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\python\eia_id_mapping.csv'), dtype={'Utility Number': str})

eia_id_dict = eia_ids.set_index('Utility Number')['Utility Name'].to_dict()

res_agents = (pd.read_pickle(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\input_agents\agent_df_base_res_wa_revised.pkl')
                .assign(utility=lambda x: x['eia_id'].map(eia_id_dict))
                .merge(county_map, on='county_id', how='left'))


com_agents = (pd.read_pickle(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\input_agents\agent_df_base_com_wa_revised.pkl')
                .assign(utility=lambda x: x['eia_id'].map(eia_id_dict))
                .merge(county_map, on='county_id', how='left'))

all_agents = pd.concat([res_agents.assign(sector='res'), com_agents.assign(sector='com')])
print(all_agents.head())

print(pd.pivot(all_agents.groupby(['eia_id', 'utility', 'sector'], dropna=False)['bldg_id'].nunique().reset_index(),
                index=['utility', 'eia_id'], 
                columns='sector', 
                values='bldg_id'))

all_agents.query('utility=="Avista Corp"').to_csv(output_root / 'agent_example.csv')



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

for f in input_files:
    