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

eia_ids = pd.read_csv(Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\python\nc_eia_map.csv'), dtype={'Utility Number': str})

eia_id_dict = eia_ids.set_index('Utility Number')['Utility Name'].to_dict()

nc_com_agents = (pd.read_pickle(r"C:\Users\collin\Downloads\V1_Agent_Pickles_NREL_Data_Catalog\V1_Agent_Pickles_NREL_Data_Catalog\V1_Agent_Pickles_NREL_Data_Catalog\com_agents\agent_df_base_com_nc_revised.pkl")
                   .assign(utility=lambda x: x['eia_id'].map(eia_id_dict)))

nc_res_agents = (pd.read_pickle(r"C:\Users\collin\Downloads\V1_Agent_Pickles_NREL_Data_Catalog\V1_Agent_Pickles_NREL_Data_Catalog\V1_Agent_Pickles_NREL_Data_Catalog\res_agents\agent_df_base_res_nc_revised.pkl")
                   .assign(utility=lambda x: x['eia_id'].map(eia_id_dict)))

nc_agents = pd.concat([nc_com_agents.assign(sector='com'), nc_res_agents.assign(sector='res')])
print(pd.pivot(nc_agents.groupby(['eia_id', 'utility', 'sector'], dropna=False)['bldg_id'].nunique().reset_index(),
                index=['utility', 'eia_id'], 
                columns='sector', 
                values='bldg_id'))


(pd.pivot(nc_agents.groupby(['eia_id', 'utility', 'sector'], dropna=False)['bldg_id'].nunique().reset_index(),
            index=['utility', 'eia_id'], 
            columns='sector', 
            values='bldg_id')
   .reset_index()
   .to_clipboard(sep='~', index=False))