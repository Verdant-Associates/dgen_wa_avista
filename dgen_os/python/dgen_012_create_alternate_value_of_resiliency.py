from pathlib import Path
import json 
import sqlalchemy
import psycopg2 
import sys 
import os
import pandas.io.sql as sqlio
import settings
import pandas as pd
import numpy as np
from dgen_000_setup import * 
import pandas as pd #* Dgen is Pandas, so for now we'll use Pandas for everything.
print(sys.executable)

#* This has the alternate scenario data:
scen_data = r'V:\Projects\Avista DER Forecast\Output\dgen_inputs_for_review_JS.xlsx'

input_data_path = Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\input_data')

'''
Value of Resilience
Value of Resilience
Value of Resilience
'''

orig_vor = pd.read_csv(input_data_path / 'value_of_resiliency' / 'vor_FY20_mid.csv')
alt_vor = (pd.read_excel(scen_data, sheet_name='vor_FY20_mid_JS', engine='openpyxl'))

scenarios = alt_vor.query('verdant_scenario.notnull()')['verdant_scenario'].unique().tolist()
print('New VOR:')
print(alt_vor.info())
# print(alt_vor.head())
print('Original VOR:')
print(orig_vor.info())
# print(orig_vor.head())

vor_cols = [col for col in orig_vor.columns if col not in ['sector', 'sector_abbr', 'state_abbr']]
print(vor_cols)

print(scenarios)
scen_abbrs = {'NREL Original': 'nrel_orig',
              'High CA Based Case ($10/hour for 38 hours)': 'verdant_high',
              'Mid Case ($5/hour for 15 hours)': 'verdant_mid'}




for scen in scenarios:
    alt_vor_scen = alt_vor[alt_vor['verdant_scenario'] == scen]
    changed_cols = []

    for c in vor_cols:
        deltas = (orig_vor[['sector', 'sector_abbr', 'state_abbr', c]]
                .merge(alt_vor_scen[['sector', 'sector_abbr', 'state_abbr', c]], 
                        on=['sector', 'sector_abbr', 'state_abbr'], 
                        how='inner', 
                        suffixes=('_orig', '_alt'))
                .assign(delta=lambda x: x[c + '_alt'] - x[c + '_orig']))
        if deltas['delta'].sum() != 0:
            changed_cols.append(c)
            print(c, deltas['delta'].sum())

    new_vor = (orig_vor.merge(alt_vor_scen[['sector', 'sector_abbr', 'state_abbr'] + changed_cols], 
                            on=['sector', 'sector_abbr', 'state_abbr'], 
                            how='left', 
                            indicator=True, 
                            suffixes=('_orig', '_alt')))
    print('New VOR:')
    print(new_vor.info())

    for c in changed_cols:
        #* If it merged on a value, take the alternative value, otherwise take the original value.
        new_vor[c] = np.where(new_vor['_merge'] == 'both', new_vor[f'{c}_alt'], new_vor[f'{c}_orig'])


    for x in orig_vor.columns:
        new_vor[x] = new_vor[x].astype(orig_vor[x].dtypes.name)


    new_vor = new_vor.drop(columns=['_merge'] + [f'{c}_orig' for c in changed_cols] + [f'{c}_alt' for c in changed_cols])
    new_vor = new_vor[orig_vor.columns]
    print(scen)
    print(changed_cols)
    print('New VOR:')
    print(new_vor.info())
    print('Original VOR:')
    print(orig_vor.info())
    scen_clean = scen_abbrs[scen]
    print(scen, scen_clean)
    new_vor.to_csv(input_data_path / 'value_of_resiliency' / f'vor_FY20_{scen_clean}_verdant.csv')
