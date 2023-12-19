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

#* This has the alternate scenario data:
scen_data = r'V:\Projects\Avista DER Forecast\Output\dgen_inputs_for_review_JS.xlsx'

input_data_path = Path(r'C:\Users\collin\collin_git\dgen_avista\dgen_os\input_data')

'''
Battery Prices
Battery Prices
Battery Prices
'''
# "C:\Users\collin\collin_git\dgen_avista\dgen_os\input_data\batt_prices\batt_prices_FY20_mid.csv"
orig_batt_prices = pd.read_csv(input_data_path / 'batt_prices' / 'batt_prices_FY20_mid.csv')
print('Original Battery Prices:')
print(orig_batt_prices.info())

alt_batt_prices = pd.read_excel(scen_data, sheet_name='batt_prices_FY20_mid_JS', engine='openpyxl')
# alt_batt_prices = alt_batt_prices.astype(orig_batt_prices.dtypes.to_dict())
print('Alternate Battery Prices:')
print(alt_batt_prices.info())
# print(alt_batt_prices.head())

batt_cols = [col for col in orig_batt_prices.columns if 'batt_' in col]

changed_cols = []

for c in batt_cols:
    deltas = (orig_batt_prices[['year', c]]
              .merge(alt_batt_prices[['year', c]], 
                     on='year', 
                     how='left', 
                     suffixes=('_orig', '_alt'))
              .assign(delta=lambda x: x[c + '_alt'] - x[c + '_orig']))
    if deltas['delta'].sum() != 0:
        changed_cols.append(c)
        print(c, deltas['delta'].sum())

print(changed_cols)

new_batt_prices = (orig_batt_prices.drop(columns=changed_cols)
                    .merge(alt_batt_prices[['year'] + changed_cols], 
                           on='year', 
                           how='left'))
for x in orig_batt_prices.columns:
    new_batt_prices[x] = new_batt_prices[x].astype(orig_batt_prices[x].dtypes.name)

new_batt_prices = new_batt_prices[orig_batt_prices.columns]

print('New Battery Prices:')
print(new_batt_prices.info())
print(new_batt_prices.head())
print('Original Battery Prices:')
print(orig_batt_prices.info())
print(orig_batt_prices.head())

new_batt_prices.to_csv(input_data_path / 'batt_prices' / 'batt_prices_FY20_mid_verdant.csv')


