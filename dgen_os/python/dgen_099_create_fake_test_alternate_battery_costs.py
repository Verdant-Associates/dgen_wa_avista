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

batt_cols = [col for col in orig_batt_prices.columns if 'capex' in col]

new_batt_prices = orig_batt_prices.copy()

for c in batt_cols:
    print(c)
    new_batt_prices[c] = orig_batt_prices[c] * 0.7
    
for x in orig_batt_prices.columns:
    new_batt_prices[x] = new_batt_prices[x].astype(orig_batt_prices[x].dtypes.name)

new_batt_prices = new_batt_prices[orig_batt_prices.columns]

print('New Battery Prices:')
print(new_batt_prices.info())
print(new_batt_prices.head())
print('Original Battery Prices:')
print(orig_batt_prices.info())
print(orig_batt_prices.head())

new_batt_prices.to_csv(input_data_path / 'batt_prices' / 'batt_prices_fake_70_pct_verdant.csv')


