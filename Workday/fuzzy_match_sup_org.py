import pandas as pd
from fuzzywuzzy import fuzz
import glob as glob

import config
from common import (write_to_csv, active_workers, open_as_utf8, modify_amount)

# system of record
df_contact_sheet = pd.read_excel(config.PATH_WD_IMP + 'data files\\Contact Sheet PRELIM eff 04.10.2023.xlsx', dtype='object', sheet_name='All Stores - FQSR')

print(df_contact_sheet)

# kronos
df_kronos = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', encoding='cp1251')

print(df_kronos)

df_contact_sheet['MP'] = df_contact_sheet['MP'].astype(str).str.replace('[^a-zA-Z]', '')
df_kronos['Employee Name'] = df_kronos['Employee Name'].astype(str).str.replace('[^a-zA-Z]', '')

df = df_kronos.merge(df_contact_sheet, how='cross')
df['r'] = df.apply(lambda x: fuzz.ratio(x['Employee Name'], x['MP']), axis=1)

s = df[df['r'] >= 90].sort_values('r', ascending=False).drop_duplicates(subset=['Employee Name']).set_index('Employee Name')['MP']

df_kronos['Employee Name_new'] = df_kronos['Employee Name'].map(s).fillna(df_kronos['Employee Name'])
