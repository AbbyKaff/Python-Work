# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 12:30:25 2023

@author: akaff
"""

import pandas as pd
import numpy as np
import datetime
import xlrd

xx = pd.to_datetime('4/3/2023 09:30a')
xx.tzinfo
xx.dt.tz_localize('UTC').dt.tz_convert('Central')

xx = pd.DataFrame({'Start':['4/3/2023 09:30a','4/5/2023 09:30a','4/6/2023 03:30p','4/7/2023 09:30p'],
                    'End':['4/3/2023 11:30a','4/5/2023 02:30p','4/6/2023 11:30p','4/7/2023 10:30p']})

xx.Start = pd.to_datetime(xx.Start)
xx.End = pd.to_datetime(xx.End)
xx.Start = xx.Start.dt.tz_localize('UTC').dt.tz_convert('US/Arizona')
xx.Start = xx.Start.astype(str)
xx.Start.str.replace(' ','T')



df_TimeEntries = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Parallel\Cycle 1\time_entries_hrlyees.csv")
dfc_TimeEntries = df_TimeEntries
#dfc_TimeEntries = df_TimeEntries[df_TimeEntries['Employee EIN'] =='KBP Foods']
dfc_TimeEntries = dfc_TimeEntries[dfc_TimeEntries['Is Time Off'] != 'Y']
#dfc_TimeEntries = dfc_TimeEntries[dfc_TimeEntries['Employee Id'] == 166598]
#pd.to_datetime(day1['time'], format='%H:%M').dt.time
#dfc_TimeEntries['End'] = dfc_TimeEntries['End'].str.replace('a','A')
#dfc_TimeEntries['End'] = dfc_TimeEntries['End'].str.replace('p','P')
#dfc_TimeEntries['End'] = dfc_TimeEntries['End']+'M'
#dfc_TimeEntries['End'] = dfc_TimeEntries['End'].str.replace('-M','00:00AM')
#d='10:23:34 PM'
#pd.to_datetime(d).strftime('%H:%M:%S')

#dfc_TimeEntries['End1'] = pd.to_datetime(dfc_TimeEntries['End']).dt.strftime('%H:%M')
#dfc_TimeEntries['Date'] = pd.to_datetime(dfc_TimeEntries['Date']).dt.date
dfc_TimeEntries['In Date Time'] = dfc_TimeEntries['Date'] + ' ' + dfc_TimeEntries['Start']
dfc_TimeEntries['Out Date Time'] = dfc_TimeEntries['Date2'] + ' ' + dfc_TimeEntries['End']


#dfc_TimeEntries['In Date Time'] = np.where(dfc_TimeEntries['End1'] > '00:01' & dfc_TimeEntries['End1'] < '04:15', pd.to_datetime((dfc_TimeEntries['Date'] + ' ' + dfc_TimeEntries['Start'])) + datetime.timedelta(days=1),dfc_TimeEntries['Date'] + ' ' + dfc_TimeEntries['Start'])


dfc_TimeEntries['In Date Time'] = pd.to_datetime(dfc_TimeEntries['In Date Time'])
dfc_TimeEntries['Out Date Time'] = pd.to_datetime(dfc_TimeEntries['Out Date Time'])
dfc_TimeEntries['Time Zone'] = np.where(dfc_TimeEntries['Time Zone'] == 'Greenwich Mean Time','US/Arizona','US/' + dfc_TimeEntries['Time Zone'])
#dfc_TimeEntries['In DT'] = dfc_TimeEntries['In Date Time'].dt.tz_localize('UTC').dt.tz_convert(dfc_TimeEntries['Time Zone'])

dfc_TimeEntries['Date In'] = (dfc_TimeEntries.apply(lambda row: row["In Date Time"].tz_localize(tz=row['Time Zone']).tz_convert(row['Time Zone']), axis=1))

dfc_TimeEntries['Date Out'] = (dfc_TimeEntries.apply(lambda row: row["Out Date Time"].tz_localize(tz=row['Time Zone']).tz_convert(row['Time Zone']), axis=1))

dfc_TimeEntries['Date In'] = dfc_TimeEntries['Date In'].astype(str)
dfc_TimeEntries['Date In'] = dfc_TimeEntries['Date In'].str.replace(' ','T')

dfc_TimeEntries['Date Out'] = dfc_TimeEntries['Date Out'].astype(str)
dfc_TimeEntries['Date Out'] = dfc_TimeEntries['Date Out'].str.replace(' ','T')

dfc_TimeEntries.to_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Parallel\Cycle 1\time_entries_hrlyees_utc2.csv")


df_TimeEntries = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Parallel\Cycle 1\Copy of NYFQPayrollParallelCycle1.04.03.csv")

dfc_TimeEntries = df_TimeEntries[df_TimeEntries['Employee EIN'] =='KBP Foods']
dfc_TimeEntries = dfc_TimeEntries[dfc_TimeEntries['Is Time Off'] != 'Y']
#dfc_TimeEntries = dfc_TimeEntries[dfc_TimeEntries['Employee Id'] == 166598]

dfc_TimeEntries['In Date Time'] = dfc_TimeEntries['Date'] + ' ' + dfc_TimeEntries['Start']
dfc_TimeEntries['Out Date Time'] = dfc_TimeEntries['Date'] + ' ' + dfc_TimeEntries['End']
dfc_TimeEntries['In Date Time'] = pd.to_datetime(dfc_TimeEntries['In Date Time'])
dfc_TimeEntries['Out Date Time'] = pd.to_datetime(dfc_TimeEntries['Out Date Time'])
dfc_TimeEntries['Time Zone'] = np.where(dfc_TimeEntries['Time Zone'] == 'Greenwich Mean Time','US/Arizona','US/' + dfc_TimeEntries['Time Zone'])
#dfc_TimeEntries['In DT'] = dfc_TimeEntries['In Date Time'].dt.tz_localize('UTC').dt.tz_convert(dfc_TimeEntries['Time Zone'])

dfc_TimeEntries['Date In'] = (dfc_TimeEntries.apply(lambda row: row["In Date Time"].tz_localize(tz=row['Time Zone']).tz_convert(row['Time Zone']), axis=1))

dfc_TimeEntries['Date Out'] = (dfc_TimeEntries.apply(lambda row: row["Out Date Time"].tz_localize(tz=row['Time Zone']).tz_convert(row['Time Zone']), axis=1))

dfc_TimeEntries['Date In'] = dfc_TimeEntries['Date In'].astype(str)
dfc_TimeEntries['Date In'] = dfc_TimeEntries['Date In'].str.replace(' ','T')

dfc_TimeEntries['Date Out'] = dfc_TimeEntries['Date Out'].astype(str)
dfc_TimeEntries['Date Out'] = dfc_TimeEntries['Date Out'].str.replace(' ','T')

dfc_TimeEntries.to_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Parallel\Cycle 1\nyfqpayroll_utc.csv")
