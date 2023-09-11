# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 14:41:49 2023

@author: akaff
"""

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd
import glob
import os

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)
path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files'
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\jch')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])
#combined_csv.to_csv(r'./../jch_all.csv')

cut_off_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_worker_address.txt", sep='|')
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\worker_id_fix.csv')
combined_csv['Employee Id'] = combined_csv['Employee Id'].astype(str)
combined_csv = combined_csv.loc[combined_csv['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]


#job change
job_change = combined_csv.dropna(subset=['Action'])

#remove testers
job_change = job_change[pd.to_numeric(job_change['Employee Id'], errors='coerce').notnull()]
job_change['Effective Date'] = pd.to_datetime(job_change['Effective Date'])
job_change.sort_values(by='Employee Id',inplace=True)
job_change.sort_values(by='Effective Date',inplace=True)
job_change.drop_duplicates(inplace=True)
job_change['T'] = job_change.groupby('Employee Id')['Employee Id'].transform('count')
job_change['No'] = job_change.groupby('Employee Id')['Employee Id'].cumcount() + 1

job_change['Worker ID'] = job_change['Employee Id']
job_change['Source System'] = 'Kronos'
job_change['Sequence ID'] = job_change['No']
job_change['Effective Date'] = np.where(job_change['Effective Date'] == '1900-12-31',job_change['Created'],job_change['Effective Date'])
job_change['Effective Date'] = pd.to_datetime(job_change['Effective Date']).dt.strftime("%d-%b-%Y").str.upper()
job_change['Reason'] = np.where(job_change['Job Change Reason Code'].isna(),job_change['Action'],job_change['Job Change Reason Code'])
job_change['Annual Amount'] = job_change['Annual Amount'].str.replace('$','')
job_change['Annual Amount'] = job_change['Annual Amount'].str.replace(',','')
job_change['Start Date'] = ''
job_change['End Date'] = ''

jch = job_change[['Worker ID',
'Source System',
'Sequence ID',
'Effective Date',
'Reason',
'Start Date',
'End Date',
'Employee EIN',
'Action',
'Jobs (HR)',
'Default Cost Centers',
'Employee Type',
'Annual Amount',
'Pay Grade',
'Account Status',
'Created']]

os.chdir(path)
jch.to_csv('PROD_job_hist_prev_sys_082423.txt',sep='|', encoding='utf-8', index=False)

jch = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_job_hist_prev_sysfix.csv")
jch['Effective Date'] = pd.to_datetime(jch['Effective Date']).dt.strftime("%d-%b-%Y").str.upper()
jch = jch[jch['Worker ID'].isin([158494,25330])]
