# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 09:55:56 2023

@author: akaff
"""

import pandas as pd
import numpy as np
import os
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
from api_call2 import get_rpt
import datetime
from datetime import date
from datetime import datetime
import xlrd
import glob as glob

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion')

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc'
os.chdir(path)


##############################################################################
############################ History Files ##################################
##############################################################################
# Get Active EEs 

ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\moved.20230426-192722.E2E_pay_gp_assignments_v2.txt",sep="|")
ees = ees['Employee ID']


os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\jch')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])
#combined_csv.to_csv(r'./../jch_all.csv')

#compenstation
comp_his = combined_csv.dropna(subset=['Action'])
comp_his = comp_his[comp_his['Account Status']=='Active']
comp_his = comp_his[~comp_his['Action'].str.contains('test')]
comp_his = comp_his[comp_his['Action'].str.contains("Pay")]
comp_his['Effective Date'] = pd.to_datetime(comp_his['Effective Date'])
comp_his.sort_values(by=['Employee Id','Effective Date'],inplace=True)
comp_his['T'] = comp_his.groupby('Employee Id')['Employee Id'].transform('count')
#comp_his['T'] = comp_his['Employee Id'].value_counts()
comp_his['No'] = (comp_his.groupby(['Employee Id'])['Employee Id'].cumcount() + 1)
comp_his['Effective Date'] = np.where(comp_his['Effective Date'] == '1900-12-31',comp_his['Created'],comp_his['Effective Date'])
comp_his['Effective Date'] = pd.to_datetime(comp_his['Effective Date']).dt.strftime("%d-%b-%Y").str.upper()

comp_his['Worker ID'] = comp_his['Employee Id']
comp_his['Source System'] = 'Kronos'
comp_his['Sequence ID'] = comp_his['No']
comp_his['Reason'] = np.where(comp_his['Job Change Reason Code'].isna(),'Promotion',comp_his['Job Change Reason Code'])
comp_his['Amount'] = comp_his['Annual Amount'].str.replace('$','')
comp_his['Amount'] = comp_his['Amount'].str.replace(',','')
comp_his['Currency Code'] = 'USD'
comp_his['Frequency'] = 'Annual'
comp_his['Amount Change'] = ''

comp_his['FIELD_1'] = ''
comp_his['FIELD_2'] = ''
comp_his['FIELD_3'] = ''
comp_his['FIELD_4'] = ''
comp_his['FIELD_5'] = ''
comp_his['FIELD_6'] = ''
comp_his['FIELD_7'] = ''
comp_his['FIELD_8'] = ''
comp_his['FIELD_9'] = ''
comp_his['FIELD_10'] = ''
comp_his['FIELD_11'] = ''
comp_his['FIELD_12'] = ''
comp_his['FIELD_13'] = ''
comp_his['FIELD_14'] = ''
comp_his['FIELD_15'] = ''


comp_his_export = comp_his[['Worker ID',
'Source System',
'Sequence ID',
'Effective Date',
'Reason',
'Amount',
'Currency Code',
'Frequency',
'Amount Change',
'FIELD_1',
'FIELD_2',
'FIELD_3',
'FIELD_4',
'FIELD_5',
'FIELD_6',
'FIELD_7',
'FIELD_8',
'FIELD_9',
'FIELD_10',
'FIELD_11',
'FIELD_12',
'FIELD_13',
'FIELD_14',
'FIELD_15']]

comp_his_export = pd.read_csv('test_comphis.csv')
path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc'
os.chdir(path)
comp_his_export.to_csv('E2E_comp_hist_prev_sys_v2.txt',sep='|', encoding='utf-8', index=False)


#job change
job_change = combined_csv.dropna(subset=['Action'])

#remove testers
job_change = job_change[pd.to_numeric(job_change['Employee Id'], errors='coerce').notnull()]
job_change = job_change[job_change['Account Status']=='Active']
job_change['Effective Date'] = pd.to_datetime(job_change['Effective Date'])
job_change.sort_values(by=['Employee Id','Effective Date'],inplace=True)
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

jch = pd.read_csv('jch_test.csv')
os.chdir(path)
jch.to_csv('E2E_job_hist_prev_sys_v2.txt',sep='|', encoding='utf-8', index=False)