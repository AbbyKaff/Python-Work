# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 14:33:25 2023

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
path = (r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files')

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\jch')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])
#combined_csv.to_csv(r'./../jch_all.csv')

cut_off_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\71877725.csv")
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\worker_id_fix.csv')
combined_csv['Employee Id'] = combined_csv['Employee Id'].astype(str)
combined_csv = combined_csv.loc[combined_csv['Employee Id'].isin(cut_off_ees['Employee Id'].astype(str))]

#compenstation
comp_his = combined_csv.dropna(subset=['Action'])
comp_his = comp_his[comp_his['Account Status']=='Active']
comp_his = comp_his[~comp_his['Action'].str.contains('test')]
comp_his = comp_his[(comp_his['Action'].str.contains("Pay")) | (comp_his['Action'].str.contains("Salary"))]
comp_his['Effective Date'] = pd.to_datetime(comp_his['Effective Date'])
comp_his.sort_values(by=['Employee Id'],inplace=True)
comp_his.sort_values(by=['Effective Date'],inplace=True)
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
comp_his['Amount'] = comp_his['Amount'].str.replace('-','')
comp_his['Currency Code'] = 'USD'
comp_his['Frequency'] = 'Annual'
comp_his['Amount Change'] = ''
comp_his = comp_his[comp_his['Amount'] != '']


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

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files')
comp_his_export.to_csv('fix_over400k.csv')

#fixed file
comp_his_export = pd.read_csv('fix_over400k.csv')

os.chdir(path)
comp_his_export.to_csv('comp_hist_prev_sys.txt',sep='|', encoding='utf-8', index=False)


#EMP BASE COMP
jan = pd.to_datetime('1/1/2023')
comp_his['Effective Date'] = pd.to_datetime(comp_his['Effective Date'])
comp_his2 = comp_his[comp_his['Effective Date'] < jan]
comp_his_23 = comp_his[comp_his['Effective Date'] > jan] 
com_maxdt = comp_his2.groupby('Worker ID')['Effective Date'].max()
com_maxdt = com_maxdt.reset_index()
com_maxdt = com_maxdt.drop_duplicates()
comp_his2 = comp_his2.merge(com_maxdt, on=['Worker ID','Effective Date'])
comp_his2 = comp_his2[['Worker ID','Effective Date','Amount','Frequency','Reason','Jobs (HR)']]
comp_his2.drop_duplicates(inplace=True)
comp_his2['Effective Date'] = jan
comp_his_23 = comp_his_23[['Worker ID','Effective Date','Amount','Frequency','Reason','Jobs (HR)']]
comp_his_23 = comp_his_23.drop_duplicates()

base_comp = pd.concat([comp_his2, comp_his_23])
base_comp['T'] = base_comp.groupby('Worker ID')['Worker ID'].transform('count')
base_comp['No'] = (base_comp.groupby(['Worker ID'])['Worker ID'].cumcount() + 1)

comp_grades = pd.read_csv(config.PATH_WD_IMP + "data files\comp_grades.csv", encoding="cp1251")

xxx = base_comp.merge(comp_grades, left_on='Jobs (HR)',right_on='Job Title',how='left')

base_comp.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\fullbase.csv')
