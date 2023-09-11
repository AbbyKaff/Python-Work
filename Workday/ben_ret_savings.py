# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 08:57:35 2023

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
from datetime import timedelta
import xlrd
import glob as glob

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\benefits_dc'
##############################################################################
#################### Retirement Savings Elections ############################
##############################################################################

'''active_bene_r = get_rpt('71567958')
active_bene_r = active_bene_r[active_bene_r['Deduction Code'] != '409ABN']

active_bene_r['Employee ID'] = active_bene_r['Employee Id'].astype(int)
active_bene_r['Employee ID'] = active_bene_r['Employee ID'].astype(str)
active_bene_r['Source System'] = 'Kronos'
active_bene_r['Event Date'] = pd.to_datetime(active_bene_r['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
active_bene_r['Benefit Event Type'] = 'Conversion_Retirement_Savings'
'''
"""
'409ABN'
'401K' : USA - 401(k) - John Hancock
'401KR' : USA - 401(k) Roth - John Hancock
"""
active_bene_r = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\71567958.csv")
#active_bene_r['Retirement Savings Plan'] = np.where(active_bene_r['Deduction Code'] == '401KR','USA - 401(k) Roth - John Hancock','USA - 401(k) - John Hancock')

#------------------------------------------------------------------------------
cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\worker_id_fix.csv')
active_bene_r['Employee Id'] = active_bene_r['Employee Id'].astype(str)
active_bene_r = active_bene_r.loc[active_bene_r['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
#----------------------------------------------------------------------------


active_bene_r['Employee ID'] = active_bene_r['Employee Id'].astype(str)
active_bene_r['Election Percentage'] = active_bene_r['EE Percent'].str.replace("%","")
active_bene_r['Election Amount'] = active_bene_r['EE Amount'].str.replace('-','0')
active_bene_r['Election Percentage'] = active_bene_r['Election Percentage'].str.replace('-','0')
active_bene_r['Election Percentage'] = active_bene_r['Election Percentage'].fillna(0)

#active_bene_r['Event Date'] = np.where(pd.to_datetime(active_bene_r['Begin Date']) <= '2023-01-01', '01-JAN-2023', pd.to_datetime(active_bene_r['Begin Date']).dt.strftime("%d-%b-%Y").str.upper())
active_bene_r['Event Date'] = active_bene_r['Begin Date']
active_bene_r['Source System'] = 'Kronos'
active_bene_r['Benefit Event Type'] = 'BEN_CONVERSION_RETSAVINGS'
active_bene_r['Retirement Savings Plan'] = np.where(active_bene_r['Deduction Code'] == '401KR','USA - 401(k) Roth - Principal',
                                           np.where(active_bene_r['Deduction Code'] == '401K','USA - 401(k) - Principal',
                                           np.where(active_bene_r['Deduction Code'] == '409ABN','Deferred Compensation - 409A Bonus - Principal',
                                           np.where(active_bene_r['Deduction Code'] == '409ABS','Deferred Compensation - 409A Base - Principal',
                                           np.where(active_bene_r['Deduction Code'] == '409REF','Deferred Compensation - 409A Excess - Principal','NA')))))


#No beneficiares
#dep_ff = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\dependents.csv', dtype='object', encoding='cp1251')
#dep_ff.drop_duplicates(inplace=True)
#dep_ff['Employee ID'] = dep_ff['Employee ID'].astype(str)
#dep_ff = dep_ff[['Employee ID','Beneficiary ID']]
#dep_ff = dep_ff.dropna(subset=['Beneficiary ID'])


#active_bene_r = active_bene_r.merge(dep_ff, on='Employee ID',how='left')
active_bene_r['Beneficiary ID - Beneficiary Allocation'] = ''
active_bene_r['Retirement Savings Plan - Benefits Provider Allocation'] = ''
active_bene_r['Primary Percentage - Beneficiary Allocation'] = ''
active_bene_r['Contingent Percentage - Beneficiary Allocation'] = ''
active_bene_r['Original Coverage Begin Date'] = ''
active_bene_r['Deduction Begin Date'] = ''
active_bene_r['Employee Contribution Allocation Percent - Benefits Provider Allocation'] = ''
active_bene_r['Employer Contribution Allocation Percent'] = ''
active_bene_r['Beneficiary ID - Benefits Provider Allocation'] = ''
active_bene_r['Primary Percentage - Benefits Provider Allocation'] = ''
active_bene_r['Contingent Percentage - Benefits Provider Allocation'] = ''
active_bene_r['Enrollment Signature Date'] = ''
active_bene_r['Signing Worker'] = ''

# TODO filter out 0s (KBP-4551)


re_final = active_bene_r[['Employee ID',
'Source System',
'Event Date',
'Benefit Event Type',
'Retirement Savings Plan',
'Election Percentage',
'Election Amount',
'Beneficiary ID - Beneficiary Allocation',
'Primary Percentage - Beneficiary Allocation',
'Contingent Percentage - Beneficiary Allocation',
'Original Coverage Begin Date',
'Deduction Begin Date',
'Retirement Savings Plan - Benefits Provider Allocation',
'Employee Contribution Allocation Percent - Benefits Provider Allocation',
'Employer Contribution Allocation Percent',
'Beneficiary ID - Benefits Provider Allocation',
'Primary Percentage - Benefits Provider Allocation',
'Contingent Percentage - Benefits Provider Allocation',
'Enrollment Signature Date',
'Signing Worker']]

re_final.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\ret_savings_elections.csv')

os.chdir(path)
re_final.to_csv('ret_savings_elections.txt',sep='|', encoding='utf-8', index=False)