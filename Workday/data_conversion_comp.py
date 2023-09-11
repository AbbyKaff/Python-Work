# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 06:03:49 2023

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
cut_off_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\worker_phone.txt")

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\comp_dc'
os.chdir(path)
#Single source for populating all data in the HCM Data Conversion Spreadsheet

##############################################################################
################################ OVERVIEW ####################################
##############################################################################


"""
Emp-Base Compenstation -- In scope -- Compensation -- data_conversion_hcm
EMP-Allowance Plans -- 	In Scope
EMP-Unit Allowance Plans	-- In Scope
EMP-Bonus Plans -- 	In Scope
EMP-Calculated Plans --In Scope
EMP-Merit Plans --	In Scope
Eligible Earnings --	In Scope
EMP-Stock Plans --	In Scope
EMP-Period Salary Plans --	In Scope
EMP-One-Time Payments --	In Scope
EMP-Stock Option Grant -- 	In Scope
Comp History Prev Sys -- 	In Scope
EMP-Bonus Payments --	In Scope


"""
##############################################################################
######################### EMP-Allowance Plans ##############################
##############################################################################

"""
Employee ID
Compensation Change Reason
Effective Date
Allowance Plan #1
Currency Code - Allowance Plan #1
Frequency-Allowance Plan #1
Expected End Date - Allowance Plan #1
(Up to 7 allowance plans allowed)
"""

allowance_pl = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Allowance Plans')

allowance_pl_c = allowance_pl.filter(regex='Required')
allowance_data = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Allowance Plans',skiprows=2)

allowance_data_c = allowance_data.iloc[0:0]

df1 = get_rpt('72604619')
df1['Employee ID'] = df1['Employee Id']
df1['Effective Date'] = df1['Begin Date']

#df1.to_csv('test_allowance_plans.csv')
# Transpose data to see all jobs each employee has had
df1['Earning amount'] = df1['Earning Name'] +','+df1['Amount (As Of Today)'] + ',' + df1['Frequency']

grouped_multiple = df1[['Employee ID','Effective Date','Earning amount']]
grouped_multiple['Employee ID'] = grouped_multiple['Employee ID'].astype(int)
grouped_new = grouped_multiple.groupby(['Employee ID', 'Effective Date'], as_index=False).agg({"Earning amount":", ".join})
grouped_final = pd.concat([grouped_new['Employee ID'], grouped_new['Effective Date'], grouped_new['Earning amount'].str.split(', ', expand=True).add_prefix('Subcolumn')], axis=1)

grouped_final['Allowance Plan #1'] = grouped_final['Subcolumn0'].str.split(',').str[0]
grouped_final['Amount - Allowance Plan #1'] = grouped_final['Subcolumn0'].str.split(',').str[1]
grouped_final['Amount - Allowance Plan #1'] = grouped_final['Amount - Allowance Plan #1'].str.replace("$","")
grouped_final['Frequency - Allowance Plan #1'] = grouped_final['Subcolumn0'].str.split(',').str[2]
grouped_final['Currency Code - Allowance Plan #1'] = 'USD'
grouped_final['Percent - Allowance Plan #1'] = ''
grouped_final['Expected End Date - Allowance Plan #1']=''
grouped_final['Reimbursement Start Date - Allowance Plan #1']=''


grouped_final['Allowance Plan #2'] = grouped_final['Subcolumn1'].str.split(',').str[0]
grouped_final['Amount - Allowance Plan #2'] = grouped_final['Subcolumn1'].str.split(',').str[1]
grouped_final['Amount - Allowance Plan #2'] = grouped_final['Amount - Allowance Plan #2'].str.replace("$","")
grouped_final['Frequency - Allowance Plan #2'] = grouped_final['Subcolumn1'].str.split(',').str[2]
grouped_final['Currency Code - Allowance Plan #2'] = 'USD'
grouped_final['Percent - Allowance Plan #2'] = ''
grouped_final['Expected End Date - Allowance Plan #2']=''
grouped_final['Reimbursement Start Date - Allowance Plan #2']=''

grouped_final['Allowance Plan #3'] = grouped_final['Subcolumn2'].str.split(',').str[0]
grouped_final['Amount - Allowance Plan #3'] = grouped_final['Subcolumn2'].str.split(',').str[1]
grouped_final['Amount - Allowance Plan #3'] = grouped_final['Amount - Allowance Plan #3'].str.replace("$","")
grouped_final['Frequency - Allowance Plan #3'] = grouped_final['Subcolumn2'].str.split(',').str[2]
grouped_final['Currency Code - Allowance Plan #3'] = 'USD'
grouped_final['Percent - Allowance Plan #3'] = ''
grouped_final['Expected End Date - Allowance Plan #3']=''
grouped_final['Reimbursement Start Date - Allowance Plan #3']=''

grouped_final['Allowance Plan #4'] = grouped_final['Subcolumn3'].str.split(',').str[0]
grouped_final['Amount - Allowance Plan #4'] = grouped_final['Subcolumn3'].str.split(',').str[1]
grouped_final['Amount - Allowance Plan #4'] = grouped_final['Amount - Allowance Plan #4'].str.replace("$","")
grouped_final['Frequency - Allowance Plan #4'] = grouped_final['Subcolumn3'].str.split(',').str[2]
grouped_final['Currency Code - Allowance Plan #4'] = 'USD'
grouped_final['Percent - Allowance Plan #4'] = ''
grouped_final['Expected End Date - Allowance Plan #4']=''
grouped_final['Reimbursement Start Date - Allowance Plan #4']=''


grouped_final['Allowance Plan #5']=''
grouped_final['Percent - Allowance Plan #5']=''
grouped_final['Amount - Allowance Plan #5']=''
grouped_final['Currency Code - Allowance Plan #5']=''
grouped_final['Frequency - Allowance Plan #5']=''
grouped_final['Expected End Date - Allowance Plan #5']=''
grouped_final['Reimbursement Start Date - Allowance Plan #5']=''
grouped_final['Allowance Plan #6']=''
grouped_final['Percent - Allowance Plan #6']=''
grouped_final['Amount - Allowance Plan #6']=''
grouped_final['Currency Code - Allowance Plan #6']=''
grouped_final['Frequency - Allowance Plan #6']=''
grouped_final['Expected End Date - Allowance Plan #6']=''
grouped_final['Reimbursement Start Date - Allowance Plan #6']=''
grouped_final['Allowance Plan #7']=''
grouped_final['Percent - Allowance Plan #7']=''
grouped_final['Amount - Allowance Plan #7']=''
grouped_final['Currency Code - Allowance Plan #7']=''
grouped_final['Frequency - Allowance Plan #7']=''
grouped_final['Expected End Date - Allowance Plan #7']=''
grouped_final['Reimbursement Start Date - Allowance Plan #7']=''
grouped_final['Allowance Plan #8']=''
grouped_final['Percent - Allowance Plan #8']=''
grouped_final['Amount - Allowance Plan #8']=''
grouped_final['Currency Code - Allowance Plan #8']=''
grouped_final['Frequency - Allowance Plan #8']=''
grouped_final['Expected End Date - Allowance Plan #8']=''
grouped_final['Reimbursement Start Date - Allowance Plan #8']=''


grouped_final['Effective Date'] = pd.to_datetime(grouped_final['Effective Date'])

grouped_final['Effective Date'] = grouped_final['Effective Date'].dt.strftime("%d-%b-%Y").str.upper()
grouped_final['Source System'] = 'Kronos'

grouped_final['Sequence #'] = grouped_final.groupby('Employee ID')['Employee ID'].cumcount() + 1

grouped_final['Compensation Change Reason'] = 'Request_Compensation_Change_Conversion_Conversion'
grouped_final['Position ID'] = ''
grouped_final['Remove All Plan Assignments?'] = "N"

allow = grouped_final[['Employee ID', 'Source System', 'Sequence #',
       'Compensation Change Reason', 'Position ID', 'Effective Date',
       'Remove All Plan Assignments?', 'Allowance Plan #1',
       'Percent - Allowance Plan #1', 'Amount - Allowance Plan #1',
       'Currency Code - Allowance Plan #1', 'Frequency - Allowance Plan #1',
       'Expected End Date - Allowance Plan #1',
       'Reimbursement Start Date - Allowance Plan #1', 'Allowance Plan #2',
       'Percent - Allowance Plan #2', 'Amount - Allowance Plan #2',
       'Currency Code - Allowance Plan #2', 'Frequency - Allowance Plan #2',
       'Expected End Date - Allowance Plan #2',
       'Reimbursement Start Date - Allowance Plan #2', 'Allowance Plan #3',
       'Percent - Allowance Plan #3', 'Amount - Allowance Plan #3',
       'Currency Code - Allowance Plan #3', 'Frequency - Allowance Plan #3',
       'Expected End Date - Allowance Plan #3',
       'Reimbursement Start Date - Allowance Plan #3', 'Allowance Plan #4',
       'Percent - Allowance Plan #4', 'Amount - Allowance Plan #4',
       'Currency Code - Allowance Plan #4', 'Frequency - Allowance Plan #4',
       'Expected End Date - Allowance Plan #4',
       'Reimbursement Start Date - Allowance Plan #4', 'Allowance Plan #5',
       'Percent - Allowance Plan #5', 'Amount - Allowance Plan #5',
       'Currency Code - Allowance Plan #5', 'Frequency - Allowance Plan #5',
       'Expected End Date - Allowance Plan #5',
       'Reimbursement Start Date - Allowance Plan #5', 'Allowance Plan #6',
       'Percent - Allowance Plan #6', 'Amount - Allowance Plan #6',
       'Currency Code - Allowance Plan #6', 'Frequency - Allowance Plan #6',
       'Expected End Date - Allowance Plan #6',
       'Reimbursement Start Date - Allowance Plan #6', 'Allowance Plan #7',
       'Percent - Allowance Plan #7', 'Amount - Allowance Plan #7',
       'Currency Code - Allowance Plan #7', 'Frequency - Allowance Plan #7',
       'Expected End Date - Allowance Plan #7',
       'Reimbursement Start Date - Allowance Plan #7', 'Allowance Plan #8',
       'Percent - Allowance Plan #8', 'Amount - Allowance Plan #8',
       'Currency Code - Allowance Plan #8', 'Frequency - Allowance Plan #8',
       'Expected End Date - Allowance Plan #8',
       'Reimbursement Start Date - Allowance Plan #8']]

os.chdir(path)
allow.to_csv('allowance_plans.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
######################### Unit Allowance Plans ##############################
##############################################################################

"""
Employee ID
Compensation Change Reason
Effective Date
Unit Allowance Plan
Number of Units - Unit Allowance Plan
Currency Code - Unit Allowance Plan
Frequency-Unit Allowance Plan

"""

allowance_unit = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='Unit Allowance Plans')

allowance_unit_c = allowance_unit.filter(regex='Required')
allowance_un_data = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='Unit Allowance Plans',skiprows=2)

allowance_un_data_c = allowance_un_data.iloc[0:0]



os.chdir(path)
#biographic_data.to_csv('biographic_data.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
############################ EMP-Bonus Plans #################################
##############################################################################


"""
Employee ID
Compensation Change Reason
Effective Date
Bonus Plan #1

"""
bonus_plan = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Bonus Plans')

bonus_plan_c = bonus_plan.filter(regex='Required')
bonus_data = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Bonus Plans',skiprows=2)

bonus_data_c = bonus_data.iloc[0:0]



os.chdir(path)
#biographic_data.to_csv('biographic_data.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
######################### EMP-Calculated Plans ###############################
##############################################################################

"""
Employee ID
Compensation Change Reason
Effective Date
Calculated Plan #1
Currency Code - Calculated Plan #1
Frequency - Calculated Plan #1

"""
calc_plan = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Calculated Plans')

calc_plan_c = calc_plan.filter(regex='Required')
calcd_data = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Calculated Plans',skiprows=2)

calcd_data_c = calcd_data.iloc[0:0]



os.chdir(path)
#biographic_data.to_csv('biographic_data.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
############################ EMP-Merit Plans #################################
##############################################################################

"""
Employee ID
Compensataion Change Reason
Effective Date
Merit Plan

"""
merit_plan = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Merit Plans')

merit_plan_c = merit_plan.filter(regex='Required')
merit_data = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='EMP-Merit Plans',skiprows=2)

merit_data_c = merit_data.iloc[0:0]


active_ee = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\active_ees.csv")
active_ee = active_ee[active_ee['Job Ranking'] < 45001]
active_ee['Employee ID'] = active_ee['Employee Id']
active_ee['Source System'] = 'Kronos'
active_ee['Sequence #'] = 1
active_ee['Compensation Change Reason'] = 'Request_Compensation_Change_Conversion_Conversion'
active_ee['Position ID'] = ''
active_ee['Effective Date'] = active_ee['Date Hired']
active_ee['Remove All Plan Assignments?'] = ''
active_ee['Merit Plan'] = 'Merit'
active_ee['Individual Target Amount - Merit Plan']=''
active_ee['Individual Target Percent - Merit Plan'] = active_ee['Compa Ratio']
active_ee['Guaranteed Minimum - Merit Plan'] = 'N'


merit = active_ee[['Employee ID',
'Source System',
'Sequence #',
'Compensation Change Reason',
'Position ID',
'Effective Date',
'Remove All Plan Assignments?',
'Merit Plan',
'Individual Target Amount - Merit Plan',
'Individual Target Percent - Merit Plan',
'Guaranteed Minimum - Merit Plan'
]]

os.chdir(path)
merit.to_csv('merit_plans.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
########################## Eligible Earnings #################################
##############################################################################

"""
Employee ID
Effective Date
Eligible Earnings OVerride Period
Compensation Element Amount
Currency Code
"""

eli_earn = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='Eligible Earnings')

eli_earn_c = eli_earn.filter(regex='Required')
earning = pd.read_excel('./../USA_KBP_CNP_Compensation.xlsx', sheet_name='Eligible Earnings',skiprows=2)

earning_c = earning.iloc[0:0]



os.chdir(path)
#biographic_data.to_csv('biographic_data.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
######################## Comp History Prev Sys ###############################
##############################################################################
