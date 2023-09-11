##############################################################################
######################### EMP-Allowance Plans ##############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd
import os
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    allowance_pl = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Compensation_Template_01102023 (4).xlsx', sheet_name='EMP-Allowance Plans')

    allowance_pl_c = allowance_pl.filter(regex='Required')
    allowance_data = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Compensation_Template_01102023 (4).xlsx', sheet_name='EMP-Allowance Plans', skiprows=2, nrows=0)

    df1 = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72604619.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\worker_id_fix.csv')
    df1['Employee Id'] = df1['Employee Id'].astype(str)
    df1 = df1.loc[df1['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    df1['Frequency'] = np.where(df1['Frequency'] != 'Monthly','Biweekly','Monthly')
    df1['Date Hired'] = pd.to_datetime(df1['Date Hired'])
    df1['Date Re-Hired'] = pd.to_datetime(df1['Date Re-Hired'])
    df1['Begin Date'] = pd.to_datetime(df1['Begin Date'])
    df1['Master Hire'] = np.where(df1['Date Re-Hired'] >= df1['Date Hired'],df1['Date Re-Hired'],df1['Date Hired'])
    df1['Begin Date'] = np.where(df1['Master Hire'] >= df1['Begin Date'],df1['Master Hire'],df1['Begin Date'])
    df1 = df1.replace('^[-]$', '', regex=True)
    df1 = modify_amount(df1, 'Amount (As Of Today)')
    df1 = df1.loc[df1['Amount (As Of Today)'].astype(float) > 0]

    df1['Employee ID'] = df1['Employee Id']
    df1['Begin Date'] = pd.to_datetime(df1['Begin Date'])
    jan = pd.to_datetime('1/1/23')
    df1.loc[df1['Begin Date'] < jan,'Begin Date'] = pd.to_datetime('1/1/23')
    df1['Effective Date'] = df1['Begin Date']
    df1['Earning Name'] = np.where(df1['Earning Name'] == 'Club Dues','Club Dues (taxable)',df1['Earning Name'])

    # ================   JLS BEGIN ADD   ================
    df = df1[['Employee ID', 'Effective Date']].drop_duplicates()
    df = df.sort_values(by=['Employee ID', 'Effective Date'], ascending=[True, False])
    print('*** df ***')
    print(df)

    allow1 = pd.merge(df, df1.drop_duplicates(['Employee ID', 'Effective Date']), on=['Employee ID', 'Effective Date'], how='left')
    print('*** allow1 ***')
    print(allow1)

    df1_minus_allow1 = pd.concat([df1, allow1]).drop_duplicates(keep=False)
    print('*** df1_minus_allow1 ***')
    print(df1_minus_allow1)

    allow2 = pd.merge(allow1, df1_minus_allow1.drop_duplicates(['Employee ID', 'Effective Date']), on=['Employee ID', 'Effective Date'], how='left', suffixes=(None,'_allow2'))
    print('*** allow2 ***')
    print(allow2)
    write_to_csv(allow2, 'allow2.csv')

    df1_minus_allow2 = pd.concat([df1_minus_allow1, allow2]).drop_duplicates(keep=False)
    print('*** df1_minus_allow2 ***')
    print(df1_minus_allow2)
    # ================   JLS END ADD   ================

    #df1.to_csv('test_allowance_plans.csv')
    # Transpose data to see all jobs each employee has had
    df1['Earning amount'] = df1['Earning Name'] +','+df1['Amount (As Of Today)'] + ',' + df1['Frequency']

    grouped_multiple = df1[['Employee ID','Effective Date','Earning amount']]
    grouped_multiple['Employee ID'] = grouped_multiple['Employee ID'].astype(int)
    grouped_new = grouped_multiple.groupby(['Employee ID', 'Effective Date'], as_index=False).agg({"Earning amount":", ".join})
    grouped_final = pd.concat([grouped_new['Employee ID'], grouped_new['Effective Date'], grouped_new['Earning amount'].str.split(', ', expand=True).add_prefix('Subcolumn')], axis=1)

    # df = modify_amount(df, 'Salary')

    grouped_final['Allowance Plan #1'] = grouped_final['Subcolumn0'].str.split(',').str[0]
    grouped_final['Amount - Allowance Plan #1'] = grouped_final['Subcolumn0'].str.split(',').str[1]
    grouped_final['Amount - Allowance Plan #1'] = grouped_final['Amount - Allowance Plan #1'].str.replace("$","")
    grouped_final['Frequency - Allowance Plan #1'] = grouped_final['Subcolumn0'].str.split(',').str[2]
    grouped_final['Currency Code - Allowance Plan #1'] = 'USD'
    grouped_final['Percent - Allowance Plan #1'] = ''
    grouped_final['Expected End Date - Allowance Plan #1'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #1'] = ''

    grouped_final['Allowance Plan #2'] = grouped_final['Subcolumn1'].str.split(',').str[0]
    grouped_final['Amount - Allowance Plan #2'] = grouped_final['Subcolumn1'].str.split(',').str[1]
    grouped_final['Amount - Allowance Plan #2'] = grouped_final['Amount - Allowance Plan #2'].str.replace("$","")
    grouped_final['Frequency - Allowance Plan #2'] = grouped_final['Subcolumn1'].str.split(',').str[2]
    # grouped_final['Currency Code - Allowance Plan #2'] = 'USD'
    write_to_csv(grouped_final, 'grouped_final.csv')
    grouped_final['Currency Code - Allowance Plan #2'] = np.where(grouped_final['Allowance Plan #2'].isnull(), '', 'USD')
    grouped_final['Percent - Allowance Plan #2'] = ''
    grouped_final['Expected End Date - Allowance Plan #2'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #2'] = ''

    grouped_final['Allowance Plan #3'] = grouped_final['Subcolumn2'].str.split(',').str[0]
    grouped_final['Amount - Allowance Plan #3'] = grouped_final['Subcolumn2'].str.split(',').str[1]
    grouped_final['Amount - Allowance Plan #3'] = grouped_final['Amount - Allowance Plan #3'].str.replace("$","")
    grouped_final['Frequency - Allowance Plan #3'] = grouped_final['Subcolumn2'].str.split(',').str[2]
    # grouped_final['Currency Code - Allowance Plan #3'] = 'USD'
    grouped_final['Currency Code - Allowance Plan #3'] = np.where(grouped_final['Allowance Plan #3'].isnull(), '', 'USD')
    grouped_final['Percent - Allowance Plan #3'] = ''
    grouped_final['Expected End Date - Allowance Plan #3'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #3'] = ''

    # grouped_final['Allowance Plan #4'] = grouped_final['Subcolumn3'].str.split(',').str[0]
    # grouped_final['Amount - Allowance Plan #4'] = grouped_final['Subcolumn3'].str.split(',').str[1]
    # grouped_final['Amount - Allowance Plan #4'] = grouped_final['Amount - Allowance Plan #4'].str.replace("$","")
    # grouped_final['Frequency - Allowance Plan #4'] = grouped_final['Subcolumn3'].str.split(',').str[2]
    # grouped_final['Currency Code - Allowance Plan #4'] = 'USD'
    # grouped_final['Percent - Allowance Plan #4'] = ''
    # grouped_final['Expected End Date - Allowance Plan #4'] = ''
    # grouped_final['Reimbursement Start Date - Allowance Plan #4'] = ''
    grouped_final['Allowance Plan #4'] = ''
    grouped_final['Percent - Allowance Plan #4'] = ''
    grouped_final['Amount - Allowance Plan #4'] = ''
    grouped_final['Currency Code - Allowance Plan #4'] = ''
    grouped_final['Frequency - Allowance Plan #4'] = ''
    grouped_final['Expected End Date - Allowance Plan #4'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #4'] = ''

    grouped_final['Allowance Plan #5'] = ''
    grouped_final['Percent - Allowance Plan #5'] = ''
    grouped_final['Amount - Allowance Plan #5'] = ''
    grouped_final['Currency Code - Allowance Plan #5'] = ''
    grouped_final['Frequency - Allowance Plan #5'] = ''
    grouped_final['Expected End Date - Allowance Plan #5'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #5'] = ''
    grouped_final['Allowance Plan #6'] = ''
    grouped_final['Percent - Allowance Plan #6'] = ''
    grouped_final['Amount - Allowance Plan #6'] = ''
    grouped_final['Currency Code - Allowance Plan #6'] = ''
    grouped_final['Frequency - Allowance Plan #6'] = ''
    grouped_final['Expected End Date - Allowance Plan #6'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #6'] = ''
    grouped_final['Allowance Plan #7'] = ''
    grouped_final['Percent - Allowance Plan #7'] = ''
    grouped_final['Amount - Allowance Plan #7'] = ''
    grouped_final['Currency Code - Allowance Plan #7'] = ''
    grouped_final['Frequency - Allowance Plan #7'] = ''
    grouped_final['Expected End Date - Allowance Plan #7'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #7'] = ''
    grouped_final['Allowance Plan #8'] = ''
    grouped_final['Percent - Allowance Plan #8'] = ''
    grouped_final['Amount - Allowance Plan #8'] = ''
    grouped_final['Currency Code - Allowance Plan #8'] = ''
    grouped_final['Frequency - Allowance Plan #8'] = ''
    grouped_final['Expected End Date - Allowance Plan #8'] = ''
    grouped_final['Reimbursement Start Date - Allowance Plan #8'] = ''


    grouped_final['Effective Date'] = pd.to_datetime(grouped_final['Effective Date'])

    grouped_final['Effective Date'] = grouped_final['Effective Date'].dt.strftime("%d-%b-%Y").str.upper()
    grouped_final['Effective Date'] = '01-JAN-2023'
    grouped_final['Source System'] = 'Kronos'

    grouped_final['Sequence #'] = grouped_final.groupby('Employee ID')['Employee ID'].cumcount() + 1

    grouped_final['Compensation Change Reason'] = 'Default_Request_Compensation_Change _Conversion_Conversion'
    grouped_final['Position ID'] = ''
    grouped_final['Remove All Plan Assignments?'] = 'N'

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
    
    allow.to_csv('allow_test.csv')

    write_to_csv(allow, 'allowance_plans.txt')
