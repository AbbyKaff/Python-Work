##############################################################################
########################### Deduction Recipients #############################
##############################################################################

import pandas as pd
import numpy as np
import os
import datetime
from datetime import date
from datetime import datetime
import xlrd
import glob as glob

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
#if __name__ == '__main__':
'''
    vr = pd.read_csv(config.PATH_WD_IMP + 'data sources\\70602656.csv', dtype='object', encoding='cp1251')

    vr = vr.sort_values(vr.columns[0]).reset_index(drop=True)

    vr['Deduction Recipient Name'] = vr['Name']
    vr['Source System'] = 'Kronos'
    vr['Alternate Deduction Recipient Name'] = ''
    vr = vr.reset_index()
    vr['index'] = vr['index'] + 1
    vr['Deduction Recipient ID'] = 'VR-' + ((vr['index']).astype(str)).str.pad(3,fillchar='0')
    vr['Payment Type'] = np.where(vr['Payment Type'].str.contains('ACH'),'Direct_Deposit','Check_Cheque')
    vr['Business Entity Name'] = vr['Name']
    vr['External Entity ID'] = vr['CRM Company Id']
    vr.loc[~vr['Address 1'].isnull(), 'Country ISO Code Address'] = 'USA'
    vr['Address Line #1'] = vr['Address 1']
    vr['Address Line #2'] = vr['Address 2']
    vr['Region'] = 'USA-' + vr['State']
    vr['Postal Code'] = vr['Zip Code'].str.zfill(5)
    vr['Phone Number'] = vr['Phone']

    ded = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\templates\USA_KBP_CNP_Payroll-USA_Template_01122023 (2).xlsx", sheet_name='Deduction Recipients', skiprows=2, nrows=0)

    ded['Deduction Recipient Name'] = vr['Deduction Recipient Name']

    vr2 = vr[['Deduction Recipient Name', 'Source System','Alternate Deduction Recipient Name','Deduction Recipient ID','Payment Type','Business Entity Name','External Entity ID','Country ISO Code Address','Address Line #1','Address Line #2','City','Region','Postal Code','Phone Number']]

    ded = ded.drop(['Source System','Alternate Deduction Recipient Name','Deduction Recipient ID','Payment Type','Business Entity Name','External Entity ID','Country ISO Code Address','Address Line #1','Address Line #2','City','Region','Postal Code','Phone Number'],axis=1)

    ded = ded.merge(vr2, on='Deduction Recipient Name')
'''
ded = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\\ded_rep_SW.csv', dtype='object', encoding='cp1251')
ded = ded.sort_values(ded.columns[0]).reset_index(drop=True)
ded = ded.reset_index()
ded['index'] = ded['index'] + 1
ded['Deduction Recipient ID'] = 'VR-' + ((ded['index']).astype(str)).str.pad(3,fillchar='0')
ded['External Entity ID'] = ded['Deduction Recipient ID']
ded.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\deductions_wleg.csv')
dedf = ded[['Deduction Recipient Name', 'Source System',
           'Alternate Deduction Recipient Name', 'Deduction Recipient ID',
           'Payment Type', 'Business Entity Name', 'Business Entity Tax ID',
           'External Entity ID', 'Email Address', 'Country ISO Code Phone',
           'International Phone Code', 'Phone Number', 'Phone Extension',
           'Phone Device Type', 'Type Reference', 'Use For Reference Phone',
           'Country ISO Code Address', 'Effective as of', 'Address Line #1',
           'Address Line #2', 'Address Line #3', 'Address Line #4',
           'Address Line #5', 'Address Line #6', 'Address Line #7',
           'Address Line #8', 'Address Line #9', 'City', 'City Subdivision',
           'City Subdivision 2', 'Region', 'Region Subdivision',
           'Region Subdivision 2', 'Postal Code', 'Use For Reference Address',
           'Country ISO Code Bank', 'Currency Code', 'Bank Account Nickname',
           'Bank Name', 'Bank Account Type Code', 'Routing Number (Bank ID)',
           'Branch ID', 'Branch Name', 'Bank Account Number', 'Bank Account Name',
           'Roll Number', 'Check Digit', 'IBAN', 'SWIFT Bank Identification Code']]
#dedf.to_csv('ded_test.csv')
dedf['Phone Device Type'] = np.where(dedf['Phone Number'].isna(),'','Mobile')
write_to_csv(ded, 'deduction_recipients_dd_append_082723.txt')

#merge old and new information to map other files 

#legacy = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\\Copy of Master Vendor File.csv', dtype='object', encoding='cp1251')

#legacy_m = legacy.merge(dedf, left_on='Payee',right_on='Deduction Recipient Name', how='left')

ded = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\fix_ded_rec.xlsx",converters={'Routing Number (Bank ID)':str,'Bank Account Number':str})
ded['Routing Number (Bank ID)'] = ded['Routing Number (Bank ID)'].astype(str)
ded['Routing Number (Bank ID)'] = ded['Routing Number (Bank ID)'].str.zfill(9)
