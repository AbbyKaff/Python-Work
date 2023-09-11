##############################################################################
############################# Bankruptcy (USA) ###############################
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
from common import (write_to_csv, active_workers, open_as_utf8, modify_amount)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    bank = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72603205.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    bank['Employee Id'] = bank['Employee Id'].astype(str)
    bank = bank.loc[bank['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    bank['Employee ID'] = bank['Employee Id']
    bank['Withholding Order Type ID'] = 'BANKRUPTCY'
    bank['Case Number'] = bank['Additional Info']
    bank['Order Date']  = pd.to_datetime(bank['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    bank['Received Date'] = pd.to_datetime(bank['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    bank['Begin Date'] = pd.to_datetime(bank['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    bank['Company'] = bank['Cost Centers(Company Code)'].replace({'FQSR':'FQ'})
    bank['Withholding Order Amount Type'] = np.where(bank['EE Calc Method'] == '% Of Disposable Earnings','PERCENTDE',
                                                     np.where(bank['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS','AMT'))

    bank = modify_amount(bank, 'EE Amount')
    bank['Withholding Order Amount'] = bank['EE Amount']
    bank['Withholding Order Amount as Percent'] = bank['EE Percent (As Of Today)']
    bank['Frequency ID'] = np.where(bank['Unemployment State/Province'] == 'NY','Weekly','Biweekly')
    bank['Code (Issued in Reference)'] = 'FEDERAL'

    # -----------------------------------------------------------------------------
    #vr = pd.read_csv(config.PATH_WD_IMP + 'data sources\\70602656.csv', dtype='object', encoding='cp1251')
    vr = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\\deductions_wleg.csv', dtype='object', encoding='cp1251')
    # -----------------------------------------------------------------------------

    #vr['Deduction Recipient Name'] = vr['Payee']
    vr['Source System'] = 'Kronos'
    vr['Alternate Deduction Recipient Name'] = ''
    #vr = vr.reset_index()
    #vr['index'] = vr['index'] + 1
    #vr['Deduction Recipient ID'] = 'VR-' + ((vr['index']).astype(str)).str.pad(3,fillchar='0')
    #vr['Payment Type'] = np.where(vr['Payment Type'].str.contains('ACH'),'Direct_Deposit','Check')
    #vr['Business Entity Name'] = vr['Description']
    #vr['External Entity ID'] = vr['CRM Company Id']
    #vr['Country ISO Code Address'] = 'USA'
   # vr['Address Line #1'] = vr['Address 1']
   # vr['Address Line #2'] = vr['Address 2']
   # vr['Region'] = vr['State']
    #vr['Postal Code'] = vr['Zip Code']

    #ded = pd.read_excel(config.PATH_WD_IMP + config.FILE_USA_PAYROLL, sheet_name='Deduction Recipients', skiprows=2, nrows=0)

    #ded['Deduction Recipient Name'] = vr['Deduction Recipient Name']

    #vr2 = vr[['Deduction Recipient Name', 'Source System','Alternate Deduction Recipient Name','Deduction Recipient ID','Payment Type','Business Entity Name','External Entity ID','Country ISO Code Address','Address Line #1','Address Line #2','City','Region','Postal Code']]

    #ded = ded.drop(['Source System','Alternate Deduction Recipient Name','Deduction Recipient ID','Payment Type','Business Entity Name','External Entity ID','Country ISO Code Address','Address Line #1','Address Line #2','City','Region','Postal Code'],axis=1)

    #ded = ded.merge(vr2, on='Deduction Recipient Name', how='right')

    bankc = bank.merge(vr, left_on='Vendor', right_on='Legacy Value',how='left')

    bankc = bankc[['Employee ID','Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID']]

    bankrup = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\templates\USA_KBP_CNP_Payroll-USA_Template_01122023 (2).xlsx", sheet_name='Bankruptcy (USA)', skiprows=2, nrows=0)
    bankrup['Employee ID'] = bankc['Employee ID']
    bankrup = bankrup.drop(['Withholding Order Type ID', 'Case Number', 'Order Date',
                            'Received Date', 'Begin Date', 'Company',
                            'Withholding Order Amount Type', 'Withholding Order Amount',
                            'Withholding Order Amount as Percent', 'Frequency ID',
                            'Code (Issued in Reference)','Deduction Recipient ID'] ,axis=1)

    bankrup2 = bankrup.merge(bankc, on='Employee ID')

    bankrup2['Source System'] = 'Kronos'
    bankrup2['Chapter 13'] = 'Y'

    bankrup2 = bankrup2[['Employee ID', 'Source System', 'Withholding Order Type ID',
           'Case Number', 'Withholding Order Additional Order Number',
           'Order Date', 'Received Date', 'Begin Date', 'End Date', 'Company',
           'Inactive', 'Withholding Order Amount Type', 'Withholding Order Amount',
           'Withholding Order Amount as Percent', 'Frequency ID',
           'Total Debt Amount Remaining', 'Monthly Limit',
           'Code (Issued in Reference)', 'Deduction Recipient ID',
           'Originating Authority', 'Memo', 'Currency', 'Chapter 13', 'Chapter 7',
           'Fee Amount #1', 'Fee Percent #1', 'Fee Type ID #1',
           'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
           'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
           'Fee Monthly Limit #1', 'Fee Amount #2', 'Fee Percent #2',
           'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
           'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
           'Fee Monthly Limit #2', 'Withholding Order Withholding Frequency']]

    write_to_csv(bankrup2, 'bankruptcy.txt')
