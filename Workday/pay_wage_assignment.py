##############################################################################
########################## Wage Assignment (USA) #############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8,
                    get_deduction_recipient_name)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    wage_as = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72603206.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    wage_as['Employee Id'] = wage_as['Employee Id'].astype(str)
    wage_as = wage_as.loc[wage_as['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    wage_as['Employee ID'] = wage_as['Employee Id']
    wage_as['Withholding Order Type ID'] = 'WAGE'
    wage_as['Case Number'] = wage_as['Additional Info']
    wage_as['Order Date']  = pd.to_datetime(wage_as['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    wage_as['Received Date'] = pd.to_datetime(wage_as['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    wage_as['Company'] = wage_as['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})
    wage_as['Withholding Order Amount Type'] = np.where(wage_as['EE Calc Method'] == '% Of Disposable Earnings','PERCENTDE',
                                                     np.where(wage_as['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS','AMT'))

    wage_as['Withholding Order Amount'] = wage_as['EE Amount']
    wage_as['Withholding Order Amount as Percent'] = wage_as['EE Percent (As Of Today)']
    wage_as['Frequency ID'] = np.where(wage_as['Unemployment State/Province'] == 'NY','Weekly','Biweekly')
    wage_as['Code (Issued in Reference)'] = ''

    #dedf_c = get_deduction_recipient_name()
    vr = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\\deductions_wleg.csv', dtype='object', encoding='cp1251')

    wagec = wage_as.merge(vr, left_on='Vendor', right_on='Legacy Value')

    wagec = wagec[['Employee ID','Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID']]

    wages = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Payroll-USA_Template_01122023 (2).xlsx', sheet_name='Wage Assignment (USA)', skiprows=2, nrows=0)
    # wages = wages.iloc[0:0]

    wages['Employee ID'] = wagec['Employee ID']

    wages = wages.drop(['Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID'],axis=1)

    wages2 = wages.merge(wagec, on='Employee ID')

    wages2 = wages2[['Employee ID', 'Source System', 'Withholding Order Type ID',
           'Case Number', 'Withholding Order Additional Order Number',
           'Order Date', 'Received Date', 'Begin Date', 'End Date', 'Company',
           'Inactive', 'Withholding Order Amount Type', 'Withholding Order Amount',
           'Withholding Order Amount as Percent', 'Frequency ID',
           'Total Debt Amount Remaining', 'Monthly Limit',
           'Code (Issued in Reference)', 'Deduction Recipient ID', 'Memo',
           'Currency', 'Regulated Loan', 'Head of Household', 'Married',
           'Fee Amount #1', 'Fee Percent #1', 'Fee Type ID #1',
           'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
           'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
           'Fee Monthly Limit #1', 'Fee Amount #2', 'Fee Percent #2',
           'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
           'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
           'Fee Monthly Limit #2']]

    write_to_csv(wages2, 'wage_assignment_redo_082523.txt')
    
    wages2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\PROD_wage_assignment_082523.csv", encoding='cp1251')
    
    