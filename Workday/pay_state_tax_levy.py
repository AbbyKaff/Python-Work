##############################################################################
########################### State Tax Levy (USA) #############################
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

    state_tax = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72603207.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    state_tax['Employee Id'] = state_tax['Employee Id'].astype(str)
    state_tax = state_tax.loc[state_tax['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    state_tax['Employee ID'] = state_tax['Employee Id']
    state_tax['Withholding Order Type ID'] = 'STATELEVY'
    state_tax['Case Number'] = state_tax['Additional Info']
    state_tax['Total Debt Amount Remaining'] = state_tax['Goal: Amount']

    state_tax.sort_values(by=['Employee ID', 'Additional Info'])
    state_tax['Case Number Seq'] = state_tax.groupby(['Employee ID', 'Additional Info']).cumcount()+1
    state_tax.loc[state_tax['Case Number Seq'] == 1, 'Case Number'] = state_tax['Additional Info']
    state_tax.loc[state_tax['Case Number Seq'] > 1, 'Case Number'] = state_tax['Additional Info'] + '-' + state_tax['Case Number Seq'].astype('Int64').astype(str)

    state_tax['Order Date']  = pd.to_datetime(state_tax['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    state_tax['Received Date'] = pd.to_datetime(state_tax['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    state_tax['Begin Date'] = pd.to_datetime(state_tax['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    state_tax['Company'] = state_tax['Cost Centers(Company Code)'].replace({'FQSR':'FQ'})
    state_tax['Withholding Order Amount Type'] = np.where(state_tax['EE Calc Method'] == '% Of Disposable Earnings', 'PERCENTDE',
                                                     np.where(state_tax['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS', 'AMT'))

    df_states = pd.read_csv(config.PATH_WD_IMP + 'data files\\wd_states.csv', encoding='cp1251')
    state_tax = state_tax.merge(df_states, left_on='Unemployment State/Province', right_on='Abbrev', how='left')
    state_tax['Code (Issued in Reference)'] = state_tax['Payroll']

    state_tax = state_tax.replace('^[-]$', '', regex=True)
    state_tax = modify_amount(state_tax, 'EE Amount')
    state_tax = modify_amount(state_tax, 'EE Percent (As Of Today)')
    state_tax = modify_amount(state_tax, 'Total Debt Amount Remaining')

    state_tax['Withholding Order Amount'] = state_tax['EE Amount']
    state_tax['Withholding Order Amount as Percent'] = (state_tax['EE Percent (As Of Today)'].astype(float) / 100).round(3)
    state_tax['Frequency ID'] = np.where(state_tax['Unemployment State/Province'] == 'NY', 'Weekly', 'Biweekly')
    state_tax.loc[state_tax['Employee ID'] == '108774','Vendor'] = 'NC Department OF Revenue'
    #dedf_c = get_deduction_recipient_name()
    vr = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\\deductions_wleg.csv', dtype='object', encoding='cp1251')
    state_taxc = state_tax.merge(vr, left_on='Vendor', right_on='Legacy Value',how='left')

    state_taxc = state_taxc[['Employee ID', 'Withholding Order Type ID',
        'Case Number', 'Order Date', 'Received Date', 'Begin Date', 'Company',
        'Withholding Order Amount Type', 'Withholding Order Amount',
        'Withholding Order Amount as Percent', 'Frequency ID',
        'Code (Issued in Reference)', 'Deduction Recipient ID',
        'Total Debt Amount Remaining']]

    st_tx = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Payroll-USA_Template_01122023 (2).xlsx', sheet_name='State Tax Levy (USA)', skiprows=2, nrows=0)

    st_tx['Employee ID'] = state_taxc['Employee ID']

    st_tx = st_tx.drop(['Withholding Order Type ID', 'Case Number', 'Order Date',
        'Received Date', 'Begin Date', 'Company', 'Withholding Order Amount Type',
        'Withholding Order Amount', 'Withholding Order Amount as Percent',
        'Frequency ID', 'Code (Issued in Reference)', 'Deduction Recipient ID'], axis=1)

    st_tx2 = st_tx.merge(state_taxc, on='Employee ID')

    st_tx2['Source System'] = 'Kronos'
    st_tx2['Worker is Laborer or Mechanic'] = 'N'
    st_tx2['Worker Income is Poverty Level'] = 'N'
    st_tx2.loc[st_tx2['Total Debt Amount Remaining_y'].astype(float) == 0, 'Total Debt Amount Remaining_y'] = '99999999999'
    st_tx2['Total Debt Amount Remaining'] = st_tx2['Total Debt Amount Remaining_y']

    st_tx2 = st_tx2[['Employee ID', 'Source System', 'Withholding Order Type ID',
           'Case Number', 'Withholding Order Additional Order Number',
           'Order Date', 'Received Date', 'Begin Date', 'End Date', 'Company',
           'Inactive', 'Withholding Order Amount Type', 'Withholding Order Amount',
           'Withholding Order Amount as Percent', 'Frequency ID',
           'Total Debt Amount Remaining', 'Monthly Limit',
           'Code (Issued in Reference)', 'Deduction Recipient ID',
           'Originating Authority', 'Memo', 'Currency', 'Form and Revision Number',
           'Marital Status', 'Number of Dependents',
           'Worker is Laborer or Mechanic', 'Worker Income is Poverty Level',
           'Part 3 Effective Date', 'Pay Period Exemption Override Amount',
           'Payroll Marital Status Reference', 'Personal Exemptions',
           'Additional 65 or Blind Exemptions', 'Termination Date',
           'Dependent Name', 'Dependent Identification Number',
           'Good Cause Limit Percent', 'Process Until ID', 'Prorate Until Date',
           'Fee Amount #1', 'Fee Percent #1', 'Fee Type ID #1',
           'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
           'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
           'Fee Monthly Limit #1', 'Fee Amount #2', 'Fee Percent #2',
           'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
           'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
           'Fee Monthly Limit #2']]

    #st_tx2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\state_tax_levy_v2.csv")
    #st_tx2['Order Date']  = pd.to_datetime(st_tx2['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    #st_tx2['Received Date'] = pd.to_datetime(st_tx2['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    #st_tx2['Begin Date'] = pd.to_datetime(st_tx2['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    #st_tx2['Part 3 Effective Date'] = pd.to_datetime(st_tx2['Part 3 Effective Date']).dt.strftime("%d-%b-%Y").str.upper()
    st_tx2['Number of Dependents'] = 0
    write_to_csv(st_txl, 'state_tax_levy_redo_082523.txt')
    
    st_txl = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\PROD_state_tax_levy_fix.xlsx")
    st_txl['Code (Issued in Reference)'] = st_txl['Code (Issued in Reference)'].astype(str).str.zfill(2)
