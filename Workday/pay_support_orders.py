##############################################################################
###################### Support Orders (USA) ##################################
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

    sup_order = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72603208.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    sup_order['Employee Id'] = sup_order['Employee Id'].astype(str)
    sup_order = sup_order.loc[sup_order['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    sup_order = sup_order.replace('^[-]$', '', regex=True)
    sup_order = modify_amount(sup_order, 'EE Amount')
    sup_order = modify_amount(sup_order, 'EE Percent (As Of Today)')

    sup_order = sup_order.loc[((sup_order['EE Amount'].astype(float) > 0) | (sup_order['EE Percent (As Of Today)'].astype(float) > 0))]

    work_del = pd.read_csv(config.PATH_WD_IMP + 'Workday Delivered\\Integration_IDs.csv', encoding='cp1251')

    work_del_2 = work_del[work_del['Business Object'] == 'Payroll State Authority']
    work_del_2['ID'] = work_del_2['ID'].astype(str).str.zfill(2)

    """
    Fix inconsistenties with support order names:

    Arizona Child Support - KBP Inspired
    Connecticut Child Support - KBP Foods
    DNU - CO Child Support - KBP Bells
    Florida State Disbursement Unit - Foods
    Maine Child Support - KBP Foods
    Massachusetts Child Support - KBP Foods
    New Hampshire Child Support - KBP Foods
    New Mexico Child Support
    New York Child Support - KBP Bells
    North Carolina Child Support - KBP Inspired
    Pennsylvania Child Support - KBP Inspired
    Tennessee Child Support- KBP FOODS
    Texas Child Support - KBP Inpsired
    Washington DC Child Support
    Washington State Support Registry - foods
    """

    dict_vendor = {'Arizona Child Support - KBP Inspired': 'AZ',
					'Connecticut Child Support - KBP Foods': 'CT',
					'DNU - CO Child Support - KBP Bells': 'CO',
					'Florida State Disbursement Unit - Foods': 'FL',
					'Maine Child Support - KBP Foods': 'ME',
					'Massachusetts Child Support - KBP Foods': 'MA',
					'New Hampshire Child Support - KBP Foods': 'NH',
					'New Mexico Child Support': 'NM',
					'New York Child Support - KBP Bells': 'NY',
					'North Carolina Child Support - KBP Inspired': 'NC',
					'Pennsylvania Child Support - KBP Inspired': 'PA',
					'Tennessee Child Support- KBP FOODS': 'TN',
					'Texas Child Support - KBP Inpsired': 'TX',
					'Washington DC Child Support': 'DC',
					'Washington State Support Registry - foods': 'WA'}

    #sup_order['Vendor'] = sup_order['Vendor'].replace(dict_vendor)

    sup_order['Vendor_State'] = np.where(sup_order['Vendor'] == 'Arizona Child Support - KBP Inspired','AZ',
                                np.where(sup_order['Vendor'] == 'Connecticut Child Support - KBP Foods','CT',
                                np.where(sup_order['Vendor'] == 'DNU - CO Child Support - KBP Bells','CO',
                                np.where(sup_order['Vendor'] == 'Florida State Disbursement Unit - Foods','FL',
                                np.where(sup_order['Vendor'] == 'Maine Child Support - KBP Foods','ME',
                                np.where(sup_order['Vendor'] == 'Massachusetts Child Support - KBP Foods','MA',
                                np.where(sup_order['Vendor'] == 'New Hampshire Child Support - KBP Foods','NH',
                                np.where(sup_order['Vendor'] == 'New Mexico Child Support','NM',
                                np.where(sup_order['Vendor'] == 'New York Child Support - KBP Bells','NY',
                                np.where(sup_order['Vendor'] == 'North Carolina Child Support - KBP Inspired','NC',
                                np.where(sup_order['Vendor'] == 'Pennsylvania Child Support - KBP Inspired','PA',
                                np.where(sup_order['Vendor'] == 'Tennessee Child Support- KBP FOODS','TN',
                                np.where(sup_order['Vendor'] == 'Texas Child Support - KBP Inpsired','TX',
                                np.where(sup_order['Vendor'] == 'Washington DC Child Support','DC',
                                np.where(sup_order['Vendor'] == 'Washington State Support Registry - foods','WA',sup_order['Vendor'].str.split(" ").str[0])))))))))))))))

    sup_order = sup_order.merge(work_del_2, right_on='State', left_on='Vendor_State', how='left')

    sup_order['Employee ID'] = sup_order['Employee Id']
    sup_order['Withholding Order Type ID'] = 'SUPPORT'

    sup_order.loc[sup_order['Additional Info'].isnull(), 'Additional Info'] = sup_order['Employee ID']

    sup_order.sort_values(by=['Employee ID', 'Additional Info'])
    sup_order['Case Number Seq'] = sup_order.groupby(['Employee ID', 'Additional Info']).cumcount()+1
    sup_order.loc[sup_order['Case Number Seq'] == 1, 'Case Number'] = sup_order['Additional Info']
    sup_order.loc[sup_order['Case Number Seq'] > 1, 'Case Number'] = sup_order['Additional Info'] + '-' + sup_order['Case Number Seq'].astype('Int64').astype(str)

    sup_order['Begin Date']  = pd.to_datetime(sup_order['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    sup_order['Order Date']  = pd.to_datetime(sup_order['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    sup_order['Received Date'] = pd.to_datetime(sup_order['Begin Date']).dt.strftime('%d-%b-%Y').str.upper()
    sup_order['Company'] = sup_order['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})
    sup_order['Withholding Order Amount Type'] = np.where(sup_order['EE Calc Method'] == '% Of Disposable Earnings','PERCENTDE',
                                                     np.where(sup_order['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS','AMT'))

    sup_order['Withholding Order Amount'] = sup_order['EE Amount']
    sup_order['Withholding Order Amount as Percent'] = (sup_order['EE Percent (As Of Today)'].astype(float) / 100).round(3)
    sup_order['Frequency ID'] = np.where(sup_order['Unemployment State/Province'] == 'NY','Weekly','Biweekly')
    sup_order['Code (Issued in Reference)'] = sup_order['ID'].str.zfill(2)

    #dedf_c = get_deduction_recipient_name()
    vr = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\\deductions_wleg.csv', dtype='object', encoding='cp1251')
    sup_orderc = sup_order.merge(vr, left_on='Vendor2', right_on='Legacy Value', how='left')

    sup_orderc['Order Form Amount #1'] = sup_orderc['Withholding Order Amount']
    sup_orderc['Pay Period Amount #1'] = sup_order['EE Amount']
    sup_orderc['Amount as Percent #1'] = (sup_orderc['Withholding Order Amount as Percent'].astype(float) / 100).round(3)
    sup_orderc = sup_orderc[['Employee ID','Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID','Order Form Amount #1','Pay Period Amount #1','Amount as Percent #1']]

    sup_orderc['Source System'] = 'Kronos'
    sup_orderc['Withholding Order Additional Order Number'] = ''
    sup_orderc['Inactive'] = ''
    sup_orderc['Monthly Limit'] = ''
    sup_orderc['Originating Authority'] = ''
    sup_orderc['Memo'] =''
    sup_orderc['Currency'] = 'USD'
    sup_orderc['Case Type of Original Order'] = 'Y'
    sup_orderc['Case Type of Amended Order'] = ''
    sup_orderc['Case Type of Termination Order'] = ''
    sup_orderc['Custodial Party Name'] = ''
    sup_orderc['Supports Second Family'] = ''
    sup_orderc['Remittance ID Override'] = ''
    sup_orderc['Child Name (Last, First, MI)'] = ''
    sup_orderc['Child Birth Date'] = ''
    sup_orderc['Payroll Local County Authority FIPS Code'] = ''
    sup_orderc['Support Type #1']='CS'
    sup_orderc['Arrears Over 12 Weeks #1'] = ''
    sup_orderc['Order Form Amount #2'] = ''
    sup_orderc['Pay Period Amount #2'] = ''
    sup_orderc['Amount as Percent #2'] = ''
    sup_orderc['Support Type #2'] = ''
    sup_orderc['Arrears Over 12 Weeks #2'] = ''
    sup_orderc['Fee Amount #1'] = ''
    sup_orderc['Fee Percent #1'] = ''
    sup_orderc['Fee Type ID #1'] = ''
    sup_orderc['Fee Amount Type ID #1'] = ''
    sup_orderc['Deduction Recipient ID #1'] = ''
    sup_orderc['Override Fee Schedule #1'] = ''
    sup_orderc['Begin Date #1'] = ''
    sup_orderc['End Date #1'] = ''
    sup_orderc['Fee Monthly Limit #1'] = ''
    sup_orderc['Fee Percent #2'] = ''
    sup_orderc['Fee Amount #2'] = ''
    sup_orderc['Fee Type ID #2'] = ''
    sup_orderc['Fee Amount Type ID #2'] = ''
    sup_orderc['Deduction Recipient ID #2'] = ''
    sup_orderc['Override Fee Schedule #2'] = ''
    sup_orderc['Begin Date #2'] = ''
    sup_orderc['End Date #2'] = ''
    sup_orderc['Fee Monthly Limit #2'] = ''

    support2 = sup_orderc[['Employee ID', 'Source System', 'Withholding Order Type ID',
           'Case Number', 'Withholding Order Additional Order Number',
           'Order Date', 'Received Date', 'Begin Date', 'Company', 'Inactive',
           'Withholding Order Amount Type', 'Withholding Order Amount',
           'Withholding Order Amount as Percent', 'Frequency ID', 'Monthly Limit',
           'Code (Issued in Reference)', 'Deduction Recipient ID',
           'Originating Authority', 'Memo', 'Currency',
           'Case Type of Original Order', 'Case Type of Amended Order',
           'Case Type of Termination Order', 'Custodial Party Name',
           'Supports Second Family', 'Remittance ID Override',
           'Child Name (Last, First, MI)', 'Child Birth Date',
           'Payroll Local County Authority FIPS Code', 'Order Form Amount #1',
           'Pay Period Amount #1', 'Amount as Percent #1', 'Support Type #1',
           'Arrears Over 12 Weeks #1', 'Order Form Amount #2',
           'Pay Period Amount #2', 'Amount as Percent #2', 'Support Type #2',
           'Arrears Over 12 Weeks #2', 'Fee Amount #1', 'Fee Percent #1',
           'Fee Type ID #1', 'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
           'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
           'Fee Monthly Limit #1', 'Fee Percent #2', 'Fee Amount #2',
           'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
           'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
           'Fee Monthly Limit #2']]

    write_to_csv(support2, 'support_orders.txt')
