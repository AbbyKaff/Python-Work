##############################################################################
############################# Payee Input Data ###############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    kbpc = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72593428.csv', dtype='object', encoding='cp1251')
    adv = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72866648.csv', dtype='object', encoding='cp1251')
    #------------------------------------------------------------------------------
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
    #kbpc['Employee Id'] = kbpc['Employee Id'].astype(str)
    #kbpc = kbpc.loc[kbpc['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    kbpc['Employee ID'] = kbpc['Employee Id']
    kbpc['Start Date'] = pd.to_datetime(kbpc['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    kbpc['Deduction Code'] = 'KBPC'
    kbpc['Amount'] = kbpc['EE Amount (As Of Today)']

    kbpc = kbpc.loc[~kbpc['Amount'].isnull()]

    kbpc = modify_amount(kbpc, 'Amount')

    pid = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Payroll Common_Template_01102023 (1).xlsx', sheet_name='Payee Input Data', skiprows=2, nrows=0)

    pid['Employee ID'] = kbpc['Employee ID']

    kbpc = kbpc[['Employee ID','Start Date','Deduction Code','Amount']]

    pid = pid.drop(['Start Date','Deduction Code','Amount'], axis=1)

    pid = pid.merge(kbpc, on='Employee ID')

    pid['Source System'] = 'Kronos'
    pid['Ongoing Input'] = 'Y'

    pid_f = pid[['Employee ID', 'Source System', 'Company', 'Cost Center', 'Position ID',
               'Ongoing Input', 'Start Date', 'End Date', 'Earning Code',
               'Deduction Code', 'Amount', 'Hours', 'Rate', 'Adjustment?', 'Comment',
               'Currency', 'State Authority', 'Flexible Payment Deduction Worktag',
               'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
               'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
               'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
               'Custom Worktag #10', 'Allocation Pool', 'Appropriation',
               'Related Calculation ID', 'Input Value', 'Related Calculation ID #2',
               'Input Value #2', 'Related Calculation ID #3', 'Input Value #3']]
    
    adv['Employee ID'] = adv['Employee Id']
    adv['Start Date'] = pd.to_datetime(adv['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
    adv['Deduction Code'] = 'ADV'
    adv['Amount'] = adv['EE Amount (As Of Today)']
    adv['Related Calculation ID'] = 'GOALAMOUNT'
    adv['Input Value'] = adv['Goal: Amount']
    adv = modify_amount(adv, 'Amount')
    adv = modify_amount(adv, 'Input Value')
    adv_c = adv[['Employee ID','Start Date','Deduction Code','Amount','Related Calculation ID','Input Value']]
    pid = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Payroll Common_Template_01102023 (1).xlsx', sheet_name='Payee Input Data', skiprows=2, nrows=0)
    pid['Employee ID'] = adv_c['Employee ID']
    pid2 = pid.drop(['Start Date','Deduction Code','Amount','Related Calculation ID','Input Value'],axis=1)
   
    pid2 = pid2.merge(adv_c, on='Employee ID')
    pid_2 = pid2[['Employee ID', 'Source System', 'Company', 'Cost Center', 'Position ID',
               'Ongoing Input', 'Start Date', 'End Date', 'Earning Code',
               'Deduction Code', 'Amount', 'Hours', 'Rate', 'Adjustment?', 'Comment',
               'Currency', 'State Authority', 'Flexible Payment Deduction Worktag',
               'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
               'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
               'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
               'Custom Worktag #10', 'Allocation Pool', 'Appropriation',
               'Related Calculation ID', 'Input Value', 'Related Calculation ID #2',
               'Input Value #2', 'Related Calculation ID #3', 'Input Value #3']]

    pid_all = pd.concat([pid_f,pid_2])
    
    write_to_csv(pid_all, 'payee_input_data.txt')
