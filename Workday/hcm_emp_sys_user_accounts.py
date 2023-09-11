##############################################################################
######################## EMP-System User Accounts ############################
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

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877396.csv', dtype='object', encoding='cp1251')
    df2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\employee_terms_all.csv', dtype='object', encoding='cp1251')
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    df = pd.concat([df,df2,loas])
    df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\fix missing.csv")
    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    df['Employee Id'] = df['Employee Id'].astype(str)
    df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    
    contacts = pd.read_excel(config.PATH_WD_IMP + 'data files\\Q3 P9 2023 Contact Sheet PRELIM eff 08.15.2023.xlsx', sheet_name='KBP Contacts')

    #remove non work people
    contacts_df = contacts.dropna(subset = ['Kronos EmpID'])
    contacts_df = contacts_df.dropna(subset = ['E-mail'])
    contacts_df = contacts_df[contacts_df['Kronos EmpID'] != 'na']
    contacts_df = contacts_df[['Kronos EmpID','E-mail']]

    contacts_df['Kronos EmpID'] = contacts_df['Kronos EmpID'].astype(int)
    df['Employee Id'] = df['Employee Id'].astype(int)
    df = df.merge(contacts_df, left_on='Employee Id',right_on='Kronos EmpID', how='left')
    
    df['Employee ID'] = df['Employee Id']
    df['User Name'] = np.where(df['E-mail'].isna(),df['Username'].str.lower(),df['E-mail'].str.lower())
    
    df['Source System'] = 'Kronos'
    df['Password'] = 'Workday@1234'
    df['Require New Password at Next Sign In?'] = ''
    df['Exempt from Delegated Authentication'] = ''
    df['OpenID Connect Internal Identifier'] = ''
    df['User Language'] = ''

    df_sys_accounts = df[['Employee ID', 'Source System', 'User Name',
                          'Password', 'Require New Password at Next Sign In?',
                          'Exempt from Delegated Authentication',
                          'OpenID Connect Internal Identifier',
                          'User Language']]

    write_to_csv(df_sys_accounts, 'emp_sys_user_accounts_append_082323.txt')
