##############################################################################
########################## EMP-Terminations ##################################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    filename = config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx'
    emp_terms_df = pd.read_excel(filename, sheet_name='EMP-Terminations', skiprows=2, nrows=0)

    filename = config.PATH_WD_IMP + 'data sources\\employee_terms_all.csv'
    df2 = pd.read_csv(filename, dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    df2['Employee Id'] = df2['Employee Id'].astype(str)
    df2 = df2.loc[df2['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    #------------------------------------------------------------------------------
    # Remove anyone from file who is currently active
    actives = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    actives['Date Hired'] = pd.to_datetime(actives['Date Hired'])
    actives = actives[pd.to_datetime(actives['Date Hired']) <= '08/14/2023']
    actives = actives[actives['Employee Status'] == 'Active']

    actives['Employee Id'] = actives['Employee Id'].astype(str)
    df2 = df2.loc[~df2['Employee Id'].isin(actives['Employee Id'].astype(str))]
    #------------------------------------------------------------------------------

    filename = config.PATH_WD_IMP + 'data files\\term_reasons.xlsx'
    df_term_reasons = pd.read_excel(filename, header=0)

    emp_terms_df['Employee ID'] = df2['Employee Id']
    df2['Employee ID'] = df2['Employee Id']
    df2['Termination Date'] = pd.to_datetime(df2['Date Terminated']).dt.strftime('%d-%b-%Y').str.upper()
    # df2['Termination Date'] = df2['Date Terminated']
    # df2 = df2[pd.to_datetime(df2['Date Terminated']) > '1/20/2021']

    df2 = df2.merge(df_term_reasons, left_on='Termination Reason (Last)', right_on='Kronos_Term_Reason', how='left')
    df2['Primary Reason'] = df2['WD_Term_Reason']
    df2.loc[df2['Primary Reason'].isnull(), 'Primary Reason'] = 'Voluntary_New Job'

    terms = df2[['Employee ID', 'Termination Date', 'Primary Reason']]

    #------------------------------------------------------------------------------
    # Keep only one row - max Term Date - of each EE
    list_group_by = ['Employee ID', 'Primary Reason']
    terms['Termination Date'] = pd.to_datetime(terms['Termination Date'])
    terms = terms.groupby(list_group_by, as_index=False)['Termination Date'].max()
    terms['Termination Date'] = pd.to_datetime(terms['Termination Date']).dt.strftime('%d-%b-%Y').str.upper()
    terms['Last Day of Work'] = pd.to_datetime(terms['Termination Date']).dt.strftime('%d-%b-%Y').str.upper()
    #------------------------------------------------------------------------------

    emp_terms_df = emp_terms_df.drop(['Termination Date', 'Last Day of Work', 'Primary Reason'], axis=1)
    emp_terms_df = emp_terms_df.merge(terms, on='Employee ID')

    emp_terms_df['Source System'] = 'Kronos'

    emp_terms_df = emp_terms_df[['Employee ID', 'Source System', 'Termination Date', 'Last Day of Work',
           'Primary Reason', 'Secondary Reason', 'Local Termination Reason',
           'Pay Through Date', 'Resignation Date', 'Agreement Signature Date',
           'Dismissal Process Start Date', 'Notify Employee By Date',
           'Notice Period Start Date', 'Regrettable', 'Eligible for Rehire']]

    write_to_csv(emp_terms_df, 'emp_terminations.txt')
