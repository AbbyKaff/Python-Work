##############################################################################
########################## Custom Work IDS ###################################
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

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72604657.csv', dtype='object', encoding='cp1251')
    terms = pd.read_csv(config.PATH_WD_IMP + 'data sources\\employee_terms_all.csv', dtype='object', encoding='cp1251')
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    df = pd.concat([df, terms,loas])

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    df['Employee Id'] = df['Employee Id'].astype(str)
    df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    df['Worker ID'] = df['Employee Id']
    df['Source System'] = 'Kronos'
    df['Custom ID'] = df['Employee Id']
    df['Custom ID Type'] = 'Kronos ID'
    df['Issued Date'] = pd.to_datetime(df['Created']).dt.strftime("%d-%b-%Y").str.upper()
    df['Expiration Date'] = ''
    df['Organization'] = ''
    df['Description'] = ''

    df_ee_id = df[['Worker ID', 'Source System', 'Custom ID',
                         'Custom ID Type', 'Issued Date', 'Expiration Date',
                         'Organization', 'Description']]

    # external Id
    df_ex = df[~pd.isnull(df['External Id'])]

    df_ex['Worker ID'] = df_ex['Employee Id']
    df_ex['Source System'] = 'Kronos'
    df_ex['Custom ID'] = df_ex['External Id']
    df_ex['Custom ID Type'] = 'KFC GPN'
    df_ex['Issued Date'] = ''
    df_ex['Expiration Date'] = ''
    df_ex['Organization'] = ''
    df_ex['Description'] = ''

    df_ex = df_ex[['Worker ID', 'Source System','Custom ID', 'Custom ID Type',
                   'Issued Date', 'Expiration Date', 'Organization',
                   'Description']]

    #rti employee number
    df_rti_1 = df[~pd.isnull(df['RTI Employee #'])]

    df_rti_1['Worker ID'] = df_rti_1['Employee Id']
    df_rti_1['Source System'] = 'Kronos'
    df_rti_1['Custom ID'] = df_rti_1['RTI Employee #']
    df_rti_1['Custom ID Type'] = 'RTI Employee #'
    df_rti_1['Issued Date'] = ''
    df_rti_1['Expiration Date'] = ''
    df_rti_1['Organization'] = ''
    df_rti_1['Description'] = ''

    df_rti_1 = df_rti_1[['Worker ID', 'Source System', 'Custom ID',
                         'Custom ID Type', 'Issued Date', 'Expiration Date',
                         'Organization', 'Description']]

    #rti external Ref
    df_rti2 = df[~pd.isnull(df['RTI External Ref'])]

    df_rti2['Worker ID'] = df_rti2['Employee Id']
    df_rti2['Source System'] = 'Kronos'
    df_rti2['Custom ID'] = df_rti2['RTI Employee #']
    df_rti2['Custom ID Type'] = 'RTI External Ref​'
    df_rti2['Issued Date'] = ''
    df_rti2['Expiration Date'] = ''
    df_rti2['Organization'] = ''
    df_rti2['Description'] = ''

    df_rti2 = df_rti2[['Worker ID', 'Source System','Custom ID', 'Custom ID Type',
                       'Issued Date', 'Expiration Date','Organization',
                       'Description']]

    #Par ID
    df_par = df[~pd.isnull(df['PAR ID'])]

    df_par['Worker ID'] = df_par['Employee Id']
    df_par['Source System'] = 'Kronos'
    df_par['Custom ID'] = df_par['PAR ID']
    df_par['Custom ID Type'] = 'PAR ID​'
    df_par['Issued Date'] = ''
    df_par['Expiration Date'] = ''
    df_par['Organization'] = ''
    df_par['Description'] = ''

    df_par = df_par[['Worker ID', 'Source System','Custom ID', 'Custom ID Type',
                     'Issued Date', 'Expiration Date','Organization',
                     'Description']]


    #ulti id
    df_uid = df[~pd.isnull(df['Ulti EE ID'])]

    df_uid['Worker ID'] = df_uid['Employee Id']
    df_uid['Source System'] = 'Kronos'
    df_uid['Custom ID'] = df_uid['Ulti EE ID'].str[:-2]
    df_uid['Custom ID Type'] = 'Ulti EE ID​'
    df_uid['Issued Date'] = ''
    df_uid['Expiration Date'] = ''
    df_uid['Organization'] = ''
    df_uid['Description'] = ''


    df_uid = df_uid[['Worker ID', 'Source System','Custom ID', 'Custom ID Type',
                     'Issued Date', 'Expiration Date','Organization',
                     'Description']]
    
    #Exec
    ex_df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\exec member ids.csv")
    exc_df = ex_df[ex_df['Type'] == 'EXEC']
    
    exc_df['Worker ID'] = exc_df['EMPID']
    exc_df['Source System'] = 'Kronos'
    exc_df['Custom ID'] = exc_df['EMPID']
    exc_df['Custom ID Type'] = 'Benefits Class Exec ID​'
    exc_df['Issued Date'] = ''
    exc_df['Expiration Date'] = ''
    exc_df['Organization'] = ''
    exc_df['Description'] = ''

    exc_df = exc_df[['Worker ID', 'Source System','Custom ID', 'Custom ID Type',
                     'Issued Date', 'Expiration Date','Organization',
                     'Description']]
    
    #Member
    exm_df = ex_df[ex_df['Type'] == 'MEMBER']
    
    exm_df['Worker ID'] = exm_df['EMPID']
    exm_df['Source System'] = 'Kronos'
    exm_df['Custom ID'] = exm_df['EMPID']
    exm_df['Custom ID Type'] = 'Benefits Class Member ID​'
    exm_df['Issued Date'] = ''
    exm_df['Expiration Date'] = ''
    exm_df['Organization'] = ''
    exm_df['Description'] = ''

    exm_df = exm_df[['Worker ID', 'Source System','Custom ID', 'Custom ID Type',
                     'Issued Date', 'Expiration Date','Organization',
                     'Description']]


    #Deferred Comp
    def_df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\deferred_comp_eligible.csv")

    def_df['Worker ID'] = def_df['Employee Id']
    def_df['Source System'] = 'Kronos'
    def_df['Custom ID'] = def_df['Employee Id']
    def_df['Custom ID Type'] = 'Benefits Def Comp ID​'
    def_df['Issued Date'] = ''
    def_df['Expiration Date'] = ''
    def_df['Organization'] = ''
    def_df['Description'] = ''

    def_df = def_df[['Worker ID', 'Source System','Custom ID', 'Custom ID Type',
                     'Issued Date', 'Expiration Date','Organization',
                     'Description']]

    #concat all files
    cust_ids = pd.concat([df_ee_id, df_ex, df_rti_1, df_rti2, df_par, df_uid,exc_df,exm_df,def_df])
    cust_ids['Worker ID'] = cust_ids['Worker ID'].astype(int)
    write_to_csv(cust_ids, 'worker_custom_ids.txt')
