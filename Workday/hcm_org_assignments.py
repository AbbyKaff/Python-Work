##############################################################################
######################## Organization Assignments ############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd
import os
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877396.csv', dtype='object', encoding='cp1251')
    df2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\employee_terms_all.csv', dtype='object', encoding='cp1251')
    df2['Default Cost Centers'] = df2['Default Cost Centers'].str[:-2]
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    #df2['Default Cost Centers'] = df2['Default Cost Centers'].str[:-2]
    df = pd.concat([df, df2, loas])
    df['Employee Id'] = df['Employee Id'].astype(int)
    df = df.sort_values('Employee Id')
    df = df.groupby("Employee Id").first()
    df.reset_index(inplace=True)

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\worker_id_fix.csv')
    df['Employee Id'] = df['Employee Id'].astype(str)
    df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    df.rename(columns={"Employee Id": "Worker ID"}, inplace=True)
    df['Default Cost Centers'] = df['Default Cost Centers'].astype(int)
    df['Cost Center Organization'] = "CC" + (df['Default Cost Centers'].astype(str).str.zfill(5))
    df['Company Organization'] = df['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})
    df['Source System'] = 'Kronos'
    df['Position ID'] = ''
    df['Effective Date'] = ''
    df['Region Organization'] = ''
    df['Business Unit'] = ''
    df['Custom Org #1'] = ''
    df['Custom Org #2'] = ''
    df['Custom Org #3'] = ''
    df['Custom Org #4'] = ''
    df['Custom Org #5'] = ''
    df['Custom Org #6'] = ''
    df['Custom Org #7'] = ''
    df['Custom Org #8'] = ''
    df['Custom Org #9'] = ''

    df_orga= df[['Worker ID', 'Source System', 'Position ID', 'Effective Date',
                 'Cost Center Organization', 'Company Organization',
                 'Region Organization', 'Business Unit', 'Custom Org #1',
                 'Custom Org #2', 'Custom Org #3', 'Custom Org #4', 'Custom Org #5',
                 'Custom Org #6', 'Custom Org #7', 'Custom Org #8', 'Custom Org #9']]
    #df_orga['Effective Date'] = pd.to_datetime(df_orga['Effective Date'])
    #df_orga[df_orga.groupby('Worker ID')['Effective Date'].transform('max') == df_orga['Effective Date']]
    #df_orga['Cost Center Organization'] = df_orga['Cost Center Organization'].str[:-2] 
    write_to_csv(df_orga, 'organization_assignments.txt')
    
    #df_orgs = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\org_assign_miss.csv")
