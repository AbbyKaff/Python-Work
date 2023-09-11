##############################################################################
########################### Government Ids  ##################################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd
import os
import glob
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    active_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    terms = pd.read_csv(config.PATH_WD_IMP + 'data sources\\employee_terms_all.csv', dtype='object', encoding='cp1251')
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    active_ees = pd.concat([active_ees, terms,loas])
    active_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\fix missing.csv")

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    active_ees['Employee Id'] = active_ees['Employee Id'].astype(str)
    active_ees = active_ees.loc[active_ees['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_ees.rename(columns={"Employee Id": "Worker ID"}, inplace=True)

    gov_id_df = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx',
                    sheet_name='Worker Government IDs', skiprows=2, nrows=0)

    active_ees['Source System'] = 'Kronos'
    active_ees['Country ISO Code'] = 'USA'
    active_ees['Type'] = 'National'
    active_ees['Workday ID Type'] = 'USA-SSN'
    active_ees['ID'] = active_ees['SS#'].replace('-', '', regex=True).astype(str)
    gov_df = active_ees[['Worker ID','Country ISO Code','Type','Workday ID Type','ID']]
    gov_id_df = gov_id_df.drop(['Country ISO Code','Type','Workday ID Type','ID'],axis=1)

    gov_id_df['Worker ID'] = active_ees['Worker ID']

    gov_id_df = gov_id_df.merge(gov_df, on='Worker ID')

    gov_id_df = gov_id_df[['Worker ID', 'Source System', 'Country ISO Code', 'Type',
           'Workday ID Type', 'ID', 'Issued Date', 'Expiration Date',
           'Verification Date', 'Series - National ID',
           'Issuing Agency - National ID']]

    write_to_csv(gov_id_df, 'government_ids_append_082323.txt')
    
    #test = cut_off_ees.merge(gov_id_df, on='Worker ID')
