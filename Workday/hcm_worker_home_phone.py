##############################################################################
########################## Worker Phone Numbers ##############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd
import glob as glob
import os
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    contacts = pd.read_excel(config.PATH_WD_IMP + 'data files\\Q3 P9 2023 Contact Sheet PRELIM eff 08.15.2023.xlsx', sheet_name='KBP Contacts')

    #remove non work people
    contacts_df = contacts.dropna(subset = ['Kronos EmpID'])
    contacts_df = contacts_df.dropna(subset = ['Phone Number'])

    contacts_df['Worker ID'] = contacts_df['Kronos EmpID'].astype('Int64')
    contacts_df['Source System'] = 'Contact Sheet'
    contacts_df['Type'] = 'Work'
    contacts_df['Primary'] = 'Y'
    contacts_df['Public'] = 'N'
    contacts_df['Country ISO Code'] = 'USA'
    contacts_df['Phone Extension'] = contacts_df['Extension']
    contacts_df['Phone Device Type'] = ''
    contacts_df['Phone Device Type'] = np.where(contacts_df['Extension']>=1, 'Landline','Mobile')

    print(contacts_df['Source System'])
    contacts_df = contacts_df[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]

    contact_sheet_ids = contacts_df['Worker ID']

    active_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71250922.csv', dtype='object', encoding='cp1251')
    active_ees['Date Hired'] = pd.to_datetime(active_ees['Date Hired'])
    termed_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\employee_terms_all.csv", encoding='cp1251')
    
    loa_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    active_ees = pd.concat([active_ees, termed_ees,loa_ees])
    active_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\fix missing.csv")
    
    # active_ees = active_ees[active_ees['Date Hired'] < '01/21/2023']
    #active_ees = active_ees[active_ees['Employee Status'] == 'Active']
    active_ees['Source System'] = 'Kronos'
    active_ees['Country ISO Code'] = 'USA'

    print(active_ees.columns)
    active_ees = active_ees.rename(columns={'Employee Id': 'Employee ID'})
    print(active_ees.columns)
    contact_df2 = active_ees[active_ees['Employee ID'].isin(contact_sheet_ids) == False]

    #contact_df2['Phone Number'] = np.where(contact_df2['Home Phone'].isna(),contact_df2['Work Phone'],contact_df2['Home Phone'])

    contact_df2_wp = contact_df2
    contact_df2_wp['Phone Number'] = contact_df2_wp['Work Phone']
    contact_df2_wp['Type'] = 'Work'
    contact_df2_wp['Primary'] = 'Y'
    contact_df2_wp['Public'] = 'N'
    contact_df2_wp = contact_df2_wp.dropna(subset = ['Phone Number'])
    contact_df2_wp['Phone Extension'] = ''
    contact_df2_wp['Phone Device Type'] = 'Mobile'

    print(contact_df2_wp.columns)
    contact_df2_wp = contact_df2_wp.rename(columns={'Employee ID': 'Worker ID'})
    contact_df2_wp = contact_df2_wp[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]
    contact_df2['Phone Number'] = contact_df2['Home Phone']
    contact_df2['Type'] = 'Home'
    contact_df2['Primary'] = 'Y'
    contact_df2['Public'] = 'N'
    contact_df2['Phone Extension'] = ''
    contact_df2['Phone Device Type'] = 'Mobile'

    print(contacts_df['Source System'])

    contact_df2 = contact_df2.rename(columns={'Employee ID': 'Worker ID'})
    contact_df2 = contact_df2[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]

    worker_phone = pd.concat([contacts_df, contact_df2, contact_df2_wp])

    worker_phone['Phone Number'] = worker_phone['Phone Number'].str.replace(r'\D+', '', regex=True)
    worker_phone = worker_phone.loc[~worker_phone['Phone Number'].isnull()]
    worker_phone = worker_phone.loc[worker_phone['Phone Number'] != '0']
    worker_phone = worker_phone.loc[worker_phone['Phone Number'] != '']

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep="|")
    worker_phone['Worker ID'] = worker_phone['Worker ID'].astype(str)
    worker_phone = worker_phone.loc[worker_phone['Worker ID'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    worker_phone.drop_duplicates(inplace=True)
    worker_phone.sort_values(by = ['Worker ID'],inplace = True)
    worker_phone['Primary'] = (~worker_phone['Worker ID'].duplicated().astype(int))
    worker_phone['Primary'] = np.where(worker_phone['Primary'] == -1,'Y','N')
    worker_phone['International Phone Code'] = ''
    worker_phone['Area Code'] = ''
    worker_phone['Worker ID'] = worker_phone['Worker ID'].astype(int)
    worker_phone['Phone Extension'] = ''
    worker_phone = worker_phone[['Worker ID', 'Source System', 'Type',
                                'Primary', 'Public', 'Country ISO Code',
                                'International Phone Code',
                                'Area Code', 'Phone Number', 'Phone Extension',
                                'Phone Device Type']]


    write_to_csv(worker_phone, 'worker_phone_append_082323.txt')
