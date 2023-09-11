##############################################################################
########################## Worker Phone Numbers ##############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    contacts = pd.read_excel(config.PATH_WD_IMP + 'data files\\Q1 P3 2023 Contact Sheet FINAL (03.20.23).xlsx',
                    sheet_name='All Stores - FQSR')

    #remove non work people
    contacts_df = contacts.dropna(subset = ['Kronos EmpID'])
    contacts_df = contacts_df.dropna(subset = ['Phone Number'])

    contacts_df['Worker ID'] = contacts_df['Kronos EmpID']
    contacts_df['Source System'] = 'Contact Sheet'
    contacts_df['Type'] = 'Work'
    contacts_df['Primary'] = 'Y'
    contacts_df['Public'] = 'N'
    contacts_df['Country ISO Code'] = 'USA'
    contacts_df['Phone Extension'] = contacts_df['Extension']
    contacts_df['Phone Device Type'] = ''
    contacts_df['Phone Device Type'] = np.where(contacts_df['Extension']>=1, 'Landline', 'Mobile')

    contacts_df = contacts_df[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]

    contact_sheet_ids = contacts_df['Worker ID']

    contact_df2 = active_ees[active_ees['Employee Id'].isin(contact_sheet_ids) == False]

    contact_df2['Phone Number'] = np.where(contact_df2['Account Contact #1: Cell Phone'].isna(),contact_df2['Account Contact #1: Home Phone'],contact_df2['Account Contact #1: Cell Phone'])

    contact_df2_wp = contact_df2
    contact_df2_wp['Phone Number'] = contact_df2_wp['Account Contact #1: Work Phone']
    contact_df2_wp['Type'] = 'Work'
    contact_df2_wp['Primary'] = 'Y'
    contact_df2_wp['Public'] = 'N'
    contact_df2_wp = contact_df2_wp.dropna(subset = ['Phone Number'])
    contact_df2_wp['Phone Extension'] = ''
    contact_df2_wp['Phone Device Type'] = 'Mobile'
    contact_df2_wp = contact_df2_wp[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]

    contact_df2['Type'] = 'Home'
    contact_df2['Primary'] = ''
    contact_df2['Public'] = 'N'
    contact_df2['Phone Extension'] = ''
    contact_df2['Phone Device Type'] = 'Mobile'

    contact_df2 = contact_df2[['Worker ID', 'Source System', 'Type', 'Primary',
                                'Public', 'Country ISO Code', 'Phone Number',
                                'Phone Extension', 'Phone Device Type']]

    worker_phone = pd.concat([contacts_df, contact_df2, contact_df2_wp])

    worker_phone['International Phone Code'] = ''
    worker_phone['Area Code'] = ''
    worker_phone['Worker ID'] = worker_phone['Worker ID'].astype(int)
    worker_phone['Phone Extension'] = ''
    worker_phone = worker_phone[['Worker ID', 'Source System', 'Type', 'Primary',
                                'Public', 'Country ISO Code',
                                'International Phone Code', 'Area Code',
                                'Phone Number', 'Phone Extension',
                                'Phone Device Type']]

    write_to_csv(worker_phone, 'worker_phone.txt')
