##############################################################################
########################## Emergency Contact #################################
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

    active_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\E2E_name_and_email.txt', sep="|")
    active_ees['Employee Id'] = active_ees['Employee Id'].astype(str)
    active_ees = active_ees.loc[active_ees['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_ees.rename(columns={"Employee Id": "Worker ID"}, inplace=True)

    emerg_cont_df = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx', sheet_name='Emergency Contacts', skiprows=2, nrows=0)

    active_ees['Emergency Contact ID'] = (active_ees['Worker ID'].astype(str) + '-01').astype(str)
    active_ees['Related Person Relationship'] = active_ees['Account Contact #1: Relationship']
    active_ees['Related Person Relationship'] = active_ees['Related Person Relationship'].fillna('other')
    active_ees['Emergency Contact Priority'] = '1'

    active_ees['Legal Last Name'] = active_ees['Account Contact #1: Last Name']
    active_ees['Legal First Name'] = active_ees['Account Contact #1: First Name']

    active_ees = active_ees.loc[~active_ees['Legal Last Name'].isnull()]

    active_ees['Phone Number - Primary Home'] = np.where(active_ees['Account Contact #1: Home Phone'].notnull(), active_ees['Account Contact #1: Home Phone'], active_ees['Account Contact #1: Cell Phone'])
    active_ees['Phone Number - Primary Home'] = active_ees['Phone Number - Primary Home'].replace('-', '', regex=True).astype(str)
    active_ees['Phone Number - Primary Home'] = active_ees['Phone Number - Primary Home'].replace('nan', '', regex=True).astype(str)

    active_ees.loc[active_ees['Phone Number - Primary Home'] != '', 'Phone Device Type - Primary Home'] = 'Mobile'
    active_ees.loc[active_ees['Phone Number - Primary Home'] != '', 'Country ISO Code - Primary Home'] = 'USA'

    emerg_cont_df['Worker ID'] = active_ees['Worker ID']

    df2 = active_ees[['Worker ID', 'Emergency Contact ID', 'Related Person Relationship', 'Emergency Contact Priority', 'Phone Device Type - Primary Home', 'Country ISO Code - Primary Home', 'Phone Number - Primary Home', 'Legal First Name', 'Legal Last Name']]

    emerg_cont_df = emerg_cont_df.drop(['Emergency Contact ID', 'Related Person Relationship', 'Emergency Contact Priority', 'Phone Device Type - Primary Home', 'Country ISO Code - Primary Home', 'Phone Number - Primary Home', 'Legal First Name', 'Legal Last Name'],axis=1)

    emerg_cont_df = emerg_cont_df.merge(df2, on='Worker ID')

    emerg_cont_df = emerg_cont_df[['Worker ID', 'Source System', 'Emergency Contact ID',
           'Related Person Relationship', 'Emergency Contact Priority', 'Language',
           'Country ISO Code', 'Legal First Name', 'Legal Middle Name',
           'Legal Last Name', 'Name Prefix', 'Name Suffix',
           'Email Address - Primary Home', 'Email Address - Primary Work',
           'Email Address - Additional Home', 'Email Address - Additional Work',
           'Country ISO Code - Primary Home',
           'International Phone Code - Primary Home', 'Area Code - Primary Home',
           'Phone Number - Primary Home', 'Phone Extension - Primary Home',
           'Phone Device Type - Primary Home',
           'Country ISO Code - Additional Home #1',
           'International Phone Code - Additional Home #1',
           'Area Code - Additional Home #1', 'Phone Number - Additional Home #1',
           'Phone Extension - Additional Home #1',
           'Phone Device Type - Additional Home #1',
           'Country ISO Code - Additional Home #2',
           'International Phone Code - Additional Home #2',
           'Area Code - Additional Home #2', 'Phone Number - Additional Home #2',
           'Phone Extension - Additional Home #2',
           'Phone Device Type - Additional Home #2',
           'Country ISO Code - Primary Work',
           'International Phone Code - Primary Work', 'Area Code - Primary Work',
           'Phone Number - Primary Work', 'Phone Extension - Primary Work',
           'Phone Device Type - Primary Work',
           'Country ISO Code - Additional Work #1',
           'International Phone Code - Additional Work #1',
           'Area Code - Additional Work #1', 'Phone Number - Additional Work #1',
           'Phone Extension - Additional Work #1',
           'Phone Device Type - Additional Work #1',
           'Country ISO Code - Additional Work #2',
           'International Phone Code - Additional Work #2',
           'Area Code - Additional Work #2', 'Phone Number - Additional Work #2',
           'Phone Extension - Additional Work #2',
           'Phone Device Type - Additional Work #2', 'Country ISO Code - Home',
           'Address Line #1 - Home', 'Address Line #2 - Home', 'City - Home',
           'Region - Home', 'Region Subdivision - Home', 'Postal Code - Home',
           'Web Address', 'Usage Type - Web Address', 'Instant Messenger Address',
           'Instant Messenger Provider', 'Usage Type - Instant Messenger']]

    write_to_csv(emerg_cont_df, 'emergency_contacts.txt')
