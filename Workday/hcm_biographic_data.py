##############################################################################
########################### biographic_data ##################################
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
    terms = pd.read_csv(config.PATH_WD_IMP + 'data sources\\employee_terms_all.csv', dtype='object', encoding='cp1251')
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    active_ees = pd.concat([active_ees, terms,loas])

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    active_ees['Employee Id'] = active_ees['Employee Id'].astype(str)
    active_ees = active_ees.loc[active_ees['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    bio_data_c = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx',
                    sheet_name='Biographic Data', skiprows=2, nrows=0)

    bio_data_c['Worker ID'] = active_ees['Employee Id']

    bio_merge = active_ees[['Employee Id', 'Date Birthday', 'Gender']]

    bio_merge.rename(columns={'Employee Id': 'Worker ID',
                              'Date Birthday': 'Date of Birth'},
                              inplace=True)
    bio_data_c = bio_data_c.drop(['Date of Birth'],axis=1)
    bio_data_c2 = bio_data_c.merge(bio_merge, left_on='Worker ID', right_on='Worker ID')
    bio_data_c2['Source System'] = 'Kronos'
    bio_data_c2['Date of Birth'] = pd.to_datetime(bio_data_c2['Date of Birth']).dt.strftime("%d-%b-%Y").str.upper()

    bio_data_c2['Gender'] = bio_data_c2['Gender_y'].replace({'M': 'Male', 'F': 'Female','U':'Choose not to Disclose','':'Choose not to Disclose'})
    bio_data_c2['Gender'] = bio_data_c2['Gender'].fillna('Choose not to Disclose')
    biographic_data = bio_data_c2[['Worker ID', 'Source System', 'Date of Birth',
            'Country of Birth', 'Region of Birth', 'City of Birth',
            'Date of Death', 'Gender', 'Last Medical Exam Date',
            'Last Medical Exam Valid To', 'Medical Exam Notes', 'Disability #1',
            'Disability Status Date #1',
            'Disability Date Known #1', 'Disability End Date #1',
            'Disability Grade #1', 'Disability Degree #1',
            'Disability Remaining Capacity #1',
            'Disability Certification Authority #1', 'Disability Certified At #1',
            'Disability Certification ID #1', 'Disability Certification Basis #1',
            'Disability Severity Recognition Date #1',
            'Disability FTE Toward Quota #1', 'Disability Work Restrictions #1',
            'Disability Accommodations Requested #1',
            'Disability Accommodations Provided #1',
            'Disability Rehabilitation Requested #1',
            'Disability Rehabilitation Provided #1', 'Note #1', 'Disability #2',
            'Disability Status Date #2', 'Disability Date Known #2',
            'Disability End Date #2', 'Disability Grade #2', 'Disability Degree #2',
            'Disability Remaining Capacity #2',
            'Disability Certification Authority #2', 'Disability Certified At #2',
            'Disability Certification ID #2', 'Disability Certification Basis #2',
            'Disability Severity Recognition Date #2',
            'Disability FTE Toward Quota #2', 'Disability Work Restrictions #2',
            'Disability Accommodations Requested #2',
            'Disability Accommodations Provided #2',
            'Disability Rehabilitation Requested #2',
            'Disability Rehabilitation Provided #2', 'Note #2',
            'Tobacco User Status', 'Blood Type', 'Sexual Orientation',
            'Gender Identity', 'Pronoun', 'LGBT Identification #1',
            'LGBT Identification #2', 'Relative Type #1',
            'Country ISO Code-Relative Name #1', 'Prefix Data - Title Reference #1',
            'Prefix Data - Salutation Reference #1', 'First Name #1',
            'Middle Name #1', 'Last Name #1', 'Secondary Last Name #1',
            'Social Suffix #1', 'Academic Suffix #1', 'Hereditary Suffix #1',
            'Honorary Suffix #1', 'Professional Suffix #1', 'Religious Suffix #1',
            'Royal Suffix #1', 'Relative Type #2',
            'Country ISO Code-Relative Name #2', 'Prefix Data - Title Reference #2',
            'Prefix Data - Salutation Reference #2', 'First Name #2',
            'Middle Name #2', 'Last Name #2', 'Secondary Last Name #2',
            'Social Suffix #2', 'Academic Suffix #2', 'Hereditary Suffix #2',
            'Honorary Suffix #2', 'Professional Suffix #2', 'Religious Suffix #2',
            'Royal Suffix #2']]

    write_to_csv(biographic_data, 'biographic_data.txt')
