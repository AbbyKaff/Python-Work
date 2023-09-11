##############################################################################
############################## Name & Email ##################################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd
import glob as glob
import os
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8,clean_up_workers)


# -----------------------------------------------------------------------------
def set_name(df):

    df['Worker ID'] = df['Employee Id']
    df['Source System'] = 'Kronos'
    df['Country ISO Code'] = 'USA'
    df['Legal First Name'] = df['First Name'].str.title()
    df['Legal Last Name'] = df['Last Name'].str.title()
    df['Email Address - Primary Home'] = df['Primary Email']
    df['Public - Primary Home'] = 'N'
    df['Preferred Last Name'] = ''

    #df.loc[(~(df['Nickname'].isna()) & (
      #  df['Nickname'].str.upper() != 'UNKNOWN') & (
     #  df['Nickname'] != df['First Name'])), 'Preferred First Name'] = (
      #  df['Nickname'].str.title())
            
    #np.where((~(df['Nickname'].isna())) & (df['Nickname'].str.upper()!= 'UNKNOWN') & (df['Nickname'] != df['First Name']), df['Nickname'].str.title())

    #df.loc[(~(df['Nickname'].isna()) & (
     #   df['Nickname'].str.upper() != 'UNKNOWN') & (
      #  df['Nickname'] != df['First Name'])), 'Preferred Last Name'] = (
       # df['Last Name'].str.title())

    name_email = df[['Worker ID', 'Source System',
                        'Country ISO Code', 'Legal First Name', 'Legal Last Name',
                        'Email Address - Primary Home','Public - Primary Home',
                        'Preferred First Name', 'Preferred Last Name']]

    return name_email


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\terms')
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    terms = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])
    terms = terms[pd.to_datetime(terms['Date Terminated']) > '12/31/2020']
    terms = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\fix missing.csv")
    terms = terms.drop_duplicates(subset=None, keep="first",inplace=False)
    terms = terms[terms['Employee Id'] != 'BeyondPay3']
    terms['Employee Id'] = terms['Employee Id'].astype(int)
    terms = terms.sort_values('Employee Id')
    terms = terms.groupby("Employee Id").first()
    terms.reset_index(inplace=True)
    sev = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867409.csv', dtype='object', encoding='cp1251') 
    terms = pd.concat([terms,sev])
    
    
    actives = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    actives['Date Hired'] = pd.to_datetime(actives['Date Hired'])
    terms['Employee Id'] = terms['Employee Id'].astype(str)
    terms = terms.loc[~terms['Employee Id'].isin(actives['Employee Id'].astype(str))]
    

    terms['Preferred First Name'] = np.where((~(terms['Nickname'].isna())) & (terms['Nickname'].str.upper()!= 'UNKNOWN') & (terms['Nickname'] != terms['First Name']), terms['Nickname'].str.title(), '')
    terms['Preferred First Name'] = np.where(terms['Preferred First Name'] == terms['First Name'],'',terms['Preferred First Name'])
    terms['Primary Email'] = terms['Primary Email'].fillna('kbp@noemail.com')
    terms.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\employee_terms_all.csv')

    terms = set_name(terms)
    
    actives = actives[actives['Hourly Pay']!= '-']
    #actives = actives[pd.to_datetime(actives['Date Hired']) <= '03/27/2023']
    #actives = actives[actives['Employee Status'].isin(['Active'])]
    
    actives['Preferred First Name'] = np.where((~(actives['Nickname'].isna())) & (actives['Nickname'].str.upper()!= 'UNKNOWN') & (actives['Nickname'] != actives['First Name']), actives['Nickname'].str.title(), '')
    actives['Preferred First Name'] = np.where(actives['Preferred First Name'] == actives['First Name'],'',actives['Preferred First Name'])
    
    actives['Cost Centers(Cost Center)'] = actives['Cost Centers(Cost Center)'].astype(int)
    email = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\store_email.csv")
    actives = actives.merge(email, left_on='Cost Centers(Cost Center)', right_on='Cost Center', how='left')
    
    actives['Primary Email'] = actives['Primary Email'].fillna(actives['Email'])    

    actives = set_name(actives)
    
    #loas = pd.read_excel(config.PATH_WD_IMP + 'data files\\Workday loa info.xlsx') 
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')

    #loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    #loas = loas[loas['Employee Id'].isin(['169276','134900','135501','5231','76947','2383','5473',
                                       # '71984','113628','121921','85692','9591','75037','165378',
                                       # '134094','153351','161770','133529','169720','105151',
                                       # '132206','102297','139531','123118','109892','7385',
                                       # '152679','24992','16585','115340','77663','153248'])]
    loas['Preferred First Name'] = np.where((~(loas['Nickname'].isna())) & (loas['Nickname'].str.upper()!= 'UNKNOWN') & (loas['Nickname'] != loas['First Name']), loas['Nickname'].str.title(), '')
    loas['Preferred First Name'] = np.where(loas['Preferred First Name'] == loas['First Name'],'',loas['Preferred First Name'])
    
    loas['Worker ID'] = loas['Employee Id']
    loas['Source System'] = 'Kronos'
    loas['Country ISO Code'] = 'USA'
    loas['Legal First Name'] = loas['First Name'].str.title()
    loas['Legal Last Name'] = loas['Last Name'].str.title()
    loas['Email Address - Primary Home'] = loas['Primary Email']
    loas.loc[~loas['Primary Email'].isna(), 'Public - Primary Home'] = 'N'
    loas['Preferred Last Name'] = ''
    loas['Primary Email'] = loas['Primary Email'].fillna('kbp@noemail.com')
    
    loas = loas[['Worker ID', 'Source System',
                        'Country ISO Code', 'Legal First Name', 'Legal Last Name',
                        'Email Address - Primary Home','Public - Primary Home',
                        'Preferred First Name', 'Preferred Last Name']]

    #loas = set_name(loas)
    
    name_email = pd.concat([actives, terms, loas])
    name_email = terms
    name_email = clean_up_workers(name_email, 'Worker ID')
    
    name_email.drop_duplicates(subset = ['Worker ID'], keep='first', inplace=True)
    #Fix actual name
    name_email.loc[name_email['Worker ID'] == '140643', 'Legal First Name'] = 'Null'
    #Check Nulls
    name_email.loc[name_email['Worker ID'].isna()]
    name_email['Preferred Last Name'] = np.where(name_email['Preferred First Name'] !='',name_email['Legal Last Name'],'')

    name_email['Applicant Source'] = ''
    name_email['Full Name for Singapore and Malaysia'] = ''
    name_email['Legal Middle Name'] = ''
    name_email['Legal Secondary Last Name'] = ''
    name_email['Name Prefix'] = ''
    name_email['Name Suffix'] = ''
    name_email['Family Name Prefix'] = ''
    name_email['Preferred Secondary Last Name'] = ''
    name_email['Preferred Name Prefix'] = ''
    name_email['Preferred Name Suffix'] = ''
    name_email['Preferred Middle Name'] = ''
    name_email['Additional Country ISO Code'] = ''
    name_email['Additional First Name'] = ''
    name_email['Additional Middle Name'] = ''
    name_email['Additional Last Name'] = ''
    name_email['Additional Secondary Last Name'] = ''
    name_email['Additional Name Type'] = ''
    name_email['Local Script First Name 1'] = ''
    name_email['Local Script Middle Name 1'] = ''
    name_email['Local Script Last Name 1'] = ''
    name_email['Local Script Secondary Name 1'] = ''
    name_email['Local Script First Name 2'] = ''
    name_email['Local Script Middle Name 2'] = ''
    name_email['Local Script Last Name 2'] = ''
    name_email['Local Script Secondary Name 2'] = ''
    name_email['Preferred Local Script First Name 1'] = ''
    name_email['Preferred Local Script Middle Name 1'] = ''
    name_email['Preferred Local Script Last Name 1'] = ''
    name_email['Preferred Local Script Secondary Name 1'] = ''
    name_email['Preferred Local Script First Name 2'] = ''
    name_email['Preferred Local Script Middle Name 2'] = ''
    name_email['Preferred Local Script Last Name 2'] = ''
    name_email['Preferred Local Script Secondary Name 2'] = ''
    name_email['Email Address - Additional Home'] = ''
    name_email['Public - Additional Home'] = ''
    name_email['Email Address - Additional Work'] = ''
    name_email['Public - Additional Work'] = ''
    name_email['Web Address'] = ''
    name_email['Usage Type - Web Address'] = ''
    name_email['Public - Web Address'] = ''
    name_email['Instant Messenger Address #1'] = ''
    name_email['Instant Messenger Provider #1'] = ''
    name_email['Usage Type - Instant Messenger #1'] = ''
    name_email['Public - Instant Messenger #1'] = ''
    name_email['Instant Messenger Address #2'] = ''
    name_email['Instant Messenger Provider #2'] = ''
    name_email['Usage Type - Instant Messenger #2'] = ''
    name_email['Public - Instant Messenger #2'] = ''
    name_email['Applicant ID'] = ''
    name_email['Email Address - Primary Work'] = ''
    name_email['Public - Primary Work'] = ''

    name_email = name_email[['Worker ID', 'Source System', 'Applicant Source',
                            'Country ISO Code', 'Full Name for Singapore and Malaysia',
                            'Legal First Name', 'Legal Middle Name', 'Legal Last Name',
                            'Legal Secondary Last Name', 'Name Prefix', 'Name Suffix',
                            'Family Name Prefix', 'Preferred First Name',
                            'Preferred Middle Name', 'Preferred Last Name',
                            'Preferred Secondary Last Name',
                            'Preferred Name Prefix', 'Preferred Name Suffix',
                            'Additional Country ISO Code', 'Additional First Name',
                            'Additional Middle Name', 'Additional Last Name',
                            'Additional Secondary Last Name', 'Additional Name Type',
                            'Local Script First Name 1', 'Local Script Middle Name 1',
                            'Local Script Last Name 1',
                            'Local Script Secondary Name 1',
                            'Local Script First Name 2',
                            'Local Script Middle Name 2',
                            'Local Script Last Name 2',
                            'Local Script Secondary Name 2',
                            'Preferred Local Script First Name 1',
                            'Preferred Local Script Middle Name 1',
                            'Preferred Local Script Last Name 1',
                            'Preferred Local Script Secondary Name 1',
                            'Preferred Local Script First Name 2',
                            'Preferred Local Script Middle Name 2',
                            'Preferred Local Script Last Name 2',
                            'Preferred Local Script Secondary Name 2',
                            'Email Address - Primary Home',
                            'Public - Primary Home', 'Email Address - Primary Work',
                            'Public - Primary Work', 'Email Address - Additional Home',
                            'Public - Additional Home',
                            'Email Address - Additional Work',
                            'Public - Additional Work',
                            'Web Address', 'Usage Type - Web Address',
                            'Public - Web Address', 'Instant Messenger Address #1',
                            'Instant Messenger Provider #1',
                            'Usage Type - Instant Messenger #1',
                            'Public - Instant Messenger #1',
                            'Instant Messenger Address #2',
                            'Instant Messenger Provider #2',
                            'Usage Type - Instant Messenger #2',
                            'Public - Instant Messenger #2', 'Applicant ID']]

    name_email.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_name_and_email_append_082323.txt', sep='|', encoding='utf-8', index=False)
    
    #Write master file for other files
    name_email.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|', encoding='utf-8', index=False)
