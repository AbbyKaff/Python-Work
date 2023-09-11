##############################################################################
############################# Worker Address #################################
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
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    active_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    
    termed_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\employee_terms_all.csv", encoding='cp1251')
    
    loa_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    active_ees = pd.concat([active_ees, termed_ees,loa_ees])
    
    active_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\fix missing.csv")
    #------------------------------------------------------------------------------
    #cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|', encoding='cp1251')
    #active_ees['Employee Id'] = active_ees['Employee Id'].astype(str)
    #active_ees = active_ees.loc[active_ees['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_ees['Worker ID'] = active_ees['Employee Id']
    active_ees['Zip Code'] = active_ees['Zip Code'].astype(str).str.zfill(5)
    active_ees['Zip Code'] = active_ees['Zip Code'].astype(str)
    wfh = active_ees[active_ees['Default Location'] == 'Work From Home']
    wfh['Usage Type'] = 'Home'
    wfh['Primary'] = 'Y'
    wfh['Public'] = 'N'
    wfh['Address Line #1'] = wfh['Address 1']
    wfh['Address Line #2'] = wfh['Address 2']
    wfh['Postal Code'] = (wfh['Zip Code'])
    wfh['Work From Home Address'] = 'Y'
    wfh['Region'] = 'USA-' + wfh['State'].astype(str)

    wfh['Country ISO Code'] = 'USA'
    wfh['Source System'] = 'Kronos'

    wfh_f = wfh[['Worker ID','Source System','Primary','Usage Type','Public','Country ISO Code','Address Line #1','Address Line #2','City','Postal Code','Work From Home Address','Region']]

    wfw = active_ees[active_ees['Default Location'] != 'Work From Home']
    wfw['Usage Type'] = 'Home'
    wfw['Primary'] = 'Y'
    wfw['Public'] = 'N'
    wfw['Address Line #1'] = wfw['Address 1']
    wfw['Address Line #2'] = wfw['Address 2']
    wfw['Postal Code'] = (wfw['Zip Code'])
    wfw['Work From Home Address'] = 'N'
    wfw['Region'] = 'USA-' + wfw['State'].astype(str)
    #wfw['Cost Centers(Cost Center)'] = wfw['Default Cost Centers'].astype(int)

    wfw['Country ISO Code'] = 'USA'
    wfw['Source System'] = 'Kronos'

    wfw_f = wfw[['Worker ID','Source System','Primary','Usage Type','Public','Country ISO Code','Address Line #1','Address Line #2','City','Postal Code','Work From Home Address','Region']]


    os.chdir(r'C:\Users\akaff\KBP Investments')

    list_of_files = glob.glob('./KBP Contact Sheet - Current/*.xlsx')
    latest_file = max(list_of_files, key=os.path.getctime)
    foods = pd.read_excel(latest_file)
    bells = pd.read_excel(latest_file, sheet_name='All Stores - KBP Bells')
    bells.dropna(how = "all", inplace=True)
    insp = pd.read_excel(latest_file, sheet_name='All Stores - Arby')
    insp.dropna(how = "all", inplace=True)
    dat = pd.concat([foods,bells,insp])

    '''   
    wfw_all = wfw.merge(dat, left_on = 'Cost Centers(Cost Center)', right_on='Store Cost Center')

    wfw_all['Usage Type'] = 'Work'
    wfw_all['Primary'] = 'N'
    wfw_all['Public'] = 'Y'
    wfw_all['Address Line #1'] = wfw_all['Address']
    wfw_all['Address Line #2'] = wfw_all['Dual Address']
    wfw_all['City'] = wfw_all['City_y']
    wfw_all['Postal Code'] = (wfw_all['Zip Code_y'])
    wfw_all['Work From Home Address'] = 'N'
    wfw_all['Region'] = 'USA-' + wfw_all['State_y'].astype(str)

    wfw_work = wfw_all[['Worker ID','Source System','Primary','Usage Type','Public','Country ISO Code','Address Line #1','Address Line  #2','City','Postal Code','Work From Home Address','Region']]
    '''
    worker_address = pd.concat([wfh_f,wfw_f])

    worker_address['Address Effective Date'] = ''
    worker_address['Address Line #3'] = ''
    worker_address['Address Line #4'] = ''
    worker_address['Address Line #5'] = ''
    worker_address['Address Line #6'] = ''
    worker_address['Address Line #7'] = ''
    worker_address['Address Line #8'] = ''
    worker_address['Address Line #9'] = ''
    worker_address['City Subdivision 1'] = ''
    worker_address['City Subdivision 2'] = ''

    worker_address['Region Subdivision 1'] = ''
    worker_address['Region Subdivision 2'] = ''
    worker_address['Use For Reference 1'] = ''
    worker_address['Use For Reference 2'] = ''
    worker_address['Address Line #1 - Local'] = ''
    worker_address['Address Line #2 - Local'] = ''
    worker_address['Address Line #3 - Local'] = ''
    worker_address['Address Line #4 - Local'] = ''
    worker_address['Address Line #5 - Local'] = ''
    worker_address['Address Line #6 - Local'] = ''
    worker_address['Address Line #7 - Local'] = ''
    worker_address['Address Line #8 - Local'] = ''
    worker_address['Address Line #9 - Local'] = ''
    worker_address['City - Local'] = ''
    worker_address['City Subdivision 1 - Local'] = ''
    worker_address['City Subdivision 2 - Local'] = ''
    worker_address['Region Subdivision 1 - Local'] = ''
    worker_address['Region Subdivision 2 - Local'] = ''


    worker_address = worker_address[['Worker ID', 'Source System',
    'Address Effective Date',
    'Primary',
    'Usage Type',
    'Public',
    'Country ISO Code',
    'Address Line #1',
    'Address Line #2',
    'Address Line #3',
    'Address Line #4',
    'Address Line #5',
    'Address Line #6',
    'Address Line #7',
    'Address Line #8',
    'Address Line #9',
    'City',
    'City Subdivision 1',
    'City Subdivision 2',
    'Region',
    'Region Subdivision 1',
    'Region Subdivision 2',
    'Postal Code',
    'Use For Reference 1',
    'Use For Reference 2',
    'Work From Home Address',
    'Address Line #1 - Local',
    'Address Line #2 - Local',
    'Address Line #3 - Local',
    'Address Line #4 - Local',
    'Address Line #5 - Local',
    'Address Line #6 - Local',
    'Address Line #7 - Local',
    'Address Line #8 - Local',
    'Address Line #9 - Local',
    'City - Local',
    'City Subdivision 1 - Local',
    'City Subdivision 2 - Local',
    'Region Subdivision 1 - Local',
    'Region Subdivision 2 - Local']]

    worker_address['Worker ID'] = worker_address['Worker ID'].astype(int)
    write_to_csv(worker_address, 'worker_address_append_082323.txt')
