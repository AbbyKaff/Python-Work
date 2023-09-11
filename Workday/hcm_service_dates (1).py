##############################################################################
############################ Service Dates ###################################
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

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72604657.csv', dtype='object', encoding='cp1251')
    df2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\employee_terms_all.csv', dtype='object', encoding='cp1251')
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    df = pd.concat([df,df2,loas])

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    df['Employee Id'] = df['Employee Id'].astype(str)
    df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    dfx = df.loc[~df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))] 
    #------------------------------------------------------------------------------

    df['Worker ID'] = df['Employee Id']
    df['Source System'] = 'Kronos'
    df['Created'] = pd.to_datetime(df['Created'])
    df['Date Hired'] = pd.to_datetime(df['Date Hired'])
    df['Date Re-Hired'] = pd.to_datetime(df['Date Re-Hired'])
    df['OG_H_Date'] = np.where(df['Date Re-Hired']<df['Date Hired'],df['Date Re-Hired'],df['Date Hired'])
    df['Original Hire Date'] = np.where(df['Created']<df['OG_H_Date'],df['Created'],df['OG_H_Date'])
    df['Original Hire Date'] = df['Original Hire Date'].dt.strftime("%d-%b-%Y").str.upper()
    df['Continuous Service Date'] = pd.to_datetime(df['Date Hired']).dt.strftime("%d-%b-%Y").str.upper()
    df['Expected Retirement Date'] = ''
    df['Retirement Eligibility Date'] = ''
    df['Seniority Date'] = ''
    df['Severance Date'] = ''
    df['Benefits Service Date'] = ''
    df['Company Service Date'] = pd.to_datetime(df['Service Date']).dt.strftime("%d-%b-%Y").str.upper()
    df['Time Off Service Date'] = np.where(~pd.isnull(df['Service Date']),df['Service Date'],df['Date Hired'])
    df['Time Off Service Date'] = pd.to_datetime(df['Time Off Service Date']).dt.strftime("%d-%b-%Y").str.upper()
    df['Vesting Date'] = ''

    service_dates = df[['Worker ID','Source System','Original Hire Date', 'Continuous Service Date',
           'Expected Retirement Date', 'Retirement Eligibility Date',
           'Seniority Date', 'Severance Date', 'Benefits Service Date',
           'Company Service Date', 'Time Off Service Date', 'Vesting Date']]

    service_dates = service_dates.fillna('')
    
    

    write_to_csv(service_dates, 'service_dates.txt')
