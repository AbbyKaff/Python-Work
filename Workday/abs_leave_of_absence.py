# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:41:58 2023

@author: akaff
"""

import pandas as pd
import numpy as np
import os
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
from api_call2 import get_rpt
import datetime
from datetime import date
from datetime import datetime
import xlrd
import glob as glob

import config
from common import (write_to_csv, active_workers, modify_amount, exclude_workers)



# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    leave_reason = pd.read_csv(config.PATH_WD_IMP + 'data files\\leave_reasons.csv', dtype='object',encoding='cp1251')
    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    df['Employee Id'] = df['Employee Id'].astype(str)
    df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    
    dfc = df.merge(leave_reason, on='Employee Id', how='left')
    dfc['Worker ID'] = dfc['Employee Id']
    dfc['First Day of Leave'] = np.where(dfc['First Day of Leave'].isna(),dfc['Custom - Last Day Worked'], dfc['First Day of Leave'])
    #dfc.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\leave_validation.csv')
    dfc['Leave Type'] = dfc['Leave Type'].fillna('Leave_of_Absence_(Employee/Self)')
    dfc['First Day of Leave'] = dfc['First Day of Leave'].fillna(dfc['Date Hired'])
    dfc['Last Day of Work'] = pd.to_datetime(dfc['First Day of Leave']).dt.strftime("%d-%b-%Y").str.upper()
    #pd.to_datetime(dfc['Custom - Last Day Worked']).dt.strftime("%d-%b-%Y").str.upper()
    dfc['Estimated Last Day of Leave'] = '01-JAN-2033' 
    dfc['First Day of Leave'] = pd.to_datetime(dfc['First Day of Leave']).dt.strftime("%d-%b-%Y").str.upper()
    dfc['Source System'] = 'Kronos'
    dfc['Position ID'] = ''
    dfc['Links Back to Prior Event'] = ''
    dfc['Stop Payment Date'] = ''
    dfc['Social Security Disability Code'] = ''
    dfc['Location During Leave'] = ''
    dfc['Caesarean Section Birth'] = ''
    dfc['Childs Date of Death'] = ''
    dfc['Stillbirth Baby Deceased'] = ''
    dfc['Date Baby Arrived Home From Hospital'] = ''
    dfc['Adoption Notification Date'] = ''
    dfc['Date Child Entered Country'] = ''
    dfc['Multiple Child Indicator'] = ''
    dfc['Number of Babies Adopted Children'] = ''
    dfc['Number of Previous Births'] = ''
    dfc['Number of Previous Maternity Leaves'] = ''
    dfc['Number of Child Dependents'] = ''
    dfc['Single Parent Indicator'] = ''
    dfc['Work Related'] = ''
    dfc['Comments'] = ''
    dfc['Reason'] = ''

    dfc = dfc[['Worker ID',
'Source System',
'Position ID',
'Leave Type',
'Reason',
'Last Day of Work',
'First Day of Leave',
'Estimated Last Day of Leave',
'Links Back to Prior Event',
'Stop Payment Date',
'Social Security Disability Code',
'Location During Leave',
'Caesarean Section Birth',
'Childs Date of Death',
'Stillbirth Baby Deceased',
'Date Baby Arrived Home From Hospital',
'Adoption Notification Date',
'Date Child Entered Country',
'Multiple Child Indicator',
'Number of Babies Adopted Children',
'Number of Previous Births',
'Number of Previous Maternity Leaves',
'Number of Child Dependents',
'Single Parent Indicator',
'Work Related',
'Comments']]
    
    write_to_csv(dfc, 'loa.txt')
