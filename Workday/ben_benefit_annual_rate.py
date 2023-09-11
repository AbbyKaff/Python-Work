##############################################################################
########################### Benefit Annual Rate ##############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd
import os
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    mp_bar = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72584718.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    mp_bar['Employee Id'] = mp_bar['Employee Id'].astype(str)
    mp_bar = mp_bar.loc[mp_bar['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    mp_bar = mp_bar.replace('^[-]$', '', regex=True)

    mp_bar_c = mp_bar[mp_bar['Employee Benefit Salary 1'] != '']

    mp_bar_c['Employee ID'] = mp_bar_c['Employee Id']
    mp_bar_c['Effective Date'] = '01-JAN-2023'
    mp_bar_c['Source System'] = 'Kronos'
    mp_bar_c['Benefit Annual Rate Type'] = 'Life_LTD_STD_BAR_MP'

    mp_bar_c = modify_amount(mp_bar_c, 'Employee Benefit Salary 1')
    mp_bar_c['Benefit Annual Rate'] = mp_bar_c['Employee Benefit Salary 1'].astype(float).round(2)
    mp_bar_c['Currency'] = 'USD'

    bar = mp_bar_c[['Employee ID',
                    'Source System',
                    'Effective Date',
                    'Benefit Annual Rate Type',
                    'Benefit Annual Rate',
                    'Currency']]

    write_to_csv(bar, 'ben_benefit_annual_rate.txt')
