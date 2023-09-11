##############################################################################
###################### Local Home School District Tax #######################
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

    sch = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72604616.csv', dtype='object', encoding='cp1251')
    sch2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72879939.csv', dtype='object', encoding='cp1251')
    sch = pd.concat([sch, sch2])

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    sch['Employee Id'] = sch['Employee Id'].astype(str)
    sch = sch.loc[sch['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    sch['Employee ID'] = sch['Employee Id']
    sch['Source System'] = 'Kronos'
    sch['Company'] = sch['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})

    #sch.loc[sch['Effective As Of'] < '2023-01-01', 'Effective As Of'] = '2023-01-01'
    sch['Effective As Of'] = pd.to_datetime(sch['Created']).dt.strftime('%d-%b-%Y').str.upper()
    sch['Payroll Local Home School District Tax Authority Code'] = sch['Company Tax Name'].apply(lambda st: st[st.find("(")+1:st.find(")")])
    
    sch['Payroll Local Home School District Tax Authority Code']  =  np.where(sch['Tax State/Province'] == 'Ohio','39' + sch['Payroll Local Home School District Tax Authority Code'],'42' + sch['Payroll Local Home School District Tax Authority Code'])
                                                                                                                             
    sch['Exempt Indicator Pennsylvania'] = 'N'
    sch['Constant Text'] = ''
    sch['Previous Employer Deducted Amount Pennsylvania'] = ''
    sch['Inactive'] = 'N'

    sch = sch[['Employee ID', 'Source System', 'Company', 'Effective As Of',
               'Payroll Local Home School District Tax Authority Code',
               'Exempt Indicator Pennsylvania', 'Constant Text',
               'Previous Employer Deducted Amount Pennsylvania', 'Inactive']]

    #sch = pd.read_csv('E2E_school_district_tax.csv')
    write_to_csv(sch, 'school_district_tax.txt')
