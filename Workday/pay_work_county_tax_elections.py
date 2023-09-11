##############################################################################
###################### Local Work County Tax Elections #######################
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

    cnt_tax = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72604615.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    cnt_tax['Employee Id'] = cnt_tax['Employee Id'].astype(str)
    cnt_tax = cnt_tax.loc[cnt_tax['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    #cnt_tax = cnt_tax[cnt_tax['Location(1)'] != 'Work From Home']
    #exclude_tax = ['Alexandria - OLF','Covington - OLF','Dry Ridge - OLF','Erlanger - OLF','Florence - OLF','Fort Thomas - OLF','Georgetown - OLF','Newport - OLF','Taylor OLF']
    #cnt_tax = cnt_tax[~cnt_tax['Company Tax Name'].isin(exclude_tax)]
    work_del = pd.read_excel(config.PATH_WD_IMP + 'data files\\Kronos_Company_Taxes.xlsx')
    work_del_c = work_del
    cnt_tax = cnt_tax.merge(work_del_c, left_on=['Company Tax Name'], right_on=['Company Tax Name'], how='left')
    cnt_tax = cnt_tax[cnt_tax['WD EE Code'].str.startswith('W_CNTYW')]
    cnt_tax['Employee ID'] = cnt_tax['Employee Id']
    cnt_tax['Source System'] = 'Kronos'
    cnt_tax['Company'] = cnt_tax['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})
    cnt_tax['Effective As Of'] = pd.to_datetime(cnt_tax['Created']).dt.strftime('%d-%b-%Y').str.upper()
    cnt_tax['County Additional Amount'] = cnt_tax[['Additional Allowances']]
    cnt_tax['Inactive'] = 'N'

    
    cnt_taxw = cnt_tax[['Employee ID', 'Source System', 'Company', 'Effective As Of',
                         'Payroll Local County Tax Authority Code',
                         'County Additional Amount', 'Inactive']]

    write_to_csv(cnt_taxw, 'cnty_tax_elections_work.txt')
