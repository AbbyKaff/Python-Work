# -*- coding: utf-8 -*-
"""
Created on Thu Aug 17 22:14:10 2023

@author: akaff
"""

##############################################################################
####################### Local Other Tax Elections ########################
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

    kro_tax = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72905732.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    kro_tax['Employee Id'] = kro_tax['Employee Id'].astype(str)
    kro_tax = kro_tax.loc[kro_tax['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    #exclude = ['Work From Home','Home Office']
    #kro_tax = kro_tax[~(kro_tax['Location(1)'].isin(exclude))]
    work_del = pd.read_excel(config.PATH_WD_IMP + 'data files\\Kronos_Company_Taxes.xlsx')
    work_del_c = work_del[~work_del['Payroll Local Other Tax Authority Code'].isna()]
    work_del_c = work_del_c[['Tax Jurisdiction', 'Tax Type', 'Company Tax Name', 'WD EE Code','WD ER Code','Payroll State Tax Authority','Payroll Local County Tax Authority Code','Payroll Local City Tax Authority Code','Payroll Local Home School District Tax Authority Code','Payroll Local Other Tax Authority Code','Federal']]
    work_del_c = work_del_c.drop_duplicates()
    kro_tax = kro_tax.merge(work_del_c, left_on=['Company Tax Name'], right_on=['Company Tax Name'])
    

    kro_tax['Employee ID'] = kro_tax['Employee Id']
    kro_tax['Source System'] = 'Kronos'
    kro_tax['Company'] = kro_tax['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})
    kro_tax['Effective As Of'] = pd.to_datetime(kro_tax['Created']).dt.strftime('%d-%b-%Y').str.upper()
    kro_tax['Exempt'] = ''
    kro_tax['Inactive'] = 'N'
   
    #kro_tax['Company Tax Name'] = kro_tax['Company Tax Name'].str.replace("City Tax", "")

    kro_tax = kro_tax[['Employee ID',
      'Source System',
      'Company',
      'Effective As Of',
      'Payroll Local Other Tax Authority Code',
      'Exempt',
      'Inactive']]

    write_to_csv(kro_tax, 'local_other_tax_elections.txt')
