##############################################################################
################################## W-4 Elections #############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    mas_taxc = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72596865.csv', dtype='object', encoding='cp1251')
    #mas_taxt = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72869093.csv', dtype='object', encoding='cp1251')
    #mas_taxc = pd.concat([mas_taxc,mas_taxt])

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    mas_taxc['Employee Id'] = mas_taxc['Employee Id'].astype(str)
    mas_taxc = mas_taxc.loc[mas_taxc['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    mas_taxc = mas_taxc.replace('^[-]$', '', regex=True)
    mas_taxc = modify_amount(mas_taxc, 'Additional Withholding Amount ($)')
    mas_taxc = modify_amount(mas_taxc, 'Deduction')
    mas_taxc = modify_amount(mas_taxc, 'Other Income')
    mas_taxc = modify_amount(mas_taxc, 'Claim Dependent/Amount/Credits')

    mas_taxc['Employee ID'] = mas_taxc['Employee Id']
    mas_taxc['Effective From'] = pd.to_datetime(mas_taxc['Effective From'])
    mas_taxc['Date Hired'] = pd.to_datetime(mas_taxc['Date Hired'])
    mas_taxc['Effective As Of'] = np.where(mas_taxc['Effective From'] > mas_taxc['Date Hired'], mas_taxc['Effective From'], mas_taxc['Date Hired'])
    mas_taxc['Effective As Of'] = mas_taxc['Effective As Of'].dt.strftime("%d-%b-%Y").str.upper()
    
    mas_taxc['Effective As Of'] = pd.to_datetime(mas_taxc['Effective As Of'])
    mas_taxc['Date Hired'] = pd.to_datetime(mas_taxc['Date Hired'])
    mas_taxc['Effective As Of'] = np.where(mas_taxc['Effective As Of'] > mas_taxc['Date Hired'], mas_taxc['Effective As Of'], mas_taxc['Date Hired'])

    mas_taxc['Company'] = mas_taxc['Cost Centers(Company Code)'].replace({'FQSR':'FQ'})

    dict_marital_status = {'Head Of Household': 'Head of Household',
                            'Married': 'Married filing jointly (or Qualifying widow(er))',
                            'Married Filing Jointly': 'Married filing jointly (or Qualifying widow(er))',
                            'Single': 'Single or Married filing separately',
                            'Single Or Married Filing Separately': 'Single or Married filing separately',
                            'Married, Withhold At Higher Single Rate': 'Single or Married filing separately',
                            'Non-Residential Alien': 'Single or Married filing separately'}

    mas_taxc['Payroll W-4 Marital Status'] = mas_taxc['Filing Status'].replace(dict_marital_status)

    mas_taxc['Number of Allowances'] = mas_taxc['Withholding Exemptions']
    mas_taxc['Additional Amount'] = mas_taxc['Additional Withholding Amount ($)']
    mas_taxc['Total Dependent Amount'] = mas_taxc['Claim Dependent/Amount/Credits']
    mas_taxc['Deductions'] = mas_taxc['Deduction']
    mas_taxc['Source System'] = 'Kronos'
    mas_taxc['Multiple Jobs'] = mas_taxc['Two Jobs']
    mas_taxc['Multiple Jobs'] = mas_taxc['Multiple Jobs'].fillna('N')
    mas_taxc['Exempt'] = np.where(mas_taxc['EE Withholding Status'] == 'Exempt','Y','N')
    mas_taxc['Nonresident Alien'] = np.where(mas_taxc['Filing Status'] == 'Non-Residential Alien', 'Y', 'N')
    mas_taxc['Exempt from NRA Additional Amount'] = 'N'
    mas_taxc['Lock in Letter'] = 'N'
    mas_taxc['No Wage No Tax Indicator'] = 'N'

    mas_taxf = mas_taxc[['Employee ID', 'Source System', 'Effective As Of', 'Company',
                        'Payroll W-4 Marital Status', 'Number of Allowances',
                        'Additional Amount', 'Multiple Jobs', 'Total Dependent Amount',
                        'Other Income', 'Deductions', 'Exempt', 'Nonresident Alien',
                        'Exempt from NRA Additional Amount', 'Lock in Letter',
                        'No Wage No Tax Indicator']]

    write_to_csv(mas_taxf, 'w_4_elections_082823.txt')
