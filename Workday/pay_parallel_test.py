# -*- coding: utf-8 -*-
"""
Created on Wed May 24 14:54:00 2023

@author: akaff
"""

import pandas as pd
import numpy as np
import os
import datetime
from datetime import date
from datetime import datetime
#import xlrd
import glob as glob
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, open_as_utf8, modify_amount)

# -----------------------------------------------------------------------------
def get_tax_code_ee(df):

    df_tax_codes = pd.read_excel(config.PATH_WD_IMP+ 'data files\\Kronos_Company_Taxes.xlsx')

    df['E/D/T Code'] = df['E/D/T Code'].astype(str)
    df_tax_codes['Company Tax Code'] = df_tax_codes['Company Tax Code'].astype(str)
    df_tax_codes = df_tax_codes[['Tax Jurisdiction', 'Tax Type', 'Company Tax Code', 'WD EE Code']]
    df_tax_codes = df_tax_codes.drop_duplicates()
    df = df.merge(df_tax_codes, left_on='E/D/T Code', right_on='Company Tax Code', how='left')
    df.drop_duplicates(inplace=True)

    return df


# -----------------------------------------------------------------------------
def set_federal(df):

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'FICA')
    ), 'Deduction Code'] = 'W_OAS'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'FIT')
    ), 'Deduction Code'] = 'W_FW'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'FUTA')
    ), 'Deduction Code'] = 'DNU'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'MEDI')
    ), 'Deduction Code'] = 'W_MED'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Type'] == 'MEDI2')
    ), 'Deduction Code'] = 'W_AMEDT'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'New Mexico Worker\'s Compensation Assessment Fee - Employer')
    ), 'Deduction Code'] = 'W_NMWCE'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'New Mexico Worker\'s Compensation Assessment Fee - Employer')
    ), 'Payroll Local Other Tax Authority Code'] = '35WD025'

    return df


# -----------------------------------------------------------------------------
def set_state_tax_authority(df):

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Type'].isin(['SDI', 'SIT','SUTA','WC']))
    ), 'Payroll State Tax Authority'] = df['E/D/T Code'].str[-2:]
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'].str.startswith('SUI', na=False))
    ), 'Payroll State Tax Authority'] = df['E/D/T Code'].str[-2:]
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NJ SUI')
    ), 'Payroll State Tax Authority'] = 'NJ'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'PA SUI')
    ), 'Payroll State Tax Authority'] = 'PA'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Type'].isin(['SIT', 'SUI']))
    ), 'Deduction Code'] = df['WD EE Code'].str[:5]
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Jurisdiction'] == 'State')
        & (df['Tax Type'].isin(['FLI']))
    ), 'Payroll State Tax Authority'] = df['WD EE Code'].str[3:4]
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Jurisdiction'] == 'State')
        & (df['Tax Type'].isin(['FLI','SDI','WC','SUTA']))
    ), 'Deduction Code'] = df['WD EE Code'].str[:7]
   
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Colorado Paid Family and Medical Leave')
    ), 'Deduction Code'] = 'W_COSPF'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Colorado Paid Family and Medical Leave')
    ), 'Payroll Local Other Tax Authority Code'] = 'M08WD001'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NJ FLI')
    ), 'Deduction Code'] = 'W_NJFAM'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NJ FLI')
    ), 'Payroll Local Other Tax Authority Code'] = 'NJFLI'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'New York Family Leave Benefits')
    ), 'Deduction Code'] = 'W_NYPFL'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'New York Family Leave Benefits')
    ), 'Payroll Local Other Tax Authority Code'] = 'NYPFL'
    #df_states = pd.read_csv(config.PATH_WD_IMP + 'data files\\wd_states.csv',encoding="cp1251")
    #df = df.merge(df_states[['Abbrev', 'Payroll']], right_on='Abbrev', left_on='Payroll State Tax Authority', how='left')
    #df['Payroll State Tax Authority'] = df['Payroll'].astype('Int64')
    

    return df


# -----------------------------------------------------------------------------
def set_county_tax_authority(df):

    col = 'Payroll Local County Tax Authority Code'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (df_all['Tax Type'] == 'CNTY')
    ), col] = df['WD EE Code'].str.replace('W_CNTYR', '')

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (df_all['Tax Type'] == 'CNTY')
    ), 'Deduction Code'] = 'W_CNTYR'
    
    return df


# -----------------------------------------------------------------------------
def set_city_tax_authority(df):

    col = 'Payroll Local City Tax Authority Code'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Jurisdiction'] == 'Local')
        & (df['Tax Type'] == 'CITY')
    ), col] = df['WD EE Code'].str.replace('W_CITYR', '')

    df[col] = df[col].str.replace('W_CITYW', '')
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Jurisdiction'] == 'Local')
        & (df['Tax Type'] == 'CITY')
    ), 'Deduction Code'] = df['WD EE Code'].str[:7]

    return df


# -----------------------------------------------------------------------------
def set_local_home_tax_authority(df):

    col = 'Payroll Local Home School District Tax Authority Code'

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (df_all['Tax Type'] == 'SCHL')
    ), col] = df_all['WD EE Code'].str.replace('W_SCHDR', '')
    
    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (df_all['Tax Type'] == 'SCHL')
    ), 'Deduction Code'] = 'W_SCHDR'

    return df


# -----------------------------------------------------------------------------
def set_local_other_tax_authority(df):

    col = 'Payroll Local Other Tax Authority Code'

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (~df_all['Tax Type'].isin(['CITY', 'CNTY', 'SCHL']))
    ), col] = df_all['WD EE Code'].str[7:]
    
    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (~df_all['Tax Type'].isin(['CITY', 'CNTY', 'SCHL']))
    ), 'Deduction Code'] = df_all['WD EE Code'].str[:7]
    
    return df

# -----------------------------------------------------------------------------
def set_federal_tax_type(df):
    federal_types = ['W_MEDER','W_OASER','W_MED','W_OAS','W_FW','W_FUI','W_AMEDT']
    
    df.loc[(df['ElementValue'].isin(federal_types)),'StateProvinceTaxAuthority'] = 'Federal'
    
    return df


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df_all = pd.DataFrame()

    # for filename in glob.glob(config.PATH_WD_IMP + 'pay_hist\\EarningDeductionTaxListing_01_23.csv'):
    #for filename in glob.glob(config.PATH_WD_IMP + 'pay_hist\\EarningDeductionTaxListing_*_23.csv'):
     #   print(filename)

      #  df = pd.read_csv(filename, dtype='object', encoding='cp1251')

       # df_all = pd.concat([df_all, df], axis=0)
        #print(len(df.index))
        #rint(len(df_all.index))
    os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\payroll parallel')
    df_p1_1 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\payroll parallel\payroll_hist_parallel_2_biweekly.csv",encoding='cp1251')
    df_p1_1 = df_p1_1[df_p1_1['Payroll Name'].str.contains('Regular')]
    #print validation data
    df_p1_1.to_csv('payroll_parallel_2_validation.csv')
    df_p1_2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\payroll parallel\payroll_hist_parallel_2_weekly.csv",encoding='cp1251')
    df_p1_2 = df_p1_2[df_p1_2['Payroll Name'].str.contains('Regular')]
    #print validation data
    df_p1_2.to_csv('payroll_parallel_2_validation_weekly.csv')
    #df_p2_2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\payroll parallel\payroll_hist_parallel_2_biweekly.csv",encoding='cp1251')
    #df_p2_2 = df_p2_2[(df_p2_2['Payroll Name'].str.contains('Bi-weekly')) | (df_p2_2['Payroll Name'].str.contains('Bi-Weekly'))]
    #print validation data
    #df_p2_2.to_csv('payroll_parallel_2_validation_biweekly.csv')
    
    
    #df_all = pd.concat([df_p1,df_p2_1,df_p2_2])
    df_all = pd.concat([df_p1_1,df_p1_2])
    #------------------------------------------------------------------------------
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
    #df_all['Employee Id'] = df_all['Employee Id'].astype(str)
    #df_all = df_all.loc[df_all['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    
    df_all = df_all.replace('^[-]$', '', regex=True)

    df_all = modify_amount(df_all, 'Record Gross Subject Wages')
    df_all = modify_amount(df_all, 'Record Gross Wages')
    df_all = modify_amount(df_all, 'Record Subject Wages')
    df_all = modify_amount(df_all, 'Record Amount')
    df_all = modify_amount(df_all, 'Record Amount (ER)')
    df_all = modify_amount(df_all, 'Net Payment + Cash + Fringe')
    df_all['Record Amount'] = df_all['Record Amount'].astype(float)
    df_all['Record Amount (ER)'] = df_all['Record Amount (ER)'].astype(float)
    
    df_all['ElementType'] = np.where(df_all['Record Type'].str.startswith('Tax'), 'TAX',
                            np.where(df_all['Record Type'].str.startswith('Deduc'),'DED','ERN'))
    
    #Add in net pay column prior to repulling data
    #df_all['Net Pay + Cash + Fringe'] = ''


    # df_all = df_all.loc[df_all['Type'] == 'Regular']
    # check blanks before removing
    xx = df_all[df_all['E/D/T Code'].isnull()]
    # set(xx['Record Type'])
    df_all = df_all.loc[~df_all['E/D/T Code'].isnull()]
    df_all = df_all.loc[(df_all['Record Amount'] != 0)]
    print(len(df_all.index))
    
    
    df_all = df_all.rename(columns={'Employee Id': 'EmployeeID',
                                    'Cost Centers(Cost Center)': 'CostCenter',
                                    'Record Amount': 'Amount',
                                    'Record Gross Subject Wages': 'TaxableWages',
                                    'Record Gross Wages': 'GrossPay',
                                    'Record Subject Wages': 'SubjectWages',
                                    'Net Payment + Cash + Fringe':'NetPay'})
    
    
    dict_company = {'KBP Foods': 'FQ',
                    'KBP Bells': 'TB',
                    'Restaurant Services Group': 'RS',
                    'KBP Inspired': 'RB',
                    'KBP Cares': 'KC'}

    df_all['Company'] = df_all['Employee EIN'].replace(dict_company)
    df_all['CostCenter'] = 'CC' + df_all['CostCenter'].astype(str).str.zfill(5)

    earning_ded_codes = pd.read_csv(config.PATH_WD_IMP + 'data files\earning_ded_codes.csv')
    df_all = df_all.merge(earning_ded_codes[['Earning Code (Legacy System)', 'Earning Code*']],
                          left_on='E/D/T Code',
                          right_on='Earning Code (Legacy System)', how='left')

    print(df_all.columns)

    df_all.loc[df_all['Record Type'].str.startswith('Earning', na=False), 'Earning Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Fringe', na=False), 'Earning Code'] = df_all['Earning Code*']

    df_all.loc[df_all['Record Type'].str.startswith('Deduction', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Tax', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Reimbursement', na=False), 'Deduction Code'] = df_all['Earning Code*']
    
    dict_withholding = {'W_WOCHD':'WOHISCNV','W_WOBNK':'WOHISCNV','W_WOSTL':'WOHISCNV'}
    df_all['Earning Code*'] = df_all['Earning Code*'].replace(dict_withholding)

    df_all = get_tax_code_ee(df_all)

    df_all = set_federal(df_all)
    
    df_all = set_state_tax_authority(df_all)
    
    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
    #    & (df_all['E/D/T Code'].str.startswith(('SDI:', 'SIT:', 'SUI:', 'SUTA:'), na=False))
    #), 'Payroll State Tax Authority'] = df_all['E/D/T Code'].str[-2:]

    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
    #    & (df_all['E/D/T Code'].str.startswith(('SDI:','SIT:','SUI:','SUTA:'), na=False))
    #), 'Deduction Code1'] = 'W_SWW'
    

    df_states = pd.read_csv(config.PATH_WD_IMP + 'data files\\wd_states.csv',encoding="cp1251")
    df_states = df_states[['Abbrev', 'Payroll']]
    df_states = df_states.dropna()
    df_all = df_all.merge(df_states, right_on='Abbrev', left_on='Payroll State Tax Authority', how='left')
    
    #df_all.loc[df_all['Deduction Code1'].str.startswith('W_',na=False), 'Deduction Code'] = df_all['Deduction Code1'] + df_all['Payroll']
    #df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\test_parallel_pay.csv')
    
    #####################################################
    df_all = set_county_tax_authority(df_all)
    df_all = set_city_tax_authority(df_all)
    df_all = set_local_home_tax_authority(df_all)    
    df_all = set_local_other_tax_authority(df_all)
    
    #fix gross pay
    dfc_gp = df_all.groupby(['EmployeeID','#'])['GrossPay'].max()
    dfc_gp = dfc_gp.reset_index()
    
    df_all = df_all.merge(dfc_gp, on=['EmployeeID','#'], how='left')
    df_all['GrossPay'] = df_all['GrossPay_y']
    #merge earning ded tax into one column
    
    df_all['ElementValue'] = np.where(df_all['Earning Code'].isna(),df_all['Deduction Code'],df_all['Earning Code'])
    
    
    df_all = df_all.rename(columns={'Payroll': 'StateProvinceTaxAuthority',
                                    'Payroll Local County Tax Authority Code': 'CountyTaxAuthority',
                                    'Payroll Local City Tax Authority Code': 'CityTaxAuthority',
                                    'Payroll Local Home School District Tax Authority Code': 'SchoolDistrictTaxAuthority',
                                    'Payroll Local Other Tax Authority Code': 'OtherLocalTaxAuthority'})
    
    df_all['OnCycle'] = 'Y'
    df_all = set_federal_tax_type(df_all)
    df_all['TaxableWages'] = np.where(df_all['ElementType']!='TAX','',df_all['TaxableWages'])
    df_all['SubjectWages'] = np.where(df_all['ElementType']!='TAX','',df_all['SubjectWages'])
    
    #TODO
    #RecordNumber: for those that have multiple paychecks, use column #
    #Move everything to Element Value
    
    
    new_columns = {
        'RecordNumber': '',
        'Position': '',
        'PayGroup': '',
        'WithholdingOrder-CaseNumber': '',
        'RelatedCalculation': '',
        'HoursEarnedApplicableAmount': '',
        'YTDMedicareWages': '',
        'Location': '',
        'ActiveStatus': '',
        'PrimaryHomeAddress-StateCode': '',
        'LocationAddress-StateCode': '',
        'PrimaryHomeAddress-City': '',
        'LocationAddress-City': '',
        'AnnualSalaryUSD-1Years': '',
        'HourlyRate-Amount': ''
    }
    
    df_new = pd.DataFrame(new_columns, index=[0])
    df_all = pd.concat([df_all, df_new], axis=1)
    
    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)), 'Deduction Code'] = df_all['WD EE Code']
    #df_all_ee = df_all
    #df_all['Period End Date'] = pd.to_datetime(df_all['Period End Date']).dt.strftime("%d-%b-%Y").str.upper()
    #df_all['Payment Date'] = pd.to_datetime(df_all['Payment Date']).dt.strftime("%d-%b-%Y").str.upper()
    
    #df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\pay_hist_ee.csv')
    

    df_all_ee = df_all[['#',
                       'EmployeeID',
                       'OnCycle',
                       'Company',
                       'PayGroup',
                       'CostCenter',
                       'Position',
                       'ElementType',
                       'ElementValue',
                       'Amount',
                       'GrossPay',
                       'NetPay',
                       'TaxableWages',
                       'SubjectWages',
                       'StateProvinceTaxAuthority',
                       'CountyTaxAuthority',
                       'CityTaxAuthority',
                       'SchoolDistrictTaxAuthority',
                       'OtherLocalTaxAuthority',
                       'WithholdingOrder-CaseNumber',
                       'RelatedCalculation',
                       'HoursEarnedApplicableAmount',
                       'YTDMedicareWages',
                       'Location',
                       'ActiveStatus',
                       'PrimaryHomeAddress-StateCode',
                       'LocationAddress-StateCode',
                       'PrimaryHomeAddress-City',
                       'LocationAddress-City',
                       'AnnualSalaryUSD-1Years',
                       'HourlyRate-Amount','E/D/T Code']]
    
    '''
    df_all_f = df_all_f.fillna('x')
    df_all_f['Taxable Wages'] = df_all_f['Subject Wages'].str.strip()
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].str.strip()
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].str.strip()
    df_all_f['Taxable Wages'] = df_all_f['Taxable Wages'].astype(float)
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].astype(float)
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].astype(float)
                                                     
    df_pivx = pd.pivot_table(df_all_f, index=['Employee ID',
                                             'Quarter',
                                             'Pay Group',
                                             'Company',
                                             'Cost Center',
                                             'Earning Code',
                                             'Deduction Code'],
                            aggfunc={'Amount':'sum',
                                     'Taxable Wages':'sum',
                                     'Subject Wages':'sum',
                                     'Gross Wages':'sum'})
    df_pivx.reset_index(inplace=True)
    '''
    
    ###########################################################################
    
    # -----------------------------------------------------------------------------
def get_tax_code_er(df):

    df_tax_codes = pd.read_excel(config.PATH_WD_IMP+ 'data files\\Kronos_Company_Taxes.xlsx')

    df['E/D/T Code'] = df['E/D/T Code'].astype(str)
    df_tax_codes['Company Tax Code'] = df_tax_codes['Company Tax Code'].astype(str)
    df_tax_codes = df_tax_codes[['Tax Jurisdiction', 'Tax Type', 'Company Tax Code', 'WD ER Code']]
    df_tax_codes.drop_duplicates(inplace=True)
    df_tax_codes = df_tax_codes.dropna()
    df = df.merge(df_tax_codes, left_on='E/D/T Code', right_on='Company Tax Code', how='left')

    return df


# -----------------------------------------------------------------------------
def get_pay_group(df):

    df_pay_group = pd.read_csv(config.PATH_WD_IMP+ 'payroll_dc\\E2E_pay_gp_assignments_v4.txt', delimiter='|')

    df['Employee ID'] = df['Employee ID'].astype(str)
    df_pay_group['Employee ID'] = df_pay_group['Employee ID'].astype(str)

    df = df.merge(df_pay_group[['Employee ID', 'Pay Group ID']], left_on='Employee ID', right_on='Employee ID', how='left')
    df = df.rename(columns={'Pay Group ID': 'Pay Group'})

    return df

# -----------------------------------------------------------------------------
def set_federal_er(df):

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'FICA')
    ), 'Deduction Code'] = 'W_OASER'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'FIT')
    ), 'Deduction Code'] = 'W_CFIT'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'FUTA')
    ), 'Deduction Code'] = 'W_FUI'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'MEDI')
    ), 'Deduction Code'] = 'W_MEDER'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Admin Assess')
    ), 'Deduction Code'] = 'W_SUIER13'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Colorado Paid Family and Medical Leave')
    ), 'Deduction Code'] = 'W_COFMS'
  
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Colorado Paid Family and Medical Leave')
    ), 'Payroll Local Other Tax Authority Code'] = '08WD001'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'ER ESA')
    ), 'Deduction Code'] = 'W_ALSEC'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'ER ESA')
    ), 'Payroll Local Other Tax Authority Code'] = '01WD072R'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Maine UPAF')
    ), 'Deduction Code'] = 'W_MEUPA'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Maine UPAF')
    ), 'Payroll Local Other Tax Authority Code'] = '23WD083R'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'New Mexico Worker\'s Compensation Assessment Fee - Employer')
    ), 'Deduction Code'] = 'W_NMWCR'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'New Mexico Worker\'s Compensation Assessment Fee - Employer')
    ), 'Payroll Local Other Tax Authority Code'] = '35WD025'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NJ Work Force Development/Supplemental Work Force')
    ), 'Deduction Code'] = 'W_NJTD'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NJ Work Force Development/Supplemental Work Force')
    ), 'Payroll Local Other Tax Authority Code'] = 'ENJ-TDB'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NY MCTMT')
    ), 'Deduction Code'] = 'W_NYMCT'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NY MCTMT')
    ), 'Payroll Local Other Tax Authority Code'] = '36WD092R'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Re-employment')
    ), 'Deduction Code'] = 'W_NYREM'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Re-employment')
    ), 'Payroll Local Other Tax Authority Code'] = '36WD088R'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Newark, NJ')
    ), 'Deduction Code'] = 'W_NJWCA'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Newark, NJ')
    ), 'Payroll Local Other Tax Authority Code'] = 'NJWATER'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Colorado Paid Family and Medical Leave')
    ), 'Deduction Code'] = 'W_COSPF'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Colorado Paid Family and Medical Leave')
    ), 'Payroll Local Other Tax Authority Code'] = 'M08WD001'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'SUTA_SC:NH')
    ), 'Deduction Code'] = 'W_NHADM'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'SUTA_SC:NH')
    ), 'Payroll Local Other Tax Authority Code'] = '33WD086R'
    


    return df


# -----------------------------------------------------------------------------
def set_state_tax_authority_er(df):

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Type'].isin(['SDI', 'SIT', 'SUI', 'SUTA','WC']))
    ), 'Payroll State Tax Authority'] = df['E/D/T Code'].str[-2:]

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Type'].isin(['SIT', 'SUI']))
    ), 'Deduction Code'] = df['WD ER Code'].str[:5]
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Jurisdiction'] == 'State')
        & (df['Tax Type'].isin(['FLI']))
    ), 'Payroll State Tax Authority'] = df['WD ER Code'].str[3:4]
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['Tax Jurisdiction'] == 'State')
        & (df['Tax Type'].isin(['FLI','SDI','WC','SUTA']))
    ), 'Deduction Code'] = df['WD ER Code'].str[:7]
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'SUTA_SC:ME')
    ), 'Deduction Code'] = 'W_SUIER'
    
    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'SUTA_SC:ME')
    ), 'Payroll State Tax Authority'] = '23'

    return df


# -----------------------------------------------------------------------------
def set_local_other_tax_authority_er(df):

    col = 'Payroll Local Other Tax Authority Code'

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (~df_all['Tax Type'].isin(['CITY', 'CNTY', 'SCHL']))
    ), col] = df_all['WD ER Code'].str[7:]
    
    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (~df_all['Tax Type'].isin(['CITY', 'CNTY', 'SCHL']))
    ), 'Deduction Code'] = df_all['WD ER Code'].str[:7]
    
    df.loc[(df['Record Type'].str.startswith('Ded', na=False)
        & (df['E/D/T Code'] == '401K')
    ), 'Deduction Code'] = '401KER'

    return df

# -----------------------------------------------------------------------------
def set_federal_tax_type(df):
    federal_types = ['W_MEDER','W_OASER','W_MED','W_OAS','W_FW','W_FUI','W_AMEDT']
    
    df.loc[(df['ElementValue'].isin(federal_types)),'StateProvinceTaxAuthority'] = 'Federal'
    
    return df
# -----------------------------------------------------------------------------
if __name__ == '__main__':

    #df_all = pd.concat([df_p1,df_p2_1,df_p2_2])
    df_all = pd.concat([df_p1_1,df_p1_2])
    #------------------------------------------------------------------------------
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
    ##df_all['Employee Id'] = df_all['Employee Id'].astype(str)
    #df_all = df_all.loc[df_all['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    df_all = df_all.replace('^[-]$', '', regex=True)

    df_all = modify_amount(df_all, 'Record Gross Subject Wages')
    df_all = modify_amount(df_all, 'Record Gross Wages')
    df_all = modify_amount(df_all, 'Record Subject Wages')
    df_all = modify_amount(df_all, 'Record Amount')
    df_all = modify_amount(df_all, 'Record Amount (ER)')
    df_all = modify_amount(df_all, 'Net Payment + Cash + Fringe')
    df_all['Record Amount'] = df_all['Record Amount'].astype(float)
    df_all['Record Amount (ER)'] = df_all['Record Amount (ER)'].astype(float)
    
    df_all['ElementType'] = np.where(df_all['Record Type'].str.startswith('Tax'), 'TAX',
                            np.where(df_all['Record Type'].str.startswith('Deduc'),'DED','ERN'))

    # df_all = df_all.loc[df_all['Type'] == 'Regular']
    df_all = df_all.loc[~df_all['E/D/T Code'].isnull()]
    df_all = df_all.loc[(df_all['Record Amount (ER)'] != 0)]
    print(len(df_all.index))

    df_all = df_all.rename(columns={'Employee Id': 'EmployeeID',
                                    'Cost Centers(Cost Center)': 'CostCenter',
                                    'Record Amount (ER)': 'Amount',
                                    'Record Gross Subject Wages': 'TaxableWages',
                                    'Record Gross Wages': 'GrossPay',
                                    'Record Subject Wages': 'SubjectWages',
                                    'Net Payment + Cash + Fringe':'NetPay'})

    #df_all['Source System'] = 'Kronos'
    #df_all['Quarter'] = pd.PeriodIndex(df_all['Payment Date'], freq='Q')

    dict_company = {'KBP Foods': 'FQ',
                    'KBP Bells': 'TB',
                    'Restaurant Services Group': 'RS',
                    'KBP Inspired': 'RB',
                    'KBP Cares': 'KC'}

    df_all['Company'] = df_all['Employee EIN'].replace(dict_company)
    df_all['CostCenter'] = 'CC' + df_all['CostCenter'].astype(str).str.zfill(5)

    earning_ded_codes = pd.read_csv(config.PATH_WD_IMP + 'data files\earning_ded_codes.csv')
    df_all = df_all.merge(earning_ded_codes[['Earning Code (Legacy System)', 'Earning Code*']],
                          left_on='E/D/T Code',
                          right_on='Earning Code (Legacy System)', how='left')

    print(df_all.columns)

    df_all.loc[df_all['Record Type'].str.startswith('Earning', na=False), 'Earning Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Fringe', na=False), 'Earning Code'] = df_all['Earning Code*']

    df_all.loc[df_all['Record Type'].str.startswith('Deduction', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Tax', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Reimbursement', na=False), 'Deduction Code'] = df_all['Earning Code*']

    df_all = get_tax_code_er(df_all)
    #df_all = get_pay_group(df_all)
    df_all = set_federal_er(df_all)
    ####################################################
    df_all = set_state_tax_authority_er(df_all)
    
    df_all = df_all.merge(df_states, right_on='Abbrev', left_on='Payroll State Tax Authority', how='left')
    #df['Payroll State Tax Authority'] = df['Payroll'].astype('Int64')
    #df_all['Deduction Code'] = df_all['Deduction Code1'] + df_all['Payroll']

    ####################################################    
    #df_all = set_county_tax_authority(df_all)
    #df_all1 = set_city_tax_authority(df_all)

    df_all = set_local_other_tax_authority_er(df_all)
    
    #fix gross pay
    dfc_gp = df_all.groupby(['EmployeeID','#'])['GrossPay'].max()
    dfc_gp = dfc_gp.reset_index()
    
    df_all = df_all.merge(dfc_gp, on=['EmployeeID','#'], how='left')
    df_all['GrossPay'] = df_all['GrossPay_y']
    
    #fix missing Ded codes
    
    df_all.loc[(df_all['Deduction Code'].isna()),'Payroll State Tax Authority'] = df_all['WD ER Code'].str[7:]
    df_all.loc[(df_all['Deduction Code'].isna()),'Deduction Code'] = df_all['WD ER Code'].str[:7]
    
    df_all = df_all.rename(columns={'Payroll': 'StateProvinceTaxAuthority',
                                    'Payroll Local County Tax Authority Code': 'CountyTaxAuthority',
                                    'Payroll Local City Tax Authority Code': 'CityTaxAuthority',
                                    'Payroll Local Home School District Tax Authority Code': 'SchoolDistrictTaxAuthority',
                                    'Payroll Local Other Tax Authority Code': 'OtherLocalTaxAuthority',
                                    'Deduction Code': 'ElementValue'})
    df_all['OnCycle'] = 'Y'
    df_all = set_federal_tax_type(df_all)
    df_all['TaxableWages'] = np.where(df_all['ElementType']!='TAX','',df_all['TaxableWages'])
    df_all['SubjectWages'] = np.where(df_all['ElementType']!='TAX','',df_all['SubjectWages'])    
    
    #df_all.loc[~df_all['Record Type'].str.startswith('Tax', na=False), 'Taxable Wages'] = ''
    #df_all.loc[~df_all['Record Type'].str.startswith('Tax', na=False), 'Subject Wages'] = ''
    #df_all.loc[~df_all['Record Type'].str.startswith('Tax', na=False), 'Gross Wages'] = ''

    new_columns = {
        'RecordNumber': '',
        'Position': '',
        'PayGroup': '',
        'WithholdingOrder-CaseNumber': '',
        'RelatedCalculation': '',
        'HoursEarnedApplicableAmount': '',
        'YTDMedicareWages': '',
        'Location': '',
        'ActiveStatus': '',
        'PrimaryHomeAddress-StateCode': '',
        'LocationAddress-StateCode': '',
        'PrimaryHomeAddress-City': '',
        'LocationAddress-City': '',
        'AnnualSalaryUSD-1Years': '',
        'HourlyRate-Amount': '',
        'CountyTaxAuthority':'',
        'CityTaxAuthority':'',
        'SchoolDistrictTaxAuthority':''
    }
    df_new = pd.DataFrame(new_columns, index=[0])
    df_all = pd.concat([df_all, df_new], axis=1)
    
    
    df_all_f = df_all[['#',
                       'EmployeeID',
                       'OnCycle',
                       'Company',
                       'PayGroup',
                       'CostCenter',
                       'Position',
                       'ElementType',
                       'ElementValue',
                       'Amount',
                       'GrossPay',
                       'NetPay',
                       'TaxableWages',
                       'SubjectWages',
                       'StateProvinceTaxAuthority',
                       'CountyTaxAuthority',
                       'CityTaxAuthority',
                       'SchoolDistrictTaxAuthority',
                       'OtherLocalTaxAuthority',
                       'WithholdingOrder-CaseNumber',
                       'RelatedCalculation',
                       'HoursEarnedApplicableAmount',
                       'YTDMedicareWages',
                       'Location',
                       'ActiveStatus',
                       'PrimaryHomeAddress-StateCode',
                       'LocationAddress-StateCode',
                       'PrimaryHomeAddress-City',
                       'LocationAddress-City',
                       'AnnualSalaryUSD-1Years',
                       'HourlyRate-Amount','E/D/T Code']]
    
    #df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\test_parallel_payer.csv')
    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)), 'Deduction Code'] = df_all['WD ER Code']

    #df_all['Period End Date'] = pd.to_datetime(df_all['Period End Date']).dt.strftime("%d-%b-%Y").str.upper()
    #df_all['Payment Date'] = pd.to_datetime(df_all['Payment Date']).dt.strftime("%d-%b-%Y").str.upper()
    #df_all['Earning Code'] = df_all['Earning Code*']
    #df_all['Deduction Code'] = df_all['WD ER Code']
    
    df_parallel = pd.concat([df_all_ee,df_all_f])
    
    df_parallel.sort_values(by = ['EmployeeID','#'],inplace=True)
    
    df_check_nums = df_parallel[['EmployeeID','#']]
    df_check_nums.drop_duplicates(inplace=True)
    df_check_nums['T'] = df_check_nums.groupby('EmployeeID')['#'].transform('count')
    df_check_nums['RecordNumber'] = df_check_nums.groupby('EmployeeID')['#'].cumcount() + 1
    
    df_check_nums = df_check_nums[['EmployeeID','#','RecordNumber']]
    df_parallel = df_parallel.merge(df_check_nums, on=['EmployeeID','#'])
    
    df_parallel = df_parallel[['RecordNumber',
                       'EmployeeID',
                       'OnCycle',
                       'Company',
                       'PayGroup',
                       'CostCenter',
                       'Position',
                       'ElementType',
                       'ElementValue',
                       'Amount',
                       'GrossPay',
                       'NetPay',
                       'TaxableWages',
                       'SubjectWages',
                       'StateProvinceTaxAuthority',
                       'CountyTaxAuthority',
                       'CityTaxAuthority',
                       'SchoolDistrictTaxAuthority',
                       'OtherLocalTaxAuthority',
                       'WithholdingOrder-CaseNumber',
                       'RelatedCalculation',
                       'HoursEarnedApplicableAmount',
                       'YTDMedicareWages',
                       'Location',
                       'ActiveStatus',
                       'PrimaryHomeAddress-StateCode',
                       'LocationAddress-StateCode',
                       'PrimaryHomeAddress-City',
                       'LocationAddress-City',
                       'AnnualSalaryUSD-1Years',
                       'HourlyRate-Amount','E/D/T Code']]
    
    df_parallel.sort_values(by = ['EmployeeID','RecordNumber'],inplace=True)

    df_parallel.to_excel(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\parallel_payroll_cycle2.xlsx',index=False)
