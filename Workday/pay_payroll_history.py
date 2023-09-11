# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 08:08:07 2023

@author: akaff
"""

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
#import xlrd-
import glob as glob
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, open_as_utf8, modify_amount)

# -----------------------------------------------------------------------------
def get_tax_code_ee(df):

    df_tax_codes = pd.read_excel(config.PATH_WD_IMP+ 'data files\\Kronos_Company_Taxes.xlsx')

    df['E/D/T Code'] = df['E/D/T Code'].astype(str)
    df_tax_codes['Company Tax Code'] = df_tax_codes['Company Tax Code'].astype(str)
    df_tax_codes = df_tax_codes[['Tax Jurisdiction', 'Tax Type', 'Company Tax Code', 'WD EE Code','Payroll State Tax Authority','Payroll Local County Tax Authority Code','Payroll Local City Tax Authority Code','Payroll Local Home School District Tax Authority Code','Payroll Local Other Tax Authority Code','Federal']]
    df_tax_codes = df_tax_codes.drop_duplicates()
    df = df.merge(df_tax_codes, left_on='E/D/T Code', right_on='Company Tax Code', how='left')
    df.drop_duplicates(inplace=True)

    return df

# -----------------------------------------------------------------------------
def get_sup_wages(df):
    codes = ['ABON','APRBNS','BBON','CBON','CSIBN','GABON','LJSBON',
             'LPSMBN','MKBON','MBON','PRDBN','PREM','PBON','PTBON',
             'QBON','RBON','RTNBON','SEVR','SOBON','TRBON','TRNSBNS',
             'VOCBN','YrEndBonus']
    
    df.loc[(df['Record Type'].str.startswith('Earn',na=False) & 
               (df['E/D/T Code'].isin(codes))),
               'Supplemental Wages'] = df['Amount']
    
    
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
def set_state_tax_authority_a(df):
    df['Payroll State Tax Authority']  = df['Payroll State Tax Authority'].str.zfill(2)
    df['WD EE Code'] = np.where((~df['Payroll State Tax Authority'].isna()), df['WD EE Code'][:-2],df['WD EE Code'])
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
    
    df.loc[(df['E/D/T Code'].isin(federal_types)),'StateProvinceTaxAuthority'] = 'Federal'
    
    return df

# -----------------------------------------------------------------------------
def pay_group(df):
    ny_stores = ['CC05325',
'CC05326',
'CC05338',
'CC05339',
'CC05340',
'CC05341',
'CC05342',
'CC05343',
'CC05344',
'CC05345',
'CC05346',
'CC05348',
'CC05349',
'CC05350',
'CC05351',
'CC05352',
'CC05353',
'CC05414',
'CC05415',
'CC05416',
'CC05417',
'CC05418',
'CC05419',
'CC05420',
'CC05421',
'CC05422',
'CC05423',
'CC05424',
'CC05426',
'CC05427',
'CC05429',
'CC05431',
'CC05461',
'CC05471',
'CC05596',
'CC05613',
'CC05614',
'CC05615',
'CC05616',
'CC05651',
'CC05658',
'CC05741',
'CC05742',
'CC05753',
'CC05775',
'CC05798',
'CC05853',
'CC05858',
'CC05865',
'CC05878',
'CC05879',
'CC05886',
'CC05887',
'CC05888',
'CC05889',
'CC05890',
'CC05891',
'CC05947',
'CC05949',
'CC39995',
'CC39996',
'CC39997',
'CC39998',
'CC39999',
'CC40009',
'CC40328',
'CC40600']
    df['Pay Group'] = np.where((df['Company'] == 'FQ' & df['Cost Center'].isin(ny_stores)),'USA_Weekly',
                      np.where((df['Company'] == 'TB' & df['Cost Center'].isin(ny_stores)),'USA_TB_Weekly',
                      np.where((~df['Company'] == 'TB' & df['Cost Center'].isin(ny_stores)),'USA_TB_Bi-Weekly','USA_Bi-Weekly')))
    
    return df

# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df_all = pd.DataFrame()
    ny_stores = ['CC05325',
'CC05326',
'CC05338',
'CC05339',
'CC05340',
'CC05341',
'CC05342',
'CC05343',
'CC05344',
'CC05345',
'CC05346',
'CC05348',
'CC05349',
'CC05350',
'CC05351',
'CC05352',
'CC05353',
'CC05414',
'CC05415',
'CC05416',
'CC05417',
'CC05418',
'CC05419',
'CC05420',
'CC05421',
'CC05422',
'CC05423',
'CC05424',
'CC05426',
'CC05427',
'CC05429',
'CC05431',
'CC05461',
'CC05471',
'CC05596',
'CC05613',
'CC05614',
'CC05615',
'CC05616',
'CC05651',
'CC05658',
'CC05741',
'CC05742',
'CC05753',
'CC05775',
'CC05798',
'CC05853',
'CC05858',
'CC05865',
'CC05878',
'CC05879',
'CC05886',
'CC05887',
'CC05888',
'CC05889',
'CC05890',
'CC05891',
'CC05947',
'CC05949',
'CC39995',
'CC39996',
'CC39997',
'CC39998',
'CC39999',
'CC40009',
'CC40328',
'CC40600']
    # for filename in glob.glob(config.PATH_WD_IMP + 'pay_hist\\EarningDeductionTaxListing_01_23.csv'):
    #for filename in glob.glob(config.PATH_WD_IMP + 'pay_hist\\EarningDeductionTaxListing_*_23.csv'):
     #   print(filename)

      #  df = pd.read_csv(filename, dtype='object', encoding='cp1251')

       # df_all = pd.concat([df_all, df], axis=0)
        #print(len(df.index))
        #rint(len(df_all.index))
    os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\pay_hist\all quarters')

    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    combined_csv = pd.concat([pd.read_csv(f,encoding="cp1252") for f in all_filenames ])
    df_all = combined_csv
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
    
    #df_all['ElementType'] = np.where(df_all['Record Type'].str.startswith('Tax'), 'TAX',
                           # np.where(df_all['Record Type'].str.startswith('Deduc'),'DED','ERN'))
    
    #Add in net pay column prior to repulling data
    #df_all['Net Pay + Cash + Fringe'] = ''


    # df_all = df_all.loc[df_all['Type'] == 'Regular']
    # check blanks before removing
    xx = df_all[df_all['E/D/T Code'].isnull()]
    # set(xx['Record Type'])
    df_all = df_all.loc[~df_all['E/D/T Code'].isnull()]
    df_all = df_all.loc[(df_all['Record Amount'] != 0)]
    print(len(df_all.index))
    
    
    df_all = df_all.rename(columns={'Employee Id': 'Employee ID',
                                    'Cost Centers(Cost Center)': 'Cost Center',
                                    'Record Amount': 'Amount',
                                    'Record Gross Subject Wages': 'Taxable Wages',
                                    'Record Gross Wages': 'Gross Wages',
                                    'Record Subject Wages': 'Subject Wages',
                                    'Net Payment + Cash + Fringe':'Net Pay'})
    
    df_all['Source System'] = 'Kronos'
    df_all['Quarter'] = pd.PeriodIndex(df_all['Pay Date'], freq='Q')
    
    dict_company = {'KBP Foods': 'FQ',
                    'KBP Bells': 'TB',
                    'Restaurant Services Group': 'RS',
                    'KBP Inspired': 'RB',
                    'KBP Cares': 'KC'}

    df_all['Company'] = df_all['Employee EIN'].replace(dict_company)
    df_all['Cost Center'] = 'CC' + df_all['Cost Center'].astype(str).str.zfill(5)

    earning_ded_codes = pd.read_csv(config.PATH_WD_IMP + 'data files\earning_ded_codes.csv')
    df_all = df_all.merge(earning_ded_codes[['Earning Code (Legacy System)', 'Earning Code*']],
                          left_on='E/D/T Code',
                          right_on='Earning Code (Legacy System)', how='left')

    print(df_all.columns)

    df_all.loc[df_all['Record Type'].str.startswith('Earning', na=False), 'Earning Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Fringe', na=False), 'Earning Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Reimbursement', na=False), 'Earning Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Deduction', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Tax', na=False), 'Deduction Code'] = df_all['Earning Code*']

    
    dict_withholding = {'W_WOCHD':'WOHISCNV','W_WOBNK':'WOHISCNV','W_WOSTL':'WOHISCNV'}
    df_all['Earning Code*'] = df_all['Earning Code*'].replace(dict_withholding)

    df_all = get_tax_code_ee(df_all)
    
    #df_all.loc[df_all['E/D/T Code'] == 'Wilmington CSD', 'WD EE Code'] = 'W_SCHDR'
    #df_all.loc[df_all['E/D/T Code'] == 'Wilmington CSD', 'Payroll Local Home School District Tax Authority Code'] = '391404'
    
    df_all = get_sup_wages(df_all)
    
    #fix State withhold resident 
    fixes = df_all[df_all['WD EE Code'] == 'W_CITYR']
    
    df_all.loc[(df_all['Employee ID'].isin(fixes['Employee ID'])) & (df_all['WD EE Code'] == 'W_SWW') , 'WD EE Code'] = df_all['WD EE Code'].str.replace('W_SWW','W_SWR')
    
    
    #df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\testPayHist.csv')

    #df_all = set_federal(df_all)
    
    #df_all = set_state_tax_authority_a(df_all)
    
    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
    #    & (df_all['E/D/T Code'].str.startswith(('SDI:', 'SIT:', 'SUI:', 'SUTA:'), na=False))
    #), 'Payroll State Tax Authority'] = df_all['E/D/T Code'].str[-2:]

    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
    #    & (df_all['E/D/T Code'].str.startswith(('SDI:','SIT:','SUI:','SUTA:'), na=False))
    #), 'Deduction Code1'] = 'W_SWW'
    

    #df_states = pd.read_csv(config.PATH_WD_IMP + 'data files\\wd_states.csv',encoding="cp1251")
    #df_states = df_states[['Abbrev', 'Payroll']]
    #df_states = df_states.dropna()
    #df_all = df_all.merge(df_states, right_on='Abbrev', left_on='Payroll State Tax Authority', how='left')
    
    #df_all.loc[df_all['Deduction Code1'].str.startswith('W_',na=False), 'Deduction Code'] = df_all['Deduction Code1'] + df_all['Payroll']
    #df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\test_parallel_pay.csv')
    
    #####################################################
   # df_all = set_county_tax_authority(df_all)
    #df_all = set_city_tax_authority(df_all)
    #df_all = set_local_home_tax_authority(df_all)    
    #df_all = set_local_other_tax_authority(df_all)
    
    #fix gross pay
    #dfc_gp = df_all.groupby(['EmployeeID','#'])['GrossPay'].max()
    #dfc_gp = dfc_gp.reset_index()
    
    #f_all = df_all.merge(dfc_gp, on=['EmployeeID','#'], how='left')
    #df_all['Gross Pay'] = df_all['Gross Pay_y']
    #merge earning ded tax into one column
    
    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)), 'Deduction Code'] = df_all['WD EE Code']
    
    
    
    #df_all['OnCycle'] = 'Y'
    #df_all = set_federal_tax_type(df_all)
    df_all['Taxable Wages'] = np.where(df_all['Record Type']!='Tax','',df_all['Taxable Wages'])
    df_all['Subject Wages'] = np.where(df_all['Record Type']!='Tax','',df_all['Subject Wages'])
    
    #TODO
    #RecordNumber: for those that have multiple paychecks, use column #
    #Move everything to Element Value
    
    
    new_columns = {
        'Position ID': '',
        'Third Party Sick Pay': '',
        'Withholding Order - Case Number': '',
        'Flexible Payment Deduction Worktag': '',
        'Custom Worktag #1': '',
        'Custom Worktag #2': '',
        'Custom Worktag #3': '',
        'Custom Worktag #4': '',
        'Custom Worktag #5': '',
        'Custom Worktag #6': '',
        'Custom Worktag #7': '',
        'Custom Worktag #8': '',
        'Custom Worktag #9': '',
        'Custom Worktag #10': '',
        'Custom Worktag #11': '',
        'Custom Worktag #12': '',
        'Custom Worktag #13': '',
        'Custom Worktag #14': '',
        'Custom Worktag #15': '',
        'Allocation Pool': '',
        'Appropriation': '',
        'Related Calculation ID': '',
        'Related Calc Input Value': ''
    }
    
    df_new = pd.DataFrame(new_columns, index=[0])
    df_all = pd.concat([df_all, df_new], axis=1)
    
    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)), 'Deduction Code'] = df_all['WD EE Code']
    #df_all_ee = df_all
    df_all['Period End Date'] = pd.to_datetime(df_all['Pay Period End']).dt.strftime("%d-%b-%Y").str.upper()
    df_all['Payment Date'] = pd.to_datetime(df_all['Pay Date']).dt.strftime("%d-%b-%Y").str.upper()
    df_all['Pay Group'] = np.where((df_all['Company'] == 'FQ') & (df_all['Cost Center'].isin(ny_stores)),'USA_Weekly',
                      np.where((df_all['Company'] == 'TB') & (df_all['Cost Center'].isin(ny_stores)),'USA_TB_Weekly',
                      np.where(((df_all['Company'] == 'TB') & (~df_all['Cost Center'].isin(ny_stores))),'USA_TB_Bi-Weekly','USA_Bi-Weekly')))
    #df_all_f.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\pay_hist_ee.csv')
    

    df_all_f = df_all[['Employee ID', 'Source System', 'Quarter', 'Pay Group',
                     'Company', 'Cost Center', 'Position ID', 'Third Party Sick Pay', 'Earning Code',
                     'Deduction Code', 'Amount', 'Taxable Wages', 'Subject Wages',
                     'Gross Wages', 'Supplemental Wages',
                     'Withholding Order - Case Number',
                     'Payroll State Tax Authority',
                     'Payroll Local County Tax Authority Code',
                     'Payroll Local City Tax Authority Code',
                     'Payroll Local Home School District Tax Authority Code',
                     'Payroll Local Other Tax Authority Code',
                     'Flexible Payment Deduction Worktag',
                     'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
                     'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
                     'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
                     'Custom Worktag #10', 'Custom Worktag #11',
                     'Custom Worktag #12', 'Custom Worktag #13',
                     'Custom Worktag #14', 'Custom Worktag #15', 'Allocation Pool',
                     'Appropriation', 'Related Calculation ID',
                     'Related Calc Input Value','Record Type']]
    df_all_f['Supplemental Wages'] = np.where(df_all_f['Supplemental Wages'].isna(),'0',df_all_f['Supplemental Wages'])
    df_all_f = df_all_f.fillna('x')
    df_all_f['Taxable Wages'] = df_all_f['Taxable Wages'].str.strip()
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].str.strip()
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].str.strip()
    df_all_f['Supplemental Wages'] = df_all_f['Supplemental Wages'].str.strip()
    df_all_f['Taxable Wages'] = df_all_f['Taxable Wages'].replace('',0)
    df_all_f['Taxable Wages'] = df_all_f['Taxable Wages'].astype(float)
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].replace('',0)
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].astype(float)
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].replace('',0)
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].astype(float)
    df_all_f['Supplemental Wages'] = df_all_f['Supplemental Wages'].replace('',0)
    df_all_f['Supplemental Wages'] = df_all_f['Supplemental Wages'].astype(float)
    df_all_f = df_all_f.replace('','x')
                                                     
    df_pivx = pd.pivot_table(df_all_f, index=['Employee ID',
                                             'Quarter',
                                             'Pay Group',
                                             'Company',
                                             'Cost Center',
                                             'Earning Code',
                                             'Deduction Code',
                                             'Payroll State Tax Authority',
                                             'Payroll Local County Tax Authority Code',
                                             'Payroll Local City Tax Authority Code',
                                             'Payroll Local Home School District Tax Authority Code',
                                             'Payroll Local Other Tax Authority Code'],
                            aggfunc={'Amount':'sum',
                                     'Taxable Wages':'sum',
                                     'Subject Wages':'sum',
                                     'Gross Wages':'sum',
                                     'Supplemental Wages':'sum'})
    df_pivx.reset_index(inplace=True)
    
    df_pivx['Earning Code'] = df_pivx['Earning Code'].str.replace('x','')
    df_pivx['Deduction Code'] = df_pivx['Deduction Code'].str.replace('x','')
    df_pivx['Payroll State Tax Authority'] = df_pivx['Payroll State Tax Authority'].astype(str)
    df_pivx['Payroll State Tax Authority'] = df_pivx['Payroll State Tax Authority'].str.replace('x','')
    df_pivx['Payroll Local County Tax Authority Code'] = df_pivx['Payroll Local County Tax Authority Code'].astype(str)
    df_pivx['Payroll Local County Tax Authority Code'] = df_pivx['Payroll Local County Tax Authority Code'].str.replace('x','')
    df_pivx['Payroll Local City Tax Authority Code'] = df_pivx['Payroll Local City Tax Authority Code'].astype(str)
    df_pivx['Payroll Local City Tax Authority Code'] = df_pivx['Payroll Local City Tax Authority Code'].str.replace('x','')
    df_pivx['Payroll Local Home School District Tax Authority Code'] = df_pivx['Payroll Local Home School District Tax Authority Code'].astype(str)
    df_pivx['Payroll Local Home School District Tax Authority Code'] = df_pivx['Payroll Local Home School District Tax Authority Code'].str.replace('x','')
    df_pivx['Payroll Local Other Tax Authority Code'] = df_pivx['Payroll Local Other Tax Authority Code'].astype(str)
    df_pivx['Payroll Local Other Tax Authority Code'] = df_pivx['Payroll Local Other Tax Authority Code'].str.replace('x','')
    df_pivx = modify_amount(df_pivx, 'Amount')
    df_pivx = modify_amount(df_pivx, 'Taxable Wages')
    df_pivx = modify_amount(df_pivx, 'Subject Wages')
    df_pivx = modify_amount(df_pivx, 'Gross Wages')
    
    new_columns = {
        'Position ID': '',
        'Third Party Sick Pay': '',
        'Withholding Order - Case Number': '',
        'Flexible Payment Deduction Worktag': '',
        'Custom Worktag #1': '',
        'Custom Worktag #2': '',
        'Custom Worktag #3': '',
        'Custom Worktag #4': '',
        'Custom Worktag #5': '',
        'Custom Worktag #6': '',
        'Custom Worktag #7': '',
        'Custom Worktag #8': '',
        'Custom Worktag #9': '',
        'Custom Worktag #10': '',
        'Custom Worktag #11': '',
        'Custom Worktag #12': '',
        'Custom Worktag #13': '',
        'Custom Worktag #14': '',
        'Custom Worktag #15': '',
        'Allocation Pool': '',
        'Appropriation': '',
        'Related Calculation ID': '',
        'Related Calc Input Value': ''
    }
    df_new = pd.DataFrame(new_columns, index=[0])
    df_pivx = pd.concat([df_pivx, df_new], axis=1)
    
    df_pivx['Source System'] = 'Kronos'
    
    df_pivx = df_pivx[['Employee ID', 'Source System', 'Quarter', 'Pay Group',
                     'Company', 'Cost Center', 'Position ID','Third Party Sick Pay', 'Earning Code',
                     'Deduction Code', 'Amount', 'Taxable Wages', 'Subject Wages',
                     'Gross Wages', 'Supplemental Wages',
                     'Withholding Order - Case Number',
                     'Payroll State Tax Authority',
                     'Payroll Local County Tax Authority Code',
                     'Payroll Local City Tax Authority Code',
                     'Payroll Local Home School District Tax Authority Code',
                     'Payroll Local Other Tax Authority Code',
                     'Flexible Payment Deduction Worktag',
                     'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
                     'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
                     'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
                     'Custom Worktag #10', 'Custom Worktag #11',
                     'Custom Worktag #12', 'Custom Worktag #13',
                     'Custom Worktag #14', 'Custom Worktag #15', 'Allocation Pool',
                     'Appropriation', 'Related Calculation ID',
                     'Related Calc Input Value']]
    
    
    df_pivx_ee = df_pivx
    
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
    df_pivx_ee.to_excel(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\payroll_history1.xlsx',index=False)
    ###########################################################################
    
    # -----------------------------------------------------------------------------
def get_tax_code_er(df):

    df_tax_codes = pd.read_excel(config.PATH_WD_IMP+ 'data files\\Kronos_Company_Taxes.xlsx')

    df['E/D/T Code'] = df['E/D/T Code'].astype(str)
    df_tax_codes['Company Tax Code'] = df_tax_codes['Company Tax Code'].astype(str)
    df_tax_codes = df_tax_codes[['Tax Jurisdiction', 'Tax Type', 'Company Tax Code', 'WD ER Code','Payroll State Tax Authority','Payroll Local County Tax Authority Code','Payroll Local City Tax Authority Code','Payroll Local Home School District Tax Authority Code','Payroll Local Other Tax Authority Code','Federal']]
    df_tax_codes = df_tax_codes.drop_duplicates()
    df = df.merge(df_tax_codes, left_on='E/D/T Code', right_on='Company Tax Code', how='left')
    df.drop_duplicates(inplace=True)

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
    df_all = combined_csv
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
    
    #df_all['ElementType'] = np.where(df_all['Record Type'].str.startswith('Tax'), 'TAX',
                           # np.where(df_all['Record Type'].str.startswith('Deduc'),'DED','ERN'))

    # df_all = df_all.loc[df_all['Type'] == 'Regular']
    df_all = df_all.loc[~df_all['E/D/T Code'].isnull()]
    df_all = df_all.loc[(df_all['Record Amount (ER)'] != 0)]
    print(len(df_all.index))

    df_all = df_all.rename(columns={'Employee Id': 'Employee ID',
                                    'Cost Centers(Cost Center)': 'Cost Center',
                                    'Record Amount (ER)': 'Amount',
                                    'Record Gross Subject Wages': 'Taxable Wages',
                                    'Record Gross Wages': 'Gross Wages',
                                    'Record Subject Wages': 'Subject Wages',
                                    'Net Payment + Cash + Fringe':'Net Pay'})

    #df_all['Source System'] = 'Kronos'
    df_all['Quarter'] = pd.PeriodIndex(df_all['Pay Date'], freq='Q')

    dict_company = {'KBP Foods': 'FQ',
                    'KBP Bells': 'TB',
                    'Restaurant Services Group': 'RS',
                    'KBP Inspired': 'RB',
                    'KBP Cares': 'KC'}

    df_all['Company'] = df_all['Employee EIN'].replace(dict_company)
    df_all['Cost Center'] = 'CC' + df_all['Cost Center'].astype(str).str.zfill(5)

    earning_ded_codes = pd.read_csv(config.PATH_WD_IMP + 'data files\earning_ded_codes.csv')
    df_all = df_all.merge(earning_ded_codes[['Earning Code (Legacy System)', 'Earning Code*']],
                          left_on='E/D/T Code',
                          right_on='Earning Code (Legacy System)', how='left')

    print(df_all.columns)

    df_all.loc[df_all['Record Type'].str.startswith('Earning', na=False), 'Earning Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Fringe', na=False), 'Earning Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Reimbursement', na=False), 'Earning Code'] = df_all['Earning Code*']

    df_all.loc[df_all['Record Type'].str.startswith('Deduction', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Tax', na=False), 'Deduction Code'] = df_all['Earning Code*']

    df_all = get_tax_code_er(df_all)
    #df_all = get_pay_group(df_all)
    #df_all = set_federal_er(df_all)
    ####################################################
    #df_all = set_state_tax_authority_er(df_all)
    
    #df_all = df_all.merge(df_states, right_on='Abbrev', left_on='Payroll State Tax Authority', how='left')
    #df['Payroll State Tax Authority'] = df['Payroll'].astype('Int64')
    #df_all['Deduction Code'] = df_all['Deduction Code1'] + df_all['Payroll']

    ####################################################    
    #df_all = set_county_tax_authority(df_all)
    #df_all1 = set_city_tax_authority(df_all)

    #df_all = set_local_other_tax_authority_er(df_all)
    
    #fix gross pay
    #dfc_gp = df_all.groupby(['EmployeeID','#'])['GrossPay'].max()
    #dfc_gp = dfc_gp.reset_index()
    
    #df_all = df_all.merge(dfc_gp, on=['EmployeeID','#'], how='left')
    #df_all['GrossPay'] = df_all['GrossPay_y']
    
    #fix missing Ded codes
    
    #df_all.loc[(df_all['Deduction Code'].isna()),'Payroll State Tax Authority'] = df_all['WD ER Code'].str[7:]
    #df_all.loc[(df_all['Deduction Code'].isna()),'Deduction Code'] = df_all['WD ER Code'].str[:7]

    #df_all = set_federal_tax_type(df_all)
    df_all['Taxable Wages'] = np.where(df_all['Record Type']!='Tax','',df_all['Taxable Wages'])
    df_all['Subject Wages'] = np.where(df_all['Record Type']!='Tax','',df_all['Subject Wages'])    
    
    #df_all.loc[~df_all['Record Type'].str.startswith('Tax', na=False), 'Taxable Wages'] = ''
    #df_all.loc[~df_all['Record Type'].str.startswith('Tax', na=False), 'Subject Wages'] = ''
    #df_all.loc[~df_all['Record Type'].str.startswith('Tax', na=False), 'Gross Wages'] = ''

    new_columns = {
        'Position ID': '',
        'Third Party Sick Pay': '',
        'Supplemental Wages': '',
        'Withholding Order - Case Number': '',
        'Flexible Payment Deduction Worktag': '',
        'Custom Worktag #1': '',
        'Custom Worktag #2': '',
        'Custom Worktag #3': '',
        'Custom Worktag #4': '',
        'Custom Worktag #5': '',
        'Custom Worktag #6': '',
        'Custom Worktag #7': '',
        'Custom Worktag #8': '',
        'Custom Worktag #9': '',
        'Custom Worktag #10': '',
        'Custom Worktag #11': '',
        'Custom Worktag #12': '',
        'Custom Worktag #13': '',
        'Custom Worktag #14': '',
        'Custom Worktag #15': '',
        'Allocation Pool': '',
        'Appropriation': '',
        'Related Calculation ID': '',
        'Related Calc Input Value': ''
    }
    df_new = pd.DataFrame(new_columns, index=[0])
    df_all = pd.concat([df_all, df_new], axis=1)
    
    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)), 'Deduction Code'] = df_all['WD ER Code']
    df_all['Period End Date'] = pd.to_datetime(df_all['Pay Period End']).dt.strftime("%d-%b-%Y").str.upper()
    df_all['Payment Date'] = pd.to_datetime(df_all['Pay Date']).dt.strftime("%d-%b-%Y").str.upper()
    df_all['Pay Group'] = np.where((df_all['Company'] == 'FQ') & (df_all['Cost Center'].isin(ny_stores)),'USA_Weekly',
                      np.where((df_all['Company'] == 'TB') & (df_all['Cost Center'].isin(ny_stores)),'USA_TB_Weekly',
                      np.where(((df_all['Company'] == 'TB') & (~df_all['Cost Center'].isin(ny_stores))),'USA_TB_Bi-Weekly','USA_Bi-Weekly')))
    df_all['Source System'] = 'Kronos'
    
    #df_all['Earning Code'] = df_all['Earning Code*']
    #df_all['Deduction Code'] = df_all['WD ER Code']
    df_all_c = df_all[['Employee ID', 'Source System', 'Quarter', 'Pay Group',
                     'Company', 'Cost Center', 'Position ID', 'Period End Date',
                     'Payment Date', 'Third Party Sick Pay', 'Earning Code',
                     'Deduction Code', 'Amount', 'Taxable Wages', 'Subject Wages',
                     'Gross Wages', 'Supplemental Wages',
                     'Withholding Order - Case Number',
                     'Payroll State Tax Authority',
                     'Payroll Local County Tax Authority Code',
                     'Payroll Local City Tax Authority Code',
                     'Payroll Local Home School District Tax Authority Code',
                     'Payroll Local Other Tax Authority Code',
                     'Flexible Payment Deduction Worktag',
                     'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
                     'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
                     'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
                     'Custom Worktag #10', 'Custom Worktag #11',
                     'Custom Worktag #12', 'Custom Worktag #13',
                     'Custom Worktag #14', 'Custom Worktag #15', 'Allocation Pool',
                     'Appropriation', 'Related Calculation ID',
                     'Related Calc Input Value','Record Type']]
    
    df_all_f = df_all_c.fillna('x')
 
    df_all_f['Taxable Wages'] = df_all_f['Taxable Wages'].str.strip()
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].str.strip()
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].str.strip()
    df_all_f['Taxable Wages'] = df_all_f['Taxable Wages'].replace('',0)
    df_all_f['Taxable Wages'] = df_all_f['Taxable Wages'].astype(float)
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].replace('',0)
    df_all_f['Subject Wages'] = df_all_f['Subject Wages'].astype(float)
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].replace('',0)
    df_all_f['Gross Wages'] = df_all_f['Gross Wages'].astype(float)
    df_all_f = df_all_f.replace('','x')
               
    df_pivx = pd.pivot_table(df_all_f, index=['Employee ID',
                                             'Quarter',
                                             'Pay Group',
                                             'Company',
                                             'Cost Center',
                                             'Earning Code',
                                             'Deduction Code',
                                             'Payroll State Tax Authority',
                                             'Payroll Local County Tax Authority Code',
                                             'Payroll Local City Tax Authority Code',
                                             'Payroll Local Home School District Tax Authority Code',
                                             'Payroll Local Other Tax Authority Code'],
                            aggfunc={'Amount':'sum',
                                     'Taxable Wages':'sum',
                                     'Subject Wages':'sum',
                                     'Gross Wages':'sum'})
    df_pivx.reset_index(inplace=True)
    
    df_pivx['Earning Code'] = df_pivx['Earning Code'].str.replace('x','')
    df_pivx['Deduction Code'] = df_pivx['Deduction Code'].str.replace('x','')
    df_pivx['Payroll State Tax Authority'] = df_pivx['Payroll State Tax Authority'].astype(str)
    df_pivx['Payroll State Tax Authority'] = df_pivx['Payroll State Tax Authority'].str.replace('x','')
    df_pivx['Payroll Local County Tax Authority Code'] = df_pivx['Payroll Local County Tax Authority Code'].astype(str)
    df_pivx['Payroll Local County Tax Authority Code'] = df_pivx['Payroll Local County Tax Authority Code'].str.replace('x','')
    df_pivx['Payroll Local City Tax Authority Code'] = df_pivx['Payroll Local City Tax Authority Code'].astype(str)
    df_pivx['Payroll Local City Tax Authority Code'] = df_pivx['Payroll Local City Tax Authority Code'].str.replace('x','')
    df_pivx['Payroll Local Home School District Tax Authority Code'] = df_pivx['Payroll Local Home School District Tax Authority Code'].astype(str)
    df_pivx['Payroll Local Home School District Tax Authority Code'] = df_pivx['Payroll Local Home School District Tax Authority Code'].str.replace('x','')
    df_pivx['Payroll Local Other Tax Authority Code'] = df_pivx['Payroll Local Other Tax Authority Code'].astype(str)
    df_pivx['Payroll Local Other Tax Authority Code'] = df_pivx['Payroll Local Other Tax Authority Code'].str.replace('x','')
    df_pivx = modify_amount(df_pivx, 'Amount')
    df_pivx = modify_amount(df_pivx, 'Taxable Wages')
    df_pivx = modify_amount(df_pivx, 'Subject Wages')
    df_pivx = modify_amount(df_pivx, 'Gross Wages')
    
    
    new_columns = {
        'Position ID': '',
        'Third Party Sick Pay': '',
        'Supplemental Wages': '0',
        'Withholding Order - Case Number': '',
        'Flexible Payment Deduction Worktag': '',
        'Custom Worktag #1': '',
        'Custom Worktag #2': '',
        'Custom Worktag #3': '',
        'Custom Worktag #4': '',
        'Custom Worktag #5': '',
        'Custom Worktag #6': '',
        'Custom Worktag #7': '',
        'Custom Worktag #8': '',
        'Custom Worktag #9': '',
        'Custom Worktag #10': '',
        'Custom Worktag #11': '',
        'Custom Worktag #12': '',
        'Custom Worktag #13': '',
        'Custom Worktag #14': '',
        'Custom Worktag #15': '',
        'Allocation Pool': '',
        'Appropriation': '',
        'Related Calculation ID': '',
        'Related Calc Input Value': ''
    }
    df_new = pd.DataFrame(new_columns, index=[0])
    df_pivx = pd.concat([df_pivx, df_new], axis=1)
    
    df_pivx['Source System'] = 'Kronos'
    
    df_pivx = df_pivx[['Employee ID', 'Source System', 'Quarter', 'Pay Group',
                     'Company', 'Cost Center', 'Position ID', 'Third Party Sick Pay', 'Earning Code',
                     'Deduction Code', 'Amount', 'Taxable Wages', 'Subject Wages',
                     'Gross Wages', 'Supplemental Wages',
                     'Withholding Order - Case Number',
                     'Payroll State Tax Authority',
                     'Payroll Local County Tax Authority Code',
                     'Payroll Local City Tax Authority Code',
                     'Payroll Local Home School District Tax Authority Code',
                     'Payroll Local Other Tax Authority Code',
                     'Flexible Payment Deduction Worktag',
                     'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
                     'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
                     'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
                     'Custom Worktag #10', 'Custom Worktag #11',
                     'Custom Worktag #12', 'Custom Worktag #13',
                     'Custom Worktag #14', 'Custom Worktag #15', 'Allocation Pool',
                     'Appropriation', 'Related Calculation ID',
                     'Related Calc Input Value']]
    
    #df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\test_parallel_payer.csv')
    #df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)), 'Deduction Code'] = df_all['WD ER Code']

    #df_all['Period End Date'] = pd.to_datetime(df_all['Period End Date']).dt.strftime("%d-%b-%Y").str.upper()
    #df_all['Payment Date'] = pd.to_datetime(df_all['Payment Date']).dt.strftime("%d-%b-%Y").str.upper()
    #df_all['Earning Code'] = df_all['Earning Code*']
    #df_all['Deduction Code'] = df_all['WD ER Code']
    
    df_final = pd.concat([df_pivx_ee,df_pivx])
    
    df_final = df_final[df_final['Deduction Code'] != 'TIF']
    df_final['Earning Code'] = np.where(df_final['Earning Code'] == 'MISC','EMISC',df_final['Earning Code'])
    df_final['Deduction Code'] = np.where(df_final['Deduction Code'] == 'MISC','DMISC',df_final['Deduction Code'])
    #df_final['Taxable Wages'] = np.where(df_final['Taxable Wages'] == 0,'', df_final['Taxable Wages'])
    #df_final['Subject Wages'] = np.where(df_final['Subject Wages'] == 0,'', df_final['Subject Wages'])
    df_final.loc[(~df_final['Deduction Code'].str.startswith('W_')),'Taxable Wages'] = ''
    df_final.loc[(~df_final['Deduction Code'].str.startswith('W_')),'Subject Wages'] = ''
    df_final['Supplemental Wages'] = df_final['Supplemental Wages'].astype(float)
    df_final_x = df_final.groupby(['Employee ID','Quarter'])['Supplemental Wages'].aggregate('sum')
    df_final_x= df_final_x.reset_index()
    df_final = df_final.merge(df_final_x, on=['Employee ID','Quarter'])
    df_final.loc[df_final['Deduction Code'] == 'W_FW','Supplemental Wages_x'] = df_final['Supplemental Wages_y']
    df_final.rename(columns={'Supplemental Wages_x':'Supplemental Wages'},inplace=True)
    
    gross_drop = ['401K','401KR','W_COSPF','W_NMWCE','W_COFMS','W_COSPFM']
    df_final.loc[df_final['Deduction Code'].isin(gross_drop),'Gross Wages'] = ''
    sup_drop = ['ABNS','APBNS','BBNS','LJBNS','LSBNS','MBNS','PBNS',
             'PMBNS','PTBNS','QBNS','RBNS','RNBNS','SOBNS','TRBNS',
             'TDBNS','YEBNS','BNSHIS','PRFBNS']
    df_final.loc[(~df_final['Earning Code'].isin(sup_drop)) & (df_final['Deduction Code'] != 'W_FW'),'Supplemental Wages'] = ''
    df_final.loc[df_final['Payroll State Tax Authority'] == '', 'Payroll State Tax Authority'] = 0 
    df_final['Payroll State Tax Authority'] = df_final['Payroll State Tax Authority'].astype(float)
    df_final['Payroll State Tax Authority'] = df_final['Payroll State Tax Authority'].astype(int)
    df_final['Payroll State Tax Authority'] = df_final['Payroll State Tax Authority'].astype(str).str.zfill(2)
    df_final.loc[df_final['Payroll State Tax Authority'] == '00', 'Payroll State Tax Authority'] = ''

    #df_final['Period End Date'] = np.where((df_final['Pay Group'] == 'USA_Bi-Weekly') & (df_final['Quarter'] == 'Q1'),'31-MAR-2023',
                                         #  np.where((df_final['Pay Group'] == 'USA_Bi-Weekly') & (df_final['Quarter'] == 'Q2'),'30-JUN-2023',
                                          # np.where((df_final['Pay Group'] == 'USA_Weekly') & (df_final['Quarter'] == 'Q1'), '31-MAR-2023',
                                          # np.where((df_final['Pay Group'] == 'USA_Weekly') & (df_final['Quarter'] == 'Q2'), '30-JUN-2023',
                                         #  np.where((df_final['Pay Group'] == 'USA_TB_Bi-Weekly') & (df_final['Quarter'] == 'Q1') ,'25-JUL-2023',
                                          # np.where((df_final['Pay Group'] == 'USA_TB_Bi-Weekly') & (df_final['Quarter'] == 'Q1') ,'25-JUL-2023','25-JUL-2023'))))))
    
    df_final['Period End Date'] = np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_Bi-Weekly'),'20-MAR-2023', 
        np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_TB_Bi-Weekly'),'21-MAR-2023',
        np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_Weekly'),'20-MAR-2023',
        np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_TB_Weekly'),'21-MAR-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_Bi-Weekly'),'12-JUN-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_TB_Bi-Weekly'),'13-JUN-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_Weekly'),'26-JUN-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_TB_Weekly'),'27-JUN-2023','NA'))))))))
    
    df_final['Payment Date'] = np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_Bi-Weekly'),'27-MAR-2023', 
        np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_TB_Bi-Weekly'),'27-MAR-2023',
        np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_Weekly'),'27-MAR-2023',
        np.where((df_final['Quarter'] == '2023Q1') & (df_final['Pay Group'] == 'USA_TB_Weekly'),'27-MAR-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_Bi-Weekly'),'16-JUN-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_TB_Bi-Weekly'),'16-JUN-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_Weekly'),'30-JUN-2023',
        np.where((df_final['Quarter'] == '2023Q2') & (df_final['Pay Group'] == 'USA_TB_Weekly'),'30-JUN-2023','NA'))))))))
    
    
    #df_final['Payment Date'] = np.where(df_final['Quarter'] == '2023Q1','31-MAR-2023','30-JUN-2023')
    
    df_final = df_final[['Employee ID', 'Source System', 'Quarter', 'Pay Group',
                     'Company', 'Cost Center', 'Position ID', 'Period End Date',
                     'Payment Date', 'Third Party Sick Pay', 'Earning Code',
                     'Deduction Code', 'Amount', 'Taxable Wages', 'Subject Wages',
                     'Gross Wages', 'Supplemental Wages',
                     'Withholding Order - Case Number',
                     'Payroll State Tax Authority',
                     'Payroll Local County Tax Authority Code',
                     'Payroll Local City Tax Authority Code',
                     'Payroll Local Home School District Tax Authority Code',
                     'Payroll Local Other Tax Authority Code',
                     'Flexible Payment Deduction Worktag',
                     'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
                     'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
                     'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
                     'Custom Worktag #10', 'Custom Worktag #11',
                     'Custom Worktag #12', 'Custom Worktag #13',
                     'Custom Worktag #14', 'Custom Worktag #15', 'Allocation Pool',
                     'Appropriation', 'Related Calculation ID',
                     'Related Calc Input Value']]
    
    #df_final.to_excel(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\payroll_history.xlsx',index=False)
    
    
    
    
    write_to_csv(df, 'payroll_history_0826232.txt')

    df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\PROD_payroll_history_082523.csv")
    