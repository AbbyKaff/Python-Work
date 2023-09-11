import pandas as pd
import numpy as np
import os
import datetime
from datetime import date
from datetime import datetime
import xlrd
import glob as glob
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, open_as_utf8, modify_amount)


# -----------------------------------------------------------------------------
def get_tax_code(df):

    df_tax_codes = pd.read_csv(config.PATH_WD_IMP+ 'data files\\Kronos_Company_Taxes.csv')

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
def set_federal(df):

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
    ), 'Deduction Code'] = 'W_COFMS08WD001'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'ER ESA')
    ), 'Deduction Code'] = 'W_ALSEC01WD072R'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Maine UPAF')
    ), 'Deduction Code'] = 'W_MEUPA23WD083R'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'New Mexico Worker\'s Compensation Assessment Fee - Employer')
    ), 'Deduction Code'] = ''

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NJ Work Force Development/Supplemental Work Force')
    ), 'Deduction Code'] = 'W_NJTDENJ-TDB'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'NY MCTMT')
    ), 'Deduction Code'] = 'W_NYMCT36WD092R'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'] == 'Re-employment')
    ), 'Deduction Code'] = 'W_NYREM36WD088R'

    return df


# -----------------------------------------------------------------------------
def set_state_tax_authority(df):

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'].str.startswith(('SDI:', 'SIT:', 'SUI:', 'SUTA:'), na=False))
    ), 'Payroll State Tax Authority'] = df['E/D/T Code'].str[-2:]

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'].str.startswith(('SDI:','SIT:','SUI:','SUTA:'), na=False))
    ), 'Deduction Code'] = 'W_SUIER'

    df_states = pd.read_csv(config.PATH_WD_IMP + 'data files\\wd_states.csv')
    df_states = df_states[['Abbrev', 'Payroll']]
    df_states = df_states.dropna()
    df = df.merge(df_states[['Abbrev', 'Payroll']], right_on='Abbrev', left_on='Payroll State Tax Authority', how='left')
    #df['Payroll State Tax Authority'] = df['Payroll'].astype('Int64')
    df['Deduction Code'] = df['Deduction Code1'] + df['Payroll']

    return df


# -----------------------------------------------------------------------------
def set_county_tax_authority(df):

    col = 'Payroll Local County Tax Authority Code'

    df.loc[(df['Record Type'].str.startswith('Tax', na=False)
        & (df['E/D/T Code'].str.startswith(('CNTY:'), na=False))
    ), col] = df['WD ER Code'].str.replace('W_CNTYR', '')

    return df


# -----------------------------------------------------------------------------
def set_city_tax_authority(df):

    col = 'Payroll Local City Tax Authority Code'

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['E/D/T Code'].str.startswith(('CITY:'), na=False))
    ), col] = df_all['WD ER Code'].str.replace('W_CITYR', '')

    df_all[col] = df_all[col].str.replace('W_CITYW', '')

    return df


# -----------------------------------------------------------------------------
def set_local_home_tax_authority(df):

    col = 'Payroll Local Home School District Tax Authority Code'

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (df_all['Tax Type'] == 'SCHL')
    ), col] = df_all['WD ER Code'].str.replace('W_SCHDR', '')

    return df


# -----------------------------------------------------------------------------
def set_local_other_tax_authority(df):

    col = 'Payroll Local Other Tax Authority Code'

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['Tax Jurisdiction'] == 'Local')
        & (~df_all['Tax Type'].isin(['CITY', 'CNTY', 'SCHL']))
    ), col] = df_all['WD ER Code']

    return df


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df_all = pd.DataFrame()

    # for filename in glob.glob(config.PATH_WD_IMP + 'pay_hist\\EarningDeductionTaxListing_01_23.csv'):
    for filename in glob.glob(config.PATH_WD_IMP + 'pay_hist\\EarningDeductionTaxListing_*_23.csv'):
        print(filename)

        df = pd.read_csv(filename, dtype='object', encoding='cp1251')

        df_all = pd.concat([df_all, df], axis=0)
        print(len(df.index))
        print(len(df_all.index))
        
        os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\pay_hist\Q1')
        extension = 'csv'
        all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
        combined_csv = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])
        df_all = combined_csv

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
    df_all['Employee Id'] = df_all['Employee Id'].astype(str)
    df_all = df_all.loc[df_all['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    df_all = df_all.replace('^[-]$', '', regex=True)

    df_all = modify_amount(df_all, 'Record Gross Subject Wages')
    df_all = modify_amount(df_all, 'Record Gross Wages')
    df_all = modify_amount(df_all, 'Record Subject Wages')
    df_all = modify_amount(df_all, 'Record Amount')
    df_all = modify_amount(df_all, 'Record Amount (ER)')
    df_all['Record Amount'] = df_all['Record Amount'].astype(float)
    df_all['Record Amount (ER)'] = df_all['Record Amount (ER)'].astype(float)

    # df_all = df_all.loc[df_all['Type'] == 'Regular']
    df_all = df_all.loc[~df_all['E/D/T Code'].isnull()]
    df_all = df_all.loc[(df_all['Record Amount (ER)'] != 0)]
    print(len(df_all.index))

    df_all = df_all.rename(columns={'Employee Id': 'Employee ID',
                                    'Cost Centers(Cost Center)': 'Cost Center',
                                    'Pay Period End': 'Period End Date',
                                    'Pay Date': 'Payment Date',
                                    'Record Amount (ER)': 'Amount',
                                    'Record Gross Subject Wages': 'Taxable Wages',
                                    'Record Gross Wages': 'Gross Wages',
                                    'Record Subject Wages': 'Subject Wages'})

    df_all['Source System'] = 'Kronos'
    df_all['Quarter'] = pd.PeriodIndex(df_all['Payment Date'], freq='Q')

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

    df_all.loc[df_all['Record Type'].str.startswith('Deduction', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Tax', na=False), 'Deduction Code'] = df_all['Earning Code*']
    df_all.loc[df_all['Record Type'].str.startswith('Reimbursement', na=False), 'Deduction Code'] = df_all['Earning Code*']

    df_all = get_tax_code(df_all)
    df_all = get_pay_group(df_all)
    df_all = set_federal(df_all)
    ####################################################
    #df_all = set_state_tax_authority(df_all)
    
    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
       & (df_all['E/D/T Code'].str.startswith(('SDI:', 'SIT:', 'SUI:', 'SUTA:'), na=False))
    ), 'Payroll State Tax Authority'] = df_all['E/D/T Code'].str[-2:]

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['E/D/T Code'].str.startswith(('SDI:','SIT:','SUI:','SUTA:'), na=False))
    ), 'Deduction Code1'] = 'W_SUIER'

    df_states = pd.read_csv(config.PATH_WD_IMP + 'data files\\wd_states.csv', encoding ='cp1251')
    df_states = df_states[['Abbrev', 'Payroll']]
    df_states = df_states.dropna()
    df_all = df_all.merge(df_states, right_on='Abbrev', left_on='Payroll State Tax Authority', how='left')
    #df['Payroll State Tax Authority'] = df['Payroll'].astype('Int64')
    df_all['Deduction Code'] = df_all['Deduction Code1'] + df_all['Payroll']

    ####################################################    
    df_all = set_county_tax_authority(df_all)
    #df_all1 = set_city_tax_authority(df_all)
    
    #col = 'Payroll Local City Tax Authority Code'

    df_all.loc[(df_all['Record Type'].str.startswith('Tax', na=False)
        & (df_all['E/D/T Code'].str.startswith(('CITY:'), na=False))
    ), 'Payroll Local City Tax Authority Code'] = df_all['WD ER Code'].str.replace('W_CITYR', '')

    df_all['Payroll Local City Tax Authority Code'] = df_all['Payroll Local City Tax Authority Code'].astype(str).str.replace('W_CITYW', '')
    
    
    df_all = set_local_home_tax_authority(df_all)
    df_all = set_local_other_tax_authority(df_all)

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

    #df_all['Period End Date'] = pd.to_datetime(df_all['Period End Date']).dt.strftime("%d-%b-%Y").str.upper()
    #df_all['Payment Date'] = pd.to_datetime(df_all['Payment Date']).dt.strftime("%d-%b-%Y").str.upper()
    df_all['Earning Code'] = df_all['Earning Code*']
    df_all['Deduction Code'] = df_all['WD ER Code']
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
                     'Related Calc Input Value']]
    
    
    df_all_c = df_all_c.fillna('x')
    df_all_c['Taxable Wages'] = df_all_c['Subject Wages'].str.strip()
    df_all_c['Subject Wages'] = df_all_c['Subject Wages'].str.strip()
    df_all_c['Gross Wages'] = df_all_c['Gross Wages'].str.strip()
    df_all_c['Taxable Wages'] = df_all_c['Taxable Wages'].astype(float)
    df_all_c['Subject Wages'] = df_all_c['Subject Wages'].astype(float)
    df_all_c['Gross Wages'] = df_all_c['Gross Wages'].astype(float)
                                                     
    df_pivy = pd.pivot_table(df_all_c, index=['Employee ID',
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
    df_pivy.reset_index(inplace=True)
    
    df_pivy['Earning Code'] = df_pivy['Earning Code'].str.replace('x','')
    df_pivy['Deduction Code'] = df_pivy['Deduction Code'].str.replace('x','')
    df_pivy['Period End Date'] = np.where(df_pivy['Pay Group'] == 'USA_Bi-Weekly','20-MAR-2023',
                                          np.where(df_pivy['Pay Group'] == 'USA_Weekly', '20-MAR-2023',
                                                   np.where(df_pivy['Pay Group'] == 'USA_TB_Bi-Weekly','21-MAR-2023','21-MAR-2023')))

    df_pivy['Payment Date'] = '27-MAR-2023'
    
    #####Concat EE and ER
    df_payroll_hist = pd.concat([df_pivy,df_pivx])
    df_payroll_hist.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\pay_hist_ee_xx.csv')
    
    df_payroll_hist_x = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\pay_hist_ee5.csv')
    
    df_payroll_hist_x['Period End Date'] = np.where(df_payroll_hist_x['Pay Group'] == 'USA_Bi-Weekly','20-MAR-2023',
                                          np.where(df_payroll_hist_x['Pay Group'] == 'USA_Weekly', '20-MAR-2023',
                                                   np.where(df_payroll_hist_x['Pay Group'] == 'USA_TB_Bi-Weekly','21-MAR-2023','21-MAR-2023')))

    df_payroll_hist_x['Payment Date'] = '27-MAR-2023'

    df_payroll_hist_x['Payroll State Tax Authority'] = df_payroll_hist_x['Payroll State Tax Authority'].fillna(0)    
    df_payroll_hist_x['Payroll State Tax Authority'] = df_payroll_hist_x['Payroll State Tax Authority'].astype(int)
    df_payroll_hist_x['Payroll State Tax Authority'] = df_payroll_hist_x['Payroll State Tax Authority'].astype(str)
    df_payroll_hist_x['Payroll State Tax Authority'] = df_payroll_hist_x['Payroll State Tax Authority'].str.zfill(2)
    
    df_payroll_hist_x['Payroll State Tax Authority'] = df_payroll_hist_x['Payroll State Tax Authority'].str.replace('00','')
    
    df_payroll_hist_x['Payroll Local City Tax Authority Code'] = df_payroll_hist_x['Payroll Local City Tax Authority Code'].fillna(0)    
    df_payroll_hist_x['Payroll Local City Tax Authority Code'] = df_payroll_hist_x['Payroll Local City Tax Authority Code'].astype(str)
    df_payroll_hist_x.loc[df_payroll_hist_x['Payroll Local City Tax Authority Code'].str.endswith('.0',na=False),'Payroll Local City Tax Authority Code'] = df_payroll_hist_x['Payroll Local City Tax Authority Code'].str[:-2]
  
    df_payroll_hist_x['Payroll Local City Tax Authority Code'] = df_payroll_hist_x['Payroll Local City Tax Authority Code'].str.replace('0','')
    
    write_to_csv(df_payroll_hist_x, 'payroll_history_5_22_23.txt')
