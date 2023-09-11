##############################################################################
############################## State Tax Elections ###########################
##############################################################################

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
from common import (write_to_csv, active_workers, modify_amount)


# -----------------------------------------------------------------------------
def state_taxes(df):
    """ State Taxes """

    col = 'Additional Withholding Amount ($)'
    df = modify_amount(df, col)

    dict_exempt = {'Block W/H':'Y', 'Exempt':'Y', 'Yes':'N', '':'Y'}
    
    #TODO determine residence certification by comparing the last two columns of the data (need to map state abbreviation to work state first then compare) also use this function to filter out extra SIT for those who no longer have both
    df['Non Resident Flag'] = np.where(df['Cost Centers(State)'] != df['State Name'] , 'Y', 'N')
    df['Resident Flag'] = np.where(df['Cost Centers(State)'] == df['State Name'] , 'Y', 'N')


    # -------------------------------------------------------------------------
    payroll_state = '01'   # Alabama
    dict_marital_status = {'H':'Head of Family',
                           'M':'Married',
                           'MS':'Married Filing Separately',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '02'   # Alaska
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '04'   # Arizona
    df.loc[df['ID'] == payroll_state, 'Arizona Constant Percent'] = (df['State Elected Percentage Rate'].astype(float) / 100).round(3)
    # TODO: Exempt Flag?
    #df.loc[df['ID']== payroll_state, 'Exempt Flag'] = np.where((df['Arizona Constant Percent'].isna()) | (df['Arizona Constant Percent']==0),'Y','')
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    new_az_per = [[0.005,0.01,0.015,0.02,0.025,0.3,0.035]]
    df.loc[~df['Arizona Constant Percent'].isin(new_az_per), 'Effective As Of'] = '31-DEC-2019'

    # -------------------------------------------------------------------------
    payroll_state = '05'   # Arkansas
    dict_marital_status = {'H':'Head of Household',
                           'MJ':'Married Filing Jointly',
                           'S':'Single',
                           '': 'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)

    # -------------------------------------------------------------------------
    payroll_state = '06'   # California
    dict_marital_status = {'H':'Head of Household',
                           'M':'Married',
                           'S':'Single or Married (with two or more incomes)',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '08'   # Colorado
    dict_marital_status = {'M':'Married',
                           'MH':'Married filing jointly (or Qualifying widow(er))',
                           'S':'Single or Married filing separately',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'

    # -------------------------------------------------------------------------
    payroll_state = '09'   # Connecticut
    #     payroll_marital_status = 'A' # 'B' 'C' 'D' 'F'
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Reduced Withholding Amount'] = df['Reduced withholding']
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '10'   # Deleware
    dict_marital_status = {'M':'Married',
                           'MH':'Single',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']
    # -------------------------------------------------------------------------
    payroll_state = '11'   # District of Columbia
    dict_marital_status = {'H':'Head of Household',
                           'MJ':'Married/Domestic Partner filing jointly',
                           'MS':'Married Filing Separately',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']
    # TODO: get other values, if needed

    # -------------------------------------------------------------------------
    payroll_state = '12'   # Florida
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '13'   # Georgia
    dict_marital_status = {'A':'A - Single',
                           'B':'B - Married 2 Incomes',
                           'C':'C - Married 1 Income',
                           'D':'D - Married Filing Separate',
                           'E':'E - Head of Household',
                           '':'A - Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status) 
    #df.loc[(df['ID'] == payroll_state) & (df['Withholding Exemptions'] != ''), 'Withholding Exemptions'] = df['Withholding Exemptions'].astype(int)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Allowance'] = df['Additional Allowances']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    #df['Number of Allowances'] = np.where((df['ID'] == payroll_state) & (df['Number of Allowances'] == 99),0,df['Number of Allowances'])
    #df['Additional Allowance'] = np.where((df['ID'] == payroll_state) & (df['Withholding Exemptions'] != '') & (df['Number of Allowances'] > 2), df['Number of Allowances'] - 2 + df['Additional Allowance'],df['Additional Allowance'])
    #df['Number of Allowances'] = np.where((df['ID'] == payroll_state) & (df['Withholding Exemptions'] != '') & (df['Number of Allowances'] > 2),2,df['Number of Allowances'])
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '15'   # Hawaii
    dict_marital_status = {'M':'Married',
                           'MH':'Married but withhold at higher Single rate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Allowance'] = df['Additional Allowances']
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '16'   # Idaho
    dict_marital_status = {'M':'Married',
                           'MH':'Married but withhold at higher Single rate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']

    # -------------------------------------------------------------------------
    payroll_state = '17'   # Illinois
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Allowance'] = df['Additional Allowances']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '18'   # Indiana

    # df['Effective From'] = np.where(((df['ID'] == payroll_state) & (pd.to_datetime(df['Effective From']) <= '2023-01-01')), '01-JAN-2023', pd.to_datetime(df['Effective From']).dt.date)
    df['Effective From'] = '01-JAN-2023'

    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Certificate of Residence'] = df['Resident Flag']
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '19'   # Iowa
    dict_marital_status = {'M':'Married',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']

    # -------------------------------------------------------------------------
    payroll_state = '20'   # Kansas
    dict_marital_status = {'M':'Joint',
                           'MJ':'Joint',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '21'   # Kentucky
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']
    # -------------------------------------------------------------------------
    payroll_state = '22'   # Louisiana
    dict_marital_status = {'M':'Married',
                           'S':'Single',
                           '':'No exemptions or dependents claimed'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '23'   # Maine
    dict_marital_status = {'M':'Married',
                           'MH':'Married but withhold at higher Single rate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'

    # -------------------------------------------------------------------------
    payroll_state = '24'   # Maryland
    dict_marital_status = {'M':'Married (surviving spouse or unmarried Head of Household)',
                           'MH':'Married, but withhold at Single rate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '25'   # Massachusetts
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '26'   # Michigon
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']

    # -------------------------------------------------------------------------
    payroll_state = '27'   # Minnesota
    dict_marital_status = {'M':'Married',
                           'MH':'Married but withhold at higher Single rate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Certificate of Residence'] = df['Resident Flag']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '28'   # Mississippi
    dict_marital_status = {'H':'Head of Family',
                           'M':'Married',
                           'MH':'Married (Spouse Employed)',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']

    # -------------------------------------------------------------------------
    payroll_state = '29'   # Missouri
    dict_marital_status = {'H':'Head of Household',
                           'M':'Married (Spouse does not work)',
                           'S':'Single or Married Spouse Works or Married Filing Separate',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Certificate of Residence'] = df['Resident Flag']
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '30'   # Montana
    dict_marital_status = {'M':'Married',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'

    # -------------------------------------------------------------------------
    payroll_state = '31'   # Nebraska
    dict_marital_status = {'M':'Married',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'

    # -------------------------------------------------------------------------
    payroll_state = '32'   # Nevada
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '33'   # New Hampshire
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '34'   # New Jersey
    dict_marital_status = {'H':'Head of Household',
                           'MJ':'Married/Civil Union Couple Joint',
                           'MS':'Married/Civil Union Couple Separate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']

    # -------------------------------------------------------------------------
    payroll_state = '35'   # New Mexico
    dict_marital_status = {'H':'Head of Household',
                           'M':'Married filing jointly (or Qualifying widow(er))',
                           'S':'Single or Married filing separately',
                           '':'Single or Married filing separately'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)

    # -------------------------------------------------------------------------
    payroll_state = '36'   # New York
    dict_marital_status = {'H':'Single or Head of Household',
                           'M':'Married',
                           'MH':'Married but withhold at higher Single rate',
                           'S':'Single or Head of Household',
                           '':'Single or Head of Household'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '37'   # North Carolina
    dict_marital_status = {'H':'Head of Household',
                           'M':'Married or Qualifying widow(er)',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '38'   # North Dakota
    dict_marital_status = {'H':'Head of Household',
                           'M':'Married',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)

    # -------------------------------------------------------------------------
    payroll_state = '39'   # Ohio
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']

    # -------------------------------------------------------------------------
    payroll_state = '40'   # Oklahoma
    dict_marital_status = {'M':'Married',
                           'MH':'Married but withhold at higher Single rate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)

    # -------------------------------------------------------------------------
    payroll_state = '41'   # Oregon
    dict_marital_status = {'M':'Married',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'

    # -------------------------------------------------------------------------
    payroll_state = '42'   # Pennsylvania
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '44'   # Rhode Island
    dict_marital_status = {'M':'Married',
                           'MH':'Married but withhold at higher Single rate',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '45'   # South Carolina
    dict_marital_status = {'M':'Married',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '46'   # South Dakota
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '47'   # Tennessee
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '48'   # Texas
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '49'   # Utah
    dict_marital_status = {'H':'Head of Household',
                           'M':'Married filing jointly (or Qualifying widow(er)',
                           'S':'Single or Married filing separately',
                           '':'Single or Married filing separately'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Lock in Letter'] = 'N'
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '50'   # Vermont
    dict_marital_status = {'M':'Married',
                           'MJ':'Married/Civil Union Filing Jointly',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']

    # -------------------------------------------------------------------------
    payroll_state = '51'   # Virginia
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'Certificate of Residence'] = df['Resident Flag']
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '53'   # Washington
    # No State Income Tax

    # -------------------------------------------------------------------------
    payroll_state = '54'   # West Virginia
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''

    # -------------------------------------------------------------------------
    payroll_state = '55'   # Wisconsin
    dict_marital_status = {'M':'Married',
                           'S':'Single',
                           '':'Single'}
    df.loc[df['ID'] == payroll_state, 'Payroll Marital Status Reference'] = df['Filing Status.1'].replace(dict_marital_status)
    df.loc[df['ID'] == payroll_state, 'Number of Allowances'] = df['Withholding Exemptions']
    df.loc[df['ID'] == payroll_state, 'Additional Amount'] = df['Additional Withholding Amount ($)']
    df.loc[df['ID'] == payroll_state, 'Exempt'] = df['EE Withholding Status'].replace(dict_exempt)
    df.loc[df['ID'] == payroll_state, 'No Wage No Tax Indicator'] = ''
    df.loc[df['ID'] == payroll_state, 'Certificate of Non Residence'] = df['Non Resident Flag']

    # -------------------------------------------------------------------------
    payroll_state = '56'   # Wyoming
    # No State Income Tax

# -----------------------------------------------------------------------------
if __name__ == '__main__':

    state = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72596866.csv', dtype='object', encoding='cp1251')
    #state2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72869092.csv', dtype='object', encoding='cp1251')
    #state = pd.concat([state,state2])
    state['Home State'] = state['State']
    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    state['Employee Id'] = state['Employee Id'].astype(str)
    state = state.loc[state['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    work_del = pd.read_csv(config.PATH_WD_IMP + 'Workday Delivered\Integration_IDs.csv', encoding="cp1251")
    work_del_2 = work_del[work_del['Business Object'] == 'Payroll State Authority']
    work_del_2['ID'] = work_del_2['ID'].astype(str).str.zfill(2)

    statec = state.merge(work_del_2, left_on='Company Tax Name', right_on='Instance')

    # statec = statec.replace('|-|', '||')
    statec = statec.replace('^[-]$', '', regex=True)

    new_cols = {
    	'Additional Allowance': '',
    	'Additional Amount': '',
    	'Arizona Constant Percent': '',
    	"Certificate of Non Residence": '',
    	"Certificate of Residence": '',
    	'Exempt': '',
    	'Lock in Letter': '',
    	'No Wage No Tax Indicator':'',
    	'Number of Allowances': '',
    	'Payroll Marital Status Reference': '',
    	'Reduced Withholding Amount': '',
     'Non Resident Flag':'',
     'Resident Flag' : ''
    }
    df_new = pd.DataFrame(new_cols, index=[0])
    statec = pd.concat([statec, df_new], axis=1)
    
    statefile = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\state_match.csv")
    statec = statec.merge(statefile, left_on='Home State', right_on='State Abbreviation')

    state_taxes(statec)
    statec.to_excel(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\taxcheck.xlsx',index=False)
    
    #filter out where instance is not in Cost Centers(State) or State Name
    statec = statec.loc[(statec['Instance'] == statec['Cost Centers(State)']) | (statec['Instance'] == statec['State Name'])]
    statec2 = statec.loc[(statec['Instance'] != statec['Cost Centers(State)']) & (statec['Instance'] != statec['State Name'])]
    #statec1.to_excel(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\exclude_tax2.xlsx',index=False)
    #statec = state
    
    statega = statec.loc[(statec['ID'] == '13') & (statec['Number of Allowances'] != '')]
    statec = statec.loc[~((statec['ID'] == '13') & (statec['Number of Allowances'] != ''))]
    statega['Number of Allowances'] = np.where(statega['Number of Allowances'] == '99','0',statega['Number of Allowances'])
    statega['Additional Allowance'] = 0
    statega['Number of Allowances'] = statega['Number of Allowances'].astype(int)
    statega['Additional Allowance'] = statega['Additional Allowance'].astype(int)
    statega['Additional Allowance'] = np.where(statega['Number of Allowances'] > 2, statega['Number of Allowances'] - 2 + statega['Additional Allowance'], statega['Additional Allowance'])
    statega['Number of Allowances']  = np.where(statega['Number of Allowances']  > 2, statega['Number of Allowances'] - statega['Additional Allowance'],statega['Number of Allowances'])
    statec = pd.concat([statec,statega])
    
    statec['Number of Allowances'] = statec['Number of Allowances'].str.replace(',','')
    statec['Employee ID'] = statec['Employee Id']
    statec['Company'] = statec['Cost Centers(Company Code)'].replace({'FQSR':'FQ'})
    statec['Source System'] = 'Kronos'
    statec['Effective From'] = pd.to_datetime(statec['Effective From'])
    statec['Date Hired'] = pd.to_datetime(statec['Date Hired'])
    statec['Effective As Of'] = np.where(statec['Effective From'] > statec['Date Hired'], statec['Effective From'], statec['Date Hired'])
    statec['Effective As Of'] = statec['Effective As Of'].dt.strftime("%d-%b-%Y").str.upper()
    statec['Payroll State Tax Authority'] = statec['ID']

    new_cols = {
    	'Active Duty Oklahoma': '',
    	'Additional Percent': '',
    	'Allocation Percent': statec['State Elected Percentage Rate'],
    	'Allowance on Special Deduction': '',
    	'Annual Withholding Amount': '',
    	'Certificate of Withholding Exemption and County Status': '',
    	'Dependent Allowance': statec['Dependent Allowances'],
    	'Domicile State Tax Authority': '',
    	'Employee Blind': '',
    	'Entrepreneur Exemption': '',
    	'Estimated Deductions': statec['Deduction'],
    	'Estimated Tax Credit per Period': '',
    	'Exempt Reason': '',
    	'Exemption for Dependents Complete': '',
    	'Exemption for Dependents Joint Custody': '',
    	'Exemptions for Mississippi': '',
    	'Fort Campbell Exempt Kentucky': '',
    	'Full-time Student Indicator': '',
    	'Head of Household': '',
    	'Inactivate State Tax': '',
    	'Increase or Decrease Withholding Amount': '',
    	'Lower Tax Rate': '',
    	'Married Filing Jointly Optional Calculation': '',
    	'MSRR Exempt': '',
    	'MSRR Exempt Entrepreneur Exemption': '',
    	'New Jersey Rate Table Specification': '',
    	'Reduced Withholding per Pay Period': '',
    	'Services Localized in Illinois': '',
    	'Spouse Indicator': '',
    	'Veteran Exemption': '',
    	'Withholding Exemption': statec['Withholding Exemptions'],
    	'Withholding Substantiated': 'N'
    }
    df_new = pd.DataFrame(new_cols, index=[0])
    statec = pd.concat([statec, df_new], axis=1)

    ste_data = statec[['Employee ID', 'Source System', 'Company', 'Effective As Of',
           'Payroll Marital Status Reference', 'Payroll State Tax Authority',
           'Married Filing Jointly Optional Calculation', 'Veteran Exemption',
           'Exemption for Dependents Complete',
           'Exemption for Dependents Joint Custody',
           'Allowance on Special Deduction', 'New Jersey Rate Table Specification',
           'Number of Allowances', 'Exemptions for Mississippi',
           'Estimated Deductions', 'Dependent Allowance', 'Additional Allowance',
           'Withholding Exemption', 'Allocation Percent', 'Additional Percent',
           'Services Localized in Illinois', 'Arizona Constant Percent',
           'Additional Amount', 'Annual Withholding Amount',
           'Reduced Withholding Amount', 'Estimated Tax Credit per Period',
           'Exempt', 'Exempt Reason', 'Withholding Substantiated',
           'Certificate of Non Residence', 'Certificate of Residence',
           'Certificate of Withholding Exemption and County Status',
           'Head of Household', 'Employee Blind', 'Spouse Indicator',
           'Full-time Student Indicator', 'Lower Tax Rate', 'Inactivate State Tax',
           'Lock in Letter', 'Active Duty Oklahoma',
           'Fort Campbell Exempt Kentucky', 'MSRR Exempt',
           'Entrepreneur Exemption', 'Domicile State Tax Authority',
           'No Wage No Tax Indicator', 'Increase or Decrease Withholding Amount',
           'Reduced Withholding per Pay Period', 'Annual Withholding Allowance']]

    #ste_data = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\state_tax_elections.csv")
    ste_data.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\state_tax.csv')
    ste_data['Effective As Of'] = pd.to_datetime(ste_data['Effective As Of'])
    ste_data['Effective As Of'] = ste_data['Effective As Of'].dt.strftime("%d-%b-%Y").str.upper()
    
    ste_data.loc[ste_data['Payroll State Tax Authority'] == '17','Effective As Of'] = '01-JAN-2023'
    
    ste_data.loc[ste_data['Employee ID'] == '194280','Payroll Marital Status Reference'] = 'M'
    write_to_csv(ste_data, 'state_tax_elections.txt')
