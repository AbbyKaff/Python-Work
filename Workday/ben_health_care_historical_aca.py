##############################################################################
###################### Local Work County Tax Elections #######################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8, set_max_event_date)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    active_bene = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72578980.csv', dtype='object', encoding='cp1251')
    # active_bene = active_bene[active_bene['Coverage Name'] != 'Waived']

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    active_bene['Employee Id'] = active_bene['Employee Id'].astype(str)
    active_bene = active_bene.loc[active_bene['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_benec = active_bene[active_bene['Benefit Type'].isin(['Medical'])]
    active_benec = active_benec.rename(columns={'Employee Id': 'Employee ID'})
    
    df1 = active_benec.groupby('Employee ID')['Amount EE 1'].sum()
    df1 = df1.reset_index()
    df1.loc[df1['Amount EE 1'] == 0,'Waived Flag'] = 'Y'
    
    active_benec = active_benec.merge(df1, on='Employee ID')
    
    active_bened = active_benec[active_benec['Waived Flag'] == 'Y']
    active_benec = active_benec[active_benec['Waived Flag'] != 'Y']
    active_benec = active_benec[active_benec['Coverage Name']!= 'Waived']
    

    active_benec = set_max_event_date(active_benec)
    active_benec.loc[active_benec['Event Date'] > '01-JAN-2023', 'Event Date'] = '01-JAN-2023'

    active_benec['Benefit Event Type'] = 'BEN_CONVERSION_HEALTHCARE'
    active_benec['Health Care Coverage Plan'] = active_benec['Benefit Plan Name']

    dict_coverage_target = {'Employee Only': 'EE_Only',
                          'Employee + Spouse': 'EE+Spouse',
                          'Employee + Child': 'EE+Child(ren)',
                          'Employee + Children': 'EE+Child(ren)',
                          'Employee + Family': 'EE+Family'}

    active_benec['Health Care Coverage Target'] = active_benec['Coverage Name'].replace(dict_coverage_target)

    active_benec['Employee ID'] = active_benec['Employee ID'].astype(int)

    #merge dependent id from related person
    dep_ff = pd.read_csv(config.PATH_WD_IMP + 'data sources\\dependents.csv', dtype='object', encoding='cp1251')
    dep_ff.drop_duplicates(inplace=True)
    dep_ff['Employee ID'] = dep_ff['Employee ID'].astype(str)
    active_benec['Employee ID'] = active_benec['Employee ID'].astype(str)
    dep_ff = dep_ff[dep_ff['Benefit Type'].isin(['Medical', 'Dental', 'Vision', 'Voya Accident'])]
    erp_c = dep_ff[['Employee ID', 'Dependent ID','Benefit Type']]
    active_benec = active_benec.merge(erp_c, on=['Employee ID','Benefit Type'],how='left')

    active_benec = active_benec[['Employee ID', 'Event Date', 'Benefit Event Type',
                                 'Health Care Coverage Plan', 'Health Care Coverage Target',
                                 'Dependent ID']]

    active_benec['Source System'] = 'Kronos'
    active_benec['Waived'] = np.where(active_benec['Health Care Coverage Target'] == 'Waived', 'Y', 'N')
    active_benec['Provider ID (Primary Physician)'] = ''
    active_benec['Provider ID (Primary Physician) - Dependent'] = ''
    # active_benec['Original Coverage Begin Date'] = ''
    active_benec['Deduction Begin Date'] = ''
    active_benec['Employee Cost PreTax'] = ''
    active_benec['Employee Cost PostTax'] = ''
    active_benec['Employer Cost NonTaxable'] = ''
    active_benec['Employer Cost Taxable'] = ''
    # active_benec['Enrollment Signature Date'] = ''
    # active_benec['Signing Worker'] = ''

    # hce_data = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Benefits_Template_01122023 (1).xlsx',
    #                          sheet_name='Health Care Elections', skiprows=2, nrows=0)

    hce_data = active_benec[['Employee ID', 'Source System', 'Event Date', 'Waived',
                           'Health Care Coverage Plan', 'Health Care Coverage Target',
                           'Provider ID (Primary Physician)', 'Dependent ID',
                           'Provider ID (Primary Physician) - Dependent',
                           'Deduction Begin Date',
                           'Employee Cost PreTax', 'Employee Cost PostTax',
                           'Employer Cost NonTaxable', 'Employer Cost Taxable']]

    dict_coverage_plan = {'Accident': 'USA - Voluntary Accident - Voya',
                          'Dental A': 'USA - Dental - Delta Dental Plan A',
                          'Dental A Company Paid': 'USA - Dental - Delta Dental Plan A',
                          'Dental B': 'USA - Dental - Delta Dental Plan B',
                          'MEC Bronze': 'USA - Medical - American Worker (MEC) Plan Bronze',
                          'MEC Gold': 'USA - Medical - American Worker (MEC) Plan Gold',
                          'MEC Silver': 'USA - Medical - American Worker (MEC) Plan Silver',
                          'Medical A': 'USA - Medical - Surest/UHC Plan A',
                          'Medical A - Company Paid': 'USA - Medical - Surest/UHC Plan A',
                          'Medical B': 'USA - Medical - Surest/UHC Plan B',
                          'Vision A': 'USA - Vision - Surency Plan A',
                          'Vision A- Company Paid': 'USA - Vision - Surency Plan A',
                          'Vision B': 'USA - Vision - Surency Plan B',
                          'Vol Delta Dental of KS - Pre-Tax': 'USA - Dental - Delta Dental Plan B'}

    hce_data['Health Care Coverage Plan'] = hce_data['Health Care Coverage Plan'].replace(dict_coverage_plan)
    
    
    #add waived records
    active_bened = set_max_event_date(active_bened)
    active_bened.loc[active_bened['Event Date'] > '01-JAN-2023', 'Event Date'] = '01-JAN-2023'
    active_bened = active_bened[['Employee ID','Event Date','Waived Flag']]
    active_bened.drop_duplicates(inplace=True)
    active_bened.rename(columns={'Waived Flag':'Waived'}, inplace=True)
    active_bened['Source System'] = 'Kronos'
    active_bened['Provider ID (Primary Physician)'] = ''
    active_bened['Provider ID (Primary Physician) - Dependent'] = ''
    # active_benec['Original Coverage Begin Date'] = ''
    active_bened['Deduction Begin Date'] = ''
    active_bened['Employee Cost PreTax'] = ''
    active_bened['Employee Cost PostTax'] = ''
    active_bened['Employer Cost NonTaxable'] = ''
    active_bened['Employer Cost Taxable'] = ''
    active_bened['Health Care Coverage Plan'] = ''
    active_bened['Health Care Coverage Target'] = ''
    active_bened['Dependent ID'] = ''
    hce_data2 = active_bened[['Employee ID', 'Source System', 'Event Date', 'Waived',
                           'Health Care Coverage Plan', 'Health Care Coverage Target',
                           'Provider ID (Primary Physician)', 'Dependent ID',
                           'Provider ID (Primary Physician) - Dependent',
                           'Deduction Begin Date',
                           'Employee Cost PreTax', 'Employee Cost PostTax',
                           'Employer Cost NonTaxable', 'Employer Cost Taxable']]
    hce_data = pd.concat([active_benec, active_bened])
    
    hce_data.to_csv('hist_health_care_elections_aca_WD.csv')

    write_to_csv(hce_data, 'historical_health_care_append_.txt')
    
    hce = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\hist_healthcare_fixes.xlsx")
    hce['Event Date'] = pd.to_datetime(hce['Event Date']).dt.strftime("%d-%b-%Y").str.upper()
