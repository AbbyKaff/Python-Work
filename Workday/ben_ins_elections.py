# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 06:24:25 2023

@author: akaff
"""

##############################################################################
######################### Insurance Elections ##############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
#import xlrd
import os
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8, set_max_event_date)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    active_bene = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72830427.csv', dtype='object', encoding='cp1251')
    active_bene = active_bene[active_bene['Coverage Name'] != 'Waived']
    active_bene['Coverage Effective From'] = pd.to_datetime(active_bene['Coverage Effective From'])
    active_bene = active_bene[active_bene['Coverage Effective From'] <= '9/1/2023']

    #------------------------------------------------------------------------------
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
    #active_bene['Employee Id'] = active_bene['Employee Id'].astype(str)
    #active_bene = active_bene.loc[active_bene['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_bene = modify_amount(active_bene, 'Coverage Amount 1')

    active_bene_i = active_bene[active_bene['Benefit Plan Name'].isin(['Basic AD&D Voya',
 'Basic Life Voya',
 'Child Critical Illness',
 'Critical Illness',
  'Long Term Disability Voya',
 'Northwestern Mutual Long Term Disability',
 'Short Term Disability Voya',
 'Spouse Critical Illness',
 'Voluntary Child Life Voya',
 'Voluntary Employee Life Voya',
 'Voluntary Spouse Life Voya'])]
    active_bene_i = active_bene_i.rename(columns={'Employee Id': 'Employee ID'})

    active_bene_i = set_max_event_date(active_bene_i)
    active_bene_i['Coverage Effective From'] = active_bene_i['Coverage Effective From_x']
    active_bene_i = active_bene_i.drop(['Coverage Effective From_x','Coverage Effective From_y'],axis=1)
    
    # TODO only keep one benefit, find anyone missing in life and only has ADD bc no beneficiaries from ADD converted
    
    life = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Basic Life Voya']
    life['life_id'] = life['Employee ID']
    add = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Basic AD&D Voya']
    add['life_id'] = ''
    fix_life = life.merge(add,on='Employee ID',how='outer')
    fix_life_c = fix_life[fix_life['life_id_x'].isna()]
    fix_life_c.columns = fix_life_c.columns.str.replace('_y','')
    fix_life_c['Coverage Effective From_y'] = fix_life_c['Coverage Effective From']
    #fix_life_c=fix_life_c.drop(fix_life_c.columns[11], axis=1)
    #fix_life_c = fix_life_c.rename(columns={fix_life_c.columns[11]: 'Coverage Effective From2'})
    fix_life_c = fix_life_c[['Employee ID', 'First Name', 'Last Name', 'Employee EIN',
           'Employee Status', 'Benefit Type', 'Benefit Plan Name', 'Coverage Name',
           'Amount EE 1', 'Amount ER 1', 'Coverage Effective To',
           'Benefit Plan Coverage Type 1', 'Beneficiaries',
           'Benefit Plan Description', 'Coverage Amount 1', 'Coverage Amount 2',
           'Coverage Amount 3', 'Conditional Offer', '# Units', '# Beneficiaries',
           '# Dependents', '# Spouses', 'Dependents', 'Event Date',
           'Coverage Effective From']]
    life = life.drop(['life_id'],axis=1)
    df_life = pd.concat([life,fix_life_c])
    
    active_bene_i = active_bene_i[~active_bene_i['Benefit Plan Name'].isin(['Basic Life Voya','Basic AD&D Voya'])]
    active_bene_i = pd.concat([active_bene_i, df_life])

    # TODO check if pre req EE level enrollment for Life and Critical Illness
    ee_ci = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Critical Illness']
    sp_ci = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Spouse Critical Illness']
    ch_ci = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Child Critical Illness']
    ee_ci = ee_ci[['Employee ID','Benefit Plan Name','Coverage Name']]    
    sp_ci = sp_ci[['Employee ID','Benefit Plan Name','Coverage Name']]      
    ch_ci = ch_ci[['Employee ID','Benefit Plan Name','Coverage Name']]  
    
    ee_ci_c = ee_ci.merge(sp_ci, on='Employee ID', how='outer')
    ee_ci_c = ee_ci_c[ee_ci_c['Benefit Plan Name_x'].isna()]
    ee_ci_c.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\out_of_compliance_SP_CI.csv')
    
    ee_ci_d = ee_ci.merge(ch_ci, on='Employee ID', how='outer')
    ee_ci_d = ee_ci_d[ee_ci_d['Benefit Plan Name_x'].isna()]
    ee_ci_d.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\out_of_compliance_CH_CI.csv')
    
    ee_life = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Voluntary Employee Life Voya']
    sp_life = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Voluntary Spouse Life Voya']
    ch_life = active_bene_i[active_bene_i['Benefit Plan Name'] == 'Voluntary Child Life Voya']
    ee_life = ee_life[['Employee ID','Benefit Plan Name','Coverage Name']]    
    sp_life = sp_life[['Employee ID','Benefit Plan Name','Coverage Name']]      
    ch_life = ch_life[['Employee ID','Benefit Plan Name','Coverage Name']]  
    
    ee_lifec = ee_life.merge(ch_life, on='Employee ID', how='outer')
    ee_lifec = ee_lifec[ee_lifec['Benefit Plan Name_x'].isna()]
    ee_lifec.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\out_of_compliance_CH_life.csv')
    
    
        #merge dependent id from related person
    dep_ff = pd.read_csv(config.PATH_WD_IMP + 'data sources\\dependents.csv', dtype='object', encoding='cp1251')
    dep_ff.drop_duplicates(inplace=True)
    dep_ff['Employee ID'] = dep_ff['Employee ID'].astype(str)
    dep_ff = dep_ff[dep_ff['Benefit Plan Name'].isin(['Basic AD&D Voya',
 'Basic Life Voya',
 'Child Critical Illness',
 'Critical Illness',
  'Long Term Disability Voya',
 'Northwestern Mutual Long Term Disability',
 'Short Term Disability Voya',
 'Spouse Critical Illness',
 'Voluntary Child Life Voya',
 'Voluntary Employee Life Voya',
 'Voluntary Spouse Life Voya'])]
    #deps = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc\E2E_emp_rel_per_v2.txt", sep="|")

    erp_c = dep_ff[['Employee ID', 'Dependent ID', 'Beneficiary ID','Benefit Plan Name','Dependent % Of Distribution']]
    #don't include beneficiary's per Jira KBP-4382
    nonben = ['Voluntary Child Life Voya','Voluntary Spouse Life Voya','Long Term Disability Voya','Child Critical Illness','Critical Illness','Spouse Critical Illness']
    nodep = ['Critical Illness','Basic Life Voya','Basic AD&D Voya','Long Term Disability Voya','Voluntary Employee Life Voya']
    erp_c.loc[(erp_c['Benefit Plan Name'].isin(nonben)),'Beneficiary ID'] = np.nan
    erp_c.loc[(erp_c['Benefit Plan Name'].isin(nonben)),'Dependent % Of Distribution'] = ''
    erp_c.loc[(erp_c['Benefit Plan Name'].isin(nodep)),'Dependent ID'] = np.nan
    
    erp_c.drop_duplicates(inplace=True)
    erp_c = erp_c[~(erp_c['Dependent ID'].isna() & erp_c['Beneficiary ID'].isna())]
    
    active_bene_i = active_bene_i.merge(erp_c, on=['Employee ID','Benefit Plan Name'],how='left')

    active_bene_i['Benefit Event Type'] = 'BEN_CONVERSION_INSURANCE'

    active_bene_i['Insurance Coverage Plan'] = active_bene_i['Benefit Plan Name']
    active_bene_i = active_bene_i[active_bene_i['Insurance Coverage Plan'] != 'Short Term Disability - Employer Paid']

    active_bene_i['Coverage Amount 1K'] = ((np.ceil(active_bene_i['Coverage Amount 1'].astype(float) / 1000) * 1).astype(int)).astype(str) + 'K'
    active_bene_i['Coverage Amount 5K'] = ((np.ceil(active_bene_i['Coverage Amount 1'].astype(float) / 5000) * 5).astype(int)).astype(str) + 'K'
    active_bene_i['Coverage Amount 10K'] = ((np.ceil(active_bene_i['Coverage Amount 1'].astype(float) / 10000) * 10).astype(int)).astype(str) + 'K'

    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Basic AD&D Voya', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_EE_' + active_bene_i['Coverage Amount 10K']
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Basic Life Voya', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_EE_' + active_bene_i['Coverage Amount 10K']
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Voluntary Child Life Voya', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_CH_' + active_bene_i['Coverage Amount 1K']
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Voluntary Employee Life Voya', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_EE_' + active_bene_i['Coverage Amount 10K']
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Voluntary Spouse Life Voya', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_SP_' + active_bene_i['Coverage Amount 5K']

    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Long Term Disability', (
        'Insurance Coverage')] = 'PCT_COV_MASTER_AMOUNT_STD_LTD_60%Salary'
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Long Term Disability Voya', (
        'Insurance Coverage')] = 'PCT_COV_MASTER_AMOUNT_STD_LTD_60%Salary'
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Northwestern Mutual Long Term Disability', (
        'Insurance Coverage')] = 'PCT_COV_MASTER_AMOUNT_STD_LTD_60%Salary'

    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Short Term Disability Voya', (
        'Insurance Coverage')] = 'PCT_COV_MASTER_AMOUNT_STD_LTD_60%Salary'

    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Child Critical Illness', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_CH_' + active_bene_i['Coverage Amount 5K']
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Critical Illness', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_EE_' + active_bene_i['Coverage Amount 10K']
    active_bene_i.loc[active_bene_i['Benefit Plan Name'] == 'Spouse Critical Illness', (
        'Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_SP_' + active_bene_i['Coverage Amount 5K']
    
    active_bene_i.loc[(active_bene_i['Benefit Plan Name'] == 'Critical Illness') & (active_bene_i['Insurance Coverage'] == 'CUR_COV_MASTER_AMOUNT_EE_30K') ,('Insurance Coverage')] = 'CUR_COV_MASTER_AMOUNT_EE_20K'

    active_bene_i['Primary Percentage'] = active_bene_i['Dependent % Of Distribution']
    active_bene_i['Primary Percentage'] = active_bene_i['Primary Percentage'].str.replace('%','')
    active_bene_i['Primary Percentage'] = active_bene_i['Primary Percentage'].str[:-3]

    active_bene_i.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\check_BENINS.csv')

    active_bene_i = active_bene_i[['Employee ID', 'Event Date', 'Benefit Event Type',
                                   'Insurance Coverage Plan', 'Insurance Coverage',
                                   'Dependent ID', 'Beneficiary ID','Primary Percentage']]

    active_bene_i['Source System'] = 'Kronos'
    active_bene_i['Contingent Percentage'] = ''
    active_bene_i['Original Coverage Begin Date'] = ''
    active_bene_i['Deduction Begin Date'] = ''
    active_bene_i['Employee Cost PreTax'] = ''
    active_bene_i['Employee Cost PostTax'] = ''
    active_bene_i['Employer Cost NonTaxable']=''
    active_bene_i['Employer Cost Taxable']=''
    active_bene_i['Enrollment Signature Date']=''
    active_bene_i['Signing Worker']=''

    active_bene_i = active_bene_i[['Employee ID', 'Source System', 'Event Date', 'Benefit Event Type',
           'Insurance Coverage Plan', 'Dependent ID', 'Beneficiary ID',
           'Primary Percentage', 'Contingent Percentage',
           'Original Coverage Begin Date', 'Deduction Begin Date',
           'Insurance Coverage', 'Employee Cost PreTax', 'Employee Cost PostTax',
           'Employer Cost NonTaxable', 'Employer Cost Taxable',
           'Enrollment Signature Date', 'Signing Worker']]
    

    
    
    


    dict_coverage_plan = {'Basic AD&D Voya': 'USA - BLIFADD-Voya_EE',
                          'Basic Life Voya': 'USA - BLIFADD-Voya_EE',
                          'Child Critical Illness': 'USA - Vol_CHCI-Voya_CH',
                          'Critical Illness': 'USA - Vol_EECI-Voya_EE',
                          'Long Term Disability Voya': 'USA - LTD-Voya_EE',
                          'Northwestern Mutual Long Term Disability': 'USA - LTD-NW_Mutual_Exec_EE',
                          'Short Term Disability Voya': 'USA - STD-Voya_EE',
                          'Spouse Critical Illness': 'USA - Vol_SPCI-Voya_SP',
                          'Voluntary Child Life Voya': 'USA - Vol_CHLIF-Voya_CH',
                          'Voluntary Employee Life Voya': 'USA - Vol_EELIF-Voya_EE',
                          'Voluntary Spouse Life Voya': 'USA - Vol_SPLIF-Voya_SP'}

    active_bene_i['Insurance Coverage Plan'] = active_bene_i['Insurance Coverage Plan'].replace(dict_coverage_plan)
    active_bene_i = active_bene_i[active_bene_i['Insurance Coverage Plan'] != 'USA - LTD-NW_Mutual_Exec_EE']
    active_bene_i = active_bene_i.drop_duplicates()
    active_bene_i['Insurance Coverage'] = np.where(active_bene_i['Insurance Coverage Plan'] == 'USA - BLIFADD-Voya_EE','Multiplier Based Coverage Levels (BLIF - 1x)',active_bene_i['Insurance Coverage'])

    #add in exec benefits
    df_execBens = pd.read_csv(config.PATH_WD_IMP + 'data files\\exec_life_ltd.csv', dtype='object', encoding='cp1251')
    df_execBens['Employee ID'] = df_execBens['EMPID']
    dfc_execBens = df_execBens[['Employee ID','Employer Cost NonTaxable','Insurance Coverage','Insurance Coverage Plan']]
    
    dfc_execBens['Source System'] = 'Kronos'
    dfc_execBens['Contingent Percentage'] = ''
    dfc_execBens['Original Coverage Begin Date'] = ''
    dfc_execBens['Deduction Begin Date'] = ''
    dfc_execBens['Employee Cost PreTax'] = ''
    dfc_execBens['Employee Cost PostTax'] = ''
    dfc_execBens['Employer Cost Taxable']=''
    dfc_execBens['Enrollment Signature Date']=''
    dfc_execBens['Signing Worker']=''
    dfc_execBens['Event Date'] = '01-JAN-2023'
    dfc_execBens['Benefit Event Type'] = 'BEN_CONVERSION_INSURANCE'
    dfc_execBens['Dependent ID'] = ''
    dfc_execBens['Beneficiary ID'] = ''
    dfc_execBens['Primary Percentage'] = ''
    
    dfc_execBens = dfc_execBens[['Employee ID', 'Source System', 'Event Date', 'Benefit Event Type',
           'Insurance Coverage Plan', 'Dependent ID', 'Beneficiary ID',
           'Primary Percentage', 'Contingent Percentage',
           'Original Coverage Begin Date', 'Deduction Begin Date',
           'Insurance Coverage', 'Employee Cost PreTax', 'Employee Cost PostTax',
           'Employer Cost NonTaxable', 'Employer Cost Taxable',
           'Enrollment Signature Date', 'Signing Worker']]
    
    df_insurance_elections = pd.concat([active_bene_i,dfc_execBens])
    #Check spouse plans that don't have a dependent 
    active_bene_i.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\ins_elections.csv')
    #Add in exec benefits
    active_bene_i = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\ins_elections.csv')


    write_to_csv(df_insurance_elections, 'ben_insurance_elections.txt')
