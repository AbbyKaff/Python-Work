# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 19:08:54 2023

@author: akaff
"""


import pandas as pd
import numpy as np
import os
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
from api_call2 import get_rpt
import datetime
from datetime import date
from datetime import datetime
from datetime import timedelta
import xlrd
import glob as glob

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion')
cut_off_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\worker_phone.txt",sep="|")
cut_off_ees['Employee ID'] = cut_off_ees['Worker ID']
cut_off_ids = cut_off_ees[['Employee ID']]

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\benefits_dc'
os.chdir(path)
#Single source for populating all data in the HCM Data Conversion Spreadsheet

##############################################################################
################################ OVERVIEW ####################################
##############################################################################


"""
Employee Related Person -- 	In Scope
Benefit Annual Rate --	In Scope
Health Care Elections --	In Scope
Historical Health Care-ACA --	In Scope
Insurance Elections --	In Scope
Retirement Savings Election --	In Scope
Additional Benefits Election --	In Scope
Worker Wellness --	In Scope

"""



##############################################################################
######################### Employee Related Person #############################
##############################################################################

"""
Employee ID
Dependent ID	
Beneficiary ID	
Emergency Contact ID
Related Person Relationship

"""

#Benefits > Dependents
dep = get_rpt('72593419')
dep_cc = dep[['Employee Id','Coverage Name','Dependent Type','Dependent First Name','Dependent Last Name','Dependent Gender','Dependent Date Birthday']]
dep_cc = dep_cc[dep_cc['Coverage Name'] != 'Employee']
dep_cc = dep_cc[(dep_cc['Dependent Type'] == 'Child') | (dep_cc['Dependent Type'] == 'Spouse')]

#Dependent
dep_c = dep_cc[['Employee Id','Dependent Type','Dependent First Name','Dependent Last Name','Dependent Gender','Dependent Date Birthday']]

dep_c.drop_duplicates(inplace=True)

dep_c['T'] = dep_c.groupby('Employee Id')['Employee Id'].transform('count')
dep_c['No'] = dep_c.groupby('Employee Id')['Employee Id'].cumcount() + 1
dep_c['Dependent ID'] = dep_c['Employee Id'].astype(str) + '-D0' + dep_c['No'].astype(str)
dep_c['dep_full'] = dep_c['Dependent First Name'] + '_' + dep_c['Dependent Last Name']

#Beneficiary
dep_bb = dep[(dep['Dependent Type'] == 'Beneficiary') | (dep['Dependent Type'] == 'Contingent Beneficiary')]
dep_b = dep_bb[['Employee Id','Dependent First Name','Dependent Last Name','Dependent % Of Distribution']]

dep_b.drop_duplicates(inplace=True)

dep_b['T'] = dep_b.groupby('Employee Id')['Employee Id'].transform('count')
dep_b['No'] = dep_b.groupby('Employee Id')['Employee Id'].cumcount() + 1
dep_b['Beneficiary ID'] = dep_b['Employee Id'].astype(str) + '-B0' + dep_b['No'].astype(str)
dep_b['dep_full'] = dep_b['Dependent First Name'] + '_' + dep_b['Dependent Last Name']

dep_b = dep_b[['dep_full','Beneficiary ID','Dependent % Of Distribution']]

dep_df = dep_c.merge(dep_b, on='dep_full',how='outer')

dep_df = dep_df[['dep_full','Beneficiary ID','Dependent ID','Dependent Gender','Dependent Date Birthday']]


#Clean all
dep_c2 = dep
dep_c2['dep_full'] = dep_c2['Dependent First Name'] + '_' + dep_c2['Dependent Last Name']
dep_c2 = dep_c2[(dep_c2['Dependent Type'] == 'Child') | (dep_c2['Dependent Type'] == 'Spouse')]
dep_c2 = dep_c2[['Employee Id','dep_full','Dependent Type']]

dep_df2 = dep_df.merge(dep_c2, on='dep_full', how='left')

dep_df2['Employee ID'] = np.where(dep_df2['Employee Id'].isna(),dep_df2['Beneficiary ID'].str[:-4],dep_df2['Employee Id'])
dep_df2['Related Person Relationship'] = np.where(dep_df2['Dependent Type'].isna(),'Other',dep_df2['Dependent Type'])
dep_df2.drop_duplicates(inplace=True)

dep_df2['Legal First Name'] = dep_df2['dep_full'].str.split('_').str[0]
dep_df2['Legal Last Name'] = dep_df2['dep_full'].str.split('_').str[1]
dep_df2['Trust Name'] = np.where(dep_df2['Employee ID'] == '104418',dep_df2['dep_full'],'na')
dep_df2['Legal First Name'] = np.where(dep_df2['Employee ID'] == '104418','na',dep_df2['Legal First Name'])
dep_df2['Legal Last Name'] = np.where(dep_df2['Employee ID'] == '104418','na',dep_df2['Legal Last Name'])
dep_df2['Employee ID'] = dep_df2['Employee ID'].astype(int)

dep_f = dep_df2[['Employee ID','Dependent ID','Beneficiary ID','Related Person Relationship','Legal First Name','Legal Last Name','Trust Name','Dependent Gender','Dependent Date Birthday']]
dep_f['Date of Birth'] = pd.to_datetime(dep_f['Dependent Date Birthday']).dt.strftime("%d-%b-%Y").str.upper()
dep_f['Gender'] = np.where(dep_f['Dependent Gender'] ==  'F','Female',
                           np.where(dep_f['Dependent Gender'] == 'M','Male','Undefined'))
dep_f.to_csv(r'manual_fix_ERP.csv')
dep_ff = pd.read_csv(r'fixed_erp.csv')
dep_ff['Date of Birth'] = pd.to_datetime(dep_ff['Date of Birth']).dt.strftime("%d-%b-%Y").str.upper()

"""
erp = pd.read_excel('./../USA_KBP_CNP_Benefits_Template.xlsx', sheet_name='Employee Related Person')

erp_c = erp.filter(regex='Required')
erp_data = pd.read_excel('./../USA_KBP_CNP_Benefits_Template.xlsx', sheet_name='Employee Related Person',skiprows=2)

erp_data = erp_data.iloc[0:0]
erp_data.columns
erp_data['Employee ID'] = dep_f['Employee ID'].astype(int)
erp_data = erp_data.drop(['Dependent ID','Beneficiary ID','Related Person Relationship','Legal First Name','Legal Last Name','Trust Name'],axis=1)

erp_data = erp_data.merge(dep_f, on='Employee ID')

erp_data = erp_data[['Employee ID', 'Source System', 'Emergency Contact ID',
       'Country ISO Code - Legal Name', 'Legal Middle Name', 'Prefix',
       'Suffix', 'Language', 'Emergency Contact Priority',
       'Same Phone as Employee?', 'Country ISO Code - Primary Home',
       'International Phone Code - Primary Home',
       'Area Code - Primary Home', 'Phone Number - Primary Home',
       'Phone Extension - Primary Home',
       'Phone Device Type - Primary Home',
       'Country ISO Code - Additional Home #1',
       'International Phone Code - Additional Home #1',
       'Area Code - Additional Home #1',
       'Phone Number - Additional Home #1',
       'Phone Extension - Additional Home #1',
       'Phone Device Type - Additional Home #1',
       'Country ISO Code - Additional Home #2',
       'International Phone Code - Additional Home #2',
       'Area Code - Additional Home #2',
       'Phone Number - Additional Home #2',
       'Phone Extension - Additional Home #2',
       'Phone Device Type - Additional Home #2',
       'Country ISO Code - Primary Work',
       'International Phone Code - Primary Work',
       'Area Code - Primary Work', 'Phone Number - Primary Work',
       'Phone Extension - Primary Work',
       'Phone Device Type - Primary Work',
       'Country ISO Code - Additional Work #1',
       'International Phone Code - Additional Work #1',
       'Area Code - Additional Work #1',
       'Phone Number - Additional Work #1',
       'Phone Extension - Additional Work #1',
       'Phone Device Type - Additional Work #1',
       'Country ISO Code - Additional Work #2',
       'International Phone Code - Additional Work #2',
       'Area Code - Additional Work #2',
       'Phone Number - Additional Work #2',
       'Phone Extension - Additional Work #2',
       'Phone Device Type - Additional Work #2',
       'Email Address - Primary Home', 'Email Address - Primary Work',
       'Email Address - Additional Home',
       'Email Address - Additional Work', 'Same Address as Employee?',
       'Country ISO Code - Home', 'Address Line #1 - Home',
       'Address Line #2 - Home', 'City - Home', 'City Subdivision - Home',
       'Region - Home', 'Region Subdivision - Home', 'Postal Code - Home',
       'Country ISO Code - Alt Home #1', 'Address Line #1 - Alt Home #1',
       'Address Line #2 - Alt Home #1', 'City - Alt Home #1',
       'City Subdivision - Alt Home #1', 'Region - Alt Home #1',
       'Region Subdivision - Alt Home #1', 'Postal Code - Alt Home #1',
       'Country ISO Code - Alt Home #2', 'Address Line #1 - Alt Home #2',
       'Address Line #2 - Alt Home #2', 'City - Alt Home #2',
       'City Subdivision - Alt Home #2', 'Region - Alt Home #2',
       'Region Subdivision - Alt Home #2', 'Postal Code - Alt Home #2',
       'Date of Birth', 'Gender', 'National ID', 'National ID Type',
       'Effective Date', 'Reason', 'Uses Tobacco?', 'Full-time Student?',
       'Dependent for Payroll Purposes', 'Student Status Start Date',
       'Student Status End Date', 'Disabled?',
       'Could Be Covered For Health Care Coverage Elsewhere',
       'Could Be Covered For Health Care Coverage Elsewhere Date',
       'Benefit Coverage Type - Medical', 'Start Date - Medical',
       'End Date - Medical', 'Benefit Coverage Type - Dental',
       'Start Date - Dental', 'End Date - Dental',
       'Benefit Coverage Type - Vision', 'Start Date - Vision',
       'End Date - Vision', 'Custom ID - Custom ID #1',
       'Custom ID Type - Custom ID #1', 'Issued Date - Custom ID #1',
       'Expiration Date - Custom ID #1', 'Dependent ID', 'Beneficiary ID',
       'Related Person Relationship', 'Legal First Name',
       'Legal Last Name', 'Trust Name']]
"""

mask = dep_ff['Employee ID'].isin(cut_off_ids)
dep_ff = dep_ff.loc[~mask]
os.chdir(path)
dep_ff.to_csv('emp_rel_per.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
########################### Benefit Annual Rate ##############################
##############################################################################

'''
Employee ID
Source System
Effective Date
Benefit Annual Rate Type
Benefit Annual Rate
Currency
'''

mp_bar = get_rpt('72584718')

mp_bar_c = mp_bar[mp_bar['Employee Benefit Salary 1'] != '-']
mp_bar_c = mp_bar_c[mp_bar_c['Employee Status'] == 'Active']

mp_bar_c['Employee ID'] = mp_bar_c['Employee Id']
mp_bar_c['Effective Date'] = '01-JAN-2023'
mp_bar_c['Source System'] = 'Kronos'
mp_bar_c['Benefit Annual Rate Type'] = ''
mp_bar_c['Benefit Annual Rate'] = mp_bar_c['Employee Benefit Salary 1']
mp_bar_c['Currency'] = 'USD'


bar = mp_bar_c[['Employee ID',
'Source System',
'Effective Date',
'Benefit Annual Rate Type',
'Benefit Annual Rate',
'Currency']]



os.chdir(path)
bar.to_csv('benefit_annual_rate.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
######################### Health Care Elections ##############################
##############################################################################

'''

'''

active_bene = get_rpt('72578980')

active_bene = active_bene[active_bene['Coverage Name'] != 'Waived']
active_benec = active_bene[(active_bene['Benefit Type'] == 'Medical') | (active_bene['Benefit Type'] == 'Dental') | (active_bene['Benefit Type'] == 'Vision') | (active_bene['Benefit Type'] == 'Voya Accident')]

active_benec['Event Date'] = pd.to_datetime(active_benec['Coverage Effective From']).dt.strftime("%d-%b-%Y").str.upper()

active_benec['Benefit Event Type'] = 'Conversion_Health_Care'

active_benec['Health Care Coverage Plan'] = active_benec['Benefit Plan Name']

active_benec['Health Care Coverage Target'] = active_benec['Coverage Name']
active_benec['Employee ID'] = active_benec['Employee Id'].astype(int)

#merge dependent id from related person
erp_c = dep_ff[['Employee ID','Dependent ID']]
active_benec = active_benec.merge(erp_c, on='Employee ID')

active_benec = active_benec[['Employee ID','Event Date','Benefit Event Type','Health Care Coverage Plan','Health Care Coverage Target','Dependent ID']]

hce_data = pd.read_excel('./../USA_KBP_CNP_Benefits_Template.xlsx', sheet_name='Health Care Elections',skiprows=2)

hce_data = hce_data.iloc[0:0]


active_benec['Source System'] = 'Kronos'
active_benec['Provider ID (Primary Physician)'] = ''
active_benec['Provider ID (Primary Physician) - Dependent'] = ''
active_benec['Original Coverage Begin Date'] = ''
active_benec['Deduction Begin Date'] = ''
active_benec['Employee Cost PreTax'] = ''
active_benec['Employee Cost PostTax'] =''
active_benec['Employer Cost NonTaxable']=''
active_benec['Employer Cost Taxable']=''
active_benec['Enrollment Signature Date']=''
active_benec['Signing Worker']=''



hce_data = active_benec[['Employee ID', 'Source System', 'Event Date', 'Benefit Event Type',
       'Health Care Coverage Plan', 'Health Care Coverage Target',
       'Provider ID (Primary Physician)', 'Dependent ID',
       'Provider ID (Primary Physician) - Dependent',
       'Original Coverage Begin Date', 'Deduction Begin Date',
       'Employee Cost PreTax', 'Employee Cost PostTax',
       'Employer Cost NonTaxable', 'Employer Cost Taxable',
       'Enrollment Signature Date', 'Signing Worker']]

mask = hce_data['Employee ID'].isin(cut_off_ids)
hce_data = hce_data.loc[~mask]

hce_data['Health Care Coverage Plan'] = np.where(hce_data['Health Care Coverage Plan'] == 'Dental B','USA - Dental - Delta Dental Plan B',
                                        np.where((hce_data['Health Care Coverage Plan'] == 'Medical A') | (hce_data['Health Care Coverage Plan'] == 'Medical A - Company Paid'),'USA - Medical - Surest/UHC Plan A',
                                        np.where((hce_data['Health Care Coverage Plan'] == 'Vision A')|(hce_data['Health Care Coverage Plan'] == 'Vision A- Company Paid'),'USA - Vision - Surency Plan A',
                                        np.where((hce_data['Health Care Coverage Plan'] == 'Dental A')|(hce_data['Health Care Coverage Plan'] == 'Dental A Company Paid'),'USA - Dental - Delta Dental Plan A',
                                        np.where(hce_data['Health Care Coverage Plan'] == 'MEC Bronze','USA - Medical - American Worker (MEC) Plan Bronze',
                                        np.where(hce_data['Health Care Coverage Plan'] == 'Accident','USA - Voluntary Accident - Voya',
                                        np.where(hce_data['Health Care Coverage Plan'] == 'MEC Gold','USA - Medical - American Worker (MEC) Plan Gold',
                                        np.where(hce_data['Health Care Coverage Plan'] == 'MEC Silver','USA - Medical - American Worker (MEC) Plan Silver',
                                        np.where(hce_data['Health Care Coverage Plan'] == 'Vision B','USA - Vision - Surency Plan B',
                                        np.where(hce_data['Health Care Coverage Plan'] == 'Medical B','USA - Medical - Surest/UHC Plan B',hce_data['Health Care Coverage Plan']))))))))))


os.chdir(path)
hce_data.to_csv('health_care_elections.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
######################### Insurance Elections ##############################
##############################################################################

active_bene_i = active_bene[(active_bene['Benefit Type'] == 'AD&D') | (active_bene['Benefit Type'] == 'Life') | (active_bene['Benefit Type'] == 'STD') | (active_bene['Benefit Type'] == 'LTD')|(active_bene['Benefit Type'] == 'Voya Critical Illness')]


ie_data = pd.read_excel('./../USA_KBP_CNP_Benefits_Template.xlsx', sheet_name='Insurance Elections',skiprows=2)

ie_data = ie_data.iloc[0:0]


active_bene_i['Event Date'] = pd.to_datetime(active_bene_i['Coverage Effective From']).dt.strftime("%d-%b-%Y").str.upper()

active_bene_i['Benefit Event Type'] = 'Conversion_Insurance'

active_bene_i['Insurance Coverage Plan'] = active_bene_i['Benefit Plan Name']

#active_bene_i['Health Care Coverage Target'] = active_bene_i['Coverage Name']
active_bene_i['Employee ID'] = active_bene_i['Employee Id'].astype(int)
active_bene_i['Insurance Coverage'] = active_bene_i['Coverage Amount 1']
active_bene_i['Insurance Coverage'] = active_bene_i['Insurance Coverage'].str.replace("$","")

#merge dependent id from related person
erp_c = dep_ff[['Employee ID','Dependent ID','Beneficiary ID']]
active_bene_i = active_bene_i.merge(erp_c, on='Employee ID')

active_bene_i = active_bene_i[['Employee ID','Event Date','Benefit Event Type','Insurance Coverage Plan','Insurance Coverage','Dependent ID','Beneficiary ID']]



active_bene_i['Source System'] = 'Kronos'
active_bene_i['Primary Percentage'] = ''
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


mask = active_bene_i['Employee ID'].isin(cut_off_ids)
active_bene_i = active_bene_i.loc[~mask]

active_bene_i = active_bene_i[active_bene_i['Insurance Coverage Plan'] != 'Short Term Disability - Employer Paid']

active_bene_i['Insurance Coverage Plan'] = np.where(active_bene_i['Insurance Coverage Plan'] == 'Long Term Disability Voya','USA - Long Term Disability - Voya (Employee)',
                                           np.where(active_bene_i['Insurance Coverage Plan'] == 'Basic Life Voya','USA - Base Life and AD&D - Voya (Employee)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Voluntary Employee Life Voya','USA - Base Life and AD&D - Voya (Employee)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Basic AD&D Voya','USA - Base Life and AD&D - Voya (Employee)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Short Term Disability Voya','USA - Short Term Disability - Voya (Employee)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Critical Illness','USA - Voluntary Employee Critical Illness - Voya (Employee)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Child Critical Illness','USA - Voluntary Child Critical Illness - Voya (Child)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Voluntary Child Life Voya','USA - Voluntary Child Life - Voya (Child)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Voluntary Spouse Life Voya','USA - Voluntary Spouse Life - Voya (Spouse)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Spouse Critical Illness','USA - Voluntary Spouse Critical Illness - Voya (Spouse)',
                                            np.where(active_bene_i['Insurance Coverage Plan'] == 'Northwestern Mutual Long Term Disability','USA - Long Term Disability - Northwest Mutual Exec (Employee)','')))))))))))

active_bene_i = active_bene_i.drop_duplicates()

os.chdir(path)
active_bene_i.to_csv('insurance_elections.txt',sep='|', encoding='utf-8', index=False)





##############################################################################
#################### Retirement Savings Elections ############################
##############################################################################

'''active_bene_r = get_rpt('71567958')
active_bene_r = active_bene_r[active_bene_r['Deduction Code'] != '409ABN']

active_bene_r['Employee ID'] = active_bene_r['Employee Id'].astype(int)
active_bene_r['Employee ID'] = active_bene_r['Employee ID'].astype(str)
active_bene_r['Source System'] = 'Kronos'
active_bene_r['Event Date'] = pd.to_datetime(active_bene_r['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
active_bene_r['Benefit Event Type'] = 'Conversion_Retirement_Savings'
'''
"""
'409ABN'
'401K' : USA - 401(k) - John Hancock
'401KR' : USA - 401(k) Roth - John Hancock
"""
active_bene_r = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\ScheduledDeductions-ProdAPI_Ded_401K_1682022257410.csv")
#active_bene_r['Retirement Savings Plan'] = np.where(active_bene_r['Deduction Code'] == '401KR','USA - 401(k) Roth - John Hancock','USA - 401(k) - John Hancock')
active_bene_r['Election Percentage'] = active_bene_r['EE Percent'].str.replace("%","")
active_bene_r['Election Amount'] = active_bene_r['EE Amount']
active_bene_r['Election Percentage'] = active_bene_r['Election Percentage'].str.replace('-','0')
active_bene_r['Election Percentage'] = active_bene_r['Election Percentage'].fillna(0)
active_bene_r['Event Date'] = '01-JAN-2023'
active_bene_r['Source System'] = 'Kronos'
active_bene_r['Benefit Event Type'] = 'BEN_CONVERSION_RETSAVINGS'
active_bene_r['Retirement Savings Plan'] = np.where(active_bene_r['Deduction Type: Name'] == 'Roth 401k','USA - 401(k) Roth - Principal','USA - 401(k) - Principal')
emp_rel = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\moved.20230421-081709.E2E_emp_rel_per.txt",sep='|')
emp_rel = emp_rel[['Employee ID','Beneficiary ID']]
emp_rel = emp_rel.dropna(subset=['Beneficiary ID'])
active_bene_r['Employee ID'] = active_bene_r['Employee Id']
active_bene_r = active_bene_r.merge(emp_rel, on='Employee ID',how='left')
active_bene_r['Beneficiary ID - Beneficiary Allocation'] = active_bene_r['Beneficiary ID']
active_bene_r['Retirement Savings Plan - Benefits Provider Allocation'] = np.where(active_bene_r['Deduction Type: Name'] == 'Roth 401k','USA - 401(k) Roth - Principal','USA - 401(k) - Principal')
active_bene_r['Primary Percentage - Beneficiary Allocation'] = ''
active_bene_r['Contingent Percentage - Beneficiary Allocation'] = ''
active_bene_r['Original Coverage Begin Date'] = ''
active_bene_r['Deduction Begin Date'] = ''
active_bene_r['Employee Contribution Allocation Percent - Benefits Provider Allocation'] = ''
active_bene_r['Employer Contribution Allocation Percent'] = ''
active_bene_r['Beneficiary ID - Benefits Provider Allocation'] = ''
active_bene_r['Primary Percentage - Benefits Provider Allocation'] = ''
active_bene_r['Contingent Percentage - Benefits Provider Allocation'] = ''
active_bene_r['Enrollment Signature Date'] = ''
active_bene_r['Signing Worker'] = ''

re_final = active_bene_r[['Employee ID',
'Source System',
'Event Date',
'Benefit Event Type',
'Retirement Savings Plan',
'Election Percentage',
'Election Amount',
'Beneficiary ID - Beneficiary Allocation',
'Primary Percentage - Beneficiary Allocation',
'Contingent Percentage - Beneficiary Allocation',
'Original Coverage Begin Date',
'Deduction Begin Date',
'Retirement Savings Plan - Benefits Provider Allocation',
'Employee Contribution Allocation Percent - Benefits Provider Allocation',
'Employer Contribution Allocation Percent',
'Beneficiary ID - Benefits Provider Allocation',
'Primary Percentage - Benefits Provider Allocation',
'Contingent Percentage - Benefits Provider Allocation',
'Enrollment Signature Date',
'Signing Worker']]

os.chdir(path)
re_final.to_csv('ret_savings_elections.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
#################### Additional Benefit Plans ############################
##############################################################################

active_bene_a = active_bene[(active_bene['Benefit Type'] == 'EAP') | (active_bene['Benefit Type'] == 'Identity Protection') | (active_bene['Benefit Type'] == 'Legal')]

ab_data = pd.read_excel('./../USA_KBP_CNP_Benefits_Template.xlsx', sheet_name='Additional Benefits Election',skiprows=2)

ab_data = ab_data.iloc[0:0]


active_bene_a['Event Date'] = pd.to_datetime(active_bene_a['Coverage Effective From']).dt.strftime("%d-%b-%Y").str.upper()

active_bene_a['Benefit Event Type'] = 'Conversion_Insurance'

active_bene_a['Additional Benefits Plan'] = active_bene_a['Benefit Plan Name']

active_bene_a['Additional Benefits Coverage Target'] = active_bene_a['Coverage Name']
active_bene_a['Employee ID'] = active_bene_a['Employee Id'].astype(int)
#active_bene_a['Insurance Coverage'] = active_bene_a['# Units']


active_bene_a = active_bene_a[['Employee ID','Event Date','Benefit Event Type','Additional Benefits Plan', 'Additional Benefits Coverage Target']]

active_bene_a['Source System'] = 'Kronos'
active_bene_a['Original Coverage Begin Date'] =''
active_bene_a['Deduction Begin Date']=''
active_bene_a['Percentage Contribution Value']=''
active_bene_a['Flat Contribution Amount']=''
active_bene_a['Employee Cost PreTax']=''
active_bene_a['Employee Cost PostTax']=''
active_bene_a['Employer Cost NonTaxable']=''
active_bene_a['Employer Cost Taxable']=''
active_bene_a['Enrollment Signature Date']=''
active_bene_a['Signing Worker']=''

active_bene_a = active_bene_a[['Employee ID', 'Source System', 'Event Date', 'Benefit Event Type',
       'Additional Benefits Plan', 'Additional Benefits Coverage Target',
       'Original Coverage Begin Date', 'Deduction Begin Date',
       'Percentage Contribution Value', 'Flat Contribution Amount',
       'Employee Cost PreTax', 'Employee Cost PostTax',
       'Employer Cost NonTaxable', 'Employer Cost Taxable',
       'Enrollment Signature Date', 'Signing Worker']]

mask = active_bene_a['Employee ID'].isin(cut_off_ids)
active_bene_a = active_bene_a.loc[~mask]




os.chdir(path)
active_bene_a.to_csv('add_bene_elections.txt',sep='|', encoding='utf-8', index=False)

