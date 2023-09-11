# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 14:42:09 2023

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
import xlrd
import glob as glob
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, open_as_utf8, modify_amount)
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion')
cut_off_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\worker_phone.txt")

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\payroll_dc'
os.chdir(path)
#Single source for populating all data in the HCM Data Conversion Spreadsheet

##############################################################################
################################ OVERVIEW ####################################
##############################################################################

'''
Payroll Balances:
USA-EE Payroll History

Payroll Common:
Pay Group Assignments --	In Scope
Pay Election Enrollment --	In Scope
Paycheck Delivery Options --	In Scope
Payee Input Data --	In Scope

Payroll USA:
FICA Medicare Exempt --	In Scope
FICA OASDI Exempt --	In Scope
FUTA Exempt --	In Scope
W-4 Elections --	In Scope
State Tax Elections --	In Scope
SUTA Exempt --	In Scope
Local Home City Tax Elections --	In Scope
Local Work City Tax Elections  --	In Scope
Local Home County Tax Elections --	In Scope
Local Work County Tax Elections --	In Scope
Local Other Work Tax Elections --	In Scope
Jurisdiction Tax Elections --	In Scope
Actual Hours Worked --	In Scope
Deduction Recipients --	In Scope
Bankruptcy (USA) --	In Scope
Creditor (USA) --	In Scope
Wage Assignment (USA) --	In Scope
State Tax Levy (USA) --	In Scope
Federal Tax Levy (USA) --	In Scope
Student Loan (USA) --	In Scope
Federal Admin Wage Garn (USA) --	In Scope
Support Orders (USA) --	In Scope
Payroll Reporting Codes --	In Scope

'''


##############################################################################
############################# USA-EE Payroll #################################
##############################################################################


'''
Employee ID
Quarter
Pay Group
Company
Cost Center
Period End Date
Payment Date
Amount

'''






##############################################################################
######################### Pay Group Assignments ##############################
##############################################################################

'''
Employee ID
Pay Group ID

US_BW
US_WK
US-TBBW
US_TBWK

'''

pg_data = get_rpt('72584718')

pg_data['Pay Group ID'] = np.where((pg_data['Unemployment State/Province'] == 'NY') & (pg_data['Employee EIN'] == 'KBP Bells'),'US_TBWK',
                          np.where((pg_data['Unemployment State/Province'] == 'NY') & (pg_data['Employee EIN'] == 'KBP Foods'),'US_WK',
                          np.where((pg_data['Unemployment State/Province'] != 'NY') & (pg_data['Employee EIN'] == 'KBP Bells'),'US_TBBW','US_BW')))


pg_data['Employee ID'] = pg_data['Employee Id']
pg_data['Source System'] = 'Kronos'
pg_data['Effective Date'] = ''

pg_datac = pg_data[['Employee ID','Source System','Effective Date','Pay Group ID']]

os.chdir(path)
pg_datac.to_csv('pay_gp_assignments.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
########################## Pay Election Enrollment ###########################
##############################################################################

'''
Employee ID
Country ISO Code
Currency Code
Payment Election Rule
Distribution Order
Distribution Amount
Distribution Percentage
Payment Type
Bank Account Name
Account Number
Bank Name
Bank ID Number  -- Routing #

'''

dd_data = get_rpt('71457227')
dd_data['Employee ID'] = dd_data['Employee Id']
dd_data['Country ISO Code'] = 'USA'
dd_data['Currency Code'] = 'USD'
dd_data['Payment Election Rule'] = 'Regular'
dd_data['Distribution Order'] = dd_data['Sequence']
dd_data['Distribution Amount'] = dd_data['Amount']
dd_data['Distribution Percentage'] = dd_data['Amount %']
dd_data['Payment Type'] = 'Direct_Deposit'
dd_data['Bank Account Name'] = ''
dd_data['Account Number'] = dd_data['Account #']
dd_data['Bank Name'] = ''
dd_data['Bank ID Number'] = dd_data['Routing #']
dd_data['Source System'] = 'Kronos'
dd_data['Distribution Balance'] = ''
dd_data['Bank Account Nickname'] = ''
dd_data['Roll Number'] = ''
dd_data['Account Type'] = ''
dd_data['IBAN'] = ''
dd_data['BIC'] = ''
dd_data['Check Digit'] = ''
dd_data['Branch Name'] =''
dd_data['Branch ID Number'] = ''

dd_dataf = dd_data[['Employee ID',
'Source System',
'Country ISO Code',
'Currency Code',
'Payment Election Rule',
'Distribution Order',
'Distribution Amount',
'Distribution Percentage',
'Distribution Balance',
'Payment Type',
'Bank Account Nickname',
'Bank Account Name',
'Account Number',
'Roll Number',
'Account Type',
'Bank Name',
'IBAN',
'Bank ID Number',
'BIC',
'Check Digit',
'Branch Name',
'Branch ID Number']]



os.chdir(path)
dd_dataf.to_csv('pay_elections.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
############################# Payee Input Data ###############################
##############################################################################


'''
Employee ID
Start Date
Deduction Code
Amount

'''

kbpc = get_rpt('72593428')

kbpc['Employee ID'] = kbpc['Employee Id']
kbpc['Start Date'] = pd.to_datetime(kbpc['Begin Date']).dt.strftime("%d-%b-%Y").str.upper()
kbpc['Deduction Code'] = 'KBPC'
kbpc['Amount'] = kbpc['EE Amount (As Of Today)']

pid = pd.read_excel('./../USA_KBP_CNP_Payroll Common_Template.xlsx', sheet_name='Payee Input Data',skiprows=2)

pid = pid.iloc[0:0]

pid['Employee ID'] = kbpc['Employee ID']

kbpc = kbpc[['Employee ID','Start Date','Deduction Code','Amount']]
pid = pid.drop(['Start Date','Deduction Code','Amount'],axis=1)

pid = pid.merge(kbpc,on='Employee ID')

pid = pid[['Employee ID', 'Source System', 'Company', 'Cost Center', 'Position ID',
       'Ongoing Input', 'Start Date', 'End Date', 'Earning Code',
       'Deduction Code', 'Amount', 'Hours', 'Rate', 'Adjustment?', 'Comment',
       'Currency', 'State Authority', 'Flexible Payment Deduction Worktag',
       'Custom Worktag #1', 'Custom Worktag #2', 'Custom Worktag #3',
       'Custom Worktag #4', 'Custom Worktag #5', 'Custom Worktag #6',
       'Custom Worktag #7', 'Custom Worktag #8', 'Custom Worktag #9',
       'Custom Worktag #10', 'Allocation Pool', 'Appropriation',
       'Related Calculation ID', 'Input Value', 'Related Calculation ID #2',
       'Input Value #2', 'Related Calculation ID #3', 'Input Value #3']]

os.chdir(path)
pid.to_csv('payee_input_data.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
########################### FICA Medicare Exempt #############################
##############################################################################

'''
Employee ID
Company
Effective As Of
Exempt from Medicare

'''

# No EEs currently

##############################################################################
############################## FICA OASDI Exempt #############################
##############################################################################

'''
Employee ID
Company 
Effective As Of
Exempt from OASDI
'''
# No EEs currently

##############################################################################
################################### FUTA Exempt #############################
##############################################################################

'''
Employee ID
Company
Effetive As Of
FUTA Exempt
'''
# No EEs currently

##############################################################################
################################## W-4 Elections #############################
##############################################################################

'''
Employee ID
Effective As Of
Company
Payroll W-4 Marital Status
Number of Allowances
Additional Amount
Total Dependent Amount
Other Income
Deductions
Exempt

'''

mas_tax = get_rpt('72596865')

mas_taxc = mas_tax

mas_taxc['Employee ID'] = mas_taxc['Employee Id']

mas_taxc['Effective From'] = pd.to_datetime(mas_taxc['Effective From'])

mas_taxc['Date Hired'] = pd.to_datetime(mas_taxc['Date Hired'])

mas_taxc['Effective As Of'] = np.where(mas_taxc['Effective From'] > mas_taxc['Date Hired'], mas_taxc['Effective From'] ,mas_taxc['Date Hired'])


mas_taxc['Effective As Of'] = mas_taxc['Effective As Of'].dt.strftime("%d-%b-%Y").str.upper()

mas_taxc['Company'] = mas_taxc['Cost Centers(Company Code)']
mas_taxc['Payroll W-4 Marital Status'] = mas_taxc['Filing Status']
mas_taxc['Number of Allowances'] = mas_taxc['Personal Allowances']
mas_taxc['Additional Amount'] = mas_taxc['Additional Allowances']
mas_taxc['Total Dependent Amount'] = mas_taxc['Dependent Allowances']

mas_taxc['Deductions'] = mas_taxc['Deduction']
mas_taxc['Source System'] = 'Kronos'
mas_taxc['Multiple Jobs'] = ''
mas_taxc['Other Income'] = ''
mas_taxc['Exempt'] = 'N'
mas_taxc['Nonresident Alien'] = np.where(mas_taxc['Filing Status'] == 'Non-Residential Alien','Y','N')
mas_taxc['Exempt from NRA Additional Amount'] = 'N'
mas_taxc['Lock in Letter'] = 'N'
mas_taxc['No Wage No Tax Indicator'] = 'N'

w4 = pd.read_excel('./../USA_KBP_CNP_Payroll-USA_Template.xlsx', sheet_name='W-4 Elections',skiprows=2)

w4 = w4.iloc[0:0]

mas_taxf = mas_taxc[['Employee ID', 'Source System', 'Effective As Of', 'Company',
       'Payroll W-4 Marital Status', 'Number of Allowances',
       'Additional Amount', 'Multiple Jobs', 'Total Dependent Amount',
       'Other Income', 'Deductions', 'Exempt', 'Nonresident Alien',
       'Exempt from NRA Additional Amount', 'Lock in Letter',
       'No Wage No Tax Indicator']]


os.chdir(path)
mas_taxf.to_csv('w_4_elections.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
############################## State Tax Elections ###########################
##############################################################################

'''
Employee ID
Company
Effective As Of
Payroll State Tax Authority

'''

state = get_rpt('72596866')

work_del = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\Workday Delivered\Integration_IDs.csv",encoding="cp1251")
work_del_2 = work_del[work_del['Business Object'] == 'Payroll State Authority']
work_del_2['ID'] = work_del_2['ID'].astype(str).str.zfill(2)
statec = state.merge(work_del_2, left_on='Company Tax Name',right_on='Instance')

#statec = state
statec['Employee ID'] = statec['Employee Id']
statec['Company'] = statec['Cost Centers(Company Code)']
statec['Source System'] = 'Kronos'
statec['Effective From'] = pd.to_datetime(statec['Effective From'])
statec['Date Hired'] = pd.to_datetime(statec['Date Hired'])
statec['Effective As Of'] = np.where(statec['Effective From'] > statec['Date Hired'], statec['Effective From'] ,statec['Date Hired'])
statec['Effective As Of'] = statec['Effective As Of'].dt.strftime("%d-%b-%Y").str.upper()
#workday lookup value tax marital reference
statec['Payroll Marital Status Reference'] = statec['Filing Status.1']
statec['Payroll State Tax Authority'] = statec['ID']
statec['New Jersey Rate Table Specification'] = ''
statec['Number of Allowances'] = statec['Total Allowances - NYC']
statec['Exemptions for Mississippi'] = ''
statec['Estimated Deductions'] = statec['Deduction']
statec['Dependent Allowance'] = statec['Dependent Allowances']
statec['Additional Allowance'] = statec['Additional Allowances']
statec['Withholding Exemption'] = statec['Withholding Exemptions']
statec['Allocation Percent'] = statec['State Elected Percentage Rate']
statec['Additional Percent'] = ''
# we have these states I just don't know where to pull the info
statec['Services Localized in Illinois'] = ''
statec['Arizona Constant Percent'] = ''

statec['Additional Amount'] = ''
statec['Annual Withholding Amount'] = statec['Additional Withholding Amount ($)']
statec['Reduced Withholding Amount'] = statec['Reduced withholding']
statec['Estimated Tax Credit per Period'] = ''
statec['Exempt'] = np.where(statec['Maryland State Tax Exemption for PA residents'] == 'True','Y','N')
statec['Exempt Reason'] = ''
statec['Withholding Substantiated'] = 'N'
statec["Certificate of Non Residence"]	= ''
statec["Certificate of Residence"]	= ''
statec['Certificate of Withholding Exemption and County Status']	=''
statec['Head of Household']=''
statec['Employee Blind'] = statec['Age and Blindness Exemptions']
statec['Spouse Indicator']= ''
statec['Full-time Student Indicator']=''
statec['Lower Tax Rate']	=''
statec['Inactivate State Tax']=''	
statec['Lock in Letter'] = 'N'
statec['Active Duty Oklahoma']	=''
statec['Fort Campbell Exempt Kentucky'] =''	
statec['MSRR Exempt	Entrepreneur Exemption'] =''	
statec['Domicile State Tax Authority'] =''
statec['No Wage No Tax Indicator']	=''
statec['Increase or Decrease Withholding Amount']	=''
statec['Reduced Withholding per Pay Period']=''


ste = pd.read_excel('./../USA_KBP_CNP_Payroll-USA_Template.xlsx', sheet_name='State Tax Elections',skiprows=2)

ste = ste.iloc[0:0]


statec['Married Filing Jointly Optional Calculation'] = ''
statec['Veteran Exemption'] = ''
statec['Exemption for Dependents Complete'] = ''
statec['Exemption for Dependents Joint Custody'] = ''
statec['Allowance on Special Deduction'] = ''
statec['MSRR Exempt'] = ''
statec['Entrepreneur Exemption'] = ''

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

os.chdir(path)
ste_data.to_csv('state_tax_elections.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
################################## SUTA Exempt ###############################
##############################################################################

'''
Employee ID
Company
Effective As Of
Exempt Indicator
Payroll Tax Authority Code
'''

##############################################################################
####################### Local Home City Tax Elections ########################
##############################################################################

'''
Employee ID
Company
Effective As Of
Payroll Local City Tax Authority Code
Number of Allowances
Additional Amount

WORKER DATA (Required)	Text	Employee ID	Required	Employee Id
WORKER DATA (Required)	Text	Source System	Optional	Kronos
WORKER DATA (Required)	Text	Company	Required	Cost Centers(Company Code
LOCAL HOME CITY TAX ELECTIONS (Required)	DD-MON-YYYY	Effective As Of	Required	Created
LOCAL HOME CITY TAX ELECTIONS (Required)	Text	Payroll Local City Tax Authority Code	Required	integration ids
LOCAL HOME CITY TAX ELECTIONS (Required)	Integer	Number of Allowances	Optional	Total Allowances - NYC
LOCAL HOME CITY TAX ELECTIONS (Required)	NUMERIC(8,2)	Additional Amount	Optional	Additional Allowances
LOCAL HOME CITY TAX ELECTIONS (Required)	Y/N	Exempt Indicator Pennsylvania	Optional	Maryland State Tax Exemption for PA Residents
LOCAL HOME CITY TAX ELECTIONS (Required)	Text	Constant Text	Optional	
LOCAL HOME CITY TAX ELECTIONS (Required)	Y/N	Inactive	Optional	N


'''

kro_tax = get_rpt('72604614')
kro_tax = kro_tax[kro_tax['Location(1)']=='Work From Home']
work_del = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\Workday Delivered\Integration_IDs.csv",encoding="cp1251")
work_del_c = work_del[work_del['Type'] == 'Payroll_Local_City_Authority_Tax_Code']

kro_tax['Employee ID'] = kro_tax['Employee Id']
kro_tax['Source System'] = 'Kronos'
kro_tax['Company'] = kro_tax['Cost Centers(Company Code)']
kro_tax['Effective As Of'] = kro_tax['Created']
kro_tax['Number of Allowances'] = kro_tax['Total Allowances - NYC']
kro_tax['Additional Amount'] = kro_tax['Additional Allowances']
kro_tax['Exempt Indicator Pennsylvania'] = kro_tax['Maryland State Tax Exemption for PA residents']
kro_tax['Constant Text'] = ''
kro_tax['Inactive'] = 'N'

kro_tax['Company Tax Name'] = kro_tax['Company Tax Name'].str.replace("City Tax", "")

kro_taxc = kro_tax.merge(work_del_c, left_on=['Tax State/Province','Company Tax Name'], right_on=['State','Authority'])
kro_taxc['Payroll Local City Tax Authority Code'] = kro_taxc['ID']

city_tax = kro_taxc[['Employee ID',
'Source System',
'Company',
'Effective As Of',
'Payroll Local City Tax Authority Code',
'Number of Allowances',
'Additional Amount',
'Exempt Indicator Pennsylvania',
'Constant Text',
'Inactive']]

os.chdir(path)
city_tax.to_csv('city_tax_elections_home.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
####################### Local Work City Tax Elections ########################
##############################################################################

'''
Employee ID
Company
Effective As Of
Payroll Local City Tax Authority Code
Number of Allowances (Michigan)
Constant Percent (Michigan)
Exempt Indicator (Michigan, Pennsylvania)
Additional Amount

WORKER DATA (Required)	Text	Employee ID	Required
WORKER DATA (Required)	Text	Source System	Optional
WORKER DATA (Required)	Text	Company	Required
LOCAL WORK CITY TAX ELECTIONS (Required)	DD-MON-YYYY	Effective As Of	Required
LOCAL WORK CITY TAX ELECTIONS (Required)	Text	Payroll Local City Tax Authority Code	Required
LOCAL WORK CITY TAX ELECTIONS (Required)	Integer	Number of Allowances (Michigan)	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	NUMERIC(18,6)	Constant Percent (Michigan)	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	Y/N	Exempt Indicator (Michigan, Pennsylvania)	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	Text	Constant Text	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	NUMERIC(14,2)	Previous Employer Deducted Amount (Pennsylvania)	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	Y/N	Primary EIT Pennsylvania	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	Y/N	Not Subject to EIT Pennsylvania	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	NUMERIC(8,2)	Additional Amount	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	NUMERIC(26,6)	Low Income Threshold	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	Text	Currency Code	Optional
LOCAL WORK CITY TAX ELECTIONS (Required)	Y/N	Inactive	Optional


'''

kro_tax = get_rpt('72604614')
kro_tax = kro_tax[kro_tax['Location(1)']!='Work From Home']
work_del = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\Workday Delivered\Integration_IDs.csv",encoding="cp1251")
work_del_c = work_del[work_del['Type'] == 'Payroll_Local_City_Authority_Tax_Code']

kro_tax['Employee ID'] = kro_tax['Employee Id']
kro_tax['Source System'] = 'Kronos'
kro_tax['Company'] = kro_tax['Cost Centers(Company Code)']
kro_tax['Effective As Of'] = kro_tax['Created']
kro_tax['Number of Allowances'] = kro_tax['Total Allowances - NYC']
kro_tax['Additional Amount'] = kro_tax['Additional Allowances']
kro_tax['Exempt Indicator (Michigan, Pennsylvania)'] = kro_tax['Maryland State Tax Exemption for PA residents']
kro_tax['Constant Text'] = ''
kro_tax['Inactive'] = 'N'
kro_tax['Currency Code'] = 'USD'
kro_tax['Number of Allowances (Michigan)'] = ''
kro_tax['Constant Percent (Michigan)'] = ''
kro_tax['Constant Text'] =''
kro_tax['Previous Employer Deducted Amount (Pennsylvania)']=''
kro_tax['Primary EIT Pennsylvania']=''
kro_tax['Not Subject to EIT Pennsylvania']=''
kro_tax['Low Income Threshold']=''



kro_tax['Company Tax Name'] = kro_tax['Company Tax Name'].str.replace("City Tax", "")

kro_taxc = kro_tax.merge(work_del_c, left_on=['Tax State/Province','Company Tax Name'], right_on=['State','Authority'])
kro_taxc['Payroll Local City Tax Authority Code'] = kro_taxc['ID']


city_tax2 = kro_taxc[['Employee ID',
'Source System',
'Company',
'Effective As Of',
'Payroll Local City Tax Authority Code',
'Number of Allowances (Michigan)',
'Constant Percent (Michigan)',
'Exempt Indicator (Michigan, Pennsylvania)',
'Constant Text',
'Previous Employer Deducted Amount (Pennsylvania)',
'Primary EIT Pennsylvania',
'Not Subject to EIT Pennsylvania',
'Additional Amount',
'Low Income Threshold',
'Currency Code',
'Inactive']]

os.chdir(path)
city_tax2.to_csv('city_tax_elections_work.txt',sep='|', encoding='utf-8', index=False)




##############################################################################
###################### Local Home County Tax Elections #######################
##############################################################################

'''
Employee ID
Company
Effective As Of
Payroll Local County Tax Authority Code

WORKER DATA (Required)	Text	Employee ID	Required
WORKER DATA (Required)	Text	Source System	Optional
WORKER DATA (Required)	Text	Company	Required
LOCAL HOME COUNTY TAX ELECTIONS (Required)	DD-MON-YYYY	Effective As Of	Required
LOCAL HOME COUNTY TAX ELECTIONS (Required)	Text	Payroll Local County Tax Authority Code	Required
LOCAL HOME COUNTY TAX ELECTIONS (Required)	NUMERIC(8,2)	County Additional Amount	Optional
LOCAL HOME COUNTY TAX ELECTIONS (Required)	Y/N	Inactive	Optional

'''

cnt_tax = get_rpt('72604615')
cnt_tax = cnt_tax[cnt_tax['Location(1)']=='Work From Home']
work_del = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\Workday Delivered\Integration_IDs.csv",encoding="cp1251")
work_del_c = work_del[work_del['Type'] == 'Payroll_Local_County_Authority_Tax_Code']

cnt_tax['Employee ID'] = cnt_tax['Employee Id']
cnt_tax['Source System'] = 'Kronos'
cnt_tax['Company'] = cnt_tax['Cost Centers(Company Code)']
cnt_tax['Effective As Of'] = cnt_tax['Created']
cnt_tax['County Additional Amount'] = cnt_tax[['Additional Allowances']]
cnt_tax['Inactive'] = 'N'

cnt_tax['Company Tax Name'] = cnt_tax['Company Tax Name'].str.replace("County", "")
cnt_tax['authority_merge'] = cnt_tax['Tax State/Province'] + ' ' +cnt_tax['Company Tax Name']
work_del_c['authority_merge'] = work_del_c['State'] + ' ' + work_del_c['Authority']

#cnt_taxc = cnt_tax.merge(work_del_c, on='authority_merge')
cnt_tax['Payroll Local County Tax Authority Code'] = ''

cnt_taxc = cnt_tax[['Employee ID',
'Source System',
'Company',
'Effective As Of',
'Payroll Local County Tax Authority Code',
'County Additional Amount',
'Inactive']]

os.chdir(path)
cnt_taxc.to_csv('cnty_tax_elections_home.txt',sep='|', encoding='utf-8', index=False)





##############################################################################
###################### Local Work County Tax Elections #######################
##############################################################################

'''
Employee ID
Company
Effective As Of
Payroll Local County Tax Authority Code

'''
cnt_tax = get_rpt('72604615')
cnt_tax = cnt_tax[cnt_tax['Location(1)']!='Work From Home']
work_del = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\Workday Delivered\Integration_IDs.csv",encoding="cp1251")
work_del_c = work_del[work_del['Type'] == 'Payroll_Local_County_Authority_Tax_Code']

cnt_tax['Employee ID'] = cnt_tax['Employee Id']
cnt_tax['Source System'] = 'Kronos'
cnt_tax['Company'] = cnt_tax['Cost Centers(Company Code)']
cnt_tax['Effective As Of'] = cnt_tax['Created']
cnt_tax['County Additional Amount'] = cnt_tax[['Additional Allowances']]
cnt_tax['Inactive'] = 'N'

cnt_tax['Company Tax Name'] = cnt_tax['Company Tax Name'].str.replace("City Tax", "")

cnt_taxc = cnt_tax.merge(work_del_c, left_on=['Tax State/Province','Company Tax Name'], right_on=['State','Authority'])
cnt_taxc['Payroll Local County Tax Authority Code'] = cnt_taxc['ID']

cnt_taxw = cnt_taxc[['Employee ID',
'Source System',
'Company',
'Effective As Of',
'Payroll Local County Tax Authority Code',
'County Additional Amount',
'Inactive']]

os.chdir(path)
cnt_taxw.to_csv('cnty_tax_elections_work.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
###################### Local Home School District Tax #######################
##############################################################################

'''
WORKER DATA (Required)	Text	Employee ID	Required
WORKER DATA (Required)	Text	Source System	Optional
WORKER DATA (Required)	Text	Company	Required
LOCAL HOME SCHOOL DISTRICT TAX ELECTIONS (Required)	DD-MON-YYYY	Effective As Of	Required
LOCAL HOME SCHOOL DISTRICT TAX ELECTIONS (Required)	Text	Payroll Local Home School District Tax Authority Code	Required (Add 39 to current code)
LOCAL HOME SCHOOL DISTRICT TAX ELECTIONS (Required)	Y/N	Exempt Indicator Pennsylvania	Optional
LOCAL HOME SCHOOL DISTRICT TAX ELECTIONS (Required)	Text	Constant Text	Optional
LOCAL HOME SCHOOL DISTRICT TAX ELECTIONS (Required)	NUMERIC(14,2)	Previous Employer Deducted Amount Pennsylvania	Optional
LOCAL HOME SCHOOL DISTRICT TAX ELECTIONS (Required)	Y/N	Inactive	Optional

'''

sch = get_rpt('72604616')

sch['Employee ID'] = sch['Employee Id']
sch['Source System'] = 'Kronos'
sch['Company'] = sch['Cost Centers(Company Code)']
sch['Effective As Of'] = sch['Created']
sch['Payroll Local Home School District Tax Authority Code'] = sch['Company Tax Name'].apply(lambda st: st[st.find("(")+1:st.find(")")])
sch['Payroll Local Home School District Tax Authority Code']  = "39" + sch['Payroll Local Home School District Tax Authority Code'] 
sch['Exempt Indicator Pennsylvania'] = 'N'
sch['Constant Text'] = ''
sch['Previous Employer Deducted Amount Pennsylvania'] = ''
sch['Inactive'] = 'N'

sch = sch[['Employee ID',
'Source System',
'Company',
'Effective As Of',
'Payroll Local Home School District Tax Authority Code',
'Exempt Indicator Pennsylvania',
'Constant Text',
'Previous Employer Deducted Amount Pennsylvania',
'Inactive']]

os.chdir(path)
sch.to_csv('school_district_tax.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
######################## Jurisdiction Tax Elections ##########################
##############################################################################

'''
Employee ID
Effective Date
Company
Payroll State Tax Authority Code
Payroll Local County Tax Authority Code
Payroll Local City Tax Authority Code
Payroll Local Other Tax Authority Code
Allocation Percent

'''


##############################################################################
############################ Actual Hours Worked #############################
##############################################################################

'''
Employee ID
Company
Pay Group
Period End Date
Payment Date
Earning
Related Calculation
Hours Worked
'''

hrs = get_rpt('72604618')

hrs['Employee ID'] = hrs['Employee Id']

hrs['Company'] = hrs['Cost Centers(Company Code)']
hrs['Period End Date'] = hrs['Pay Date']
hrs['Payment Date'] = hrs['Pay Date']
hrs['Earning'] = hrs['Type']
hrs['Related Calculation'] = np.where(hrs['Pay Type'] == 'Hourly','HRSWRK','SALARY')
hrs['Hours Worked'] = hrs['Total Work Hours']
hrs['Position'] = ''

hrsc = hrs.merge(pg_datac,on='Employee ID')
hrsc['Pay Group'] = hrsc['Pay Group ID']


hrsc = hrsc[['Employee ID',
'Source System',
'Company',
'Pay Group',
'Position',
'Period End Date',
'Payment Date',
'Earning',
'Related Calculation',
'Hours Worked']]

os.chdir(path)
hrsc.to_csv('actual_work_hrs.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
########################### Deduction Recipients #############################
##############################################################################

'''
Deduction Recipient Name
Deduction Recipient ID
Payment Type
Business Entity Name
External Entity ID
Country ISO Code Phone
Phone Number

'''
#vendor report

vr = get_rpt('70602656')

vr['Deduction Recipient Name'] = vr['Name']
vr['Source System'] = 'Kronos'
vr['Alternate Deduction Recipient Name'] = ''
vr = vr.reset_index()
vr['index'] = vr['index'] +1
vr['Deduction Recipient ID'] = 'VR-' + ((vr['index']).astype(str)).str.pad(3,fillchar='0')
vr['Payment Type'] = np.where(vr['Payment Type'].str.contains('ACH'),'Direct_Deposit','Check')
vr['Business Entity Name'] = vr['Description']
vr['External Entity ID'] = vr['CRM Company Id']
vr['Country ISO Code Address'] = 'USA'
vr['Address Line #1'] = vr['Address 1']
vr['Address Line #2'] = vr['Address 2']
vr['Region'] = vr['State']
vr['Postal Code'] = vr['Zip Code']

ded = pd.read_excel('./../USA_KBP_CNP_Payroll-USA_Template.xlsx', sheet_name='Deduction Recipients',skiprows=2)

ded = ded.iloc[0:0]

ded['Deduction Recipient Name'] = vr['Deduction Recipient Name']

vr2 = vr[['Deduction Recipient Name', 'Source System','Alternate Deduction Recipient Name','Deduction Recipient ID','Payment Type','Business Entity Name','External Entity ID','Country ISO Code Address','Address Line #1','Address Line #2','City','Region','Postal Code']]

ded = ded.drop(['Source System','Alternate Deduction Recipient Name','Deduction Recipient ID','Payment Type','Business Entity Name','External Entity ID','Country ISO Code Address','Address Line #1','Address Line #2','City','Region','Postal Code'],axis=1)

ded = ded.merge(vr2,on='Deduction Recipient Name')

dedf = ded[['Deduction Recipient Name', 'Source System',
       'Alternate Deduction Recipient Name', 'Deduction Recipient ID',
       'Payment Type', 'Business Entity Name', 'Business Entity Tax ID',
       'External Entity ID', 'Email Address', 'Country ISO Code Phone',
       'International Phone Code', 'Phone Number', 'Phone Extension',
       'Phone Device Type', 'Type Reference', 'Use For Reference Phone',
       'Country ISO Code Address', 'Effective as of', 'Address Line #1',
       'Address Line #2', 'Address Line #3', 'Address Line #4',
       'Address Line #5', 'Address Line #6', 'Address Line #7',
       'Address Line #8', 'Address Line #9', 'City', 'City Subdivision',
       'City Subdivision 2', 'Region', 'Region Subdivision',
       'Region Subdivision 2', 'Postal Code', 'Use For Reference Address',
       'Country ISO Code Bank', 'Currency Code', 'Bank Account Nickname',
       'Bank Name', 'Bank Account Type Code', 'Routing Number (Bank ID)',
       'Branch ID', 'Branch Name', 'Bank Account Number', 'Bank Account Name',
       'Roll Number', 'Check Digit', 'IBAN', 'SWIFT Bank Identification Code']]



os.chdir(path)
dedf.to_csv('deduction_recipients.txt',sep='|', encoding='utf-8', index=False)

dedf_c = dedf[['Deduction Recipient Name','Deduction Recipient ID']]

##############################################################################
############################# Bankruptcy (USA) ###############################
##############################################################################

'''
Employee ID
Withholding Order Type ID
Case Number
Order Date
Received Date
Begin Date
Company
Witholding Order Amount Type
Frequency ID
Code
Deduction Recipient ID
Fee Amount #1
Fee Percent #1
Fee Type ID #1
Fee Amount Type ID #1
Fee Monthly Limit #1	
Fee Amount #2	
Fee Percent #2	
Fee Type ID #2	
Fee Amount Type ID #2
Fee Monthly Limit #2

'''

bank = get_rpt('72603205')
bank['Employee ID'] = bank['Employee Id']
bank['Withholding Order Type ID'] = 'BANKRUPTCY'
bank['Case Number'] = bank['Additional Info']
bank['Order Date']  = bank['Begin Date']
bank['Received Date'] = bank['Begin Date']

bank['Company'] = bank['Cost Centers(Company Code)']
bank['Withholding Order Amount Type'] = np.where(bank['EE Calc Method'] == '% Of Disposable Earnings','PERCENTDE',
                                                 np.where(bank['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS','AMT'))


bank['Withholding Order Amount'] = bank['EE Amount']
bank['Withholding Order Amount as Percent'] = bank['EE Percent (As Of Today)']
bank['Frequency ID'] = np.where(bank['Unemployment State/Province'] == 'NY','Weekly','Bi_weekly')
bank['Code (Issued in Reference)'] = 'FEDERAL'

bankc = bank.merge(dedf_c,left_on='Vendor',right_on='Deduction Recipient Name')



bankc = bankc[['Employee ID','Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID']]


bankrup = pd.read_excel('./../USA_KBP_CNP_Payroll-USA_Template.xlsx', sheet_name='Bankruptcy (USA)',skiprows=2)

bankrup = bankrup.iloc[0:0]

bankrup['Employee ID'] = bankc['Employee ID']

bankrup = bankrup.drop(['Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID'],axis=1)

bankrup2 = bankrup.merge(bankc, on='Employee ID')

bankrup2 = bankrup2[['Employee ID', 'Source System', 'Withholding Order Type ID',
       'Case Number', 'Withholding Order Additional Order Number',
       'Order Date', 'Received Date', 'Begin Date', 'End Date', 'Company',
       'Inactive', 'Withholding Order Amount Type', 'Withholding Order Amount',
       'Withholding Order Amount as Percent', 'Frequency ID',
       'Total Debt Amount Remaining', 'Monthly Limit',
       'Code (Issued in Reference)', 'Deduction Recipient ID',
       'Originating Authority', 'Memo', 'Currency', 'Chapter 13', 'Chapter 7',
       'Fee Amount #1', 'Fee Percent #1', 'Fee Type ID #1',
       'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
       'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
       'Fee Monthly Limit #1', 'Fee Amount #2', 'Fee Percent #2',
       'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
       'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
       'Fee Monthly Limit #2', 'Withholding Order Withholding Frequency']]


os.chdir(path)
bankrup2.to_csv('bankruptcy.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
############################## Creditor (USA) ################################
##############################################################################

'''
Employee ID
Withholding Order Type ID
Case Number
Order Date
Received Date
Begin Date
Company
Witholding Order Amount Type
Frequency ID
Code
Deduction Recipient ID
Fee Amount #1
Fee Percent #1
Fee Type ID #1
Fee Amount Type ID #1
Fee Monthly Limit #1	
Fee Amount #2	
Fee Percent #2	
Fee Type ID #2	
Fee Amount Type ID #2
Fee Monthly Limit #2
'''



##############################################################################
########################## Wage Assignment (USA) #############################
##############################################################################

'''
Employee ID
Withholding Order Type ID
Case Number
Order Date

'''

wage_as = get_rpt('72603206')
wage_as['Employee ID'] = wage_as['Employee Id']
wage_as['Withholding Order Type ID'] = 'WAGE'
wage_as['Case Number'] = wage_as['Additional Info']
wage_as['Order Date']  = wage_as['Begin Date']
wage_as['Received Date'] = wage_as['Begin Date']

wage_as['Company'] = wage_as['Cost Centers(Company Code)']
wage_as['Withholding Order Amount Type'] = np.where(wage_as['EE Calc Method'] == '% Of Disposable Earnings','PERCENTDE',
                                                 np.where(wage_as['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS','AMT'))


wage_as['Withholding Order Amount'] = wage_as['EE Amount']
wage_as['Withholding Order Amount as Percent'] = wage_as['EE Percent (As Of Today)']
wage_as['Frequency ID'] = np.where(wage_as['Unemployment State/Province'] == 'NY','Weekly','Bi_weekly')
wage_as['Code (Issued in Reference)'] = ''

wagec = wage_as.merge(dedf_c,left_on='Vendor',right_on='Deduction Recipient Name')



wagec = wagec[['Employee ID','Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID']]


wages = pd.read_excel('./../USA_KBP_CNP_Payroll-USA_Template.xlsx', sheet_name='Wage Assignment (USA)',skiprows=2)

wages = wages.iloc[0:0]

wages['Employee ID'] = wagec['Employee ID']

wages = wages.drop(['Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID'],axis=1)

wages2 = wages.merge(wagec, on='Employee ID')

wages2 = wages2[['Employee ID', 'Source System', 'Withholding Order Type ID',
       'Case Number', 'Withholding Order Additional Order Number',
       'Order Date', 'Received Date', 'Begin Date', 'End Date', 'Company',
       'Inactive', 'Withholding Order Amount Type', 'Withholding Order Amount',
       'Withholding Order Amount as Percent', 'Frequency ID',
       'Total Debt Amount Remaining', 'Monthly Limit',
       'Code (Issued in Reference)', 'Deduction Recipient ID', 'Memo',
       'Currency', 'Regulated Loan', 'Head of Household', 'Married',
       'Fee Amount #1', 'Fee Percent #1', 'Fee Type ID #1',
       'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
       'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
       'Fee Monthly Limit #1', 'Fee Amount #2', 'Fee Percent #2',
       'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
       'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
       'Fee Monthly Limit #2']]


os.chdir(path)
wages2.to_csv('wage_assignment.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
########################### State Tax Levy (USA) #############################
##############################################################################

'''
Employee ID
Withholding Order Type ID
Case Number
Order Date
Received Date
Begin Date
Company
Withholding Order Amount Type
Frequency ID
Total Debt Amount Remaining
Code (Issued in Reference)
Deduction Recipient ID
Number of Dependents
Worker is Laborer or Mechanic
Worker Income is Poverty Level
Fee Type ID #1
Fee Amount Type ID #1
Fee Type ID #2	
Fee Amount Type ID #2

'''
state_tax = get_rpt('72603207')
state_tax['Employee ID'] = state_tax['Employee Id']
state_tax['Withholding Order Type ID'] = 'STATELEVY'
state_tax['Case Number'] = state_tax['Additional Info']
state_tax['Order Date']  = state_tax['Begin Date']
state_tax['Received Date'] = state_tax['Begin Date']

state_tax['Company'] = state_tax['Cost Centers(Company Code)']
state_tax['Withholding Order Amount Type'] = np.where(state_tax['EE Calc Method'] == '% Of Disposable Earnings','PERCENTDE',
                                                 np.where(state_tax['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS','AMT'))


state_tax['Withholding Order Amount'] = state_tax['EE Amount']
state_tax['Withholding Order Amount as Percent'] = state_tax['EE Percent (As Of Today)']
state_tax['Frequency ID'] = np.where(state_tax['Unemployment State/Province'] == 'NY','Weekly','Bi_weekly')
state_tax['Code (Issued in Reference)'] = ''

state_taxc = state_tax.merge(dedf_c,left_on='Vendor',right_on='Deduction Recipient Name')



state_taxc = state_taxc[['Employee ID','Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID']]


st_tx = pd.read_excel('./../USA_KBP_CNP_Payroll-USA_Template.xlsx', sheet_name='State Tax Levy (USA)',skiprows=2)

st_tx = st_tx.iloc[0:0]

st_tx['Employee ID'] = state_taxc['Employee ID']

st_tx = st_tx.drop(['Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID'],axis=1)

st_tx2 = st_tx.merge(state_taxc, on='Employee ID')

st_tx2 = st_tx2[['Employee ID', 'Source System', 'Withholding Order Type ID',
       'Case Number', 'Withholding Order Additional Order Number',
       'Order Date', 'Received Date', 'Begin Date', 'End Date', 'Company',
       'Inactive', 'Withholding Order Amount Type', 'Withholding Order Amount',
       'Withholding Order Amount as Percent', 'Frequency ID',
       'Total Debt Amount Remaining', 'Monthly Limit',
       'Code (Issued in Reference)', 'Deduction Recipient ID',
       'Originating Authority', 'Memo', 'Currency', 'Form and Revision Number',
       'Marital Status', 'Number of Dependents',
       'Worker is Laborer or Mechanic', 'Worker Income is Poverty Level',
       'Part 3 Effective Date', 'Pay Period Exemption Override Amount',
       'Payroll Marital Status Reference', 'Personal Exemptions',
       'Additional 65 or Blind Exemptions', 'Termination Date',
       'Dependent Name', 'Dependent Identification Number',
       'Good Cause Limit Percent', 'Process Until ID', 'Prorate Until Date',
       'Fee Amount #1', 'Fee Percent #1', 'Fee Type ID #1',
       'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
       'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
       'Fee Monthly Limit #1', 'Fee Amount #2', 'Fee Percent #2',
       'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
       'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
       'Fee Monthly Limit #2']]


os.chdir(path)
st_tx2.to_csv('state_tax_levy.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
########################## Federal Tax Levy (USA) ############################
##############################################################################

'''
Employee ID
Withholding Order Type ID
Case Number
Order Date
Received Date
Begin Date
Company 
Withholding Order Amount Type
Frequency ID
Total Debt Amount Remaining
Code (Issued in Reference)
Deduction Recipient ID
Fee Amount #1	
Fee Percent #1	
Fee Type ID #1	
Fee Amount Type ID #1
Fee Monthly Limit #1	
Fee Amount #2	
Fee Percent #2	
Fee Type ID #2	
Fee Amount Type ID #2
Fee Monthly Limit #2

'''


##############################################################################
###################### Federal Admin Wage Garn (USA) #########################
##############################################################################

'''
Employee ID
Withholding Order Type ID
Case Number
Order Date
Received Date
Begin Date
Company
Withholding Order Amount Type
Frequency ID
Total Debt Amount Remaining
Code (Issued in Reference)
Deduction Recipient ID
Fee Amount #1	
Fee Percent #1	
Fee Type ID #1	
Fee Amount Type ID #1
Fee Monthly Limit #1	
Fee Amount #2	
Fee Percent #2	
Fee Type ID #2	
Fee Amount Type ID #2
Fee Monthly Limit #2

'''


##############################################################################
###################### Support Orders (USA) ##################################
##############################################################################


'''
Employee ID
Withholding Order Type ID
Case Number
Order Date
Received Date
Begin Date
Company
Withholding Order Amount Type
Frequency ID
Code (Issued in Reference)
Deduction Recipient ID
Order Form Amount #1
Pay Period Amount #1
Support Type #1
Order Form Amount #2
Pay Period Amount #2
Support Type #2
Fee Amount #1
Fee Percent #1
Fee Type ID #1
Fee Amount Type ID #1
Fee Monthly Limit #1	
Fee Amount #2	
Fee Percent #2	
Fee Type ID #2	
Fee Amount Type ID #2
Fee Monthly Limit #2

'''

sup_order = get_rpt('72603208')
work_del_2 = work_del[work_del['Business Object'] == 'Payroll State Authority']
work_del_2['ID'] = work_del_2['ID'].astype(str).str.zfill(2)

"""
Fix inconsistenties with support order names:
    
Arizona Child Support - KBP Inspired
Connecticut Child Support - KBP Foods
DNU - CO Child Support - KBP Bells
Florida State Disbursement Unit - Foods
Maine Child Support - KBP Foods
Massachusetts Child Support - KBP Foods
New Hampshire Child Support - KBP Foods
New Mexico Child Support
New York Child Support - KBP Bells
North Carolina Child Support - KBP Inspired
Pennsylvania Child Support - KBP Inspired
Tennessee Child Support- KBP FOODS
Texas Child Support - KBP Inpsired
Washington DC Child Support
Washington State Support Registry - foods
"""

sup_order['Vendor_State'] = np.where(sup_order['Vendor'] == 'Arizona Child Support - KBP Inspired','AZ',
                            np.where(sup_order['Vendor'] == 'Connecticut Child Support - KBP Foods','CT',
                            np.where(sup_order['Vendor'] == 'DNU - CO Child Support - KBP Bells','CO',
                            np.where(sup_order['Vendor'] == 'Florida State Disbursement Unit - Foods','FL',
                            np.where(sup_order['Vendor'] == 'Maine Child Support - KBP Foods','ME',
                            np.where(sup_order['Vendor'] == 'Massachusetts Child Support - KBP Foods','MA',
                            np.where(sup_order['Vendor'] == 'New Hampshire Child Support - KBP Foods','NH',
                            np.where(sup_order['Vendor'] == 'New Mexico Child Support','NM',
                            np.where(sup_order['Vendor'] == 'New York Child Support - KBP Bells','NY',
                            np.where(sup_order['Vendor'] == 'North Carolina Child Support - KBP Inspired','NC',
                            np.where(sup_order['Vendor'] == 'Pennsylvania Child Support - KBP Inspired','PA',
                            np.where(sup_order['Vendor'] == 'Tennessee Child Support- KBP FOODS','TN',
                            np.where(sup_order['Vendor'] == 'Texas Child Support - KBP Inpsired','TX',
                            np.where(sup_order['Vendor'] == 'Washington DC Child Support','DC',
                            np.where(sup_order['Vendor'] == 'Washington State Support Registry - foods','WA',sup_order['Vendor'].str.split(" ").str[0])))))))))))))))










sup_order = sup_order.merge(work_del_2, right_on='State',left_on='Vendor_State')
sup_order['Employee ID'] = sup_order['Employee Id']
sup_order['Withholding Order Type ID'] = 'SUPPORT'
sup_order['Case Number'] = sup_order['Additional Info']
sup_order['Order Date']  = sup_order['Begin Date']
sup_order['Received Date'] = sup_order['Begin Date']

sup_order['Company'] = sup_order['Cost Centers(Company Code)']
sup_order['Withholding Order Amount Type'] = np.where(sup_order['EE Calc Method'] == '% Of Disposable Earnings','PERCENTDE',
                                                 np.where(sup_order['EE Calc Method'] == '% Of Gross Earnings', 'PERCENTGROSS','AMT'))


sup_order['Withholding Order Amount'] = sup_order['EE Amount']
sup_order['Withholding Order Amount as Percent'] = sup_order['EE Percent (As Of Today)']
sup_order['Frequency ID'] = np.where(sup_order['Unemployment State/Province'] == 'NY','Weekly','Bi_weekly')
sup_order['Code (Issued in Reference)'] = sup_order['ID']

sup_orderc = sup_order.merge(dedf_c,left_on='Vendor',right_on='Deduction Recipient Name')

sup_orderc['Order Form Amount #1']= sup_orderc['Withholding Order Amount'] 
sup_orderc['Pay Period Amount #1']='AMT'
sup_orderc['Amount as Percent #1']=sup_orderc['Withholding Order Amount as Percent'] 

sup_orderc = sup_orderc[['Employee ID','Withholding Order Type ID','Case Number','Order Date','Received Date','Begin Date','Company','Withholding Order Amount Type','Withholding Order Amount','Withholding Order Amount as Percent','Frequency ID','Code (Issued in Reference)','Deduction Recipient ID','Order Form Amount #1','Pay Period Amount #1','Amount as Percent #1']]


#support = pd.read_excel('./../USA_KBP_CNP_Payroll-USA_Template.xlsx', sheet_name='Support Orders (USA)',skiprows=2)

#support = support.iloc[0:0]


sup_orderc['Source System'] = 'Kronos'
sup_orderc['Withholding Order Additional Order Number'] = ''
sup_orderc['Inactive'] = ''
sup_orderc['Monthly Limit'] = ''
sup_orderc['Originating Authority'] = ''
sup_orderc['Memo'] =''
sup_orderc['Currency'] = 'USD'
sup_orderc['Case Type of Original Order'] = ''
sup_orderc['Case Type of Amended Order'] = ''
sup_orderc['Case Type of Termination Order'] = ''
sup_orderc['Custodial Party Name'] = ''
sup_orderc['Supports Second Family'] =''
sup_orderc['Remittance ID Override']=''
sup_orderc['Child Name (Last, First, MI)']=''
sup_orderc['Child Birth Date']=''
sup_orderc['Payroll Local County Authority FIPS Code']=''

sup_orderc['Support Type #1']='CS'
sup_orderc['Arrears Over 12 Weeks #1']=''
sup_orderc['Order Form Amount #2']=''
sup_orderc['Pay Period Amount #2']=''
sup_orderc['Amount as Percent #2']=''
sup_orderc['Support Type #2']=''
sup_orderc['Arrears Over 12 Weeks #2']=''
sup_orderc['Fee Amount #1']=''
sup_orderc['Fee Percent #1']=''
sup_orderc['Fee Type ID #1']=''
sup_orderc['Fee Amount Type ID #1']=''
sup_orderc['Deduction Recipient ID #1']=''
sup_orderc['Override Fee Schedule #1']=''
sup_orderc['Begin Date #1']=''
sup_orderc['End Date #1']=''
sup_orderc['Fee Monthly Limit #1']='' 
sup_orderc['Fee Percent #2']=''
sup_orderc['Fee Amount #2']=''
sup_orderc['Fee Type ID #2']='' 
sup_orderc['Fee Amount Type ID #2']='' 
sup_orderc['Deduction Recipient ID #2']=''
sup_orderc['Override Fee Schedule #2']=''
sup_orderc['Begin Date #2']=''
sup_orderc['End Date #2']=''
sup_orderc['Fee Monthly Limit #2']=''




support2 = sup_orderc[['Employee ID', 'Source System', 'Withholding Order Type ID',
       'Case Number', 'Withholding Order Additional Order Number',
       'Order Date', 'Received Date', 'Begin Date', 'Company', 'Inactive',
       'Withholding Order Amount Type', 'Withholding Order Amount',
       'Withholding Order Amount as Percent', 'Frequency ID', 'Monthly Limit',
       'Code (Issued in Reference)', 'Deduction Recipient ID',
       'Originating Authority', 'Memo', 'Currency',
       'Case Type of Original Order', 'Case Type of Amended Order',
       'Case Type of Termination Order', 'Custodial Party Name',
       'Supports Second Family', 'Remittance ID Override',
       'Child Name (Last, First, MI)', 'Child Birth Date',
       'Payroll Local County Authority FIPS Code', 'Order Form Amount #1',
       'Pay Period Amount #1', 'Amount as Percent #1', 'Support Type #1',
       'Arrears Over 12 Weeks #1', 'Order Form Amount #2',
       'Pay Period Amount #2', 'Amount as Percent #2', 'Support Type #2',
       'Arrears Over 12 Weeks #2', 'Fee Amount #1', 'Fee Percent #1',
       'Fee Type ID #1', 'Fee Amount Type ID #1', 'Deduction Recipient ID #1',
       'Override Fee Schedule #1', 'Begin Date #1', 'End Date #1',
       'Fee Monthly Limit #1', 'Fee Percent #2', 'Fee Amount #2',
       'Fee Type ID #2', 'Fee Amount Type ID #2', 'Deduction Recipient ID #2',
       'Override Fee Schedule #2', 'Begin Date #2', 'End Date #2',
       'Fee Monthly Limit #2']]

support2.to_csv('supporttest.csv')
support2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\supporttest.csv")
os.chdir(path)
support2.to_csv('support_orders.txt',sep='|', encoding='utf-8', index=False)

