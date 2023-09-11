# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 09:34:52 2022

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

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion')

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc'
os.chdir(path)
#Single source for populating all data in the HCM Data Conversion Spreadsheet

##############################################################################
################################ OVERVIEW ####################################
##############################################################################


"""
Name and Email -- In scope -- Applicant / Contact Data
Worker Address -- In scope -- Applicant / Contact Data
Worker Phone Numbers -- In scope -- Applicant / Contact Data
EMP - Position Mgt -- In scope -- Hire Information
Supervisory Organizations -- In scope -- Hire Information
Organization Assignments -- In scope -- Post-Hire Data (Group 1)
EMP-System User Accounts -- In scope -- Post-Hire Data (Group 1)
Change Job -- In scope -- Hire information
Emp-Base Compenstation -- In scope -- Compensation

"""

##############################################################################
############################## Name & Email ##################################
##############################################################################

"""
Worker ID
Source System
Applicant Source
Country ISO Code
Legal First Name
Legal Middle Name
Legal Last Name
Email Address - Primary Work
Public - Primary Work (Y)

Resolutions: Fix Email primary work vs primary home
"""


# All Active EE's from Kronos
#active_ees = pd.read_csv('71877725')
active_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\71877725.csv',encoding='cp1251')
active_ees['Date Hired'] = pd.to_datetime(active_ees['Date Hired'])
#active_ees = active_ees[active_ees['Date Hired'] < '01/21/2023']
active_ees = active_ees[active_ees['Employee Status'] == 'Active']

active_ees['Worker ID'] = active_ees['Employee Id']
active_ees['Legal First Name'] = active_ees['First Name']
active_ees['Legal Last Name'] = active_ees['Last Name']
active_ees['Email Address - Primary Home'] = active_ees['Primary Email']
active_ees['Public - Primary Home'] = 'N'
active_ees['Source System'] = 'UKG'
active_ees['Applicant Source'] = ''
active_ees['Country ISO Code'] = 'USA'


name_email = active_ees[['Worker ID','Source System','Applicant Source','Country ISO Code','Legal First Name','Legal Last Name','Email Address - Primary Home','Public - Primary Home']]

name_email['Full Name for Singapore and Malaysia'] = ''
name_email['Legal Middle Name'] = ''
name_email['Legal Secondary Last Name'] = ''
name_email['Name Prefix'] = ''
name_email['Name Suffix'] = ''
name_email['Family Name Prefix'] = ''
name_email['Preferred First Name'] = ''
name_email['Preferred Middle Name'] = ''
name_email['Preferred Last Name'] = ''
name_email['Preferred Secondary Last Name'] = ''
name_email['Preferred Name Prefix'] = ''
name_email['Preferred Name Suffix'] = ''
name_email['Additional Country ISO Code'] = ''
name_email['Additional First Name']=''
name_email['Additional Middle Name']=''
name_email['Additional Last Name']=''
name_email['Additional Secondary Last Name']=''
name_email['Additional Name Type']=''
name_email['Local Script First Name 1']=''
name_email['Local Script Middle Name 1']=''
name_email['Local Script Last Name 1']=''
name_email['Local Script Secondary Name 1']=''
name_email['Local Script First Name 2']=''
name_email['Local Script Middle Name 2']=''
name_email['Local Script Last Name 2']=''
name_email['Local Script Secondary Name 2']=''
name_email['Preferred Local Script First Name 1']=''
name_email['Preferred Local Script Middle Name 1']=''
name_email['Preferred Local Script Last Name 1']=''
name_email['Preferred Local Script Secondary Name 1']=''
name_email['Preferred Local Script First Name 2']=''
name_email['Preferred Local Script Middle Name 2']=''
name_email['Preferred Local Script Last Name 2']=''
name_email['Preferred Local Script Secondary Name 2']=''
name_email['Email Address - Additional Home'] = ''
name_email['Public - Additional Home']=''
name_email['Email Address - Additional Work']=''
name_email['Public - Additional Work']=''
name_email['Web Address']=''
name_email['Usage Type - Web Address']=''
name_email['Public - Web Address']=''
name_email['Instant Messenger Address #1']=''
name_email['Instant Messenger Provider #1']=''
name_email['Usage Type - Instant Messenger #1']=''
name_email['Public - Instant Messenger #1']=''
name_email['Instant Messenger Address #2']=''
name_email['Instant Messenger Provider #2']=''
name_email['Usage Type - Instant Messenger #2']=''
name_email['Public - Instant Messenger #2']=''
name_email['Applicant ID']=''
name_email['Email Address - Primary Work']=''
name_email['Public - Primary Work']=''


name_email = name_email[['Worker ID',
'Source System',
'Applicant Source',
'Country ISO Code',
'Full Name for Singapore and Malaysia',
'Legal First Name',
'Legal Middle Name',
'Legal Last Name',
'Legal Secondary Last Name',
'Name Prefix',
'Name Suffix',
'Family Name Prefix',
'Preferred First Name',
'Preferred Middle Name',
'Preferred Last Name',
'Preferred Secondary Last Name',
'Preferred Name Prefix',
'Preferred Name Suffix',
'Additional Country ISO Code',
'Additional First Name',
'Additional Middle Name',
'Additional Last Name',
'Additional Secondary Last Name',
'Additional Name Type',
'Local Script First Name 1',
'Local Script Middle Name 1',
'Local Script Last Name 1',
'Local Script Secondary Name 1',
'Local Script First Name 2',
'Local Script Middle Name 2',
'Local Script Last Name 2',
'Local Script Secondary Name 2',
'Preferred Local Script First Name 1',
'Preferred Local Script Middle Name 1',
'Preferred Local Script Last Name 1',
'Preferred Local Script Secondary Name 1',
'Preferred Local Script First Name 2',
'Preferred Local Script Middle Name 2',
'Preferred Local Script Last Name 2',
'Preferred Local Script Secondary Name 2',
'Email Address - Primary Home',
'Public - Primary Home',
'Email Address - Primary Work',
'Public - Primary Work',
'Email Address - Additional Home',
'Public - Additional Home',
'Email Address - Additional Work',
'Public - Additional Work',
'Web Address',
'Usage Type - Web Address',
'Public - Web Address',
'Instant Messenger Address #1',
'Instant Messenger Provider #1',
'Usage Type - Instant Messenger #1',
'Public - Instant Messenger #1',
'Instant Messenger Address #2',
'Instant Messenger Provider #2',
'Usage Type - Instant Messenger #2',
'Public - Instant Messenger #2',
'Applicant ID']]



name_email.to_csv('name_and_email.txt', sep='|', encoding='utf-8', index=False)

##############################################################################
############################# Worker Address #################################
##############################################################################

"""
Worker ID
Source System
Primary (Address) (Y/N)
Usage Type (Home, Work) Usage Type must = Home when Work From Home Address = Y.
Public - Home addresses can only be marked Public=Y if the person works from home (Y/N)
Country ISO Code
Address Line #1
Address Line #2
City
Postal Code
Work From Home Address (Y/N)

"""
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config
from common import (write_to_csv, active_workers, open_as_utf8, modify_amount)

    cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
    dep['Employee Id'] = dep['Employee Id'].astype(str)
    dep = dep.loc[dep['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    
    
active_ees['Zip Code'] = active_ees['Zip Code'].astype(str).str.zfill(5)
active_ees['Zip Code'] = active_ees['Zip Code'].astype(str)
wfh = active_ees[active_ees['Default Location'] == 'Work From Home']
wfh['Usage Type'] = 'Home'
wfh['Primary'] = 'Y'
wfh['Public'] = 'Y'
wfh['Address Line #1'] = wfh['Address 1']
wfh['Address Line #2'] = wfh['Address 2']
wfh['Postal Code'] = (wfh['Zip Code'])
wfh['Work From Home Address'] = 'Y'
wfh['Region'] = 'USA-' + wfh['State'].astype(str)

wfh_f = wfh[['Worker ID','Source System','Primary','Usage Type','Public','Country ISO Code','Address Line #1','Address Line #2','City','Postal Code','Work From Home Address','Region']]

wfw = active_ees[active_ees['Default Location'] != 'Work From Home']
wfw['Usage Type'] = 'Home'
wfw['Primary'] = 'Y'
wfw['Public'] = 'N'
wfw['Address Line #1'] = wfw['Address 1']
wfw['Address Line #2'] = wfw['Address 2']
wfw['Postal Code'] = (wfw['Zip Code'])
wfw['Work From Home Address'] = 'N'
wfw['Region'] = 'USA-' + wfw['State'].astype(str)


wfw_f = wfw[['Worker ID','Source System','Primary','Usage Type','Public','Country ISO Code','Address Line #1','Address Line #2','City','Postal Code','Work From Home Address','Region']]


os.chdir(r'C:\Users\akaff\KBP Investments')

list_of_files = glob.glob('./KBP Contact Sheet - Current/*.xlsx')
latest_file = max(list_of_files, key=os.path.getctime)
foods = pd.read_excel(latest_file)
bells = pd.read_excel(latest_file, sheet_name='All Stores - KBP Bells')
bells.dropna(how = "all", inplace=True)
insp = pd.read_excel(latest_file, sheet_name='All Stores - Arby')
insp.dropna(how = "all", inplace=True)
dat = pd.concat([foods,bells,insp])

wfw_all = wfw.merge(dat, left_on = 'Cost Centers(Cost Center)', right_on='Store Cost Center')

wfw_all['Usage Type'] = 'Work'
wfw_all['Primary'] = 'N'
wfw_all['Public'] = 'Y'
wfw_all['Address Line #1'] = wfw_all['Address']
wfw_all['Address Line #2'] = wfw_all['Dual Address']
wfw_all['City'] = wfw_all['City_y']
wfw_all['Postal Code'] = (wfw_all['Zip Code_y'])
wfw_all['Work From Home Address'] = 'N'
wfw_all['Region'] = 'USA-' + wfw_all['State_y'].astype(str)


wfw_work = wfw_all[['Worker ID','Source System','Primary','Usage Type','Public','Country ISO Code','Address Line #1','Address Line #2','City','Postal Code','Work From Home Address','Region']]

worker_address = pd.concat([wfh_f,wfw_f])

worker_address['Address Effective Date']=''
worker_address['Address Line #3']=''
worker_address['Address Line #4']=''
worker_address['Address Line #5']=''
worker_address['Address Line #6']=''
worker_address['Address Line #7']=''
worker_address['Address Line #8']=''
worker_address['Address Line #9']=''
worker_address['City Subdivision 1']=''
worker_address['City Subdivision 2']=''

worker_address['Region Subdivision 1']=''
worker_address['Region Subdivision 2']=''
worker_address['Use For Reference 1']=''
worker_address['Use For Reference 2']=''
worker_address['Address Line #1 - Local']=''
worker_address['Address Line #2 - Local']=''
worker_address['Address Line #3 - Local']=''
worker_address['Address Line #4 - Local']=''
worker_address['Address Line #5 - Local']=''
worker_address['Address Line #6 - Local']=''
worker_address['Address Line #7 - Local']=''
worker_address['Address Line #8 - Local']=''
worker_address['Address Line #9 - Local']=''
worker_address['City - Local']=''
worker_address['City Subdivision 1 - Local']=''
worker_address['City Subdivision 2 - Local']=''
worker_address['Region Subdivision 1 - Local']=''
worker_address['Region Subdivision 2 - Local']=''


worker_address = worker_address[['Worker ID',
'Source System',
'Address Effective Date',
'Primary',
'Usage Type',
'Public',
'Country ISO Code',
'Address Line #1',
'Address Line #2',
'Address Line #3',
'Address Line #4',
'Address Line #5',
'Address Line #6',
'Address Line #7',
'Address Line #8',
'Address Line #9',
'City',
'City Subdivision 1',
'City Subdivision 2',
'Region',
'Region Subdivision 1',
'Region Subdivision 2',
'Postal Code',
'Use For Reference 1',
'Use For Reference 2',
'Work From Home Address',
'Address Line #1 - Local',
'Address Line #2 - Local',
'Address Line #3 - Local',
'Address Line #4 - Local',
'Address Line #5 - Local',
'Address Line #6 - Local',
'Address Line #7 - Local',
'Address Line #8 - Local',
'Address Line #9 - Local',
'City - Local',
'City Subdivision 1 - Local',
'City Subdivision 2 - Local',
'Region Subdivision 1 - Local',
'Region Subdivision 2 - Local']]


os.chdir(path)
worker_address.to_csv('E2E_worker_address_v2.txt', sep='|', encoding='utf-8', index=False)


##############################################################################
########################## Worker Phone Numbers ##############################
##############################################################################

"""
Worker ID
Source System
Type
Primary
Public
Country ISO Code
Phone Number
Phone Extension
Phone Device Type
"""

#grab everyone from contact sheet

os.chdir(r'C:\Users\akaff\KBP Investments')

list_of_files = glob.glob('./KBP Contact Sheet - Current/*.xlsx')
contacts = pd.read_excel(latest_file, sheet_name='KBP Contacts')

#remove non work people
contacts_df = contacts.dropna(subset = ['Kronos EmpID'])
contacts_df = contacts_df.dropna(subset = ['Phone Number'])

contacts_df['Worker ID'] = contacts_df['Kronos EmpID']
contacts_df['Source System'] = 'Contact Sheet'
contacts_df['Type'] = 'Work'
contacts_df['Primary'] = 'Y'
contacts_df['Public'] = 'N'
contacts_df['Country ISO Code'] = 'USA'
contacts_df['Phone Extension'] = contacts_df['Extension']
contacts_df['Phone Device Type'] = ''
contacts_df['Phone Device Type'] = np.where(contacts_df['Extension']>=1, 'Landline','Mobile')

contacts_df = contacts_df[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]

contact_sheet_ids = contacts_df['Worker ID']

contact_df2 = active_ees[active_ees['Employee Id'].isin(contact_sheet_ids) == False]

contact_df2['Phone Number'] = np.where(contact_df2['Account Contact #1: Cell Phone'].isna(),contact_df2['Account Contact #1: Home Phone'],contact_df2['Account Contact #1: Cell Phone'])


contact_df2_wp = contact_df2
contact_df2_wp['Phone Number'] = contact_df2_wp['Account Contact #1: Work Phone']
contact_df2_wp['Type'] = 'Work'
contact_df2_wp['Primary'] = 'Y'
contact_df2_wp['Public'] = 'N'
contact_df2_wp = contact_df2_wp.dropna(subset = ['Phone Number'])
contact_df2_wp['Phone Extension'] = ''
contact_df2_wp['Phone Device Type'] = 'Mobile'
contact_df2_wp = contact_df2_wp[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]


contact_df2['Type'] = 'Home'
contact_df2['Primary'] = ''
contact_df2['Public'] = 'N'
contact_df2['Phone Extension'] = ''
contact_df2['Phone Device Type'] = 'Mobile'

contact_df2 = contact_df2[['Worker ID','Source System','Type','Primary','Public','Country ISO Code','Phone Number','Phone Extension','Phone Device Type']]



worker_phone = pd.concat([contacts_df,contact_df2,contact_df2_wp])

worker_phone['International Phone Code'] = ''
worker_phone['Area Code'] = ''
worker_phone['Worker ID'] = worker_phone['Worker ID'].astype(int)
worker_phone['Phone Extension'] = ''
worker_phone = worker_phone[['Worker ID',
'Source System',
'Type',
'Primary',
'Public',
'Country ISO Code',
'International Phone Code',
'Area Code',
'Phone Number',
'Phone Extension',
'Phone Device Type']]

#worker_phone = pd.read_csv(r'worker_phone.txt')

os.chdir(path)
worker_phone.to_csv('worker_phone.txt', sep='|', encoding='utf-8', index=False)

##############################################################################
######################## Supervisory Organizations ###########################
##############################################################################

"""
Manager Employee ID
Supervisory Organization ID
Job Management
Position Management
Supervisory Org StubType
Location
Superior Organization ID


Job Man ARLs
Pos Man Store Level
Dept Home office
Team Field


"""
df = get_rpt('71877396')
fins = pd.read_excel('.\..\FINs Docs\Copy of FIN_-_Extract_Locations (1).xlsx', skiprows=(5))
df2 = df.copy()
dd = df[['Employee Id','Employee EIN','Cost Centers(Company Code)']]

df2['Supervisory Organization ID'] = df2['Direct Supervisor Employee Id']
df2['Superior Organization ID'] = df2['Indirect Supervisor Employee Id']


sup_org = df2[['Supervisory Organization ID','Direct Supervisor Employee Id','Superior Organization ID']]
sup_org.drop_duplicates(inplace=True)


xx = sup_org.merge(df, left_on='Direct Supervisor Employee Id',right_on='Employee Id')



xx['Supervisory Organization ID'] = np.where(xx['Employee EIN'] == 'Restaurant Services Group',"RSG_" + xx['Supervisory Organization ID'].astype(str)
,xx['Cost Centers(Company Code)'].astype(str)+ "_" + xx['Supervisory Organization ID'].astype(str))

xx.drop(columns=['Superior Organization ID'],inplace=True)
xx.drop_duplicates(inplace=True)
xx = xx.merge(dd,left_on='Direct Supervisor Employee Id_y',right_on='Employee Id')
xx['Direct Sup EIN'] = xx['Employee EIN_y']
xx['Direct Sup CC'] =  xx['Cost Centers(Company Code)_y']

xx['Superior Organization ID'] = np.where(xx['Direct Sup EIN'] == 'Restaurant Services Group',"RSG_" + xx['Direct Supervisor Employee Id_y'].astype(str)
,xx['Direct Sup CC'].astype(str)+ "_" + xx['Direct Supervisor Employee Id_y'].astype(str))


xx.to_csv('supervisory_organizations_test2.csv',index=False)


xx['Job Management'] = 'N'
xx['Position Management'] = 'Y'
xx['Supervisory Org SubType'] = np.where(xx['Employee EIN_x'] == 'Restaurant Services Group','Department','Team')
xx['Manager Employee ID'] = xx['Direct Supervisor Employee Id_x']
sup_org_df = xx.merge(fins, left_on='Default Cost Centers', right_on='Cost Center', how='left')
sup_org_df['Location'] = sup_org_df['Reference ID']
sup_org_df['state_full'] = sup_org_df['Default Cost Centers Full Path'].str.split('/').str[1]
sup_org_df['Location'] = np.where(sup_org_df['Default Location'] == 'Home Office','LC_Home Office',
                         np.where(sup_org_df['Default Location'] == 'Work From Home','LC_State '+ sup_org_df['state_full'], "LC"+(sup_org_df['Cost Center'].astype(str)).str.zfill(5)))

sup_org_df = sup_org_df[['Manager Employee ID','Supervisory Organization ID','Job Management','Position Management','Supervisory Org SubType','Superior Organization ID','Location','state_full','Location(1)']]

sup_org_df['Supervisory Organization ID'] = sup_org_df['Supervisory Organization ID'].str[:-2]
sup_org_df['Superior Organization ID'] = sup_org_df['Superior Organization ID'].str[:-2]
sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].astype(str)
sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].str[:-2]


sup_org_df = sup_org_df[['Manager Employee ID','Supervisory Organization ID','Job Management','Position Management','Supervisory Org SubType','Location','Superior Organization ID']]
#sup_org_df.Location.fillna('Default Location', inplace=True)


sup_org_df.drop_duplicates(inplace=True)

sup_org_df['Source System']=''
sup_org_df['Supervisory Organization Name']=''
sup_org_df['Supervisory Organization Code']=''



sup_org_df = sup_org_df[['Manager Employee ID',
'Source System',
'Supervisory Organization ID',
'Supervisory Organization Name',
'Supervisory Organization Code',
'Job Management',
'Position Management',
'Supervisory Org SubType',
'Location',
'Superior Organization ID']]





os.chdir(path)
#sup_org_df.to_csv('supervisory_organizations_test.txt',index=False)
sup_org_df.to_csv('supervisory_organizations.txt',sep='|', encoding='utf-8', index=False)

sup_org_df = pd.read_csv(r"C:/Users/akaff/OneDrive - KBP Investments/Workday Implentation/Data Conversion/data files/E2E_sup_org_v2.txt",sep="\t")



##############################################################################
############################ EMP-Position Mgt ################################
##############################################################################

"""
Employee ID
Position ID
Employee Type
Hire Date
Supervisdory Organization ID
Job Posting Title
Job Code
Work Location
Default Weekly Hours
Scheduled Weekly Hours
Time Type
Pay Rate Type
"""
df = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\71877396.csv',encoding='cp1251')

cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
df['Employee Id'] = df['Employee Id'].astype(str)
df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    
df['Date Hired'] = pd.to_datetime(df['Date Hired'])
state = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\state_info.csv')
df = df.merge(state, left_on='State',right_on='State Abbreviation')
#df = df[df['Date Hired'] < '01/21/2023']
#df = df.merge(fins, left_on='Default Cost Centers', right_on='Cost Center', how='left')
#df['state_full'] = df['Default Cost Centers Full Path'].str.split('/').str[1]
#df['Default Cost Centers'] = df['Default Cost Centers'].astype(int)
df['Work Location'] = np.where(df['Location(1)'] == 'Home Office','LC10000',
                         np.where(df['Location(1)'] == 'Work From Home','LC_State '+ df['State Name'], "LC"+(df['Default Cost Centers'].astype(str)).str.zfill(5)))



df['Employee ID'] = df['Employee Id']

df['Position ID'] = 'P-' + df['Employee ID'].astype(str)
df['Default Jobs (HR) External Id'] = df['Default Jobs (HR) External Id'].astype(int)
df['Job Code'] = df['Default Jobs (HR) External Id'].astype(str).str.zfill(4)
df['Job Code'] = df['Job Code'].astype(str).str.zfill(4)
df['Hire Date'] = df['Date Hired']
df['Job Posting Title (for the Position)'] = df['Default Jobs (HR)']

#Fix locations

df['Default Weekly Hours'] = np.where(df['Default Jobs (HR)'] == 'Hrly Co Manager',50,40)
df['Scheduled Weekly Hours'] = df['Default Weekly Hours'] 
df['Time Type'] = np.where(df['Default Jobs (HR)'] == 'Team Member', 'Part_Time','Full_Time')
df['Pay Rate Type'] = df['Pay Type']
df['Employee Type2'] = np.where(df['Time Type'] =='Part_Time','Regular Part Time','Regular Full Time')
df['Employee Type2'] = np.where(df['Employee Type'] == 'Member','Member',df['Employee Type2'])
df['Employee Type'] = df['Employee Type2']

df['Direct Supervisor Employee Id'] = df['Direct Supervisor Employee Id'].astype(str)
df['Direct Supervisor Employee Id'] = df['Direct Supervisor Employee Id'].str[:-2]
#sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].astype(int)
#sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].astype(str)
sup_org_df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\moved.20230425-125845.E2E_sup_org_v2.txt",sep='|')
sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].astype(str)
sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].str[:-2]
df = df.merge(sup_org_df, left_on='Direct Supervisor Employee Id', right_on='Manager Employee ID', how='left')

df_sup = df

df_pm = df[['Employee ID',
'Position ID',
'Employee Type',
'Hire Date',
'Supervisory Organization ID',
'Job Posting Title (for the Position)',
'Job Code',
'Work Location',
'Default Weekly Hours',
'Scheduled Weekly Hours',
'Time Type',
'Pay Rate Type']]

ee_ids = ['100069',	'10051',	'101964',	'103441',	'104418',	'1047',	'105171',	'105960',	'106120',	'106510',	'106693',	'107877',	'109354',	'110122',	'111261',	'111273',	'111628',	'114508',	'116141',	'116236',	'117372',	'1190',	'120984',	'122136',	'122788',	'123227',	'124651',	'124943',	'125050',	'125793',	'1285',	'129386',	'130414',	'131130',	'131194',	'131713',	'131714',	'132489',	'132704',	'133179',	'133232',	'133235',	'133360',	'133504',	'133505',	'133506',	'133507',	'133508',	'133509',	'133510',	'133553',	'133572',	'135074',	'135227',	'135228',	'135604',	'135630',	'135859',	'136091',	'137833',	'137958',	'137959',	'137960',	'137962',	'137964',	'137965',	'138359',	'140080',	'141355',	'141430',	'143389',	'14483',	'14487',	'14786',	'14793',	'148550',	'14856',	'14869',	'14871',	'14875',	'14881',	'14889',	'149708',	'150025',	'15035',	'15039',	'150444',	'150445',	'150947',	'151112',	'151589',	'15159',	'15161',	'151796',	'151827',	'151850',	'152640',	'152656',	'153422',	'153424',	'15345',	'15346',	'15347',	'15348',	'15350',	'15355',	'15356',	'15358',	'154095',	'155723',	'155739',	'156815',	'156816',	'158148',	'158855',	'16027',	'16230',	'16231',	'16232',	'16233',	'16238',	'16239',	'16241',	'16246',	'16247',	'16248',	'16249',	'16360',	'16397',	'164159',	'16421',	'167989',	'16822',	'169075',	'171833',	'171916',	'172217',	'17256',	'176579',	'176643',	'177272',	'177695',	'178279',	'178316',	'178864',	'178865',	'178910',	'179112',	'179815',	'180510',	'180513',	'180516',	'18307',	'183406',	'22115',	'22116',	'22157',	'22418',	'22420',	'22430',	'22474',	'22477',	'22480',	'22488',	'22501',	'22503',	'22504',	'22507',	'22791',	'22793',	'22794',	'22977',	'22979',	'22987',	'22988',	'22991',	'23863',	'23864',	'23867',	'23868',	'23870',	'23871',	'23872',	'23875',	'23879',	'23880',	'24387',	'24822',	'25505',	'29063',	'29745',	'29747',	'29789',	'30060',	'30101',	'30106',	'30120',	'30135',	'30136',	'30422',	'30428',	'30609',	'30611',	'30613',	'30616',	'30619',	'30620',	'30633',	'31493',	'31494',	'31495',	'31496',	'31501',	'31502',	'31503',	'31505',	'31506',	'31507',	'31509',	'31566',	'31567',	'31570',	'31572',	'31574',	'31575',	'31576',	'31577',	'31578',	'31700',	'31785',	'43820',	'43934',	'46892',	'47222',	'48871',	'49374',	'6517',	'6849',	'6858',	'7155',	'7162',	'7217',	'7223',	'7235',	'7240',	'7241',	'7249',	'7714',	'7718',	'7724',	'7726',	'78476',	'78608',	'78617',	'78819',	'85129',	'8587',	'8598',	'8600',	'8601',	'8602',	'8607',	'8609',	'8610',	'8613',	'8616',	'8618',	'8635',	'86772',	'88236',	'88237',	'88238',	'8965',	'90782',	'9210',	'94035',	'9588',	'96603',	'96738',	'97461',	'97519',	'97521',	'97522',	'97547',	'97892',	'99642',
]

mask = df_pm['Employee ID'].isin(ee_ids)
df_pm = df_pm.loc[~mask]

df_pm['Job Requisition ID'] = ''
df_pm['Source System'] = ''
df_pm['Hire Reason']=''
df_pm['First Day of Work']=''
df_pm['Probation Start Date']=''
df_pm['Probation End Date']=''
df_pm['End Employment Date']=''
df_pm['Position Start Date for Conversion']=''
df_pm['Job Profile Start Date for Conversion']=''

df_pm['Position Title']=''
df_pm['Business Title']=''
df_pm['Work Space']=''
df_pm['Paid FTE']=''
df_pm['Working FTE']=''
df_pm['Company Insider Type']=''
df_pm['Company Insider Type #2']=''
df_pm['Company Insider Type #3']=''
df_pm['Company Insider Type #4']=''
df_pm['Company Insider Type #5']=''
df_pm['Work Shift']=''
df_pm['Additional Job Classification #1']=''
df_pm['Additional Job Classification #2']=''
df_pm['Additional Job Classification #3']=''
df_pm['Additional Job Classification #4']=''
df_pm['Additional Job Classification #5']=''
df_pm['Additional Job Classification #6']=''
df_pm['Workers Compensation Code']=''



df_pm = df_pm[['Employee ID',
'Source System',
'Position ID',
'Job Requisition ID',
'Employee Type',
'Hire Reason',
'First Day of Work',
'Hire Date',
'Probation Start Date',
'Probation End Date',
'End Employment Date',
'Position Start Date for Conversion',
'Supervisory Organization ID',
'Job Posting Title (for the Position)',
'Job Code',
'Job Profile Start Date for Conversion',
'Position Title',
'Business Title',
'Work Location',
'Work Space',
'Default Weekly Hours',
'Scheduled Weekly Hours',
'Paid FTE',
'Working FTE',
'Time Type',
'Pay Rate Type',
'Company Insider Type',
'Company Insider Type #2',
'Company Insider Type #3',
'Company Insider Type #4',
'Company Insider Type #5',
'Work Shift',
'Additional Job Classification #1',
'Additional Job Classification #2',
'Additional Job Classification #3',
'Additional Job Classification #4',
'Additional Job Classification #5',
'Additional Job Classification #6',
'Workers Compensation Code']]




os.chdir(path)
df_pm.to_csv('emp_position_mgt.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
############################### Change Job ###################################
##############################################################################

"""
Worker ID
Sequence #
Effective Date
Reason
Position ID
Employee/CW Type

"""
ee_ids = [
'177695',
'179699',
'179813',
'179820',
'181122',
'181638',
'181673',
'181808',
'183257',
'183663',
'185202',
'185532',
'185908',
'186050',
'186978',
'187046',
'187116',
'187139',
'187385',
'187824',
'188354',
'189058',
'189067',
'190196',
'190473',
'190676',
'190860',
'192368',
'192569',
'192711',
'1928',
'192966',
'195324',
'22115',
'22184',
'22504',
'23051',
'23863',
'23872',
'24112',
'24852',
'25108',
'26929',
'27160',
'31602',
'31785',
'31911',
'36210',
'49374',
'49881',
'76999',
'77470',
'77488',
'80826',
'83894',
'8587',
'100127',
'103227',
'103441',
'105703',
'109600',
'112863',
'115276',
'115577',
'116493',
'119241',
'121441',
'122700',
'125818',
'126762',
'131130',
'133209',
'133225',
'133238',
'133508',
'13554',
'136770',
'137452',
'137578',
'139103',
'139529',
'140913',
'143270',
'145096',
'150445',
'151810',
'151876',
'152334',
'152418',
'153422',
'154095',
'155739',
'157576',
'161344',
'164001',
'164834',
'16750',
'169567',
'169867',
'170434',
'171695',
'172302',
'173137',
'176086',
'176536',
'86341',
'9005',
'93210',
'93333',
'9343',
'95213',
'97519',
'98100',
'9847']



df['Worker ID'] = df['Employee ID']
df['Sequence #'] = '1'
df['Effective Date'] = df['Date Hired']
df['Reason'] = 'Default_Change_Job_Conversion'
df['Employee/CW Type']=df['Employee Type']

df_cj = df[['Worker ID','Source System','Sequence #','Effective Date','Reason','Supervisory Organization ID','Position ID','Employee/CW Type','Job Code','Work Location','Default Weekly Hours',
'Scheduled Weekly Hours',
'Time Type',
'Pay Rate Type']]

df_cj = df_cj[df_cj['Worker ID'].isin(ee_ids)]


df_cj['Employee/CW Type'] = np.where(df_cj['Employee/CW Type'] == 'Exempt','Regular Full Time',df_cj['Employee/CW Type'])



df_cj['End Employment Date']=''
df_cj['Job Posting Title (for the Position)']=''
df_cj['Position Title']=''
df_cj['Business Title']=''
df_cj['Work Space']=''
df_cj['Company Insider Type']=''
df_cj['Work Shift']=''
df_cj['Additional Job Classification #1']=''
df_cj['Additional Job Classification #2']=''
df_cj['Additional Job Classification #3']=''
df_cj['Additional Job Classification #4']=''
df_cj['Paid FTE'] = ''
df_cj['Working FTE'] = ''


df_cj = df_cj[['Worker ID',
'Source System',
'Sequence #',
'Effective Date',
'Reason',
'Supervisory Organization ID',
'Position ID',
'Employee/CW Type',
'End Employment Date',
'Job Posting Title (for the Position)',
'Job Code',
'Position Title',
'Business Title',
'Work Location',
'Work Space',
'Default Weekly Hours',
'Scheduled Weekly Hours',
'Paid FTE',
'Working FTE',
'Time Type',
'Pay Rate Type',
'Company Insider Type',
'Work Shift',
'Additional Job Classification #1',
'Additional Job Classification #2',
'Additional Job Classification #3',
'Additional Job Classification #4']]

df_cj['Supervisory Organization ID'] = df_cj['Supervisory Organization ID'] .fillna('SUP_30120')


write_to_csv(df_cj, 'change_job.txt')


###Fix sup org
df_cj = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\E2E_change_job_v2.csv")


##############################################################################
######################## Organization Assignments ############################
##############################################################################

"""
Worker ID
Cost Center Organization
Company Organziation

"""

#df = get_rpt('71877396')
#df['Date Hired'] = pd.to_datetime(df['Date Hired'])
#df = df[df['Date Hired'] < '01/21/2023']

df['Worker ID'] = df['Employee Id']
df['Cost Center Organization'] = "CC" + (df['Default Cost Centers'].astype(str)).str.zfill(5)
df['Company Organization'] = df['Cost Centers(Company Code)']
df['Source System'] = ''
df['Position ID'] = ''
df['Effective Date'] = ''
df['Region Organization']=''
df['Business Unit'] = ''
df['Custom Org #1'] = ''
df['Custom Org #2'] = ''
df['Custom Org #3'] = ''
df['Custom Org #4'] = ''
df['Custom Org #5'] = ''
df['Custom Org #6'] = ''
df['Custom Org #7'] = ''
df['Custom Org #8'] = ''
df['Custom Org #9'] = ''


df_orga= df[['Worker ID',
'Source System',
'Position ID',
'Effective Date',
'Cost Center Organization',
'Company Organization',
'Region Organization',
'Business Unit',
'Custom Org #1',
'Custom Org #2',
'Custom Org #3',
'Custom Org #4',
'Custom Org #5',
'Custom Org #6',
'Custom Org #7',
'Custom Org #8',
'Custom Org #9']]


df_orga['Company Organization'] = np.where(df_orga['Company Organization'] == 'FQSR','FQ',df_orga['Company Organization'])



os.chdir(path)
df_orga.to_csv('organization_assignments_v3.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
######################## EMP-System User Accounts ############################
##############################################################################

"""
Employee ID
User Name

"""

df['Employee ID'] = df['Employee Id']
df['User Name'] = df['Username']

df['Source System']=''
df['Password']=''
df['Require New Password at Next Sign In?']=''
df['Exempt from Delegated Authentication']=''
df['OpenID Connect Internal Identifier']=''
df['User Language']=''



df_sys_accounts = df[['Employee ID',
'Source System',
'User Name',
'Password',
'Require New Password at Next Sign In?',
'Exempt from Delegated Authentication',
'OpenID Connect Internal Identifier',
'User Language'
]]

os.chdir(path)
df_sys_accounts.to_csv('emp_sys_user_accounts.txt',sep='|', encoding='utf-8', index=False)




##############################################################################
######################### EMP-Base Compensation ##############################
##############################################################################

"""
Employee ID
Compensation Change Reason
Effective Date
Compensation Package
Compensation Grade
"""
comp_grades = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\comp_grades.csv",encoding="cp1251")
df['Employee Pay Grade'] = np.where(df['Default Jobs (HR)'] == 'Team Member','Team Member',
                           np.where(df['Default Jobs (HR)'] == 'Shift Manager','Shift Manager',
                                    np.where(df['Default Jobs (HR)'] == 'Hrly Co Manager','Co Manager',
                                             np.where(df['Default Jobs (HR)'] == 'Restaurant General Manager', 'General Manager',
                                                      np.where(df['Default Jobs (HR)'] == 'Managing Partner','Managing Partner',df['Employee Pay Grade'])))))



df['Compensation Change Reason'] = 'Data Change'
df['Effective Date'] = '1/20/2023'
df['Compensation Package'] = 'KBP Compensation Package'
df['Employee ID'] = df['Employee Id']
df['Source System'] = ''
df['Sequence #'] = ''
df['Position ID'] =''
df['Compensation Grade Profile']=''
df['Compensation Step']=''
df['Progression Start Date']=''
df['Primary Compensation Basis']=''
df['Primary Compensation Basis Amount Change']=''
df['Primary Compensation Basis Percent Change']=''
df['Compensation Plan - Base']=''
df['Compensation Element Amount - Base']=''
df['Currency Code - Base']=''
df['Frequency - Base']=''
df['Compensation Plan - Addl']=''
df['Compensation Element Amount - Addl']=''
df['Currency Code - Addl']=''
df['Frequency - Addl']=''
df['Unit Salary Plan']=''
df['Per Unit Amount - Unit Salary']=''
df['Currency Code - Unit Salary']=''
df['Number of Units - Unit Salary']=''
df['Frequency - Unit Salary']=''
df['Commission Plan #1']=''
df['Target Amount - Commission Plan #1']=''
df['Currency Code - Commission Plan #1']=''
df['Frequency - Commission Plan #1']=''
df['Draw Amount - Commission Plan #1']=''
df['Frequency for Draw Amount - Commission Plan #1']=''
df['Draw Duration - Commission Plan #1']=''
df['Recoverable - Commission Plan #1']=''
df['Commission Plan #2']=''
df['Target Amount - Commission Plan #2']=''
df['Currency Code - Commission Plan #2']=''
df['Frequency - Commission Plan #2']=''
df['Draw Amount - Commission Plan #2']=''
df['Frequency for Draw Amount - Commission Plan #2']=''
df['Draw Duration - Commission Plan #2']=''
df['Recoverable - Commission Plan #2']=''

#df.to_csv(r'test.csv')

df2 = df.merge(comp_grades, left_on='Employee Pay Grade',right_on='Grade/Profile Name')
df2['Compensation Grade'] = df2['Grade/Profile ID']

df2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\test.csv")

df_emp_base_comp = df2[['Employee ID',
'Source System',
'Sequence #',
'Compensation Change Reason',
'Position ID',
'Effective Date',
'Compensation Package',
'Compensation Grade',
'Compensation Grade Profile',
'Compensation Step',
'Progression Start Date',
'Primary Compensation Basis',
'Primary Compensation Basis Amount Change',
'Primary Compensation Basis Percent Change',
'Compensation Plan - Base',
'Compensation Element Amount - Base',
'Currency Code - Base',
'Frequency - Base',
'Compensation Plan - Addl',
'Compensation Element Amount - Addl',
'Currency Code - Addl',
'Frequency - Addl',
'Unit Salary Plan',
'Per Unit Amount - Unit Salary',
'Currency Code - Unit Salary',
'Number of Units - Unit Salary',
'Frequency - Unit Salary',
'Commission Plan #1',
'Target Amount - Commission Plan #1',
'Currency Code - Commission Plan #1',
'Frequency - Commission Plan #1',
'Draw Amount - Commission Plan #1',
'Frequency for Draw Amount - Commission Plan #1',
'Draw Duration - Commission Plan #1',
'Recoverable - Commission Plan #1',
'Commission Plan #2',
'Target Amount - Commission Plan #2',
'Currency Code - Commission Plan #2',
'Frequency - Commission Plan #2',
'Draw Amount - Commission Plan #2',
'Frequency for Draw Amount - Commission Plan #2',
'Draw Duration - Commission Plan #2',
'Recoverable - Commission Plan #2']]


#df_emp_base_comp = df[['Employee ID','Compensation Change Reason','Effective Date','Compensation Package','Compensation Grade']]

os.chdir(path)
#df_emp_base_comp.to_csv('emp_base_comp.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
biographic_data = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\templates\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx", sheet_name='Biographic Data')
demographic_data = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\templates\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx", sheet_name='Demographic Data')
govt_ids = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Worker Government IDs')
job_mgt = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='EMP-Job Mgt')
service_dates = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Service Dates')
job_hist_prev_sys = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Job History Prev Sys')
emp_terms = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='EMP-Terminations')
emerg_cont = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Emergency Contacts')
former_worker = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Former Worker')



##############################################################################
########################### biographic_data ##################################
##############################################################################


biographic_data_c = biographic_data.filter(regex='Required')
bio_data = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\templates\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx", sheet_name='Biographic Data',skiprows=2)

bio_data_c = bio_data.iloc[0:0]

bio_data_c['Worker ID'] = active_ees['Employee Id']

bio_merge = active_ees[['Employee Id','Date Birthday']]
bio_merge.rename(columns={"Employee Id": "Worker ID", "Date Birthday": "Date of Birth"},inplace=True)
bio_data_c = bio_data_c.drop(['Date of Birth'],axis=1)
bio_data_c2 = bio_data_c.merge(bio_merge)
bio_data_c2['Source System'] = 'Kronos'
bio_data_c2['Date of Birth'] = pd.to_datetime(bio_data_c2['Date of Birth'])
bio_data_c2['Date of Birth'] = bio_data_c2['Date of Birth'].dt.strftime("%d-%b-%Y").str.upper()

biographic_data = bio_data_c2[['Worker ID', 'Source System', 'Date of Birth', 'Country of Birth',
       'Region of Birth', 'City of Birth', 'Date of Death', 'Gender',
       'Last Medical Exam Date', 'Last Medical Exam Valid To',
       'Medical Exam Notes', 'Disability #1', 'Disability Status Date #1',
       'Disability Date Known #1', 'Disability End Date #1',
       'Disability Grade #1', 'Disability Degree #1',
       'Disability Remaining Capacity #1',
       'Disability Certification Authority #1', 'Disability Certified At #1',
       'Disability Certification ID #1', 'Disability Certification Basis #1',
       'Disability Severity Recognition Date #1',
       'Disability FTE Toward Quota #1', 'Disability Work Restrictions #1',
       'Disability Accommodations Requested #1',
       'Disability Accommodations Provided #1',
       'Disability Rehabilitation Requested #1',
       'Disability Rehabilitation Provided #1', 'Note #1', 'Disability #2',
       'Disability Status Date #2', 'Disability Date Known #2',
       'Disability End Date #2', 'Disability Grade #2', 'Disability Degree #2',
       'Disability Remaining Capacity #2',
       'Disability Certification Authority #2', 'Disability Certified At #2',
       'Disability Certification ID #2', 'Disability Certification Basis #2',
       'Disability Severity Recognition Date #2',
       'Disability FTE Toward Quota #2', 'Disability Work Restrictions #2',
       'Disability Accommodations Requested #2',
       'Disability Accommodations Provided #2',
       'Disability Rehabilitation Requested #2',
       'Disability Rehabilitation Provided #2', 'Note #2',
       'Tobacco User Status', 'Blood Type', 'Sexual Orientation',
       'Gender Identity', 'Pronoun', 'LGBT Identification #1',
       'LGBT Identification #2', 'Relative Type #1',
       'Country ISO Code-Relative Name #1', 'Prefix Data - Title Reference #1',
       'Prefix Data - Salutation Reference #1', 'First Name #1',
       'Middle Name #1', 'Last Name #1', 'Secondary Last Name #1',
       'Social Suffix #1', 'Academic Suffix #1', 'Hereditary Suffix #1',
       'Honorary Suffix #1', 'Professional Suffix #1', 'Religious Suffix #1',
       'Royal Suffix #1', 'Relative Type #2',
       'Country ISO Code-Relative Name #2', 'Prefix Data - Title Reference #2',
       'Prefix Data - Salutation Reference #2', 'First Name #2',
       'Middle Name #2', 'Last Name #2', 'Secondary Last Name #2',
       'Social Suffix #2', 'Academic Suffix #2', 'Hereditary Suffix #2',
       'Honorary Suffix #2', 'Professional Suffix #2', 'Religious Suffix #2',
       'Royal Suffix #2']]



os.chdir(path)
biographic_data.to_csv('biographic_data.txt',sep='|', encoding='utf-8', index=False)

df.to_csv('./../comp_df.csv')
##############################################################################
########################### demographic_data ##################################
##############################################################################



demographic_data = demographic_data.filter(regex='Required')
demo_data = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\templates\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx", sheet_name='Demographic Data',skiprows=2)

demo_data_c = demo_data.iloc[0:0]
    
active_ees['Ethnicity #1'] = np.where(active_ees['Ethnicity']== 'White (not Hispanic or Latino)', 'White_United_States_of_America',
              np.where(active_ees['Ethnicity']== 'Black or African American (not Hispanic or Latino)', 'Black_or_African_American_United_States_of_America',
              np.where(active_ees['Ethnicity']== 'American Indian or Alaska Native (not Hispanic or Latino)', 'American_Indian_or_Alaska_Native_United_States_of_America',
              np.where(active_ees['Ethnicity']== 'Native Hawaiian or Other Pacific Islander (not Hispanic or Latino)', 'Native_Hawaiian_or_Other_Pacific_Islander_United_States_of_America',
              np.where(active_ees['Ethnicity']== 'Asian (not Hispanic or Latino)', 'Asian_United_States_of_America',
              np.where(active_ees['Ethnicity']== 'Two or More Races (not Hispanic or Latino)', 'Two_or_More_Races_United_States_of_America',
              np.where(active_ees['Ethnicity']== 'Hispanic or Latino', 'Hispanic_or_Latino_United_States_of_America', 'Not_Specified')))))))

active_ees['Hispanic or Latino'] = np.where(active_ees['Ethnicity #1']== 'Hispanic_or_Latino_United_States_of_America', 'Y','N')

active_ees['Marital Status'] = np.where(active_ees['Actual Marital Status'] == 'Divorced','Divorced_United_States_of_America',
                               np.where(active_ees['Actual Marital Status'] == 'Married','Married_United_States_of_America',
                                np.where(active_ees['Actual Marital Status'] == 'Partnered','Partnered_United_States_of_America',
                                np.where(active_ees['Actual Marital Status'] == 'Separated','Separated_United_States_of_America',
                                np.where(active_ees['Actual Marital Status'] == 'Widowed','Widowed_United_States_of_America','Single_United_States_of_America')))))

df_d = active_ees[['Worker ID','Source System','Marital Status','Ethnicity #1']]

demo_data_c['Worker ID'] = active_ees['Worker ID']
demo_data_c1 = demo_data_c.drop(['Source System','Marital Status','Ethnicity #1'],axis=1)

demo_data_c1 = demo_data_c1.merge(df_d, on='Worker ID')


dem_df = demo_data_c1[['Worker ID', 'Source System', 'Marital Status', 'Marital Status Date',
       'Hispanic or Latino', 'Ethnicity #1', 'Ethnicity #2', 'Ethnicity #3',
       'Ethnicity #4', 'Citizenship Status #1', 'Citizenship Status #2',
       'Citizenship Status #3', 'Citizenship Status #4', 'Primary Nationality',
       'Additional Nationality #1', 'Additional Nationality #2',
       'Additional Nationality #3', 'Additional Nationality #4',
       'Military Status #1', 'Military Service Type #1',
       'Military Discharge Date #1', 'Military Status #2',
       'Military Service Type #2', 'Military Discharge Date #2',
       'Military Status #3', 'Military Service Type #3',
       'Military Discharge Date #3', 'Military Status #4',
       'Military Service Type #4', 'Military Discharge Date #4']]

os.chdir(path)
dem_df.to_csv('demographic_data.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
########################### Government Ids  ##################################
##############################################################################
#govt_ids

govt_id_c = govt_ids.filter(regex='Required')
gov_id_df = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Worker Government IDs',skiprows=2)

gov_id_df = gov_id_df.iloc[0:0]

'''
'Worker ID'
'Country ISO Code'
'Type' = 'National'
'Workday ID Type' = 'USA-SSN'
'ID' = 'ssn'
'''

active_ees['Type'] = 'National'
active_ees['Workday ID Type'] = 'USA-SSN'
active_ees['Country ISO Code'] = 'USA'
active_ees['ID'] = active_ees['SS#'].replace('-', '', regex=True).astype(str)
gov_df = active_ees[['Worker ID','Country ISO Code','Type','Workday ID Type','ID']]
gov_id_df = gov_id_df.drop(['Country ISO Code','Type','Workday ID Type','ID'],axis=1)

gov_id_df['Worker ID'] = active_ees['Worker ID']

gov_id_df = gov_id_df.merge(gov_df, on='Worker ID')

gov_id_df = gov_id_df[['Worker ID', 'Source System', 'Country ISO Code', 'Type',
       'Workday ID Type', 'ID', 'Issued Date', 'Expiration Date',
       'Verification Date', 'Series - National ID',
       'Issuing Agency - National ID']]

os.chdir(path)
gov_id_df.to_csv('government_ids.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
########################### Job Management  ##################################
##############################################################################

#job_mgt
job_mgt_c = job_mgt.filter(regex='Required')
'''
Employee ID
Employee Type
Hire Date
Supervisory Organization ID
Job Code
Work Location
Default Weekly Hours
Scheduled Weekly Hours
Time Type
Pay Rate Type
'''

df2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\terms\employee_terms.csv", encoding=("cp1251"))
df2['Date Terminated'] = pd.to_datetime(df2['Date Terminated'])
df2 = df2[df2['Date Terminated']>'1/20/2021']
df2['Employee ID'] = df2['Employee Id']
df2['Source System'] = 'Kronos'
df2['Position ID'] = 'P-' + df2['Employee ID'].astype(str)
df2['Default Jobs (HR) External Id'] = df2['Default Jobs (HR) External Id'].fillna(0)
df2['Default Jobs (HR) External Id'] = df2['Default Jobs (HR) External Id'].astype(int)
df2['Job Code'] = df2['Default Jobs (HR) External Id'].astype(str).str.zfill(4)
df2['Job Code'] = df2['Job Code'].astype(str)
df2['Hire Date'] = pd.to_datetime(df2['Date Hired']).dt.strftime("%d-%b-%Y").str.upper()
df2['Job Posting Title (for the Position)'] = df2['Default Jobs (HR)']
df2['state_full'] = df2['Default Cost Centers Full Path'].str.split('/').str[1]
df2['Work Location'] = df2['Work Location'] = np.where(df2['Location(1)'] == 'Home Office','LC_Home Office',
                         np.where(df2['Location(1)'] == 'Work From Home','LC_State '+ df2['state_full'], "LC"+(df2['Location(1)'].astype(str)).str.zfill(5)))
                         
df2['Default Weekly Hours'] = np.where(df2['Default Jobs (HR)'] == 'Hrly Co Manager',50,40)
df2['Scheduled Weekly Hours'] = df2['Default Weekly Hours'] 
df2['Time Type'] = np.where(df2['Default Jobs (HR)'] == 'Team Member', 'Part_Time','Full_Time')
df2['Pay Rate Type'] = df2['Pay Type']
df2['Employee Type2'] = np.where(df2['Time Type'] =='Part_Time','Regular Part Time','Regular Full Time')
df2['Employee Type2'] = np.where(df2['Employee Type'] == 'Member','Member',df2['Employee Type2'])
df2['Employee Type'] = df2['Employee Type2']

df2['Supervisory Organization ID'] = 'SUP_Terminated Workers'

#df2_c = df2.merge(sup_org_df, left_on='Direct Supervisor Employee Id', right_on='Manager Employee ID', how='left')

df_jm = df2
df_jm['Job Requisition ID'] = ''
df_jm['Source System'] = ''
df_jm['Hire Reason']=''
df_jm['First Day of Work']=''
df_jm['Probation Start Date']=''
df_jm['Probation End Date']=''
df_jm['End Employment Date']=''
df_jm['Position Start Date for Conversion']=''
df_jm['Job Profile Start Date for Conversion']=''

df_jm['Position Title']=''
df_jm['Business Title']=''
df_jm['Work Space']=''
df_jm['Paid FTE']=''
df_jm['Working FTE']=''
df_jm['Company Insider Type']=''
df_jm['Company Insider Type #2']=''
df_jm['Company Insider Type #3']=''
df_jm['Company Insider Type #4']=''
df_jm['Company Insider Type #5']=''
df_jm['Work Shift']=''
df_jm['Additional Job Classification #1']=''
df_jm['Additional Job Classification #2']=''
df_jm['Additional Job Classification #3']=''
df_jm['Additional Job Classification #4']=''
df_jm['Additional Job Classification #5']=''
df_jm['Additional Job Classification #6']=''
df_jm['Workers Compensation Code']=''



df_jm = df_jm[['Employee ID',
'Source System',
'Employee Type',
'Hire Reason',
'First Day of Work',
'Hire Date',
'Probation Start Date',
'Probation End Date',
'End Employment Date',
'Position Start Date for Conversion',
'Supervisory Organization ID',
'Job Code',
'Job Profile Start Date for Conversion',
'Position Title',
'Business Title',
'Work Location',
'Work Space',
'Default Weekly Hours',
'Scheduled Weekly Hours',
'Paid FTE',
'Working FTE',
'Time Type',
'Pay Rate Type',
'Company Insider Type',
'Work Shift',
'Additional Job Classification #1',
'Additional Job Classification #2',
'Additional Job Classification #3',
'Additional Job Classification #4',
'Workers Compensation Code']]




os.chdir(path)
df_jm.to_csv('emp_job_mgt.txt',sep='|', encoding='utf-8', index=False)

df_jm = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\emp-job-man_fixes.csv",encoding="cp1251")
df_jm['Job Code'] = df_jm['Job Code'].astype(str).str.zfill(4)
df_jm['Job Code'] = df_jm['Job Code'].astype(str)
df_jm.to_csv('emp_job_mgt2.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
######################## Job History Prev Sys ################################
##############################################################################
#job_hist_prev_sys
#Job History Prev Sys


job_hist_prev_sys_c = job_hist_prev_sys.filter(regex='Required')
job_hist_prev_sys_df = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Job History Prev Sys',skiprows=2)

job_hist_prev_sys_df = job_hist_prev_sys_df.iloc[0:0]


##############################################################################
########################## EMP-Terminations ##################################
##############################################################################

#emp_terms
#EMP-Terminations
'''
Employee ID
Termination Date
Last Day of Work
Primary Reason - Term Categories Reason ID
'''
df2 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\terms\employee_terms.csv", encoding=("cp1251"))

emp_terms_c = emp_terms.filter(regex='Required')
emp_terms_df = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='EMP-Terminations',skiprows=2)

emp_terms_df = emp_terms_df.iloc[0:0]

emp_terms_df['Employee ID'] = df2['Employee Id']
df2['Employee ID'] = df2['Employee Id']
df2['Date Terminated'] = pd.to_datetime(df2['Date Terminated'])
df2['Termination Date'] = df2['Date Terminated']
df2['Last Day of Work'] = df2['Date Terminated']
df2['Primary Reason'] = 'Terminate_Employee_Administration_Remove_Worker'
df2 = df2[df2['Date Terminated']>'1/20/2021']

terms = df2[['Employee ID','Termination Date','Last Day of Work','Primary Reason']]
emp_terms_df = emp_terms_df.drop(['Termination Date','Last Day of Work','Primary Reason'],axis=1)
emp_terms_df = emp_terms_df.merge(terms, on='Employee ID')

emp_terms_df = emp_terms_df[['Employee ID', 'Source System', 'Termination Date', 'Last Day of Work',
       'Primary Reason', 'Secondary Reason', 'Local Termination Reason',
       'Pay Through Date', 'Resignation Date', 'Agreement Signature Date',
       'Dismissal Process Start Date', 'Notify Employee By Date',
       'Notice Period Start Date', 'Regrettable', 'Eligible for Rehire']]

os.chdir(path)
emp_terms_df.to_csv('emp_terminations.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
########################## Emergency Contact #################################
##############################################################################


'''
Worker ID
Emergency Contact ID
Related Person Relationship
Emergency Contact Priority
Country ISO Code

Phone Device Type - Primary Home
Country ISO Code - Primary Home
Phone Number - Primary Home

'''
#emerg_cont
emerg_cont_c = emerg_cont.filter(regex='Required')
emerg_cont_df = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Emergency Contacts',skiprows=2)

emerg_cont_df = emerg_cont_df.iloc[0:0]

active_ees['Phone Number - Primary Home'] = np.where(active_ees['Account Contact #1: Home Phone'].notnull(),active_ees['Account Contact #1: Home Phone'],active_ees['Account Contact #1: Cell Phone'])
active_ees['Phone Number - Primary Home'] = active_ees['Phone Number - Primary Home'].replace('-', '', regex=True).astype(str)
active_ees['Emergency Contact ID'] = (active_ees['Worker ID'].astype(str) + '-01').astype(str)
active_ees['Related Person Relationship'] = active_ees['Account Contact #1: Relationship']
active_ees['Related Person Relationship'] = active_ees['Related Person Relationship'].fillna('other')
active_ees['Emergency Contact Priority'] = '1'
active_ees['Phone Device Type - Primary Home'] = 'Mobile'
active_ees['Country ISO Code - Primary Home'] = 'USA'
active_ees['Legal Last Name'] = active_ees['Account Contact #1: Last Name']
active_ees['Legal First Name'] = active_ees['Account Contact #1: First Name']


emerg_cont_df['Worker ID'] = active_ees['Worker ID']

df2= active_ees[['Worker ID','Emergency Contact ID','Related Person Relationship','Emergency Contact Priority','Country ISO Code','Phone Device Type - Primary Home','Country ISO Code - Primary Home','Phone Number - Primary Home', 'Legal First Name','Legal Last Name']]

emerg_cont_df = emerg_cont_df.drop(['Emergency Contact ID','Related Person Relationship','Emergency Contact Priority','Country ISO Code','Phone Device Type - Primary Home','Country ISO Code - Primary Home','Phone Number - Primary Home','Legal First Name','Legal Last Name'],axis=1)

emerg_cont_df = emerg_cont_df.merge(df2, on='Worker ID')

emerg_cont_df = emerg_cont_df[['Worker ID', 'Source System', 'Emergency Contact ID',
       'Related Person Relationship', 'Emergency Contact Priority', 'Language',
       'Country ISO Code', 'Legal First Name', 'Legal Middle Name',
       'Legal Last Name', 'Name Prefix', 'Name Suffix',
       'Email Address - Primary Home', 'Email Address - Primary Work',
       'Email Address - Additional Home', 'Email Address - Additional Work',
       'Country ISO Code - Primary Home',
       'International Phone Code - Primary Home', 'Area Code - Primary Home',
       'Phone Number - Primary Home', 'Phone Extension - Primary Home',
       'Phone Device Type - Primary Home',
       'Country ISO Code - Additional Home #1',
       'International Phone Code - Additional Home #1',
       'Area Code - Additional Home #1', 'Phone Number - Additional Home #1',
       'Phone Extension - Additional Home #1',
       'Phone Device Type - Additional Home #1',
       'Country ISO Code - Additional Home #2',
       'International Phone Code - Additional Home #2',
       'Area Code - Additional Home #2', 'Phone Number - Additional Home #2',
       'Phone Extension - Additional Home #2',
       'Phone Device Type - Additional Home #2',
       'Country ISO Code - Primary Work',
       'International Phone Code - Primary Work', 'Area Code - Primary Work',
       'Phone Number - Primary Work', 'Phone Extension - Primary Work',
       'Phone Device Type - Primary Work',
       'Country ISO Code - Additional Work #1',
       'International Phone Code - Additional Work #1',
       'Area Code - Additional Work #1', 'Phone Number - Additional Work #1',
       'Phone Extension - Additional Work #1',
       'Phone Device Type - Additional Work #1',
       'Country ISO Code - Additional Work #2',
       'International Phone Code - Additional Work #2',
       'Area Code - Additional Work #2', 'Phone Number - Additional Work #2',
       'Phone Extension - Additional Work #2',
       'Phone Device Type - Additional Work #2', 'Country ISO Code - Home',
       'Address Line #1 - Home', 'Address Line #2 - Home', 'City - Home',
       'Region - Home', 'Region Subdivision - Home', 'Postal Code - Home',
       'Web Address', 'Usage Type - Web Address', 'Instant Messenger Address',
       'Instant Messenger Provider', 'Usage Type - Instant Messenger']]

os.chdir(path)
emerg_cont_df.to_csv('emergency_contacts.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
########################## Former Worker #################################
##############################################################################


#former_worker

former_worker_c = former_worker.filter(regex='Required')
former_worker_df = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Former Worker',skiprows=2)

former_worker_df = former_worker_df.iloc[0:0]


##############################################################################
#Final Output

active_ees.to_csv('./../active_ees.csv')



##############################################################################
########################## Performance Reviews #################################
##############################################################################

rev = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\talent_dc\perf_reviews.csv")
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\talent_dc')
rev.to_csv('E2E_perf_reviews_v2.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
########################## Custom Work IDS #################################
##############################################################################

df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\72604657.csv",encoding='cp1251')
df['Date Hired'] = pd.to_datetime(df['Date Hired'])
#df = df[df['Date Hired'] < '01/21/2023']
#df = df[df['Employee Status'] == 'Active']
cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\E2E_name_and_email_v2.txt', sep="|")
df['Employee Id'] = df['Employee Id'].astype(str)
df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]

df_ex = df[~pd.isnull(df['External Id'])]
df_rti_1 = df[~pd.isnull(df['RTI Employee #'])]
df_rti2 = df[~pd.isnull(df['RTI External Ref'])]
df_par = df[~pd.isnull(df['PAR ID'])]
df_ee_id = df[~pd.isnull(df['Employee Id'])]
df_uid = df[~pd.isnull(df['Ulti EE ID'])]


#start with Kronos id because it is the same as employee id

df_ee_id['Worker ID'] = df_ee_id['Employee Id']
df_ee_id['Source System'] = 'Kronos'
df_ee_id['Custom ID'] = df_ee_id['Employee Id']
df_ee_id['Custom ID Type'] = 'Kronos ID'
df_ee_id['Issued Date'] = pd.to_datetime(df_ee_id['Created']).dt.strftime("%d-%b-%Y").str.upper()
df_ee_id['Expiration Date'] = ''
df_ee_id['Organization'] = ''
df_ee_id['Description'] = ''

df_ee_id = df_ee_id[['Worker ID', 'Source System','Custom ID', 'Custom ID Type', 'Issued Date', 
  'Expiration Date','Organization', 'Description']]

# external Id
df_ex['Worker ID'] = df_ex['Employee Id']
df_ex['Source System'] = 'Kronos'
df_ex['Custom ID'] = df_ex['External Id']
df_ex['Custom ID Type'] = 'External Employee ID'
df_ex['Issued Date'] = ''
df_ex['Expiration Date'] = ''
df_ex['Organization'] = ''
df_ex['Description'] = ''

df_ex = df_ex[['Worker ID', 'Source System','Custom ID', 'Custom ID Type', 'Issued Date', 
  'Expiration Date','Organization', 'Description']]

#rti employee number
df_rti_1['Worker ID'] = df_rti_1['Employee Id']
df_rti_1['Source System'] = 'Kronos'
df_rti_1['Custom ID'] = df_rti_1['RTI Employee #']
df_rti_1['Custom ID Type'] = 'RTI Employee Number'
df_rti_1['Issued Date'] = ''
df_rti_1['Expiration Date'] = ''
df_rti_1['Organization'] = ''
df_rti_1['Description'] = ''

df_rti_1 = df_rti_1[['Worker ID', 'Source System','Custom ID', 'Custom ID Type', 'Issued Date', 
  'Expiration Date','Organization', 'Description']]

#rti external Ref
df_rti2['Worker ID'] = df_rti2['Employee Id']
df_rti2['Source System'] = 'Kronos'
df_rti2['Custom ID'] = df_rti2['RTI Employee #']
df_rti2['Custom ID Type'] = 'RTI External Ref​'
df_rti2['Issued Date'] = ''
df_rti2['Expiration Date'] = ''
df_rti2['Organization'] = ''
df_rti2['Description'] = ''

df_rti2 = df_rti2[['Worker ID', 'Source System','Custom ID', 'Custom ID Type', 'Issued Date', 
  'Expiration Date','Organization', 'Description']]

#Par ID
df_par['Worker ID'] = df_par['Employee Id']
df_par['Source System'] = 'Kronos'
df_par['Custom ID'] = df_par['PAR ID']
df_par['Custom ID Type'] = 'PAR ID​'
df_par['Issued Date'] = ''
df_par['Expiration Date'] = ''
df_par['Organization'] = ''
df_par['Description'] = ''

df_par = df_par[['Worker ID', 'Source System','Custom ID', 'Custom ID Type', 'Issued Date', 
  'Expiration Date','Organization', 'Description']]


#ulti id
df_uid['Worker ID'] = df_uid['Employee Id']
df_uid['Source System'] = 'Kronos'
df_uid['Custom ID'] = df_uid['Ulti EE ID']
df_uid['Custom ID Type'] = 'Ulti ID​'
df_uid['Issued Date'] = ''
df_uid['Expiration Date'] = ''
df_uid['Organization'] = ''
df_uid['Description'] = ''

df_uid = df_uid[['Worker ID', 'Source System','Custom ID', 'Custom ID Type', 'Issued Date', 
  'Expiration Date','Organization', 'Description']]

#concat all files
cust_ids = pd.concat([df_ee_id,df_ex,df_rti_1,df_rti2,df_par,df_uid])

os.chdir(path)
cust_ids.to_csv('worker_custom_ids.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
############################ Service Dates ###################################
##############################################################################

#service_dates

service_dates_c = service_dates.filter(regex='Required')
service_dates_df = pd.read_excel('./../HCM_Template_01122023.xlsx', sheet_name='Service Dates',skiprows=2)

service_dates_df = service_dates_df.iloc[0:0]



df['Worker ID'] = df['Employee Id']
df['Source System'] = 'Kronos'
df['Created'] = pd.to_datetime(df['Created'])
df['Date Hired'] = pd.to_datetime(df['Date Hired'])

df['Original Hire Date'] = np.where(df['Created']<df['Date Hired'],df['Created'],df['Date Hired'])
df['Original Hire Date'] = df['Original Hire Date'].dt.strftime("%d-%b-%Y").str.upper()
df['Continuous Service Date'] = pd.to_datetime(df['Date Hired']).dt.strftime("%d-%b-%Y").str.upper()
df['Expected Retirement Date'] = ''
df['Retirement Eligibility Date'] = ''
df['Seniority Date'] = ''
df['Severance Date'] = ''
df['Benefits Service Date']=''
df['Company Service Date'] = pd.to_datetime(df['Service Date']).dt.strftime("%d-%b-%Y").str.upper()
df['Time Off Service Date'] = np.where(~pd.isnull(df['Service Date']),df['Service Date'],df['Date Hired'])
df['Time Off Service Date'] = pd.to_datetime(df['Time Off Service Date']).dt.strftime("%d-%b-%Y").str.upper()
df['Vesting Date'] = ''

service_dates = df[['Worker ID','Source System','Original Hire Date', 'Continuous Service Date',
       'Expected Retirement Date', 'Retirement Eligibility Date',
       'Seniority Date', 'Severance Date', 'Benefits Service Date',
       'Company Service Date', 'Time Off Service Date', 'Vesting Date']]

service_dates = service_dates.fillna('')

os.chdir(path)
service_dates.to_csv('service_dates.txt',sep='|', encoding='utf-8', index=False)


##############################################################################
############################ Security Roles ##################################
##############################################################################


#user based
ub = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\user_based_sec.csv")
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files")
ub.to_csv('PROD_user_based_security.txt',sep='|', encoding='utf-8', index=False)



#role based
df_sup['Worker ID'] = df_sup['Direct Supervisor Employee Id']
df_sup['Position ID'] = 'P-' + df_sup['Direct Supervisor Employee Id']

#match standard employee data to supervisory to identify what security group the supervisor falls into

df_sec = df[['Employee Id','Default Jobs (HR)','Job Ranking']]
df_sec.rename(columns={'Employee Id':'Managers ID','Default Jobs (HR)':'Manager Job','Job Ranking':'Manager Job Rank',},inplace=True)
df_sec['Managers ID'] = df_sec['Managers ID'].astype(str) 

dfc = df_sup.merge(df_sec, left_on='Worker ID',right_on='Managers ID')


#break each group into own df in case of overlap
dfc_ac = dfc[dfc['Manager Job'] == 'Area Coach']
dfc_ac['Organization Role ID'] = 'Area_Coach_Role'
dfc_ac['Organization ID'] = "SUP_" + dfc_ac['Employee Id'].astype(str).str.zfill(6)
dfc_ac = dfc_ac[['Organization ID','Worker ID','Position ID','Organization Role ID']]

dfc_acm = dfc[dfc['Default Jobs (HR)'] == 'Area Coach']
dfc_acm['Organization Role ID'] = 'Area coachs Manager'
dfc_acm['Organization ID'] = "SUP_" + dfc_acm['Employee Id'].astype(str).str.zfill(6)
dfc_acm = dfc_acm[['Organization ID','Worker ID','Position ID','Organization Role ID']]


dfc_hc = dfc[dfc['Default Jobs (HR)'] == 'Hrly Co Manager']
dfc_hc['Organization Role ID'] = 'Hourly CoManager_Role'
dfc_hc['Organization ID'] = "SUP_" + dfc_hc['Employee Id'].astype(str).str.zfill(6)
dfc_hc = dfc_hc[['Organization ID','Worker ID','Position ID','Organization Role ID']]


dfc['MIT Date'] = pd.to_datetime(dfc['End of Training Date'])
dfc_mit = dfc[dfc['MIT Date']>'8/14/2023']
dfc_mit['Organization Role ID'] = 'Manager in Training_Role'
dfc_mit['Organization ID'] = "SUP_" + dfc_mit['Managers ID'].astype(str).str.zfill(6)
dfc_mit = dfc_mit[['Organization ID','Worker ID','Position ID','Organization Role ID']]



dfc_man = dfc[(dfc['Default Jobs (HR)']=='Restaurant General Manager')|(dfc['Default Jobs (HR)']=='Managing Partner')]
dfc_man['Organization Role ID'] = 'Manager'
dfc_man['Organization ID'] = dfc_man['Cost Centers(Company Code)'] + "_" + dfc_man['Employee Id'].astype(str)
dfc_man['Worker ID'] = dfc_man['Employee Id']
dfc_man['Position ID'] = "P-" + dfc_man['Employee Id'].astype(str)
dfc_man = dfc_man[['Organization ID','Worker ID','Position ID','Organization Role ID']]


#RTL Assignments

deanna = ['LC05673',	'LC05675',	'LC05677',	'LC05678',	'LC05681',	'LC05682',	'LC05685',	'LC05674',	'LC05676',	'LC05679',	'LC05680',	'LC05683',	'LC05684',	'LC05686',	'LC05408',	'LC05412',	'LC05428',	'LC05435',	'LC05790',	'LC05874',	'LC05875',	'LC05876',	'LC05369',	'LC05371',	'LC05372',	'LC05481',	'LC05744',	'LC05746',	'LC05774',	'LC05138',	'LC05146',	'LC05148',	'LC05150',	'LC05151',	'LC05158',	'LC05165',	'LC05354',	'LC05361',	'LC05373',	'LC05375',	'LC05380',	'LC05381',	'LC05383',	'LC05484',	'LC05629',	'LC05630',	'LC05631',	'LC05632',	'LC05669',	'LC05670',	'LC05671',	'LC05362',	'LC05366',	'LC05368',	'LC05374',	'LC05376',	'LC05860',	'LC05409',	'LC05410',	'LC05411',	'LC05621',	'LC05415',	'LC05418',	'LC05419',	'LC05423',	'LC05429',	'LC05596',	'LC05420',	'LC05658',	'LC05858',	'LC05878',	'LC05886',	'LC05777',	'LC05778',	'LC05779',	'LC05780',	'LC05781',	'LC05156',	'LC05626',	'LC05627',	'LC05628',	'LC05734',	'LC05735',	'LC05139',	'LC05141',	'LC05142',	'LC05145',	'LC05147',	'LC05149',	'LC05818',	'LC05822',	'LC05825',	'LC05826',	'LC05827',	'LC05829',	'LC05832',	'LC05833',	'LC05360',	'LC05370',	'LC05377',	'LC05384',	'LC05385',	'LC05597',	'LC05600',	'LC05267',	'LC05268',	'LC05269',	'LC05280',	'LC05545',	'LC05558',	'LC05560',	'LC05121',	'LC05128',	'LC05132',	'LC05134',	'LC05137',	'LC05153',	'LC05154',	'LC05157',	'LC05159',	'LC05426',	'LC05427',	'LC05431',	'LC05461',	'LC05471',	'LC05753',	'LC05775',	'LC05949',	'LC05355',	'LC05359',	'LC05367',	'LC05772',	'LC05773',	'LC05284',	'LC05534',	'LC05540',	'LC05542',	'LC05554',	'LC05555',	'LC05824',	'LC05828',	'LC05613',	'LC05614',	'LC05615',	'LC05616',	'LC05741',	'LC05742',	'LC05798',	'LC05414',	'LC05416',	'LC05417',	'LC05422',	'LC05651',	'LC05865',	'LC05947',	'LC05270',	'LC05273',	'LC05274',	'LC05275',	'LC05819',	'LC05820',	'LC05821',	'LC05823',	'LC05834',	'LC05882',	'LC05293',	'LC05312',	'LC05314',	'LC05760',	'LC05762',	'LC05763',	'LC05764',	'LC05768',	'LC05342',	'LC05346',	'LC05348',	'LC05351',	'LC05352',	'LC05421',	'LC05301',	'LC05302',	'LC05303',	'LC05304',	'LC05323',	'LC05609',	'LC05610',	'LC05612',	'LC05271',	'LC05285',	'LC05286',	'LC05287',	'LC05288',	'LC05289',	'LC05290',	'LC05541',	'LC05692',	'LC05533',	'LC05536',	'LC05543',	'LC05548',	'LC05551',	'LC05553',	'LC05561',	'LC05340',	'LC05344',	'LC05424',	'LC05531',	'LC05535',	'LC05539',	'LC05544',	'LC05547',	'LC05549',	'LC05605',	'LC05322',	'LC05324',	'LC05325',	'LC05326',	'LC05329',	'LC05332',	'LC05338',	'LC05339',	'LC05853',	'LC05122',	'LC05123',	'LC05127',	'LC05129',	'LC05152',	'LC05155',	'LC05160',	'LC05207',	'LC05208',	'LC05341',	'LC05343',	'LC05345',	'LC05349',	'LC05350',	'LC05353',	'LC05879',	'LC05327',	'LC05328',	'LC05330',	'LC05331',	'LC05333',	'LC05335',	'LC05336',	'LC05611',	'LC05272',	'LC05276',	'LC05277',	'LC05278',	'LC05279',	'LC05281',	'LC05282',	'LC05283',	'LC05530',	'LC05532',	'LC05537',	'LC05538',	'LC05562',	'LC05693',	'LC05001',	'LC05002',	'LC05003',	'LC05004',	'LC05005',	'LC05006',	'LC05036',	'LC05206',	'LC05745',	'LC05747',	'LC05748',	'LC05749',	'LC05750',	'LC05751',	'LC05589',	'LC05590',	'LC05591',	'LC05592',	'LC05593',	'LC05594',	'LC05887',	'LC05888',	'LC05889',	'LC05890',	'LC05891',	'LC05892',	'LC05124',	'LC05126',	'LC05130',	'LC05131',	'LC05133',	'LC05135',	'LC05136',	'LC05143',	'LC05164',	'LC05291',	'LC05292',	'LC05294',	'LC05296',	'LC05307',	'LC05310',	'LC05321',	'LC05356',	'LC05357',	'LC05358',	'LC05363',	'LC05364',	'LC05379',	'LC05306',	'LC05761',	'LC05765',	'LC05766',	'LC05767']


kim = ['LC05037',	'LC05042',	'LC05044',	'LC05050',	'LC05052',	'LC05064',	'LC05038',	'LC05041',	'LC05043',	'LC05045',	'LC05048',	'LC05053',	'LC05086',	'LC05094',	'LC05096',	'LC05098',	'LC05106',	'LC05117',	'LC05244',	'LC05245',	'LC05247',	'LC05249',	'LC05250',	'LC05251',	'LC05265',	'LC05266',	'LC05394',	'LC05397',	'LC05399',	'LC05440',	'LC05442',	'LC05446',	'LC05451',	'LC05452',	'LC05453',	'LC05454',	'LC05459',	'LC05184',	'LC05185',	'LC05186',	'LC05189',	'LC05193',	'LC05202',	'LC05934',	'LC05936',	'LC05959',	'LC05960',	'LC05961',	'LC05962',	'LC05963',	'LC05964',	'LC05965',	'LC05084',	'LC05102',	'LC05103',	'LC05104',	'LC05105',	'LC05113',	'LC05116',	'LC05603',	'LC05617',	'LC05618',	'LC05620',	'LC05659',	'LC05663',	'LC05664',	'LC05092',	'LC05097',	'LC05859',	'LC05929',	'LC05930',	'LC05931',	'LC05932',	'LC05933',	'LC05078',	'LC05085',	'LC05088',	'LC05090',	'LC05093',	'LC05095',	'LC05100',	'LC05073',	'LC05075',	'LC05177',	'LC05179',	'LC05180',	'LC05181',	'LC05183',	'LC05089',	'LC05101',	'LC05108',	'LC05109',	'LC05110',	'LC05112',	'LC05115',	'LC05400',	'LC05403',	'LC05404',	'LC05407',	'LC05432',	'LC05433',	'LC05434',	'LC05462',	'LC05437',	'LC05438',	'LC05439',	'LC05441',	'LC05455',	'LC05457',	'LC05458',	'LC05926',	'LC05927',	'LC05928',	'LC05935',	'LC05937',	'LC05443',	'LC05444',	'LC05445',	'LC05447',	'LC05448',	'LC05449',	'LC05450',	'LC05456',	'LC05188',	'LC05190',	'LC05191',	'LC05205',	'LC05595',	'LC05619',	'LC05938',	'LC05040',	'LC05055',	'LC05056',	'LC05058',	'LC05059',	'LC05063',	'LC05622',	'LC05870',	'LC05070',	'LC05071',	'LC05076',	'LC05079',	'LC05111',	'LC05118',	'LC05039',	'LC05687',	'LC05688',	'LC05689',	'LC05690',	'LC05298',	'LC05316',	'LC05317',	'LC05318',	'LC05319',	'LC05066',	'LC05068',	'LC05069',	'LC05082',	'LC05087',	'LC05099',	'LC05107',	'LC05114',	'LC05072',	'LC05074',	'LC05691',	'LC05463',	'LC05464',	'LC05652',	'LC05653',	'LC05655',	'LC05656',	'LC05657',	'LC05703',	'LC05297',	'LC05299',	'LC05308',	'LC05309',	'LC05940',	'LC05941',	'LC05942',	'LC05077',	'LC05080',	'LC05081',	'LC05091',	'LC05176',	'LC05178',	'LC05187',	'LC05194',	'LC05195',	'LC05196',	'LC05197',	'LC05198',	'LC05199',	'LC05201',	'LC05203',	'LC05204',	'LC05046',	'LC05047',	'LC05049',	'LC05051',	'LC05060',	'LC05246',	'LC05950']


tammy = ['LC05209',	'LC05210',	'LC05608',	'LC05696',	'LC05700',	'LC05702',	'LC05736',	'LC05836',	'LC05838',	'LC05840',	'LC05841',	'LC05843',	'LC05844',	'LC05846',	'LC05864',	'LC05395',	'LC05396',	'LC05401',	'LC05402',	'LC05405',	'LC05425',	'LC05570',	'LC05704',	'LC05771',	'LC05851',	'LC05800',	'LC05805',	'LC05806',	'LC05807',	'LC05812',	'LC05815',	'LC05894',	'LC05636',	'LC05728',	'LC05729',	'LC05730',	'LC05776',	'LC05913',	'LC05966',	'LC05211',	'LC05213',	'LC05217',	'LC05232',	'LC05235',	'LC05238',	'LC05239',	'LC05740',	'LC05222',	'LC05224',	'LC05226',	'LC05236',	'LC05755',	'LC05794',	'LC05856',	'LC05485',	'LC05490',	'LC05494',	'LC05508',	'LC05509',	'LC05511',	'LC05672',	'LC05637',	'LC05638',	'LC05639',	'LC05640',	'LC05662',	'LC05862',	'LC05475',	'LC05476',	'LC05478',	'LC05598',	'LC05650',	'LC05754',	'LC05225',	'LC05488',	'LC05489',	'LC05497',	'LC05515',	'LC05518',	'LC05524',	'LC05706',	'LC05708',	'LC05709',	'LC05710',	'LC05712',	'LC05724',	'LC05743',	'LC05901',	'LC05905',	'LC05915',	'LC05916',	'LC05925',	'LC05945',	'LC05220',	'LC05229',	'LC05793',	'LC05943',	'LC05717',	'LC05719',	'LC05721',	'LC05725',	'LC05731',	'LC05752',	'LC05799',	'LC05804',	'LC05813',	'LC05814',	'LC05816',	'LC05895',	'LC05896',	'LC05406',	'LC05430',	'LC05472',	'LC05473',	'LC05564',	'LC05565',	'LC05474',	'LC05694',	'LC05695',	'LC05697',	'LC05699',	'LC05701',	'LC05739',	'LC05487',	'LC05493',	'LC05496',	'LC05507',	'LC05510',	'LC05514',	'LC05523',	'LC05783',	'LC05835',	'LC05845',	'LC05713',	'LC05902',	'LC05908',	'LC05917',	'LC05922',	'LC05958',	'LC05500',	'LC05501',	'LC05503',	'LC05525',	'LC05633',	'LC05666',	'LC05668',	'LC05221',	'LC05498',	'LC05505',	'LC05506',	'LC05599',	'LC05857',	'LC05252',	'LC05253',	'LC05607',	'LC05756',	'LC05782',	'LC05008',	'LC05012',	'LC05014',	'LC05017',	'LC05261',	'LC05262',	'LC05863',	'LC05705',	'LC05897',	'LC05899',	'LC05907',	'LC05910',	'LC05912',	'LC05919',	'LC05898',	'LC05903',	'LC05906',	'LC05909',	'LC05918',	'LC05920',	'LC05924',	'LC05029',	'LC05030',	'LC05033',	'LC05035',	'LC05167',	'LC05893',	'LC05513',	'LC05519',	'LC05520',	'LC05499',	'LC05502',	'LC05504',	'LC05667',	'LC05698',	'LC05757',	'LC05714',	'LC05718',	'LC05722',	'LC05732',	'LC05861',	'LC05715',	'LC05716',	'LC05723',	'LC05733',	'LC05979',	'LC05980',	'LC05634',	'LC05635',	'LC05711',	'LC05720',	'LC05726',	'LC05727',	'LC05788',	'LC05855',	'LC05212',	'LC05230',	'LC05231',	'LC05233',	'LC05237',	'LC05240',	'LC05391',	'LC05665',	'LC05784',	'LC05785',	'LC05786',	'LC05787',	'LC05837',	'LC05839',	'LC05842',	'LC05646',	'LC05647',	'LC05648',	'LC05707',	'LC05025',	'LC05026',	'LC05028',	'LC05034',	'LC05065',	'LC05120',	'LC05013',	'LC05020',	'LC05021',	'LC05023',	'LC05255',	'LC05256',	'LC05015',	'LC05018',	'LC05479',	'LC05480',	'LC05526',	'LC05527',	'LC05528',	'LC05529',	'LC05009',	'LC05386',	'LC05654',	'LC05967',	'LC05968',	'LC05969',	'LC05866',	'LC05867',	'LC05868',	'LC05869',	'LC05871',	'LC05872',	'LC05873',	'LC05900',	'LC05904',	'LC05911',	'LC05914',	'LC05921',	'LC05923',	'LC05215',	'LC05216',	'LC05227',	'LC05234',	'LC05242',	'LC05738',	'LC05801',	'LC05802',	'LC05803',	'LC05808',	'LC05809',	'LC05810',	'LC05811',	'LC05817',	'LC05465',	'LC05466',	'LC05467',	'LC05468',	'LC05469',	'LC05470',	'LC05649',	'LC05641',	'LC05642',	'LC05643',	'LC05644',	'LC05645',	'LC05759',	'LC05563',	'LC05566',	'LC05569',	'LC05571',	'LC05572',	'LC05604',	'LC05573',	'LC05574',	'LC05575',	'LC05576',	'LC05737',	'LC05939',	'LC05486',	'LC05491',	'LC05492',	'LC05495',	'LC05512',	'LC05516',	'LC05517',	'LC05522',	'LC05260',	'LC05263',	'LC05387',	'LC05388',	'LC05389',	'LC05010',	'LC05011',	'LC05022',	'LC05243',	'LC05254',	'LC05258',	'LC05259']


mask = dfc['Work Location'].isin(deanna)
dfc_rtl_d = dfc.loc[~mask]
dfc_rtl_d['Worker ID'] = '15159'
dfc_rtl_d['Position ID'] = 'P-15159'
dfc_rtl_d['Organization Role ID'] = 'Regional Training Leader'
dfc_rtl_d = dfc_rtl_d[['Supervisory Organization ID','Worker ID','Position ID','Organization Role ID']]
dfc_rtl_d.rename(columns={'Supervisory Organization ID':'Organization ID'},inplace=True)
dfc_rtl_d = dfc_rtl_d.drop_duplicates()

mask = dfc['Work Location'].isin(kim)
dfc_rtl_k = dfc.loc[~mask]
dfc_rtl_k['Worker ID'] = '14856'
dfc_rtl_k['Position ID'] = 'P-14856'
dfc_rtl_k['Organization Role ID'] = 'Regional Training Leader'
dfc_rtl_k = dfc_rtl_k[['Supervisory Organization ID','Worker ID','Position ID','Organization Role ID']]
dfc_rtl_k.rename(columns={'Supervisory Organization ID':'Organization ID'},inplace=True)
dfc_rtl_k = dfc_rtl_k.drop_duplicates()

mask = dfc['Work Location'].isin(tammy)
dfc_rtl_t = dfc.loc[~mask]
dfc_rtl_t['Worker ID'] = '31494'
dfc_rtl_t['Position ID'] = 'P-31494'
dfc_rtl_t['Organization Role ID'] = 'Regional Training Leader'
dfc_rtl_t = dfc_rtl_t[['Supervisory Organization ID','Worker ID','Position ID','Organization Role ID']]
dfc_rtl_t.rename(columns={'Supervisory Organization ID':'Organization ID'},inplace=True)
dfc_rtl_t = dfc_rtl_t.drop_duplicates()



dfc_rtl = dfc[dfc['Employee EIN'] == 'KBP Bells']
dfc_rtl['Worker ID'] = '97522'
dfc_rtl['Position ID'] = 'P-97522'
dfc_rtl['Organization Role ID'] = 'Regional Training Leader'
dfc_rtl = dfc_rtl[['Supervisory Organization ID','Worker ID','Position ID','Organization Role ID']]
dfc_rtl.rename(columns={'Supervisory Organization ID':'Organization ID'},inplace=True)
dfc_rtl = dfc_rtl.drop_duplicates()


dfc_rtl_r = dfc[dfc['Employee EIN'] == 'KBP Inspired']
dfc_rtl_r['Worker ID'] = '133507'
dfc_rtl_r['Position ID'] = 'P-133507'
dfc_rtl_r['Organization Role ID'] = 'Regional Training Leader'
dfc_rtl_r = dfc_rtl_r[['Supervisory Organization ID','Worker ID','Position ID','Organization Role ID']]
dfc_rtl_r.rename(columns={'Supervisory Organization ID':'Organization ID'},inplace=True)
dfc_rtl_r = dfc_rtl_r.drop_duplicates()

#concat all groups

security = pd.concat([dfc_ac,dfc_acm,dfc_hc,dfc_rtl_d,dfc_rtl_k,dfc_rtl_t,dfc_rtl,dfc_rtl_r])

security['Organization Type'] = 'Organization_Reference_ID'
security['Source System'] = ''
security['Effective Date']=''

security = security[['Organization ID',
'Organization Type',
'Organization Role ID',
'Source System',
'Worker ID',
'Position ID',
'Effective Date']]

security.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\role_based_security2.csv')
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files')
security = pd.read_csv(r'role_based_security2.csv')

os.chdir(path)
security.to_csv('PROD_role_based_security.txt',sep='|', encoding='utf-8', index=False)

##############################################################################
############################ History Files ##################################
##############################################################################



active_ees


os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\jch')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])
#combined_csv.to_csv(r'./../jch_all.csv')

#compenstation
comp_his = combined_csv.dropna(subset=['Action'])
comp_his = comp_his[comp_his['Account Status']=='Active']
comp_his = comp_his[~comp_his['Action'].str.contains('test')]
comp_his = comp_his[(comp_his['Action'].str.contains("Pay")) | (comp_his['Action'].str.contains("Salary"))]
comp_his['Effective Date'] = pd.to_datetime(comp_his['Effective Date'])
comp_his.sort_values(by=['Employee Id','Effective Date'],inplace=True)
comp_his['T'] = comp_his.groupby('Employee Id')['Employee Id'].transform('count')
#comp_his['T'] = comp_his['Employee Id'].value_counts()
comp_his['No'] = (comp_his.groupby(['Employee Id'])['Employee Id'].cumcount() + 1)
comp_his['Effective Date'] = np.where(comp_his['Effective Date'] == '1900-12-31',comp_his['Created'],comp_his['Effective Date'])
comp_his['Effective Date'] = pd.to_datetime(comp_his['Effective Date']).dt.strftime("%d-%b-%Y").str.upper()

comp_his['Worker ID'] = comp_his['Employee Id']
comp_his['Source System'] = 'Kronos'
comp_his['Sequence ID'] = comp_his['No']
comp_his['Reason'] = np.where(comp_his['Job Change Reason Code'].isna(),'Promotion',comp_his['Job Change Reason Code'])
comp_his['Amount'] = comp_his['Annual Amount'].str.replace('$','')
comp_his['Amount'] = comp_his['Amount'].str.replace(',','')
comp_his['Currency Code'] = 'USD'
comp_his['Frequency'] = 'Annual'
comp_his['Amount Change'] = ''

comp_his['FIELD_1'] = ''
comp_his['FIELD_2'] = ''
comp_his['FIELD_3'] = ''
comp_his['FIELD_4'] = ''
comp_his['FIELD_5'] = ''
comp_his['FIELD_6'] = ''
comp_his['FIELD_7'] = ''
comp_his['FIELD_8'] = ''
comp_his['FIELD_9'] = ''
comp_his['FIELD_10'] = ''
comp_his['FIELD_11'] = ''
comp_his['FIELD_12'] = ''
comp_his['FIELD_13'] = ''
comp_his['FIELD_14'] = ''
comp_his['FIELD_15'] = ''


comp_his_export = comp_his[['Worker ID',
'Source System',
'Sequence ID',
'Effective Date',
'Reason',
'Amount',
'Currency Code',
'Frequency',
'Amount Change',
'FIELD_1',
'FIELD_2',
'FIELD_3',
'FIELD_4',
'FIELD_5',
'FIELD_6',
'FIELD_7',
'FIELD_8',
'FIELD_9',
'FIELD_10',
'FIELD_11',
'FIELD_12',
'FIELD_13',
'FIELD_14',
'FIELD_15']]

os.chdir(path)
comp_his_export.to_csv('E2E_comp_hist_prev_sys_v2.txt',sep='|', encoding='utf-8', index=False)


#job change
job_change = combined_csv.dropna(subset=['Action'])

#remove testers
job_change = job_change[pd.to_numeric(job_change['Employee Id'], errors='coerce').notnull()]
job_change['Effective Date'] = pd.to_datetime(job_change['Effective Date'])
job_change.sort_values(by='Effective Date',inplace=True)
job_change.drop_duplicates(inplace=True)
job_change['T'] = job_change.groupby('Employee Id')['Employee Id'].transform('count')
job_change['No'] = job_change.groupby('Employee Id')['Employee Id'].cumcount() + 1

job_change['Worker ID'] = job_change['Employee Id']
job_change['Source System'] = 'Kronos'
job_change['Sequence ID'] = job_change['No']
job_change['Effective Date'] = np.where(job_change['Effective Date'] == '1900-12-31',job_change['Created'],job_change['Effective Date'])
job_change['Effective Date'] = pd.to_datetime(job_change['Effective Date']).dt.strftime("%d-%b-%Y").str.upper()
job_change['Reason'] = np.where(job_change['Job Change Reason Code'].isna(),job_change['Action'],job_change['Job Change Reason Code'])
job_change['Annual Amount'] = job_change['Annual Amount'].str.replace('$','')
job_change['Annual Amount'] = job_change['Annual Amount'].str.replace(',','')
job_change['Start Date'] = ''
job_change['End Date'] = ''

jch = job_change[['Worker ID',
'Source System',
'Sequence ID',
'Effective Date',
'Reason',
'Start Date',
'End Date',
'Employee EIN',
'Action',
'Jobs (HR)',
'Default Cost Centers',
'Employee Type',
'Annual Amount',
'Pay Grade',
'Account Status',
'Created']]

os.chdir(path)
jch.to_csv('job_hist_prev_sys.txt',sep='|', encoding='utf-8', index=False)

#payroll

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\pay_hist')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])

#tie out payroll name
#create seperate table to just grab pay periods that don't have off-cycle to match pay groups to employee id

pay_gp = combined_csv[~combined_csv['Payroll Name'].str.contains('Off')]
pay_gp['Pay Date.1'] = pd.to_datetime(pay_gp['Pay Date.1'])
pay_gp = pay_gp[['Employee Id','Payroll Name','Pay Date.1']]
pay_gp['Quarter'] = np.where(pay_gp['Pay Date.1'] < '4/1/2022','Q1',
                    np.where((pay_gp['Pay Date.1'] > '3/31/2022') & (pay_gp['Pay Date.1'] < '7/1/2022'),'Q2',
                    np.where((pay_gp['Pay Date.1'] > '6/30/2022') & (pay_gp['Pay Date.1'] < '10/1/2022'),'Q3','Q4')))

pay_gp = pay_gp[pay_gp["Payroll Name"].str.contains("Regular") == True]
pay_gp['Payroll Name'] = pay_gp['Payroll Name'].str.split("Regular").str[0]
pay_gp = pay_gp[['Employee Id','Payroll Name','Quarter']]
pay_gp.drop_duplicates(inplace=True)



pay = combined_csv
pay['Pay Date.1'] = pd.to_datetime(pay['Pay Date.1'])
pay['Quarter'] = np.where(pay['Pay Date.1'] < '4/1/2022','Q1',
                 np.where((pay['Pay Date.1'] > '3/31/2022') & (pay['Pay Date.1'] < '7/1/2022'),'Q2',
                 np.where((pay['Pay Date.1'] > '6/30/2022') & (pay['Pay Date.1'] < '10/1/2022'),'Q3','Q4')))


pay_c = pay

#clean up amounts column
pay_c['Record Amount'] = pay_c['Record Amount'].str.replace('$','')
pay_c['Record Amount'] = pay_c['Record Amount'].str.replace(',','')
pay_c['Record Amount'] = np.where(pay_c['Record Amount'] == '-',0,pay_c['Record Amount'])
pay_c['Record Amount'] = pay_c['Record Amount'].str.replace('(','-')
pay_c['Record Amount'] = pay_c['Record Amount'].str.replace(')','')
pay_c['Record Amount'] = pay_c['Record Amount'].astype('float')
pay_c['Record Amount (ER)'] = pay_c['Record Amount (ER)'].str.replace('$','')
pay_c['Record Amount (ER)'] = pay_c['Record Amount (ER)'].str.replace(',','')
pay_c['Record Amount (ER)'] = np.where(pay_c['Record Amount (ER)'] == '-',0,pay_c['Record Amount'])
pay_c['Record Amount (ER)'] = pay_c['Record Amount (ER)'].str.replace('(','-')
pay_c['Record Amount (ER)'] = pay_c['Record Amount (ER)'].str.replace(')','')
pay_c['Record Amount (ER)'] = pay_c['Record Amount (ER)'].astype('float')
pay_c['Amount'] = np.where((pay_c['Record Amount (ER)'] != 0) & (pay_c['Record Amount'] != pay_c['Record Amount (ER)']),pay_c['Record Amount (ER)'],pay_c['Record Amount'])

pay_c = pay_c[['Employee Id','Cost Centers(Cost Center)','Quarter','E/D/T Code','Record Amount']]

result = pay_c.groupby(['Employee Id','Cost Centers(Cost Center)','Quarter','E/D/T Code'])['Record Amount'].sum()
result = result.reset_index()

# read in workday earning/deduction codes
wd_codes = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\earning_ded_codes.csv")

pay_c = pay.merge(wd_codes, left_on='E/D/T Code', right_on='Earning Code (Legacy System)',how='outer')

##############################################################################
############################ Worker Documents ################################
##############################################################################
import zipfile
from zipfile import ZipFile
from pathlib import Path
path = Path(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\minors')

fun = lambda x : os.path.isfile(os.path.join(path,x))
 
files_list = filter(fun, os.listdir(path))
 
# Create a list of files in directory along with the size
size_of_file = [
    (f,os.stat(os.path.join(path, f)).st_size)
    for f in files_list
]
fun = lambda x : x[1]
 
data = []
# in this case we have its file path instead of file
for f, s in sorted(size_of_file, key=fun):
    data.append({'filename': f, 'size_mb': round(s/(1024*1024), 3)})

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

minor_docs = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\minor_docs.csv",encoding="cp1251")
df_min = df.merge(minor_docs, left_on='filename',right_on='Display Name')    
df_min['Worker ID'] = df_min['Employee Id']
df_min['File Name'] = df_min['filename']
df_min['File Type'] = df_min['Document Type']
df_min['Source System'] = 'Kronos'
df_min['Worker Type'] = ''
df_min['File Content'] = ''
df_min = df_min[df_min['File Name'] != 'image.jpg']
df_min_c = df_min


# Group the files into bins of 30 MB max
df_min['running_total_mb'] = df_min['size_mb'].cumsum()

zip_counter = 0 
mb_min = 0 
mb_max = 400 
output_path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\minors_zipfiles'
for index, row in df_min.iterrows(): 
    while True: 
        if row['running_total_mb'] > mb_min and row['running_total_mb'] < mb_max: 
            with zipfile.ZipFile(os.path.join(output_path, f'minors_zip_{zip_counter}.zip'), 'a') as arch: 
                arch.write(os.path.join(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\minors', row['filename'])) 
            with open(os.path.join(output_path, f'minor_filenames_{zip_counter}.csv'), 'a') as fname_list: 
                fname_list.write(row['filename']) 
                fname_list.write('\n') 
            df_min.drop(index, inplace=True) 
            break 
        else: 
            mb_min += 400 
            mb_max += 400 
            zip_counter += 1 
        break

dir_path = (r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\minors_zipfiles')
all_files = glob.glob(dir_path + "\*.csv")
dfx = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

# create a dataframe from the list

dfx['Zip File Name'] = dfx['Zip File Name'].str.replace('filenames','zip')

df_minor = dfx.merge(df_min_c,on='File Name')
    
df_minor = df_minor[['Worker ID',
'Source System',
'Zip File Name',
'File Name',
'Worker Type',
'File Type',
'File Content']]


##############################################################################
path = Path(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\servsafe')

fun = lambda x : os.path.isfile(os.path.join(path,x))
 
files_list = filter(fun, os.listdir(path))
 
# Create a list of files in directory along with the size
size_of_file = [
    (f,os.stat(os.path.join(path, f)).st_size)
    for f in files_list
]
fun = lambda x : x[1]
 
data = []
# in this case we have its file path instead of file
for f, s in sorted(size_of_file, key=fun):
    data.append({'filename': f, 'size_mb': round(s/(1024*1024), 3)})

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

servsafe_docs = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\servsafe_docs.csv",encoding="cp1251")

df2 = pd.DataFrame(
    [os.path.splitext(f) for f in df.filename], 
    columns=['filename_c', 'Type']
)
df2['File Name'] = df2['filename_c'] + df2['Type']
df2 = df2.merge(df, left_on='File Name', right_on='filename')

servsafe_docs['Display Name']= [os.path.splitext(x)[0] for x in servsafe_docs['Display Name']]


df_ss = df2.merge(servsafe_docs, left_on='filename_c',right_on='Display Name') 
   
df_ss['Worker ID'] = df_ss['Employee Id']
df_ss['File Name'] = df_ss['filename']
df_ss['File Type'] = df_ss['Document Type']
df_ss['Worker Type'] = ''
df_ss['File Content'] = ''
#df_ss['File Name'] = df_ss['filename']+df_ss['Type']
df_ss = df_ss[df_ss['File Name'] != 'image.jpg']
df_ss_c = df_ss


# Group the files into bins of 30 MB max
df_ss['running_total_mb'] = df_ss['size_mb'].cumsum()

zip_counter = 0 
mb_min = 0 
mb_max = 400 
output_path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\servsafe_zipfiles'
for index, row in df_ss.iterrows(): 
    while True: 
        if row['running_total_mb'] > mb_min and row['running_total_mb'] < mb_max: 
            with zipfile.ZipFile(os.path.join(output_path, f'servsafe_zip_{zip_counter}.zip'), 'a') as arch: 
                arch.write(os.path.join(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\servsafe', row['filename'])) 
            with open(os.path.join(output_path, f'servsafe_filenames_{zip_counter}.csv'), 'a') as fname_list: 
                fname_list.write(row['filename']) 
                fname_list.write('\n') 
            df_ss.drop(index, inplace=True) 
            break 
        else: 
            mb_min += 400 
            mb_max += 400 
            zip_counter += 1 
        break

dir_path = (r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\servsafe_zipfiles')
all_files = glob.glob(dir_path + "\*.csv")

dfy = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

# create a dataframe from the list

dfy['Zip File Name'] = dfy['Zip File Name'].str.replace('filenames','zip')

df_servsafe = dfy.merge(df_ss_c,on='File Name')
    
df_servsafe = df_servsafe[['Worker ID',
'Source System',
'Zip File Name',
'File Name',
'Worker Type',
'File Type',
'File Content']]


df_docs = pd.concat([df_minor,df_servsafe])