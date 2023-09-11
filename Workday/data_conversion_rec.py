# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 15:47:32 2023

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

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\rec_dc'
os.chdir(path)
#Single source for populating all data in the HCM Data Conversion Spreadsheet

df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\ApplicantData_2021-July23.csv")

df['Candidate ID'] = 'C-' + df['Candidate ID'].astype(str)
df['Source System'] = 'PeopleMatter'
df['First Name'] = df['Applicant First Name']
df['Last Name'] = df['Applicant Last Name']
df['Middle Name'] = ''
df['Secondary Last Name'] = ''
df['Name Prefix'] = ''
df['Name Suffix'] = ''
df['Family Name Prefix'] = ''
df['Preferred First Name'] = ''
df['Preferred Middle Name'] = ''
df['Preferred Last Name'] = ''
df['Preferred Secondary Last Name'] = ''
df['Phone Country Code'] = 'USA_1'
df['Phone Number'] = df['Cell Phone']
df['Phone Extension'] = ''
df['Phone Device Type'] = 'Mobile'
df['Email Address'] = df['Email']
df['Country ISO Code - Primary'] = 'USA'
df['Address Line #1 - Primary'] = df['Address 1']
df['Address Line #2 - Primary'] = df['Address 2']
df['Address Line #3 - Primary'] = ''
df['Address Line #4 - Primary'] = ''
df['Address Line #5 - Primary'] = ''
df['Address Line #6 - Primary'] = ''
df['Address Line #7 - Primary'] = ''
df['Address Line #8 - Primary'] = ''
df['Address Line #9 - Primary'] = ''
df['Country ISO Code - Local'] = ''
df['Address Line #1 - Local'] = ''
df['Address Line #2 - Local'] = ''
df['Address Line #3 - Local'] = ''
df['Address Line #4 - Local'] = ''
df['Address Line #5 - Local'] = ''
df['City - Primary'] = df['City']
df['City - Local'] = ''
df['City Subdivision - Primary'] = ''
df['City Subdivision - Local'] = ''
df['Region - Primary'] = 'USA-' + df['State']
df['Region Subdivision 1 - Primary'] = ''
df['Region Subdivision 2 - Primary'] = ''
df['Region Subdivision 1 - Local'] = ''
df['Postal Code - Primary'] = df['Zip Code']
df['Web Address URL'] = ''
df['Social Media Type'] = ''
df['Social Network Account URL'] = ''
df['Social Network Account User Name'] = ''
df['Confidential'] = ''
df['Level Reference (Management Level ID)'] = ''
df['Prospect Status'] = ''
df['Prospect Type'] = ''
df['Recruiting Source (Applicant Source ID)'] = ''
df['Added By Worker'] = ''
df['Do Not Hire']=''
df['Withdrawn']=''
df['Referred By Worker']=''
df['Referral Job']=''
df['Referral Job Area']=''
df['Referral Relationship']=''
df['Referral Consent Given']=''
df['Current Pre-Hire?']=''
df['Applicant ID']=''
df['Summary'] = df['Job Title'] + ' ' + df['Application Date']


df_c = df[['Candidate ID','Applicant First Name','Applicant Last Name']]
df_c['Applicant First Name'] = df_c['Applicant First Name'].str.lower()
df_c['Applicant Last Name'] = df_c['Applicant Last Name'].str.lower()
df_c['Full Name'] = df_c['Applicant First Name'] + ' ' + df_c['Applicant Last Name']
ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\71877725.csv",encoding='cp1251')
ees['First Name'] = ees['First Name'].str.lower()
ees['Last Name'] = ees['Last Name'].str.lower()
ees['Full Name'] = ees['First Name'] +' ' + ees['Last Name']

app_ee = df_c.merge(ees, on='Full Name')
app_ee = app_ee[['Candidate ID','Applicant First Name','Applicant Last Name','Employee Id']]
app_ee['Current Employee'] = 'Y'
app_ee['Employee ID'] = app_ee['Employee Id']
app_ee = app_ee[['Candidate ID','Current Employee','Employee ID']]
df_f = df.merge(app_ee, on='Candidate ID', how='left')
df_f['Current Employee'] = df_f['Current Employee'].fillna('N')
df_f = df_f[df_f['Current Employee'] == 'N']
df_f = df_f[['Candidate ID',
'Source System',
'First Name',
'Middle Name',
'Last Name',
'Secondary Last Name',
'Name Prefix',
'Name Suffix',
'Family Name Prefix',
'Preferred First Name',
'Preferred Middle Name',
'Preferred Last Name',
'Preferred Secondary Last Name',
'Phone Country Code',
'Phone Number',
'Phone Extension',
'Phone Device Type',
'Email Address',
'Country ISO Code - Primary',
'Address Line #1 - Primary',
'Address Line #2 - Primary',
'Address Line #3 - Primary',
'Address Line #4 - Primary',
'Address Line #5 - Primary',
'Address Line #6 - Primary',
'Address Line #7 - Primary',
'Address Line #8 - Primary',
'Address Line #9 - Primary',
'Country ISO Code - Local',
'Address Line #1 - Local',
'Address Line #2 - Local',
'Address Line #3 - Local',
'Address Line #4 - Local',
'Address Line #5 - Local',
'City - Primary',
'City - Local',
'City Subdivision - Primary',
'City Subdivision - Local',
'Region - Primary',
'Region Subdivision 1 - Primary',
'Region Subdivision 2 - Primary',
'Region Subdivision 1 - Local',
'Postal Code - Primary',
'Web Address URL',
'Social Media Type',
'Social Network Account URL',
'Social Network Account User Name',
'Confidential',
'Level Reference (Management Level ID)',
'Prospect Status',
'Prospect Type',
'Recruiting Source (Applicant Source ID)',
'Added By Worker',
'Do Not Hire',
'Withdrawn',
'Referred By Worker',
'Referral Job',
'Referral Job Area',
'Referral Relationship',
'Referral Consent Given',
'Current Pre-Hire?',
'Applicant ID',
'Current Employee',
'Employee ID',
'Summary'
]]

df_f.drop_duplicates(inplace=True)

#removed dupes by hand for quickness
df_f = pd.read_csv('candidate_name_contact.csv')

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files')
df_f.to_csv('PROD_candidate_name_contact.txt',sep='|', encoding='utf-8', index=False)
