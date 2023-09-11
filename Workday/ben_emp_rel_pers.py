# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 16:51:44 2023

@author: akaff
"""

##############################################################################
######################### Employee Related Person ############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd
import os
import config
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)

path = os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion')

# -----------------------------------------------------------------------------
if __name__ == '__main__':

    #Benefits > Dependents
    dep = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72593419.csv', dtype='object', encoding='cp1251')
    dep = dep[dep['Dependent Type']!= 'Contingent Beneficiary']
    dep = dep[dep['Employee Status'] != 'Not in Payroll']
    dep.drop_duplicates(inplace=True)
    dep['Dependent First Name'] = dep['Dependent First Name'].str.title()
    dep['Dependent Last Name'] = dep['Dependent Last Name'].str.title()
    #emer = pd.read_csv(config.PATH_WD_IMP + 'data files\\all_contacts_july.csv',encoding='cp1251')
    #------------------------------------------------------------------------------
    #cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    #cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'data files\worker_id_fix.csv')
    #dep['Employee Id'] = dep['Employee Id'].astype(str)
    #dep = dep.loc[dep['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    #Exclude certain groups
    dep = dep[dep['Benefit Plan Name']!= 'Basic AD&D Voya']
    dep = dep.rename(columns={'Employee Id': 'Employee ID'})

    dep_cc = dep[['Employee ID', 'Coverage Name', 'Dependent Type',
                  'Dependent First Name', 'Dependent Last Name',
                  'Dependent Gender', 'Dependent Date Birthday','Dependent SS#']]

    dep_cc = dep_cc[dep_cc['Coverage Name'] != 'Employee']
    dep_cc = dep_cc[dep_cc['Dependent Type'].isin(['Child', 'Spouse'])]

    #Dependent
    dep_c = dep_cc[['Employee ID', 'Dependent Type', 'Dependent First Name',
                    'Dependent Last Name', 'Dependent Gender',
                    'Dependent Date Birthday','Dependent SS#']]
    dep_c.drop_duplicates(inplace=True)
    
    #sort by Spouse first
    dep_c.sort_values(by=['Employee ID', 'Dependent Type'],ascending=False,inplace=True)

    

    dep_c['T'] = dep_c.groupby('Employee ID')['Employee ID'].transform('count')
    dep_c['No'] = dep_c.groupby('Employee ID')['Employee ID'].cumcount() + 1
    dep_c['Dependent ID'] = dep_c['Employee ID'].astype(str) + '-D0' + dep_c['No'].astype(str)
    dep_c['dep_full'] = dep_c['Dependent First Name'].str.lower() + ' ' + dep_c['Dependent Last Name'].str.lower()
    dep_c['Dependent Gender'] = dep_c['Dependent Gender'].fillna('U')
    check_spouse = dep_c[(dep_c['Dependent Type'] == 'Spouse') & (~dep_c['Dependent ID'].str.contains('D01',na=False))]
    #TODO
    #Check if self is dependent
    #Check dupe dependent issues
    #export file to filter on duplicate names, verify names with Stephanie as duplicates will appear on final file if so
    #dep_c.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\dep_dupe_check.csv')
    dep_ee = dep[['Employee ID','First Name','Last Name']]
    dep_ee['Full name'] = dep_ee['First Name'].str.lower() + ' ' + dep_ee['Last Name'].str.lower()
    dep_c = dep_c.merge(dep_ee,how='left',on='Employee ID')
    dep_c.drop_duplicates(inplace=True)
    check_ee = dep_c[dep_c['Full name'] == dep_c['dep_full']]
    check_ee.drop_duplicates(inplace=True)
    
    

    #Beneficiary
    dep_bb = dep[dep['Dependent Type'] == 'Beneficiary']
    
    dep_bb = dep_bb[dep_bb['Benefit Plan Name'].isin(['Basic Life Voya','Voluntary Employee Life Voya'])]

    dep_b = dep_bb[['Employee ID', 'Dependent First Name', 'Dependent Last Name','Dependent Date Birthday']]
    dep_b['Dependent First Name'] = dep_b['Dependent First Name'].str.title()
    dep_b['Dependent Last Name'] = dep_b['Dependent Last Name'].str.title()   
    dep_b.drop_duplicates(inplace=True)

    dep_b['T'] = dep_b.groupby('Employee ID')['Employee ID'].transform('count')
    dep_b['No'] = dep_b.groupby('Employee ID')['Employee ID'].cumcount() + 1
    dep_b['Beneficiary ID'] = dep_b['Employee ID'].astype(str) + '-B0' + dep_b['No'].astype(str)
    dep_b['dep_full'] = dep_b['Dependent First Name'].str.lower() + ' ' + dep_b['Dependent Last Name'].str.lower()
    #export file to filter on duplicate names
    #dep_b.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\ben_dupe_check.csv')
    dep_b = dep_b[['Employee ID','dep_full', 'Beneficiary ID','Dependent Date Birthday']]
    dep_b['Dependent Date Birthday'].fillna('1/1/2023',inplace=True)

    dep_df = dep_c.merge(dep_b, on=['Employee ID','dep_full','Dependent Date Birthday'], how='outer')
    dep_df.drop_duplicates(inplace=True)
    dep_df = dep_df[['dep_full', 'Beneficiary ID', 'Dependent ID',
                     'Employee ID', 'Dependent Date Birthday','Dependent SS#']]
    dep_df.drop_duplicates(inplace=True)
    #dep_df['dep_full'] = np.where(dep_df['dep_full_x'].isna(),dep_df['dep_full_y'],dep_df['dep_full_x'])
    #all_cnts.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\bene_dep_check.csv') 
    #Clean all
    
    dep['dep_full'] = dep['Dependent First Name'].str.lower() + ' ' + dep['Dependent Last Name'].str.lower()
    dep_c2 = dep
    #seperate ssns & non ssns for merging
    dep_df1 = dep_df[~dep_df['Dependent SS#'].isna()]
    dep_df1['Employee ID'] = dep_df1['Dependent ID'].str[:-4]
    dep_c2['Employee ID'] = dep_c2['Employee ID'].astype(str)
    dep_df2 = dep_df[dep_df['Dependent SS#'].isna()]
    dep_df2['Employee ID'] = dep_df2['Beneficiary ID'].str[:-4]
    dep_df2.loc[(dep_df2['Employee ID'].isna()),'Employee ID'] = dep_df2['Dependent ID'].str[:-4]
    
    all_cnts = dep_c2.merge(dep_df1,how='left',on=['dep_full','Dependent SS#','Employee ID','Dependent Date Birthday'])
    
    ben_conts = dep_c2.merge(dep_df2,how='left',on=['dep_full','Employee ID','Dependent Date Birthday'])
    
    all_cnts2 = all_cnts.loc[~((all_cnts['Dependent ID'].isna()) & (all_cnts['Beneficiary ID'].isna()))]
    ben_conts2 = ben_conts.loc[~((ben_conts['Dependent ID'].isna()) & (ben_conts['Beneficiary ID'].isna()))]
    df_all = pd.concat([all_cnts2,ben_conts2])
    
    df_all['Dependent SS#'] = df_all['Dependent SS#_x']
    #df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\checkdeps.csv')
    #merge back dep plans
    


    df_all.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\dependents.csv')
    
    #dep_c2 = dep_c2[dep_c2['Dependent Type'].isin(['Child', 'Spouse'])]
    #dep_c2 = dep_c2[['Employee ID', 'dep_full', 'Dependent Type','Dependent SS#']]
    #dep_c2.drop_duplicates(inplace=True)

    #dep_df2 = dep_df.merge(dep_c2, on=['Dependent SS#','dep_full'], how='left')

    #dep_df2['Employee ID'] = np.where(dep_df2['Employee ID'].isna(), dep_df2['Beneficiary ID'].str[:-4], dep_df2['Employee ID'])
    #dep_df2['Related Person Relationship'] = np.where(dep_df2['Dependent Type'].isna(), 'Other', dep_df2['Dependent Type'])
    #dep_df2.drop_duplicates(inplace=True)

    #dep_df2['Legal First Name'] = dep_df2['dep_full'].str.split(' ').str[0].str.title()
    #dep_df2['Legal Last Name'] = dep_df2['dep_full'].str.split(' ').str[1].str.title()
    #dep_df2['Trust Name'] = np.where(dep_df2['Employee ID'] == '104418',dep_df2['dep_full'], '')
    #dep_df2['Legal First Name'] = np.where(dep_df2['Employee ID'] == '104418','na',dep_df2['Legal First Name'])
    #dep_df2['Legal Last Name'] = np.where(dep_df2['Employee ID'] == '104418','na',dep_df2['Legal Last Name'])
    #dep_df2['National ID'] = dep_df2['Dependent SS#'].str.replace('-','')
    #dep_df2['National ID Type'] = 'USA-SSN'
    #dep_df2['Same Address as Employee?']= 'Y'
    
    df_all['Related Person Relationship'] = df_all['Dependent Relationship']
    df_all['Legal First Name'] = df_all['Dependent First Name']
    df_all['Legal Last Name'] = df_all['Dependent Last Name']
    df_all = df_all.rename(columns={'Dependent Date Birthday': 'Date of Birth',
                                  'Dependent Gender': 'Gender'})
    df_all['Date of Birth'] = pd.to_datetime(df_all['Date of Birth']).dt.strftime("%d-%b-%Y").str.upper()
    df_all['Gender'] = np.where(df_all['Gender'] ==  'F','Female',
                               np.where(df_all['Gender'] == 'M','Male','Choose not to Disclose'))
    df_all['Trust Name'] = ''
    df_all['National ID'] = df_all['Dependent SS#'].str.replace('-','')
    df_all['National ID'] = df_all['National ID'].str.zfill(9)
    df_all['National ID Type'] = 'USA-SSN'
    df_all['Same Address as Employee?'] = 'Y'
    dep_f = df_all[['Employee ID', 'Dependent ID', 'Beneficiary ID',
                     'Related Person Relationship', 'Legal First Name',
                     'Legal Last Name', 'Trust Name', 'Gender',
                     'Date of Birth','National ID','National ID Type','Same Address as Employee?']]
    dep_f.drop_duplicates(inplace=True)
    dep_f['Emergency Contact ID'] = ''
    dep_f['Emergency Contact Priority'] = ''
    dep_f['Phone Device Type - Primary Home'] = ''
    dep_f['Country ISO Code - Primary Home'] = ''
    dep_f['Phone Number - Primary Home'] = ''
    
    
    #add in full contact sheet
    emer_con = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    emer_con['Worker ID'] = emer_con['Employee Id']
    emer_con['Emergency Contact ID'] = (emer_con['Worker ID'].astype(str) + '-E01').astype(str)
    emer_con['Related Person Relationship'] = emer_con['Account Contact #1: Relationship']
    emer_con['Related Person Relationship'] = emer_con['Related Person Relationship'].fillna('other')
    emer_con['Emergency Contact Priority'] = '1'

    emer_con['Legal Last Name'] = emer_con['Account Contact #1: Last Name']
    emer_con['Legal First Name'] = emer_con['Account Contact #1: First Name']

    emer_con = emer_con.loc[~emer_con['Legal Last Name'].isnull()]

    emer_con['Phone Number - Primary Home'] = np.where(emer_con['Account Contact #1: Home Phone'].notnull(), emer_con['Account Contact #1: Home Phone'], emer_con['Account Contact #1: Cell Phone'])
    emer_con['Phone Number - Primary Home'] = emer_con['Phone Number - Primary Home'].replace('-', '', regex=True).astype(str)
    emer_con['Phone Number - Primary Home'] = emer_con['Phone Number - Primary Home'].replace('nan', '', regex=True).astype(str)

    emer_con.loc[emer_con['Phone Number - Primary Home'] != '', 'Phone Device Type - Primary Home'] = 'Mobile'
    emer_con.loc[emer_con['Phone Number - Primary Home'] != '', 'Country ISO Code - Primary Home'] = 'USA'

    emer_con['Employee ID'] = emer_con['Worker ID']
    df2 = emer_con[['Employee ID', 'Emergency Contact ID', 'Related Person Relationship', 'Emergency Contact Priority', 'Phone Device Type - Primary Home', 'Country ISO Code - Primary Home', 'Phone Number - Primary Home', 'Legal First Name', 'Legal Last Name']]
    df2.drop_duplicates(inplace=True)
    df2['Dependent ID'] = ''
    df2['Beneficiary ID'] = ''
    df2['Trust Name'] = ''
    df2['Gender'] = ''
    df2['Date of Birth'] = ''
    df2['National ID'] = ''
    df2['National ID Type'] = ''
    df2['Same Address as Employee?'] = ''
    
    df2 = df2[['Beneficiary ID',
     'Country ISO Code - Primary Home',
     'Date of Birth',
     'Dependent ID',
     'Emergency Contact ID',
     'Emergency Contact Priority',
     'Employee ID',
     'Gender',
     'Legal First Name',
     'Legal Last Name',
     'National ID',
     'National ID Type',
     'Phone Device Type - Primary Home',
     'Phone Number - Primary Home',
     'Related Person Relationship',
     'Same Address as Employee?',
     'Trust Name']]
    
    df_c = pd.concat([dep_f, df2])
    
    # dep_f.to_csv(config.PATH_WD_IMP + 'benefits_dc\\manual_fix_ERP.csv')
    # dep_ff = pd.read_csv(config.PATH_WD_IMP + 'benefits_dc\\fixed_erp.csv')

    new_columns = {
    	'Source System': '',
    	'Emergency Contact ID': '',
    	'Country ISO Code - Legal Name': '',
    	'Legal Middle Name': '',
    	'Prefix': '',
    	'Suffix': '',
    	'Language': '',
    	'Emergency Contact Priority': '',
    	'Same Phone as Employee?': '',
    	'Country ISO Code - Primary Home': '',
    	'International Phone Code - Primary Home': '',
    	'Area Code - Primary Home': '',
    	'Phone Extension - Primary Home': '',
    	'Country ISO Code - Additional Home #1': '',
    	'International Phone Code - Additional Home #1': '',
    	'Area Code - Additional Home #1': '',
    	'Phone Number - Additional Home #1': '',
    	'Phone Extension - Additional Home #1': '',
    	'Phone Device Type - Additional Home #1': '',
    	'Country ISO Code - Additional Home #2': '',
    	'International Phone Code - Additional Home #2': '',
    	'Area Code - Additional Home #2': '',
    	'Phone Number - Additional Home #2': '',
    	'Phone Extension - Additional Home #2': '',
    	'Phone Device Type - Additional Home #2': '',
    	'Country ISO Code - Primary Work': '',
    	'International Phone Code - Primary Work': '',
    	'Area Code - Primary Work': '',
    	'Phone Number - Primary Work': '',
    	'Phone Extension - Primary Work': '',
    	'Phone Device Type - Primary Work': '',
    	'Country ISO Code - Additional Work #1': '',
    	'International Phone Code - Additional Work #1': '',
    	'Area Code - Additional Work #1': '',
    	'Phone Number - Additional Work #1': '',
    	'Phone Extension - Additional Work #1': '',
    	'Phone Device Type - Additional Work #1': '',
    	'Country ISO Code - Additional Work #2': '',
    	'International Phone Code - Additional Work #2': '',
    	'Area Code - Additional Work #2': '',
    	'Phone Number - Additional Work #2': '',
    	'Phone Extension - Additional Work #2': '',
    	'Phone Device Type - Additional Work #2': '',
    	'Email Address - Primary Home': '',
    	'Email Address - Primary Work': '',
    	'Email Address - Additional Home': '',
    	'Email Address - Additional Work': '',
    	'Country ISO Code - Home': '',
    	'Address Line #1 - Home': '',
    	'Address Line #2 - Home': '',
    	'City - Home': '',
    	'City Subdivision - Home': '',
    	'Region - Home': '',
    	'Region Subdivision - Home': '',
    	'Postal Code - Home': '',
    	'Country ISO Code - Alt Home #1': '',
    	'Address Line #1 - Alt Home #1': '',
    	'Address Line #2 - Alt Home #1': '',
    	'City - Alt Home #1': '',
    	'City Subdivision - Alt Home #1': '',
    	'Region - Alt Home #1': '',
    	'Region Subdivision - Alt Home #1': '',
    	'Postal Code - Alt Home #1': '',
    	'Country ISO Code - Alt Home #2': '',
    	'Address Line #1 - Alt Home #2': '',
    	'Address Line #2 - Alt Home #2': '',
    	'City - Alt Home #2': '',
    	'City Subdivision - Alt Home #2': '',
    	'Region - Alt Home #2': '',
    	'Region Subdivision - Alt Home #2': '',
    	'Postal Code - Alt Home #2': '',
    	'Effective Date': '',
    	'Reason': '',
    	'Uses Tobacco?': '',
    	'Full-time Student?': '',
    	'Dependent for Payroll Purposes': '',
    	'Student Status Start Date': '',
    	'Student Status End Date': '',
    	'Disabled?': '',
    	'Could Be Covered For Health Care Coverage Elsewhere': '',
    	'Could Be Covered For Health Care Coverage Elsewhere Date': '',
    	'Benefit Coverage Type - Medical': '',
    	'Start Date - Medical': '',
    	'End Date - Medical': '',
    	'Benefit Coverage Type - Dental': '',
    	'Start Date - Dental': '',
    	'End Date - Dental': '',
    	'Benefit Coverage Type - Vision': '',
    	'Start Date - Vision': '',
    	'End Date - Vision': '',
    	'Custom ID - Custom ID #1': '',
    	'Custom ID Type - Custom ID #1': '',
    	'Issued Date - Custom ID #1': '',
    	'Expiration Date - Custom ID #1': ''}
    df_new = pd.DataFrame(new_columns, index=[0])
    dep_f = pd.concat([df_c, df_new], axis=1)

    dep_f['Country ISO Code - Legal Name'] = 'USA'
    
    dep_f = dep_f[['Employee ID',
'Source System',
'Dependent ID',
'Beneficiary ID',
'Emergency Contact ID',
'Related Person Relationship',
'Country ISO Code - Legal Name',
'Legal First Name',
'Legal Middle Name',
'Legal Last Name',
'Prefix',
'Suffix',
'Trust Name',
'Language',
'Emergency Contact Priority',
'Same Phone as Employee?',
'Country ISO Code - Primary Home',
'International Phone Code - Primary Home',
'Area Code - Primary Home',
'Phone Number - Primary Home',
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
'Area Code - Primary Work',
'Phone Number - Primary Work',
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
'Email Address - Primary Home',
'Email Address - Primary Work',
'Email Address - Additional Home',
'Email Address - Additional Work',
'Same Address as Employee?',
'Country ISO Code - Home',
'Address Line #1 - Home',
'Address Line #2 - Home',
'City - Home',
'City Subdivision - Home',
'Region - Home',
'Region Subdivision - Home',
'Postal Code - Home',
'Country ISO Code - Alt Home #1',
'Address Line #1 - Alt Home #1',
'Address Line #2 - Alt Home #1',
'City - Alt Home #1',
'City Subdivision - Alt Home #1',
'Region - Alt Home #1',
'Region Subdivision - Alt Home #1',
'Postal Code - Alt Home #1',
'Country ISO Code - Alt Home #2',
'Address Line #1 - Alt Home #2',
'Address Line #2 - Alt Home #2',
'City - Alt Home #2',
'City Subdivision - Alt Home #2',
'Region - Alt Home #2',
'Region Subdivision - Alt Home #2',
'Postal Code - Alt Home #2',
'Date of Birth',
'Gender',
'National ID',
'National ID Type',
'Effective Date',
'Reason',
'Uses Tobacco?',
'Full-time Student?',
'Dependent for Payroll Purposes',
'Student Status Start Date',
'Student Status End Date',
'Disabled?',
'Could Be Covered For Health Care Coverage Elsewhere',
'Could Be Covered For Health Care Coverage Elsewhere Date',
'Benefit Coverage Type - Medical',
'Start Date - Medical',
'End Date - Medical',
'Benefit Coverage Type - Dental',
'Start Date - Dental',
'End Date - Dental',
'Benefit Coverage Type - Vision',
'Start Date - Vision',
'End Date - Vision',
'Custom ID - Custom ID #1',
'Custom ID Type - Custom ID #1',
'Issued Date - Custom ID #1',
'Expiration Date - Custom ID #1']]
    
    #dep_f.to_csv('test_erp.csv')
    dep_f['Legal Last Name'] = dep_f['Legal Last Name'].str.replace('?','')
    dep_f['Legal First Name'] = dep_f['Legal First Name'].str.replace('?','')
    dep_f.drop_duplicates(inplace=True)
    dep_f['National ID Type'] = np.where((dep_f['National ID'].isna()) | (dep_f['National ID'] == ''),'','USA-SSN')
    #dep_f = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\test_erp.csv")
    
    #dep_f = pd.read_excel(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\test_erp.xlsx')
    #dep_f['National ID'] = dep_f['National ID'].astype(str).str.zfill(9)
    #dep_f['National ID'] = dep_f['National ID'].str[:-2]
    #dep_f['National ID'] = np.where(dep_f['National ID'] == '00000000n','',dep_f['National ID'])
    
    write_to_csv(dep_f, 'emp_rel_per.txt')
