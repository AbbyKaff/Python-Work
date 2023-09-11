# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 06:05:25 2023

@author: akaff
"""

##############################################################################
#################### Additional Benefit Plans ############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8, set_max_event_date)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    active_bene = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72830428.csv', dtype='object', encoding='cp1251')
    active_bene = active_bene[active_bene['Coverage Name'] != 'Waived']

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    active_bene['Employee Id'] = active_bene['Employee Id'].astype(str)
    active_bene = active_bene.loc[active_bene['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_bene_a = active_bene[active_bene['Benefit Type'].isin(['EAP', 'Identity Protection', 'Legal'])]
    active_bene_a = active_bene_a.rename(columns={'Employee Id': 'Employee ID'})
    active_bene_a['Coverage Effective From']  = pd.to_datetime(active_bene_a['Coverage Effective From'])
    jan = pd.to_datetime('1/1/2023')
    active_bene_a.loc[active_bene_a['Coverage Effective From'] < jan,'Coverage Effective From'] = pd.to_datetime('1/1/23')
    
    active_bene_a = set_max_event_date(active_bene_a)

    active_bene_a['Benefit Event Type'] = 'BEN_CONVERSION_ADDLBENS'

    dict_coverage_plan = {'Identity Protection-Employee Only': 'USA - Voluntary ID Theft - Allstate',
                          'Identity Protection-Family': 'USA - Voluntary ID Theft - Allstate',
                          'Legal Services-Legal Low Plan': 'USA - Voluntary Legal - MetLaw Low Plan',
                          'Legal Services-Legal High Plan': 'USA - Voluntary Legal - MetLaw High Plan',
                          }

    active_bene_a['Additional Benefits Plan'] = active_bene_a['Benefit Plan Name'] + '-' + active_bene_a['Coverage Name']
    active_bene_a['Additional Benefits Plan'] = active_bene_a['Additional Benefits Plan'].replace(dict_coverage_plan)

    dict_coverage_target = {'Employee Only': 'Individual',
                            'Legal Low Plan': '',
                            'Legal High Plan': ''}

    active_bene_a['Additional Benefits Coverage Target'] = active_bene_a['Coverage Name'].replace(dict_coverage_target)
    active_bene_a['Employee ID'] = active_bene_a['Employee ID'].astype(int)
    #active_bene_a['Insurance Coverage'] = active_bene_a['# Units']

    active_bene_a = active_bene_a[['Employee ID', 'Event Date', 'Benefit Event Type',
                                   'Additional Benefits Plan',
                                   'Additional Benefits Coverage Target']]
    

    active_bene_a['Source System'] = 'Kronos'
    active_bene_a['Original Coverage Begin Date'] = ''
    active_bene_a['Deduction Begin Date'] = ''
    active_bene_a['Percentage Contribution Value'] = ''
    active_bene_a['Flat Contribution Amount'] = ''
    active_bene_a['Employee Cost PreTax'] = ''
    active_bene_a['Employee Cost PostTax'] = ''
    active_bene_a['Employer Cost NonTaxable'] = ''
    active_bene_a['Employer Cost Taxable'] = ''
    active_bene_a['Enrollment Signature Date'] = ''
    active_bene_a['Signing Worker'] = ''
    
    #	USA - EAP - ComPsych
    active_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    active_ees = active_ees[['Employee Id','Date Hired']]
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    loas = loas[['Employee Id','Date Hired']]
    active_ees = pd.concat([active_ees, loas])
    active_ees['Additional Benefits Plan'] = 'USA - EAP - ComPsych'
    active_ees['Additional Benefits Coverage Target'] = 'Individual'
    active_ees['Date Hired'] = pd.to_datetime(active_ees['Date Hired'])
    active_ees['Coverage Effective From'] = active_ees['Date Hired']
    jan = pd.to_datetime('1/1/23')
    active_ees.loc[active_ees['Date Hired'] < jan,'Coverage Effective From'] = pd.to_datetime('1/1/23')
    active_ees['Employee ID'] = active_ees['Employee Id']
    active_ees['Event Date'] = active_ees['Coverage Effective From']
    active_ees['Benefit Event Type'] = 'BEN_CONVERSION_ADDLBENS'
    active_ees['Source System'] = 'Kronos'
    active_ees['Original Coverage Begin Date'] = ''
    active_ees['Deduction Begin Date'] = ''
    active_ees['Percentage Contribution Value'] = ''
    active_ees['Flat Contribution Amount'] = ''
    active_ees['Employee Cost PreTax'] = ''
    active_ees['Employee Cost PostTax'] = ''
    active_ees['Employer Cost NonTaxable'] = ''
    active_ees['Employer Cost Taxable'] = ''
    active_ees['Enrollment Signature Date'] = ''
    active_ees['Signing Worker'] = ''
    active_ees_a = active_ees[['Employee ID', 'Source System', 'Event Date',
                                   'Benefit Event Type', 'Additional Benefits Plan',
                                   'Additional Benefits Coverage Target',
                                   'Original Coverage Begin Date', 'Deduction Begin Date',
                                   'Percentage Contribution Value', 'Flat Contribution Amount',
                                   'Employee Cost PreTax', 'Employee Cost PostTax',
                                   'Employer Cost NonTaxable', 'Employer Cost Taxable',
                                   'Enrollment Signature Date', 'Signing Worker']]
    
    active_bene_a = active_bene_a[['Employee ID', 'Source System', 'Event Date',
                                   'Benefit Event Type', 'Additional Benefits Plan',
                                   'Additional Benefits Coverage Target',
                                   'Original Coverage Begin Date', 'Deduction Begin Date',
                                   'Percentage Contribution Value', 'Flat Contribution Amount',
                                   'Employee Cost PreTax', 'Employee Cost PostTax',
                                   'Employer Cost NonTaxable', 'Employer Cost Taxable',
                                   'Enrollment Signature Date', 'Signing Worker']]
    
    #active_bene_a = pd.concat([active_bene_a,active_ees_a])
    
    active_bene_a.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\check_addben.csv')
    #active_bene_a = pd.read_csv('add_ben_man.csv')
    active_bene_a['Event Date'] = pd.to_datetime(active_bene_a['Event Date'])
    
    active_bene_a['Event Date'] = active_bene_a['Event Date'].dt.strftime("%d-%b-%Y").str.upper()
    active_bene_a['Employee ID'] = active_bene_a['Employee ID'].astype(int)
    write_to_csv(active_bene_a, 'add_bene_election.txt')
