##############################################################################
######################### EMP-Base Compensation ##############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd
import os
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    df2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\employee_terms_all.csv', dtype='object', encoding='cp1251')
    df2['Default Jobs (HR) External Id'] = df2['Default Jobs (HR) External Id'].str[:-2]
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    df = pd.concat([df,df2, loas])
    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    df['Employee Id'] = df['Employee Id'].astype(str)
    df = df.loc[df['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    df = df.replace('^[-]$', '', regex=True)

    df = modify_amount(df, 'Hourly Pay')
    df = modify_amount(df, 'Salary')

    # df['Employee Pay Grade'] = np.where(df['Default Jobs (HR)'] == 'Team Member','Team Member',
    #                            np.where(df['Default Jobs (HR)'] == 'Shift Manager','Shift Manager',
    #                                     np.where(df['Default Jobs (HR)'] == 'Hrly Co Manager','Co Manager',
    #                                              np.where(df['Default Jobs (HR)'] == 'Restaurant General Manager', 'General Manager',
    #                                                       np.where(df['Default Jobs (HR)'] == 'Managing Partner','Managing Partner',df['Employee Pay Grade'])))))

    df.loc[df['Default Jobs (HR)'] == 'Hrly Co Manager', 'Employee Pay Grade'] = 'Co Manager'
    df.loc[df['Default Jobs (HR)'] == 'Restaurant General Manager', 'Employee Pay Grade'] = 'General Manager'

    df['Compensation Change Reason'] = 'Default_Request_Compensation_Change _Conversion_Conversion'
    df['Effective Date'] = np.where(pd.to_datetime(df['Date Hired']) <= '2023-01-28', '01-JAN-2023', pd.to_datetime(df['Date Hired']).dt.strftime("%d-%b-%Y").str.upper())
    df['Compensation Package'] = 'KBP Compensation Package'
    df['Employee ID'] = df['Employee Id']
    df['Source System'] = 'Kronos'
    df['Sequence #'] = ''
    df['Position ID'] =''
    df['Compensation Grade Profile'] = ''
    df['Compensation Step'] = ''
    df['Progression Start Date'] = ''
    df['Primary Compensation Basis'] = ''
    df['Primary Compensation Basis Amount Change'] = ''
    df['Primary Compensation Basis Percent Change'] = ''

    df['Compensation Plan - Base'] = np.where(df['Pay Type'] == 'Hourly', 'Hourly Plan', 'Salary Plan')
    df['Compensation Element Amount - Base'] = np.where(df['Pay Type'] == 'Hourly', df['Hourly Pay'], df['Salary'])
    df['Frequency - Base'] = np.where(df['Pay Type'] == 'Hourly', 'Hourly', 'Annual')

    df['Currency Code - Base'] = 'USD'
    df['Compensation Plan - Addl'] = ''
    df['Compensation Element Amount - Addl'] = ''
    df['Currency Code - Addl'] = ''
    df['Frequency - Addl'] = ''
    df['Unit Salary Plan'] = ''
    df['Per Unit Amount - Unit Salary'] = ''
    df['Currency Code - Unit Salary'] = ''
    df['Number of Units - Unit Salary'] = ''
    df['Frequency - Unit Salary'] = ''
    df['Commission Plan #1'] = ''
    df['Target Amount - Commission Plan #1'] = ''
    df['Currency Code - Commission Plan #1'] = ''
    df['Frequency - Commission Plan #1'] = ''
    df['Draw Amount - Commission Plan #1'] = ''
    df['Frequency for Draw Amount - Commission Plan #1'] = ''
    df['Draw Duration - Commission Plan #1'] = ''
    df['Recoverable - Commission Plan #1'] = ''
    df['Commission Plan #2'] = ''
    df['Target Amount - Commission Plan #2'] = ''
    df['Currency Code - Commission Plan #2'] = ''
    df['Frequency - Commission Plan #2'] = ''
    df['Draw Amount - Commission Plan #2'] = ''
    df['Frequency for Draw Amount - Commission Plan #2'] = ''
    df['Draw Duration - Commission Plan #2'] = ''
    df['Recoverable - Commission Plan #2'] = ''

    comp_grades = pd.read_csv(config.PATH_WD_IMP + "data files\comp_grades.csv", encoding="cp1251")
    comp_grades['Job Code'] = comp_grades['Job Code'].astype(str)
    comp_grades['Job Code'] = comp_grades['Job Code'].str.zfill(4)
    df['Default Jobs (HR) External Id'] = df['Default Jobs (HR) External Id'].astype(str)
    df['Default Jobs (HR) External Id'] = df['Default Jobs (HR) External Id'].str.zfill(4)
    df2 = df.merge(comp_grades, left_on='Default Jobs (HR) External Id', right_on='Job Code', how='left')
    # df2['Compensation Grade'] = df2['Grade/Profile ID']
    df2['Compensation Grade'] = df2['Compensation Grade ID']
    df2['Compensation Package'] = np.where(df2['Employee EIN'] == 'KBP Foods', 'KBP Compensation Package - Foods',
                                           np.where(df2['Employee EIN'] == 'KBP Bells','KBP Compensation Package - Bells',
                                                    np.where(df2['Employee EIN'] == 'KBP Inspired','KBP Compensation Package - Inspired','KBP Compensation Package - Restaurant Services Group')))
    df_emp_base_comp = df2[['Employee ID', 'Source System', 'Sequence #',
                            'Compensation Change Reason', 'Position ID',
                            'Effective Date', 'Compensation Package',
                            'Compensation Grade', 'Compensation Grade Profile',
                            'Compensation Step', 'Progression Start Date',
                            'Primary Compensation Basis',
                            'Primary Compensation Basis Amount Change',
                            'Primary Compensation Basis Percent Change',
                            'Compensation Plan - Base',
                            'Compensation Element Amount - Base',
                            'Currency Code - Base', 'Frequency - Base',
                            'Compensation Plan - Addl',
                            'Compensation Element Amount - Addl',
                            'Currency Code - Addl', 'Frequency - Addl',
                            'Unit Salary Plan', 'Per Unit Amount - Unit Salary',
                            'Currency Code - Unit Salary', 'Number of Units - Unit Salary',
                            'Frequency - Unit Salary', 'Commission Plan #1',
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


    write_to_csv(df_emp_base_comp, 'emp_base_comp.txt')
    
    df_emp_base_comp = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\manually fixed comp.xlsx")
    