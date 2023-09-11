##############################################################################
########################### Job Management  ##################################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    filename = config.PATH_WD_IMP + 'data sources\\employee_terms_all.csv'
    df2 = pd.read_csv(filename, dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    df2['Employee Id'] = df2['Employee Id'].astype(str)
    df2 = df2.loc[df2['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    #------------------------------------------------------------------------------
    # Remove anyone from file who is currently active
    actives = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    actives['Date Hired'] = pd.to_datetime(actives['Date Hired'])
    actives = actives[pd.to_datetime(actives['Date Hired']) <= '07/24/2023']
    actives = actives[actives['Employee Status'] == 'Active']

    actives['Employee Id'] = actives['Employee Id'].astype(str)
    df2 = df2.loc[~df2['Employee Id'].isin(actives['Employee Id'].astype(str))]
    #------------------------------------------------------------------------------

    df2['Date Terminated'] = pd.to_datetime(df2['Date Terminated']).dt.strftime("%d-%b-%Y").str.upper()
    # df2 = df2[df2['Date Terminated']>'1/20/2021']

    df2['Employee ID'] = df2['Employee Id']
    df2['Source System'] = 'Kronos'
    df2['Position ID'] = 'P-' + df2['Employee ID'].astype(str)
    df2 = df2[~df2['Default Jobs (HR) External Id'].isna()] 
    df2['Default Jobs (HR) External Id'] = df2['Default Jobs (HR) External Id'].fillna(0)
    df2['Default Jobs (HR) External Id'] = df2['Default Jobs (HR) External Id'].str[:-2] 
    df2['Default Jobs (HR) External Id'] = df2['Default Jobs (HR) External Id'].astype(int)
    df2['Job Code'] = df2['Default Jobs (HR) External Id'].astype(str).str.zfill(4)
    df2['Job Code'] = df2['Job Code'].astype(str)
    df2['Hire Date'] = pd.to_datetime(df2['Date Hired']).dt.strftime("%d-%b-%Y").str.upper()
    df2['Job Posting Title (for the Position)'] = df2['Default Jobs (HR)']
    df2['state_full'] = df2['Default Cost Centers Full Path'].str.split('/').str[1]
    df2['Work Location'] = df2['Work Location'] = np.where(df2['Location(1)'] == 'Home Office','LC10000',
                             np.where(df2['Location(1)'] == 'Work From Home','LC_State '+ df2['state_full'], "LC"+(df2['Location(1)'].astype(str)).str.zfill(5)))

    df2['Default Weekly Hours'] = np.where(df2['Default Jobs (HR)'] == 'Hrly Co Manager', 50, 40)
    df2['Scheduled Weekly Hours'] = df2['Default Weekly Hours']
    df2['Time Type'] = np.where(df2['Default Jobs (HR)'] == 'Team Member', 'Part_Time','Full_Time')
    df2['Pay Rate Type'] = df2['Pay Type']
    df2['Employee Type2'] = np.where(df2['Time Type'] =='Part_Time','Regular Part Time','Regular Full Time')
    df2['Employee Type2'] = np.where(df2['Employee Type'] == 'Member','Member',df2['Employee Type2'])
    df2['Employee Type'] = df2['Employee Type2']

    df2['Supervisory Organization ID'] = 'SUP_Terminated'

    #------------------------------------------------------------------------------
    # Keep only one row - max Hire Date - of each EE
    list_group_by = ['Employee ID', 'Source System', 'Employee Type',
                     'Supervisory Organization ID', 'Job Code', 'Work Location',
                     'Default Weekly Hours', 'Scheduled Weekly Hours',
                     'Time Type', 'Pay Rate Type']
    df2['Hire Datee'] = pd.to_datetime(df2['Hire Date'])
    df2 = df2.groupby(list_group_by, as_index=False)['Hire Date'].max()
    df2['Hire Date'] = pd.to_datetime(df2['Hire Date']).dt.strftime('%d-%b-%Y').str.upper()
    #------------------------------------------------------------------------------

    df_jm = df2

    df_jm['Job Requisition ID'] = ''
    # df_jm['Source System'] = ''
    df_jm['Hire Reason'] = ''
    df_jm['First Day of Work'] = ''
    df_jm['Probation Start Date'] = ''
    df_jm['Probation End Date'] = ''
    df_jm['End Employment Date'] = ''
    df_jm['Position Start Date for Conversion'] = ''
    df_jm['Job Profile Start Date for Conversion'] = ''
    df_jm['Position Title'] = ''
    df_jm['Business Title'] = ''
    df_jm['Work Space'] = ''
    df_jm['Paid FTE'] = ''
    df_jm['Working FTE'] = ''
    df_jm['Company Insider Type'] = ''
    df_jm['Company Insider Type #2'] = ''
    df_jm['Company Insider Type #3'] = ''
    df_jm['Company Insider Type #4'] = ''
    df_jm['Company Insider Type #5'] = ''
    df_jm['Work Shift'] = ''
    df_jm['Additional Job Classification #1'] = ''
    df_jm['Additional Job Classification #2'] = ''
    df_jm['Additional Job Classification #3'] = ''
    df_jm['Additional Job Classification #4'] = ''
    df_jm['Additional Job Classification #5'] = ''
    df_jm['Additional Job Classification #6'] = ''
    df_jm['Workers Compensation Code'] = ''

    df_jm = df_jm[['Employee ID', 'Source System', 'Employee Type',
                   'Hire Reason', 'First Day of Work', 'Hire Date',
                   'Probation Start Date', 'Probation End Date',
                   'End Employment Date', 'Position Start Date for Conversion',
                   'Supervisory Organization ID', 'Job Code',
                   'Job Profile Start Date for Conversion', 'Position Title',
                   'Business Title', 'Work Location', 'Work Space',
                   'Default Weekly Hours', 'Scheduled Weekly Hours', 'Paid FTE',
                   'Working FTE', 'Time Type', 'Pay Rate Type',
                   'Company Insider Type', 'Work Shift',
                   'Additional Job Classification #1',
                   'Additional Job Classification #2',
                   'Additional Job Classification #3',
                   'Additional Job Classification #4',
                   'Workers Compensation Code']]
    
    df_jm = pd.concat([df_jm,df_pm3])
    
    # df_jm = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\emp-job-man_fixes.csv",encoding="cp1251")
    # df_jm['Job Code'] = df_jm['Job Code'].astype(str).str.zfill(4)
    # df_jm['Job Code'] = df_jm['Job Code'].astype(str)
    # df_jm.to_csv('emp_job_mgt2.txt',sep='|', encoding='utf-8', index=False)
    df_jm['Employee ID'] = df_jm['Employee ID'].astype(int)
    write_to_csv(df_jm, 'emp_job_mgt.txt')
