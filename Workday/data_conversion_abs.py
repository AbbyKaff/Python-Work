import pandas as pd
import numpy as np
import os
import datetime
from datetime import date
from datetime import datetime
from datetime import timedelta
import xlrd
import glob as glob

cut_off_ees = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\hcm_dc\worker_phone.txt",sep="|")
cut_off_ids = cut_off_ees[['Worker ID']]

##############################################################################
################################ OVERVIEW ####################################
##############################################################################


"""
Leave of Absence	In Scope
Override Balances	In Scope
"""



##############################################################################
################################## LOA #######################################
##############################################################################

"""
Worker ID
Leave Type - Conversion
First Day of Leave
Estimated Last Day of Leave - 1/20/2033

"""
allowance_pl = pd.read_excel('./../USA_KBP_CNP_Absence_Template_01102023.xlsx', sheet_name='Leave of Absence')

allowance_pl_c = allowance_pl.filter(regex='Required')
allowance_data = pd.read_excel('./../USA_KBP_CNP_Absence_Template_01102023.xlsx', sheet_name='Leave of Absence',skiprows=2)

allowance_data_c = allowance_data.iloc[0:0]
df_loa = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\Workday loa info.xlsx")
#DD-MON-YYYY
df_loa['Custom - Last Day Worked'] = pd.to_datetime(df_loa['Custom - Last Day Worked'])
df_loa['First Day of Leave'] = df_loa['Custom - Last Day Worked'] + timedelta(days=1)
df_loa['First Day of Leave'] = df_loa['First Day of Leave'].dt.strftime("%d-%b-%Y").str.upper()
df_loa['Estimated Last Day of Leave'] = '20-JAN-2033'
df_loa['First Day of Leave'] = df_loa['First Day of Leave'].fillna('01-JAN-2020')
#df_loa['First Day of Leave'] = np.where(df_loa['First Day of Leave']=='nan','01-JAN-2020',df_loa['First Day of Leave'])
df_loa['Worker ID'] = df_loa['Employee Id']
df_loa['Reason'] = 'Conversion'

allowance_data_c['Worker ID'] = df_loa['Worker ID']

df_loa_c = df_loa[['Worker ID','Leave Type','First Day of Leave','Estimated Last Day of Leave']]

allowance_data_c = allowance_data_c.drop(['Reason','First Day of Leave','Estimated Last Day of Leave'],axis=1)

load = allowance_data_c.merge(df_loa_c, on='Worker ID')

load = load[['Worker ID', 'Source System', 'Position ID', 'Leave Type', 'Reason',
       'Last Day of Work', 'First Day of Leave', 'Estimated Last Day of Leave',
       'Links Back to Prior Event', 'Stop Payment Date',
       'Social Security Disability Code', 'Location During Leave',
       'Caesarean Section Birth', 'Childs Date of Death',
       'Stillbirth Baby Deceased', 'Date Baby Arrived Home From Hospital',
       'Adoption Notification Date', 'Date Child Entered Country',
       'Multiple Child Indicator', 'Number of Babies Adopted Children',
       'Number of Previous Births', 'Number of Previous Maternity Leaves',
       'Number of Child Dependents', 'Single Parent Indicator', 'Work Related',
       'Comments']]

#requires manual edits to only load those in HRBPs spreadsheet
load.to_csv('loa_man_edit.csv')
load = pd.read_csv('loa_man_edit.csv')

os.chdir(path)
#load.to_csv('leave_of_absence.txt',sep='|', encoding='utf-8', index=False)



##############################################################################
########################### Override Balances ################################
##############################################################################

"""
Worker ID
Time Off Plan ID
Override Balance Date
Override Balance Units

"""
# TODO check eligibility rules for those ees that have a balance but "shouldn't"

df_ob = get_rpt('72593409')

override_bal = pd.read_excel('./../USA_KBP_CNP_Absence_Template_01102023.xlsx', sheet_name='Override Balances')

override_bal_c = override_bal.filter(regex='Required')
override_bal_data = pd.read_excel('./../USA_KBP_CNP_Absence_Template_01102023.xlsx', sheet_name='Override Balances',skiprows=2)

override_bal_data_c = override_bal_data.iloc[0:0]

df_ob['Worker ID'] = df_ob['Employee Id']


df_ob['Time Off Plan ID'] = np.where(df_ob['Time Off'] == 'Vacation','PTO',
                               np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('NYC') & (df_ob['Employee EIN'] == 'KBP Foods')),'NY_-_Sick_-_KBP_Foods',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('NYC') & (df_ob['Employee EIN'] == 'KBP Bells')),'NY_-_Sick_-_Bells',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('Mary')),'MD_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('Maine')),'ME_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('Cook')),'Cook_County_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('AZ') | df_ob['Accrual Profile'].str.contains('Arizona')),'AZ_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('CO')),'CO_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('Dall')),'Dallas_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('MI')),'MI_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('NJ')),'NJ_-_Sick',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('NM')) & (df_ob['Employee EIN'] == 'KBP Bells'),'NM_-_Sick_-_Bells',
                                np.where((df_ob['Time Off'] == 'Leave/Sick Pay') & (df_ob['Accrual Profile'].str.contains('NM')) & (df_ob['Employee EIN'] == 'KBP Inspired'),'NM_-_Sick_-_Inspire','Floating_Holiday')))))))))))))


#DD-MON-YYYY
df_ob['Override Balance Date'] = np.where(df_ob['Time Off Plan ID']== 'Floating_Holiday', '01-JAN-2023',
                                          np.where(df_ob['Time Off Plan ID'] == 'CO_-_Sick','11-JAN-2023',
                                                   np.where(df_ob['Time Off Plan ID'] == 'NM_-_Sick_-_Bells','11-JAN-2023',
                                                            np.where(df_ob['Time Off Plan ID'] == 'NY_-_Sick_-_KBP_Foods','17-JAN-2023',
                                                                     np.where(df_ob['Time Off Plan ID'] == 'NY_-_Sick_-_Bells','18-JAN-2023','10-JAN-2023')))))

df_ob['Override Balance Units'] = df_ob['Hours Remaining']

df_ob_c = df_ob[['Worker ID','Time Off Plan ID','Override Balance Date','Override Balance Units']]

override_bal_data_c['Worker ID'] = df_ob['Worker ID']

override_bal_data_c = override_bal_data_c.drop(['Time Off Plan ID','Override Balance Date','Override Balance Units'],axis=1)

obd = override_bal_data_c.merge(df_ob_c, on='Worker ID')
obd.unique(inplace=True)

obd= obd[['Worker ID', 'Source System', 'Position ID', 'Time Off Plan ID',
       'Override Balance Date', 'Override Balance Units', 'Comments',
       'Carryover Date', 'Carryover Expiration Date',
       'Carryover Override Balance Units']]


mask = obd['Worker ID'].isin(cut_off_ids)
obd = obd.loc[~mask]

os.chdir(path)
obd.to_csv('override_balance.txt',sep='|', encoding='utf-8', index=False)
