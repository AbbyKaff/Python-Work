# -*- coding: utf-8 -*-
"""
Created on Sat Sep  9 21:45:07 2023

@author: akaff
"""
import pandas as pd


pay_hist1 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_payroll_history_0826232.txt",sep='|')



pay_hist2 = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\payroll_history1.xlsx",sheet_name = 'CNV-X-PAY-Pay History Balances ')


pay_hist1['Amount'] = pay_hist1['Amount'].astype(float)
pay_hist1['Amount'] = round(pay_hist1['Amount'],2)
pay_hist2['Amount'] = pay_hist2['Amount 2'].astype(float)
pay_hist2['Amount'] = round(pay_hist2['Amount 2'],2)
pay_hist1['Employee ID'] = pay_hist1['Employee ID'].astype(str)
pay_hist1['Employee ID'] = pay_hist1['Employee ID'].str[:-2]
pay_hist1 = pay_hist1[pay_hist1['Quarter'] == '2023Q1']
pay_hist2 = pay_hist2[pay_hist2['Quarter 2'] == '2023Q1']

pay_hist1.loc[pay_hist1['Earning Code'].isna(),'fullmerge'] = pay_hist1['Employee ID'].astype(str) + pay_hist1['Quarter'] + pay_hist1['Cost Center'] + pay_hist1['Deduction Code'] + pay_hist1['Amount'].astype(str)


pay_hist1.loc[pay_hist1['Deduction Code'].isna(),'fullmerge'] = pay_hist1['Employee ID'].astype(str) + pay_hist1['Quarter'] + pay_hist1['Cost Center'] + pay_hist1['Earning Code'] + pay_hist1['Amount'].astype(str)


pay_hist2.loc[pay_hist2['Earning Code'].isna(),'fullmerge'] = pay_hist2['Employee ID'].astype(str) + pay_hist2['Quarter 2'] + pay_hist2['Cost Center'] + pay_hist2['Deduction Code'] + pay_hist2['Amount 2'].astype(str)


pay_hist2.loc[pay_hist2['Deduction Code'].isna(),'fullmerge'] = pay_hist2['Employee ID'].astype(str) + pay_hist2['Quarter 2'] + pay_hist2['Cost Center'] + pay_hist2['Earning Code'] + pay_hist2['Amount 2'].astype(str)


valid = pay_hist2.merge(pay_hist1, on='fullmerge', how='left')
valid.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\validateq1.txt',sep='|')

valid2 = pay_hist1.merge(pay_hist2, on='fullmerge', how='left')
valid2.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\validateq1_1.txt',sep='|')

###############################################################################


pay_hist1 = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_payroll_history_0826232.txt",sep='|')



pay_hist2 = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\payroll_history1.xlsx",sheet_name = 'CNV-X-PAY-Pay History Balances ')



pay_hist1['Amount'] = pay_hist1['Amount'].astype(float)
pay_hist1['Amount'] = round(pay_hist1['Amount'],2)
pay_hist2['Amount'] = pay_hist2['Amount 2'].astype(float)
pay_hist2['Amount'] = round(pay_hist2['Amount 2'],2)
pay_hist1['Employee ID'] = pay_hist1['Employee ID'].astype(str)
pay_hist1['Employee ID'] = pay_hist1['Employee ID'].str[:-2]
pay_hist1 = pay_hist1[pay_hist1['Quarter'] == '2023Q2']
pay_hist2 = pay_hist2[pay_hist2['Quarter 2'] == '2023Q2']

pay_hist1.loc[pay_hist1['Earning Code'].isna(),'fullmerge'] = pay_hist1['Employee ID'].astype(str) + pay_hist1['Quarter'] + pay_hist1['Cost Center'] + pay_hist1['Deduction Code'] + pay_hist1['Amount'].astype(str)


pay_hist1.loc[pay_hist1['Deduction Code'].isna(),'fullmerge'] = pay_hist1['Employee ID'].astype(str) + pay_hist1['Quarter'] + pay_hist1['Cost Center'] + pay_hist1['Earning Code'] + pay_hist1['Amount'].astype(str)


pay_hist2.loc[pay_hist2['Earning Code'].isna(),'fullmerge'] = pay_hist2['Employee ID'].astype(str) + pay_hist2['Quarter 2'] + pay_hist2['Cost Center'] + pay_hist2['Deduction Code'] + pay_hist2['Amount 2'].astype(str)


pay_hist2.loc[pay_hist2['Deduction Code'].isna(),'fullmerge'] = pay_hist2['Employee ID'].astype(str) + pay_hist2['Quarter 2'] + pay_hist2['Cost Center'] + pay_hist2['Earning Code'] + pay_hist2['Amount 2'].astype(str)


valid = pay_hist2.merge(pay_hist1, on='fullmerge', how='left')
valid.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\validateq2.txt',sep='|')