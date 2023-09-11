##############################################################################
########################## Pay Election Enrollment ###########################
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

    dd_data = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71457227.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    dd_data['Employee Id'] = dd_data['Employee Id'].astype(str)
    dd_data = dd_data.loc[dd_data['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    dd_data = dd_data.drop(['Name'],axis=1)

    dd_data['Audit'] = dd_data['Calculation Method'] + dd_data['Amount']
    dd_data = dd_data[dd_data['Audit'] != 'Flat $ Amount-']
    dd_data_full = dd_data[dd_data['Calculation Method'] == 'Full']
    dd_data_full = dd_data_full.drop(['Frequency'],axis=1)
    dd_data_full['Active From'] = pd.to_datetime(dd_data_full['Active From'])
    dd_data_full['Employee Id'] = dd_data_full['Employee Id'].astype(int)
    dd_data_full = dd_data_full[dd_data_full.groupby('Employee Id')['Active From'].transform('max') == dd_data_full['Active From']]
    dd_data_full = dd_data_full[dd_data_full['Unnamed: 0'] != '1573']
    dd_data1 = dd_data[dd_data['Calculation Method'] != 'Full']
    dd_data1 = dd_data1.drop(['Frequency'],axis=1)
    dd_data1['Employee Id'] = dd_data1['Employee Id'].astype(int)
    dd_data1['Active From'] = pd.to_datetime(dd_data1['Active From'])
    dd_data_1 = pd.concat([dd_data1,dd_data_full])
    dd_data.drop_duplicates(inplace=True)
    
    dd_data = dd_data.sort_values(dd_data.columns[0]).reset_index(drop=True)
    dd_data = dd_data.drop(['Unnamed: 0','Sequence'],axis=1)
    dd_data.drop_duplicates(inplace=True)

    dd_data = dd_data.replace('^[-]$', '', regex=True)

    dd_data = modify_amount(dd_data, 'Amount')
    dd_data = modify_amount(dd_data, 'Amount %')
    dd_data['SortAmount'] = dd_data['Amount'].astype(float) + dd_data['Amount %'].astype(float)
    dd_data = dd_data.sort_values(['Employee Id','SortAmount'],ascending=False).reset_index()

    dd_data['Employee ID'] = dd_data['Employee Id']
    dd_data['Country ISO Code'] = 'USA'
    dd_data['Currency Code'] = 'USD'
    dd_data['Payment Election Rule'] = 'USA_Regular'
    # dd_data['Distribution Order'] = dd_data['Sequence']

    #dd_data['Sequence'] = dd_data['Sequence'].astype(int)
    dd_data = dd_data.sort_values(by=['Employee ID', 'Amount'], ascending=[True, False]).reset_index()
    dd_data['Distribution Order'] = np.where(dd_data['Employee ID'].notna(), dd_data.groupby('Employee ID').cumcount() + 1, '')

    dd_data['Distribution Amount'] = dd_data['Amount']
    dd_data['Distribution Percentage'] = (dd_data['Amount %'].astype(float) / 100).round(3)
    dd_data['Payment Type'] = 'Direct_Deposit'
    dd_data['Bank Account Name'] = ''
    dd_data['Account Number'] = dd_data['Account #']

    #dd_data.loc[~dd_data['Name'].isnull(), 'Bank Name'] = dd_data['Name']
    dd_data['Bank Name'] = 'Not Available'

    dd_data['Bank ID Number'] = dd_data['Routing #'].str.zfill(9)  # 9 digits zero fill
    dd_data['Source System'] = 'Kronos'

    distrib_bal = dd_data.groupby('Employee ID').agg({'Distribution Order':'max'})[['Distribution Order']].reset_index()
    distrib_bal['Distribution Balance'] = 'Y'

    dd_data = dd_data.merge(distrib_bal, left_on=['Employee ID', 'Distribution Order'], right_on=['Employee ID', 'Distribution Order'], how='left')
    dd_data['Distribution Amount'] = np.where(dd_data['Distribution Percentage'].astype(float) > 0.00,'', dd_data['Distribution Amount'])
    #dd_data['Distribution Percentage'] = np.where(dd_data['Distribution Amount'].astype(float) > 0,'',dd_data['Distribution Percentage'])
    dd_data['Distribution Amount'] = np.where(dd_data['Distribution Balance'] == 'Y','', dd_data['Distribution Amount'])
    dd_data['Distribution Percentage'] = np.where(dd_data['Distribution Balance'] == 'Y','', dd_data['Distribution Percentage'])

    #dd_data['Distribution Percentage'] = dd_data['Distribution Percentage'].str.replace('0.0','')

    #dd_data['Distribution Percentage'] = np.where(dd_data['Distribution Amount'] > 0,'',dd_data['Distribution Percentage'])
    dd_data['Bank Account Nickname'] = ''
    dd_data['Roll Number'] = ''

    dd_data.loc[dd_data['Account Type'] == 'Checking', 'Account Type'] = 'DDA'
    dd_data.loc[dd_data['Account Type'] == 'Savings', 'Account Type'] = 'SA'

    dd_data['IBAN'] = ''
    dd_data['BIC'] = ''
    dd_data['Check Digit'] = ''
    dd_data['Branch Name'] =''
    dd_data['Branch ID Number'] = ''

    dd_dataf = dd_data[['Employee ID', 'Source System', 'Country ISO Code',
                        'Currency Code', 'Payment Election Rule',
                        'Distribution Order', 'Distribution Amount',
                        'Distribution Percentage', 'Distribution Balance',
                        'Payment Type', 'Bank Account Nickname', 'Bank Account Name',
                        'Account Number', 'Roll Number', 'Account Type',
                        'Bank Name', 'IBAN', 'Bank ID Number', 'BIC', 'Check Digit',
                        'Branch Name', 'Branch ID Number']]
    
    os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files')
    #dd_dataf.to_excel('payele_test.xlsx')
    #REMOVE 0.0 for % people
    
    #dd_dataf = pd.read_excel('payele_test.xlsx',index_col=0)
    
    dd_dataf2 = dd_dataf.copy()
    dd_dataf2['Payment Election Rule'] = 'Expense_Payments'

    
    dd_dataf = pd.concat([dd_dataf,dd_dataf2])
    dd_dataf['Employee ID'] = dd_dataf['Employee ID'].astype(int)
    dd_dataf.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\payele.csv')
    write_to_csv(dd, 'pay_elections_redo_0825232.txt')
    
    dd = pd.read_excel(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\PROD_pay_elections_fix.xlsx")
    dd['Bank ID Number']  = dd['Bank ID Number'].astype(str)
    dd['Bank ID Number'] = dd['Bank ID Number'].str.zfill(9)
