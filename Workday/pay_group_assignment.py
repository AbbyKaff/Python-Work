##############################################################################
######################### Pay Group Assignments ##############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    df2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\employee_terms_all.csv', dtype='object', encoding='cp1251')
    loas = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72867407.csv', dtype='object', encoding='cp1251')
    pg_data = pd.concat([df,df2,loas])
    
    pg_data=pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\pg_catchup.csv",encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    pg_data['Employee Id'] = pg_data['Employee Id'].astype(str)
    pg_data = pg_data.loc[pg_data['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    store_jobs = ['10','9006','51','4','6']
    pg_data['Default Jobs (HR) External Id'] 
    
    pg_data['Date Terminated'] = pd.to_datetime(pg_data['Date Terminated'])
    pg_data_terms = pg_data[pg_data['Date Terminated'] >= '01/01/2023']
    pg_data_active = pg_data[pg_data['Date Terminated'].isna()]
    pg_data = pd.concat([pg_data_terms,pg_data_active])
    
    pg_data['Pay Group ID'] = np.where(pg_data['Pay Period Profile'] == 'Bi-Weekly Hourly A','USA_Bi-Weekly', 
                              np.where(pg_data['Pay Period Profile'] == 'Bi-Weekly Salary A','USA_Bi-Weekly',
                              np.where(pg_data['Pay Period Profile'] == 'KBP Bells Bi-weekly Hourly','USA_TB_Bi-Weekly', 
                              np.where(pg_data['Pay Period Profile'] == 'KBP Bells Bi-weekly Salary','USA_TB_Bi-Weekly',
                              np.where(pg_data['Pay Period Profile'] == 'KBP Bells Weekly Hourly','USA_TB_Weekly', 
                              np.where(pg_data['Pay Period Profile'] == 'KBP Bells Weekly Salary','USA_TB_Weekly',
                              np.where(pg_data['Pay Period Profile'] == 'KBP Foods Weekly Hourly','USA_Weekly',
                              np.where(pg_data['Pay Period Profile'] == 'KBP Foods Weekly Salary','USA_Weekly','NA'))))))))
    
    pg_data['Pay Group ID'] = np.where((pg_data['Unemployment State/Province'] == 'NY') & (pg_data['Employee EIN'] == 'KBP Bells') & (pg_data['Default Jobs (HR) External Id'].isin(store_jobs)),'USA_TB_Weekly',
                              np.where((pg_data['Unemployment State/Province'] == 'NY') & (pg_data['Employee EIN'] == 'KBP Foods') & (pg_data['Default Jobs (HR) External Id'].isin(store_jobs)),'USA_Weekly',
                              np.where((pg_data['Unemployment State/Province'] != 'NY') | (~pg_data['Default Jobs (HR) External Id'].isin(store_jobs)) & (pg_data['Employee EIN'] == 'KBP Bells'),'USA_TB_Bi-Weekly','USA_Bi-Weekly')))

    pg_data['Employee ID'] = pg_data['Employee Id']
    pg_data['Source System'] = 'Kronos'

    pg_data['Effective Date'] = pd.to_datetime(pg_data['Date Hired']).dt.strftime('%d-%b-%Y').str.upper()

    pg_datac = pg_data[['Employee ID', 'Source System', 'Effective Date', 'Pay Group ID']]

    write_to_csv(pg_datac, 'pay_gp_assignments_redo_082523.txt')
    
    pg_data = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\Copy of Workers Missing Pay Groups.csv")
