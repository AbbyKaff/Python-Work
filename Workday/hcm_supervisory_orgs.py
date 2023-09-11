##############################################################################
######################## Supervisory Organizations ###########################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd
import os
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877396.csv', dtype='object', encoding='cp1251')
    foods = pd.read_excel(config.PATH_WD_IMP + 'data files\\Q3 P8 2023 Contact Sheet PRELIM eff 08.01.2023.xlsx', sheet_name='All Stores - FQSR')
    bells = pd.read_excel(config.PATH_WD_IMP + 'data files\\Q3 P8 2023 Contact Sheet PRELIM eff 08.01.2023.xlsx', sheet_name='All Stores - KBP Bells')
    inspired = pd.read_excel(config.PATH_WD_IMP + 'data files\\Q3 P8 2023 Contact Sheet PRELIM eff 08.01.2023.xlsx', sheet_name='All Stores - Arby')
    bells.dropna(how = "all", inplace=True)
    inspired.dropna(how="all", inplace=True)
    dat = pd.concat([foods,bells,inspired])
    contact = pd.read_excel(config.PATH_WD_IMP + 'data files\\Q3 P9 2023 Contact Sheet PRELIM eff 08.15.2023.xlsx', sheet_name='KBP Contacts')
    dat = dat.merge(contact, left_on='AC',right_on='Person')
    sm_dat = dat[['Store Cost Center','AC','Kronos EmpID']]
    sm_dat['Direct Supervisor Employee Id'] = sm_dat['Kronos EmpID']
    df_c = df[df['Direct Supervisor Employee Id'].isna()]
    df_c['Default Cost Centers'] = df_c['Default Cost Centers'].astype(int)
    df_c = df_c.merge(sm_dat, left_on='Default Cost Centers',right_on='Store Cost Center', how='left')
    df_c['Direct Supervisor Employee Id'] = np.where(df_c['Employee Id'] == '193440','22477',df_c['Kronos EmpID'])
    df_c.drop(columns = ['Store Cost Center','AC','Kronos EmpID', 'Direct Supervisor Employee Id_x',
     'Direct Supervisor Employee Id_y'],inplace=True)
    
    
    #fins = pd.read_excel(config.PATH_WD_IMP + 'FINs Docs\\Copy of FIN_-_Extract_Locations (1).xlsx', skiprows=(5))
    df2 = df.copy()
    df2 = df2[~df2['Direct Supervisor Employee Id'].isna()]
    df2 = pd.concat([df2,df_c])
    dd = df[['Employee Id','Employee EIN','Cost Centers(Company Code)']]

    df2['Supervisory Organization ID'] = df2['Direct Supervisor Employee Id'].str[:-2]
    df2['Superior Organization ID'] = df2['Indirect Supervisor Employee Id'].str[:-2]
    df2['Direct Supervisor Employee Id'] = df2['Direct Supervisor Employee Id'].str[:-2]

    sup_org = df2[['Supervisory Organization ID','Direct Supervisor Employee Id','Superior Organization ID']]
    sup_org.drop_duplicates(inplace=True)

    xx = sup_org.merge(df, left_on='Direct Supervisor Employee Id',right_on='Employee Id')

    #xx['Supervisory Organization ID'] = np.where(xx['Employee EIN'] == 'Restaurant Services Group',"RSG_" + xx['Supervisory Organization ID'].astype(str)
    #,xx['Cost Centers(Company Code)'].astype(str)+ "_" + xx['Supervisory Organization ID'].astype(str))
    xx['Supervisory Organization ID'] = "SUP_" + xx['Supervisory Organization ID']
    xx['Superior Organization ID_x'] = xx['Superior Organization ID']
    #xx.drop(columns=['Superior Organization ID'],inplace=True)
    xx.drop_duplicates(inplace=True)
    #xx = xx.merge(dd,left_on='Direct Supervisor Employee Id_y',right_on='Employee Id')
    #xx['Direct Sup EIN'] = xx['Employee EIN_y']
    #xx['Direct Sup CC'] =  xx['Cost Centers(Company Code)_y']

    xx['Superior Organization ID'] = "SUP_" + xx['Superior Organization ID_x']

    xx.to_csv('supervisory_organizations_test2.csv',index=False)

    xx['Job Management'] = np.where((xx['Default Location']=='Work From Home')|(xx['Default Location']=='Home Office'),'N','Y')
    xx['Position Management'] = np.where((xx['Default Location']=='Work From Home')|(xx['Default Location']=='Home Office'),'Y','N')
    xx['Supervisory Org SubType'] = np.where(xx['Default Location'] == 'Home Office','Department','Team')
    xx['Manager Employee ID'] = xx['Direct Supervisor Employee Id_x']
    #sup_org_df = xx.merge(fins, left_on='Default Cost Centers', right_on='Cost Center', how='left')
    #sup_org_df['Location'] = sup_org_df['Reference ID']

    # TODO: take a look at
    sup_org_df = xx
    state_info = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\state_info.csv")
    sup_org_df = sup_org_df.merge(state_info, left_on='State',right_on='State Abbreviation')
    sup_org_df['state_full'] = sup_org_df['State Name']

    # TODO: take a look at
    sup_org_df['Location'] = np.where(sup_org_df['Default Location'] == 'Home Office','LC10000',
                             np.where(sup_org_df['Default Location'] == 'Work From Home','LC_State '+ sup_org_df['state_full'], "LC"+(sup_org_df['Default Cost Centers'].astype(str)).str.zfill(5)))
    sup_org_df['Superior Organization ID'] = np.where(sup_org_df['Superior Organization ID'].isna(), 'SUP_' + sup_org_df['Direct Supervisor Employee Id_y'].str[:-2],sup_org_df['Superior Organization ID'])
    os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files')
    sup_org_df.to_excel('sup_org_test.xlsx')

    sup_org_df = sup_org_df[['Manager Employee ID','Supervisory Organization ID','Job Management','Position Management','Supervisory Org SubType','Superior Organization ID','Location','state_full','Location(1)']]

    #sup_org_df['Supervisory Organization ID'] = sup_org_df['Supervisory Organization ID'].str[:-2]
    #sup_org_df['Superior Organization ID'] = sup_org_df['Superior Organization ID'].str[:-2]
    #sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].astype(str)
    #sup_org_df['Manager Employee ID'] = sup_org_df['Manager Employee ID'].str[:-2]

    sup_org_df = sup_org_df[['Manager Employee ID', 'Supervisory Organization ID',
                             'Job Management', 'Position Management',
                             'Supervisory Org SubType', 'Location',
                             'Superior Organization ID']]

    sup_org_df.drop_duplicates(inplace=True)

    sup_org_df['Source System'] = 'Kronos'
    sup_org_df['Supervisory Organization Name'] = ''
    sup_org_df['Supervisory Organization Code'] = ''

    sup_org_df = sup_org_df[['Manager Employee ID', 'Source System',
                             'Supervisory Organization ID', 'Supervisory Organization Name',
                             'Supervisory Organization Code', 'Job Management',
                             'Position Management', 'Supervisory Org SubType',
                             'Location', 'Superior Organization ID']]
    
    missing_dict = {'30120':'KBP_BRANDS',
                    '7249':'SUP_30120',
                    '106120':'SUP_30120',
                    '132489':'SUP_30120',
                    '151112':'SUP_30120',
                    '15358':'SUP_30120',
                    '22116':'SUP_30120',
                    '22488':'SUP_30120',
                    '131130':'SUP_30120',
                    '16230':'SUP_15358',
                    '16247':'SUP_22507',
                    '22507':'SUP_132489'}
    
    #sup_org_df['Superior Organization ID'] = np.where(sup_org_df['Superior Organization ID'].isnull(),sup_org_df['Manager Employee ID'].replace(missing_dict),sup_org_df['Superior Organization ID'])
    sup_org_df['Superior Organization ID'] = np.where(sup_org_df['Manager Employee ID'] == '189881','SUP_167989',sup_org_df['Superior Organization ID'])
    
    sup_org_df['Superior Organization ID'] = np.where(sup_org_df['Manager Employee ID'] == '30120','KBP_BRANDS',sup_org_df['Superior Organization ID'])
    
    #add top row
    top = pd.DataFrame({'Manager Employee ID':['TOP'],
                        'Source System':['Kronos'],
                        'Supervisory Organization ID':['KBP_BRANDS'],
                        'Supervisory Organization Name':[''],
                        'Supervisory Organization Code':[''],
                        'Job Management':['N'],
                        'Position Management':['Y'],
                        'Supervisory Org SubType':['Department'],
                        'Location':['LC10000'],
                        'Superior Organization ID':['']})
    sup_org_df = pd.concat([top,sup_org_df])


    sup_org_df = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_supervisory_org_append_082423.csv")
    
    sup_org_df = sup_org_df[['Manager Employee ID', 'Source System',
                             'Supervisory Organization ID', 'Supervisory Organization Name',
                             'Supervisory Organization Code', 'Job Management',
                             'Position Management', 'Supervisory Org SubType',
                             'Location', 'Superior Organization ID']]

    #write_to_csv(sup_org_df, 'supervisory_organizations.txt')
    sup_org_df.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_supervisory_organizations_append_0824232.txt',sep='|',encoding = 'utf-8', index=False)
