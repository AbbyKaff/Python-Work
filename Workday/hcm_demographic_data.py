##############################################################################
########################### demographic_data ##################################
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

    active_ees = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877725.csv', dtype='object', encoding='cp1251')
    terms = pd.read_csv(config.PATH_WD_IMP + 'terms\\employee_terms_all.csv', dtype='object', encoding='cp1251')
    active_ees = pd.concat([active_ees, terms])

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\E2E_name_and_email.txt', sep="|")
    active_ees['Employee Id'] = active_ees['Employee Id'].astype(str)
    active_ees = active_ees.loc[active_ees['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_ees['Source System'] = 'Kronos'

    active_ees.rename(columns={"Employee Id": "Worker ID"}, inplace=True)

    active_ees['Ethnicity #1'] = np.where(active_ees['Ethnicity']== 'White (not Hispanic or Latino)', 'White_United_States_of_America',
                  np.where(active_ees['Ethnicity']== 'Black or African American (not Hispanic or Latino)', 'Black_or_African_American_United_States_of_America',
                  np.where(active_ees['Ethnicity']== 'American Indian or Alaska Native (not Hispanic or Latino)', 'American_Indian_or_Alaska_Native_United_States_of_America',
                  np.where(active_ees['Ethnicity']== 'Native Hawaiian or Other Pacific Islander (not Hispanic or Latino)', 'Native_Hawaiian_or_Other_Pacific_Islander_United_States_of_America',
                  np.where(active_ees['Ethnicity']== 'Asian (not Hispanic or Latino)', 'Asian_United_States_of_America',
                  np.where(active_ees['Ethnicity']== 'Two or More Races (not Hispanic or Latino)', 'Two_or_More_Races_United_States_of_America',
                  np.where(active_ees['Ethnicity']== 'Hispanic or Latino', 'Hispanic_or_Latino_United_States_of_America', 'Not_Specified')))))))

    active_ees['Hispanic or Latino'] = np.where(active_ees['Ethnicity #1']== 'Hispanic_or_Latino_United_States_of_America', 'Y','N')

    active_ees['Marital Status'] = np.where(active_ees['Actual Marital Status'] == 'Divorced','Divorced_United_States_of_America',
                                   np.where(active_ees['Actual Marital Status'] == 'Married','Married_United_States_of_America',
                                    np.where(active_ees['Actual Marital Status'] == 'Partnered','Partnered_United_States_of_America',
                                    np.where(active_ees['Actual Marital Status'] == 'Separated','Separated_United_States_of_America',
                                    np.where(active_ees['Actual Marital Status'] == 'Widowed','Widowed_United_States_of_America','Single_United_States_of_America')))))

    demo_data_c = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_HCM_Template_01122023 (6).xlsx', sheet_name='Demographic Data', skiprows=2, nrows=0)

    demo_data_c['Worker ID'] = active_ees['Worker ID']
    demo_data_c1 = demo_data_c.drop(['Source System','Marital Status','Ethnicity #1'],axis=1)

    df_d = active_ees[['Worker ID','Source System','Marital Status','Ethnicity #1']]
    demo_data_c1 = demo_data_c1.merge(df_d, on='Worker ID')

    dem_df = demo_data_c1[['Worker ID', 'Source System', 'Marital Status', 'Marital Status Date',
           'Hispanic or Latino', 'Ethnicity #1', 'Ethnicity #2', 'Ethnicity #3',
           'Ethnicity #4', 'Citizenship Status #1', 'Citizenship Status #2',
           'Citizenship Status #3', 'Citizenship Status #4', 'Primary Nationality',
           'Additional Nationality #1', 'Additional Nationality #2',
           'Additional Nationality #3', 'Additional Nationality #4',
           'Military Status #1', 'Military Service Type #1',
           'Military Discharge Date #1', 'Military Status #2',
           'Military Service Type #2', 'Military Discharge Date #2',
           'Military Status #3', 'Military Service Type #3',
           'Military Discharge Date #3', 'Military Status #4',
           'Military Service Type #4', 'Military Discharge Date #4']]

    write_to_csv(dem_df, 'demographic_data.txt')
