import pandas as pd
import numpy as np

import config
from common import (write_to_csv, read_column_mapping, concat_column_mapping)

# -----------------------------------------------------------------------------
def filter_data(df):
    """ Filter Data """

    return df

# -----------------------------------------------------------------------------
def transform_values(df):
    """ Transform Values """

    # df.loc[df['Operation'] == 'AMSS', 'Sub Operation'] = df['Sub Operation'].str.replace(r' \(AMSS\)','')

    df['Org_ID'] = 'SUP_' + df['Manager ID'].astype(str).str.zfill(6)
    df['Org_Name'] = '' # df['Operation'] + '-' + df['Sub Operation']
    df['Org_Code'] = ''
    df['Superior_ID'] = 'SUP_' + df['Direct Supervisor Employee Id'].str.zfill(6)

    df.loc[~df['Location(1)'].str.upper().isin(['HOME OFFICE', 'WORK FROM HOME']), 'Org_Location'] = 'LC' + df['Location(1)'].str.zfill(5)
    df.loc[~df['Location(1)'].str.upper().isin(['HOME OFFICE', 'WORK FROM HOME']), 'Org_SubType'] = 'Team'
    df.loc[~df['Location(1)'].str.upper().isin(['HOME OFFICE', 'WORK FROM HOME']), 'Is_Job_Management'] = 'Y'
    df.loc[~df['Location(1)'].str.upper().isin(['HOME OFFICE', 'WORK FROM HOME']), 'Is_Position_Management'] = 'N'

    df.loc[df['Location(1)'].str.upper().isin(['HOME OFFICE']), 'Org_Location'] = 'LC_Home Office'
    df.loc[df['Location(1)'].str.upper().isin(['WORK FROM HOME']), 'Org_Location'] = 'LC_State ' + df['Name']
    df.loc[df['Location(1)'].str.upper().isin(['HOME OFFICE', 'WORK FROM HOME']), 'Org_SubType'] = 'Department'
    df.loc[df['Location(1)'].str.upper().isin(['HOME OFFICE', 'WORK FROM HOME']), 'Is_Job_Management'] = 'N'
    df.loc[df['Location(1)'].str.upper().isin(['HOME OFFICE', 'WORK FROM HOME']), 'Is_Position_Management'] = 'Y'

    # TODO Needs work
    # df.loc[df['Supv GEMS ID'].isna(), 'Supv GEMS ID'] = 'MISSING_MANAGER'
    # df.loc[((df['Person Type'] == 'Retiree')), 'Supv GEMS ID'] = 'Term'

    return df

# -----------------------------------------------------------------------------
if __name__ == '__main__':

    df_emps = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877396.csv', dtype='object', encoding='cp1251')
    df_emps['Employee Id'] = df_emps['Employee Id'].astype('UInt32')

    df = pd.read_csv(config.PATH_WD_IMP + 'data files\\reports_to_ceo.csv')
    df['Manager ID'] = pd.to_numeric(df['Manager ID'], errors='coerce').astype('UInt32')
    df = df.drop_duplicates(subset=['Manager ID'])
    print(df)

    df = df.merge(df_emps, left_on='Manager ID', right_on='Employee Id',
                    how='left', suffixes=(None, '_emps'))
    df['Direct Supervisor Employee Id'] = df['Direct Supervisor Employee Id'].astype(str)


    df_states = pd.read_csv(config.PATH_WD_IMP + 'data files\\wd_states.csv')
    df = df.merge(df_states, left_on='State', right_on='Abbrev', how='left')

    # Begin replace managers in rollup
    # df_new_manager = pd.read_csv(config.FILE_MANAGER_ROLLUP)
    # df_new_manager['Manager ID'] = df_new_manager['Manager ID'].astype(str)
    # df_new_manager['New Manager ID'] = df_new_manager['New Manager ID'].astype(str)
    # df = df.merge(df_new_manager, left_on='Supv GEMS ID', right_on='Manager ID',
    #                 how='left', suffixes=(None, '_new_mgr'))
    #
    # df.loc[~df['New Manager ID'].isnull(), 'Supv GEMS ID'] = df['New Manager ID']
    # End replace managers in rollup

    df = filter_data(df)

    if len(df.index) > 0:
        print(df)

        df = transform_values(df)
        df_col_mapping = read_column_mapping()
        df = concat_column_mapping(df, df_col_mapping)

        # df.loc[len(df.index)] = ['30120', 'Kronos', 'KBP_BRANDS', 'TOP', '', 'N', 'Y', 'Department', 'LC_Home Office', '']
        # df.loc[len(df.index)] = ['SUP_Term', 'Terminated Workers', '', '0', 'Department', 'FLS_TOP', '1', '1', '0', 'US_Irving_DAL']
        # df.loc[len(df.index)] = ['SUP_MISSING_MANAGER', 'Missing Manager', '', '0', 'Department', 'TOP', '1', '0', '1', 'LC_Home Office']

        df.loc[df['Supervisory Organization ID'] == 'SUP_030120', 'Superior Organization ID'] = 'KBP_BRANDS'

        df.sort_values(by=['Supervisory Organization ID'], inplace=True)

        write_to_csv(df, 'sup_org.txt')
    else:
        print('No results')
