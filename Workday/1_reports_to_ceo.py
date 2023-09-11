import pandas as pd
import numpy as np
import sys

import config
# from common import change_all_date_formats, read_HR_Employee_Listing, read_Active_CW

# -----------------------------------------------------------------------------
def replace_managers(df):
    """ Replace Managers """

    df['Manager ID'] = df['Manager ID'].astype(str)

    # replace Manager ID for those managers from excluded countries
    df_new_manager = pd.read_csv(config.FILE_MANAGER_ROLLUP)
    df_new_manager['Manager ID'] = df_new_manager['Manager ID'].astype(str)
    df = df.merge(df_new_manager, left_on='Manager ID', right_on='Manager ID',
                    how='left', suffixes=(None, '_new_mgr'))

    df.loc[~df['New Manager ID'].isnull(), 'Manager ID'] = df['New Manager ID']

    return df

# -----------------------------------------------------------------------------
def active_employee_with_manager_in_excluded_country(df):
    """ Active employee with no manager """

    df_exclude = pd.read_excel(config.FILE_HR_EMP_LISTING, header=2, dtype='object')

    df_exclude = df_exclude[df_exclude['Country'].isin(config.EXCLUDE_COUNTRIES)]

    df_exclude['GEMS ID'] = df_exclude['GEMS ID'].astype(str)
    df_exclude['GEMS ID'] = df_exclude['GEMS ID'].str.strip()
    df_exclude.rename(columns={'GEMS ID': 'Worker ID'}, inplace=True)

    df['Manager ID'] = df['Manager ID'].astype(str)
    df['Manager ID'] = df['Manager ID'].str.strip()

    df = df.merge(df_exclude, left_on='Manager ID', right_on='Worker ID',
                    how='inner', suffixes=(None, '_exclude'))

    df.to_csv(config.PATH_TO_OUTPUT + 'debug\\debug_df_after.csv', header=True, index=False, encoding='utf-8')

    # Active EE with Manager in excluded country
    df = df[(df['Assignment Status'] != 'Terminate Assignment')]

    print('Active employee with manager in excluded country')
    if len(df.index) > 0:
        print(df)
    else:
        print('0')


# -----------------------------------------------------------------------------
def active_employee_with_no_manager(df):
    """ Active employee with no manager """

    # Active EE with no Manager
    df = df[(df['Employee Status'] != 'Terminate Assignment')
            & (df['Manager ID'].isna())]

    print('Active employee with no manager')
    if len(df.index) > 0:
        print(df)
    else:
        print('0')


# -----------------------------------------------------------------------------
def duplicate_employees(df):
    """ Duplicate employees """

    df = df[['Worker ID', 'Latest Hire Date',
             'Employee Status', 'Actual Termination Date']]

    df = pd.concat(g for _, g in df.groupby('Worker ID') if len(g) > 1)

    df.sort_values(by=['Worker ID', 'Latest Hire Date', 'Actual Termination Date', 'Assignment Status'],
                   inplace=True, ascending=[True, False, False, True])
    print(df)
    df = df[(df['Assignment Status'] != 'Terminate Assignment')]
    print(df)

    # keep in the following order:
    # 1) Most recent 'Latest Hire Date'
    # 2) Active 'Assignment Status'
    # 3) Most recent 'Actual Termination Date'

    print('Duplicate employees')
    if len(df.index) > 0:
        print(df)
    else:
        print('0')


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    # TODO: Check circular references
    # FQSR_172790

    df = pd.read_csv(config.PATH_WD_IMP + 'data sources\\71877396.csv', dtype='object', encoding='cp1251')

    # df, df_countries = read_HR_Employee_Listing()
    # df_cw, df_countries_cw = read_Active_CW()
    # if len(df_cw.index) > 0:
    #     df = pd.concat([df, df_cw], axis=0)

    df.rename(columns={'Employee Id': 'Worker ID'}, inplace=True)
    df.rename(columns={'Direct Supervisor Employee Id': 'Manager ID'}, inplace=True)

    # df = replace_managers(df)

    df_no_manager = active_employee_with_no_manager(df)
    # df_dups = duplicate_employees(df)
    # df_exclude = active_employee_with_manager_in_excluded_country(df)

    df_managers = df.drop_duplicates('Manager ID')
    print(str(len(df_managers)))
    df_managers = df.merge(df_managers, left_on='Worker ID', right_on='Manager ID',
                           how='right', suffixes=(None, '_mgr'))
    print(str(len(df_managers)))
    # exit()

    df = df[(df['Employee Status'] != 'Terminate Assignment')]

    # EE Active with termed Manager
    df_term_mgr = df[((df['Employee Status'] != 'Terminate Assignment') &
                        (df['Manager ID'].isna()))]

    print('EE Active with termed Manager')
    if len(df_term_mgr.index) > 0:
        print(df_term_mgr)
    else:
        print('0')

    if len(df.index) > 0:
        print(df)

        df = df[['Worker ID', 'Manager ID']]

        df = df.replace('nan', 0)
        # df.to_csv(config.PATH_TO_OUTPUT + 'debug\\debug_df_manager_id_before.csv', header=True, index=False, encoding='utf-8')

        df['Worker ID'] = df['Worker ID'].astype('UInt32')
        # df['Manager ID'] = df['Manager ID'].astype('UInt32')
        df['Manager ID'] = pd.to_numeric(df['Manager ID'], errors='coerce').astype('UInt32')

        # reports to CEO
        df.loc[df['Manager ID'] == 30120, 'reports_to_CEO'] = True
        df.loc[df['Manager ID'] == 30120, 'level_from_CEO'] = 1

        # *** Begin loop ***
        level = 1
        prev_count = 0
        while True:
            df_hr = df[(df['reports_to_CEO'] == True)]
            print('length of df_hr: ' + str(len(df_hr.index)))

            if len(df_hr.index) == prev_count:
                break
            else:
                prev_count = len(df_hr.index)

            df = df.merge(df_hr, left_on='Manager ID', right_on='Worker ID',
                          how='left', suffixes=(None, '_temp'))

            df.loc[(df['reports_to_CEO_temp'] == True) & (
                df['reports_to_CEO'].isna()), 'level_from_CEO'] = level + 1
            df.loc[(df['reports_to_CEO_temp'] == True) & (
                df['reports_to_CEO'].isna()), 'reports_to_CEO'] = True

            df = df[df.columns[~df.columns.str.endswith('_temp')]]

            # df.sort_values(by=['reports_to_CEO'],
            #                inplace=True, ascending=False)
            # print(df.head(n=15))

            level += 1
        # *** End loop ***

        # CEO reports to self
        df.loc[(df['Worker ID'] == 30120), 'Manager ID'] = 30120
        df.loc[(df['Worker ID'] == 30120), 'reports_to_CEO'] = True
        df.loc[(df['Worker ID'] == 30120), 'level_from_CEO'] = 0

        df.loc[(df['reports_to_CEO'].isna()), 'reports_to_CEO'] = False

        df['Manager ID'] = df['Manager ID'].astype(str)
        # inactive_employees = ['Ex-apprentice', 'Ex-employee', 'Ex-student/intern', 'Ex-temporary', 'Retiree']
        # df.loc[(df['Person Type'].isin(inactive_employees)) & (
        #     ~df['Joint Venture Type'].isin(['Less than 50% Owned', '50% or Less Owned'])), 'Manager ID'] = 'Term'

        # df.loc[((df['Person Type'] == 'Pensioner')), 'Manager ID'] = 'Pensioner'

        df.loc[((df['Manager ID'] == '0')), 'Manager ID'] = 'MISSING_MANAGER'

        # df = df[(df['reports_to_CEO'] == True)]
        df.to_csv(config.PATH_WD_IMP + 'data files\\reports_to_ceo.csv',
                  header=True, index=False, encoding='utf-8')
