# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 16:53:22 2023

@author: akaff
"""

import os
import ast
import pandas as pd
import numpy as np
import codecs
import re
import inspect, sys
os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data conversion scripts')
import config

BUILD = 'PROD'
#path = os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion')

# -----------------------------------------------------------------------------
def modify_amount(df, col):
    """ Modify Amount """

    df[col] = df[col].astype(str)
    df[col] = df[col].str.strip()
    df[col] = df[col].str.replace(',', '')
    df[col] = df[col].str.replace('nan', '')
    df[col] = df[col].str.replace('$', '')
    df[col] = df[col].str.replace('%', '')
    df[col] = df[col].str.replace(')', '')
    df[col] = df[col].str.replace('(', '-')
    df[col] = np.where(pd.isnull(df[col]), '0', df[col])
    df.loc[(df[col] == ''), col] = '0'
    df[col] = df[col].astype(float)
    # if (('PERCENT' in col.upper()) & (df[col].gt(1).any())):
    #     df[col] = df[col].div(100)
    df[col] = df[col].map('{:.2f}'.format)

    return df


# -----------------------------------------------------------------------------
def get_deduction_recipient_name():
    """ Get Deduction Recipient Name """

    filename = ''.join([config.PATH_WD_IMP, 'payroll_dc\\Copy of Master Vendor File.csv'])

    df = pd.read_csv(filename, dtype='object', encoding='cp1251')

    return df


# -----------------------------------------------------------------------------
def open_as_utf8(filename):
    """ Open As UTF8 """

    try:
        codecs.open(filename, encoding='utf-8', errors='strict').readlines()

    except UnicodeDecodeError:
        regex = re.compile(r'[^\x00-\x7F]+')
        with open(filename, 'r') as input_file:
            entire_file = input_file.read()
        count = len(regex.findall(entire_file))

        file_utf8 = filename.replace('.csv', '_utf8.csv')

        entire_file = regex.sub(' ', entire_file)
        with open(file_utf8, 'w') as output:
            output.write(entire_file)


# -----------------------------------------------------------------------------
def set_max_event_date(df):
    """ Set Max Event Date """

    df['Employee ID'] = df['Employee ID'].astype(str)
    df['Coverage Effective From'] = pd.to_datetime(df['Coverage Effective From'])
    df.sort_values(by=['Employee ID', 'Coverage Effective From'])

    df_max_effective_date = df.groupby(['Employee ID'])['Coverage Effective From'].max()

    df = df.merge(df_max_effective_date, left_on='Employee ID', right_on='Employee ID', how='left')
    df['Event Date'] = pd.to_datetime(df['Coverage Effective From_y']).dt.strftime("%d-%b-%Y").str.upper()

    return df


# -----------------------------------------------------------------------------
def write_to_csv(df, filename):
    """ Write To CSV """

    df.drop_duplicates(inplace=True)

    filename = ''.join([config.PATH_WD_IMP, 'files\\', BUILD, '_', filename])
    # df.drop_duplicates(inplace=True)

    if 'Employee ID' in df:
        try:
            df['Employee ID'] = df['Employee ID'].astype('Int64')
        except:
            print('Employee ID not Int64')
        df = df.sort_values(by=['Employee ID']).reset_index(drop=True)
    elif 'Worker ID' in df:
        try:
            df['Worker ID'] = df['Worker ID'].astype('Int64')
        except:
            print('Worker ID not Int64')
        df = df.sort_values(by=['Worker ID']).reset_index(drop=True)
    else:
        df = df.sort_values(df.columns[0]).reset_index(drop=True)

    if filename.endswith('.csv'):
        df.to_csv(filename, header=True, index=False, encoding='utf-8')
    else:
        df.to_csv(filename, header=True, index=False, encoding='utf-8', sep='|')


# -----------------------------------------------------------------------------
def active_workers(df):
    """ Active Workers """

    file_active_ees = ''.join([config.PATH_WD_IMP, 'data files\\active_ees.csv'])

    df_active_ees = pd.read_csv(file_active_ees)
    df_active_ees = df_active_ees.merge(df, left_on='Employee Id', right_on='Employee Id', how='left')


# -----------------------------------------------------------------------------
def exclude_workers(df, id_col):
    """ Active Workers """

    df_exclude_workers = pd.read_csv(config.PATH_WD_IMP + 'data files\\exclude_workers.csv')
    
    df_exclude_workers['Kronos ID'] = df_exclude_workers['Kronos ID'].astype(int)
    df_merge = pd.merge(df, df_exclude_workers, how='outer', left_on=id_col, right_on='Kronos ID', indicator=True)

    print(df_merge)

    df.loc[df_merge['_merge'] == 'left_only']

    return df

# -----------------------------------------------------------------------------
def include_workers(df, id_col):
    """ Active Workers """

    df_exclude_workers = pd.read_csv(config.PATH_WD_IMP + 'data files\\exclude_workers.csv')
    
    df_exclude_workers['Kronos ID'] = df_exclude_workers['Kronos ID'].astype(int)
    df_merge = pd.merge(df, df_exclude_workers, how='outer', left_on=id_col, right_on='Kronos ID', indicator=True)

    print(df_merge)

    df.loc[df_merge['_merge'] == 'right_only']

    return df

# -----------------------------------------------------------------------------
def clean_up_workers(df, id_col):
    
    xx = ['138426']
    
    df[id_col] = df[id_col].astype(str)
    df = df.loc[~df[id_col].isin(xx)]
    
    return df

# -----------------------------------------------------------------------------
def read_column_mapping(in_script_name=''):
    """ Read Column Mapping """

    df = pd.read_csv(config.PATH_WD_IMP + 'data files\\column_mapping.csv')

    df.sort_values(by=['order_by'], inplace=True)

    df.replace(np.nan, '', inplace=True)

    if in_script_name != '':
        df = df.loc[df['script_name'] == in_script_name]
    else:
        script_path_name = inspect.getsourcefile(sys._getframe(1))
        script_name = os.path.basename(script_path_name)[:-3]
        df = df.loc[df['script_name'] == script_name]

    print(df)
    return df


# -----------------------------------------------------------------------------
def concat_column_mapping(df, df_col_mapping):
    """ Concat Column Mapping """

    data = '\"{'
    for index, row in df_col_mapping.iterrows():
        data += ''.join(['\'', row['column_name'], '\'', ': '])

        if row['df_legacy_column'] != '':
            data += ''.join(['df[\'', row['df_legacy_column'], '\']'])
        else:
            data += ''.join(['\'', row['default_value'], '\''])

        data += ','

    data += '}\"'
    print(data)

    new_columns = ast.literal_eval(data)
    df_new = pd.DataFrame(eval(new_columns))
    df = pd.concat([df, df_new], axis=1)

    # fill blank column values with values from column mapping file
    for index, row in df_col_mapping.iterrows():
        if row['is_na'] != '':
            print(row['df_legacy_column'] + ' ' + row['is_na'])
            df.loc[df[row['column_name']].isna(), row['column_name']] = row['is_na']
            df.loc[df[row['column_name']] == '', row['column_name']] = row['is_na']

    script_path_name = inspect.getsourcefile(sys._getframe(1))
    script_name = os.path.basename(script_path_name)[:-3]
    write_missing_required_values(df, df_col_mapping, script_name)

    # Remove unnecessary columns and order them
    df = df[df_col_mapping['column_name'].tolist()]

    return df


# -----------------------------------------------------------------------------
def write_missing_required_values(df, df_col_mapping, script_name):
    """ Write Missing Required Values """

    required_columns = ['Error Message', 'Legacy Column']

    df_required = pd.DataFrame()
    for index, row in df_col_mapping.iterrows():
        if row['required'] == 'Yes':
            required_columns.append(row['column_name'])
            df_nulls = df.loc[df[row['column_name']].isna()]
            print(row['column_name'] + ': ' + str(len(df_nulls)))
            if len(df_nulls) > 0:
                df_nulls.insert(0, 'Error Message', 'Missing value ' + row['column_name'])
                if row['underlying_legacy_column'] != '':
                    df_nulls.insert(1, 'Legacy Column', df[row['underlying_legacy_column']])
                else:
                    df_nulls.insert(1, 'Legacy Column', '')
                df_required = pd.concat([df_required, df_nulls])

    if len(df_required) > 0:
        df_required = df_required[required_columns]
        df_required.to_csv(path + 'nulls\\nulls_' + script_name + '.csv',
            header=True, index=False, encoding='utf-8')
        

# -----------------------------------------------------------------------------
"""
def reciprocity_state(df,w_col,h_col):
    
    #check to see if local taxes meet state reciprocity
    work_states = [['Arizona','District of Columbia','Illinois','Indiana','Iowa','Kentucky','Maryland','Michigan','Minnesota','New Jersey','Ohio','Pennsylvania','Virginia','West Virginia','Wisconsin']]
    
    
    
    if df[w_col].isin(states)
"""    