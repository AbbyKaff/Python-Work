# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 10:23:09 2023

@author: akaff
"""

##############################################################################
############################ Worker Documents ################################
##############################################################################
import zipfile
from zipfile import ZipFile
from pathlib import Path

import pandas as pd
import numpy as np
import os
# os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
# from api_call2 import get_rpt
import datetime
from datetime import date
from datetime import datetime
import xlrd
import glob as glob

import config



############################### Minors #####################################
path = ('C:\minor docs')

fun = lambda x : os.path.isfile(os.path.join(path,x))

files_list = filter(fun, os.listdir(path))

# Create a list of files in directory along with the size
size_of_file = [
    (f,os.stat(os.path.join(path, f)).st_size)
    for f in files_list
]
fun = lambda x : x[1]

data = []
# in this case we have its file path instead of file
for f, s in sorted(size_of_file, key=fun):
    data.append({'filename': f, 'size_mb': round(s/(1024*1024), 3)})

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

df.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\minors1.csv')

#minor_docs = pd.read_csv(config.PATH_WD_IMP + "worker_documents\\minor_docs.csv",encoding="cp1251")
df_min = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\minors1_matched.csv",encoding="cp1251")
#df_min = df.merge(minor_docs, left_on='filename',right_on='Display Name')
df_min['Worker ID'] = df_min['Employee Id']
df_min.loc[df_min['filename'] == 'i9_12297978_rec_2902455_file_1_uuid_E27303B4-FF2B-461A-BA2AB50F7AA1F59D.jpeg (960Г—1280) syncere.pdf', 'filename'] = 'i9_12297978.pdf'
df_min.loc[df_min['filename'] == 'DaвЂ™mya Hawkins WP.pdf', 'filename'] = 'Damya Hawkins WP.pdf'
df_min['File Name'] = df_min['filename']
df_min['File Type'] = df_min['Document Type']
df_min['Source System'] = 'Kronos'
df_min['Worker Type'] = ''
df_min['File Content'] = ''
df_min = df_min[df_min['File Name'] != 'image.jpg']
df_min_c = df_min


# Group the files into bins of 30 MB max
df_min['running_total_mb'] = df_min['size_mb'].cumsum()

zip_counter = 0
mb_min = 0
mb_max = 400
output_path = 'C:/minor_zip'
for index, row in df_min.iterrows():
    while True:
        if row['running_total_mb'] > mb_min and row['running_total_mb'] < mb_max:
            with zipfile.ZipFile(os.path.join(output_path, f'minors_zip_{zip_counter}.zip'), 'a') as arch:
                arch.write(os.path.join('C:/minor docs', row['filename']))
            with open(os.path.join(output_path, f'minor_filenames_{zip_counter}.csv'), 'a') as fname_list:
                fname_list.write(row['filename'])
                fname_list.write('\n')
            df_min.drop(index, inplace=True)
            break
        else:
            mb_min += 0
            mb_max += 400
            zip_counter += 1
        break

# dir_path = (r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\minors_zipfiles')
dir_path = (r'C:\minor_zip')
all_files = glob.glob(dir_path + "\*.csv")
dfx = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

# create a dataframe from the list

dfx['Zip File Name'] = dfx['Zip File Name'].str.replace('filenames','zip')

df_minor = dfx.merge(df_min_c,on='File Name')

df_minor = df_minor[['Worker ID',
'Source System',
'Zip File Name',
'File Name',
'Worker Type',
'File Type',
'File Content']]



############################### Servsafe #####################################

path = ('C:\servsafe worker docs')

fun = lambda x : os.path.isfile(os.path.join(path,x))

files_list = filter(fun, os.listdir(path))

# Create a list of files in directory along with the size
size_of_file = [
    (f,os.stat(os.path.join(path, f)).st_size)
    for f in files_list
]
fun = lambda x : x[1]

data = []
# in this case we have its file path instead of file
for f, s in sorted(size_of_file, key=fun):
    data.append({'filename': f, 'size_mb': round(s/(1024*1024), 3)})

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

df.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\servsafes2.csv')

servsafe_docs = pd.read_csv(config.PATH_WD_IMP + "worker_documents\\servsafe_docs.csv",encoding="cp1251")

df2 = pd.DataFrame(
    [os.path.splitext(f) for f in df.filename],
    columns=['filename_c', 'Type']
)
df2['File Name'] = df2['filename_c'] + df2['Type']
df2 = df2.merge(df, left_on='File Name', right_on='filename')

servsafe_docs['Display Name']= [os.path.splitext(x)[0] for x in servsafe_docs['Display Name']]


df_ss = df2.merge(servsafe_docs, left_on='filename_c',right_on='Display Name')
df_ss = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\servsafe_matched.csv",encoding="cp1251")
df_ss.loc[df_ss['filename'] == '5/10/2028', 'filename'] = '5_10_2028'
df_ss['filename'] = df_ss['filename'].str.replace('/','_')
df_ss['Worker ID'] = df_ss['Employee Id']
df_ss['File Name'] = df_ss['filename']
df_ss['File Type'] = df_ss['Document Type']
df_ss['Worker Type'] = ''
df_ss['File Content'] = ''
#df_ss['File Name'] = df_ss['filename']+df_ss['Type']
df_ss = df_ss[df_ss['File Name'] != 'image.jpg']
df_ss_c = df_ss


# Group the files into bins of 30 MB max
df_ss['running_total_mb'] = df_ss['size_mb'].cumsum()

zip_counter = 0
mb_min = 0
mb_max = 400
# output_path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\worker_documents\servsafe_zipfiles'
output_path = 'C:/servsafe_zip'
for index, row in df_ss.iterrows():
    print(row)
    while True:
        if row['running_total_mb'] > mb_min and row['running_total_mb'] < mb_max:
            with zipfile.ZipFile(os.path.join(output_path, f'servsafe_zip_{zip_counter}.zip'), 'a') as arch:
                arch.write(os.path.join('C:/servsafe worker docs', row['filename']))
            with open(os.path.join(output_path, f'servsafe_filenames_{zip_counter}.csv'), 'a') as fname_list:
                fname_list.write(row['filename'])
                fname_list.write('\n')
            df_ss.drop(index, inplace=True)
            break
        else:
            mb_min += 0
            mb_max += 400
            zip_counter += 1
        break

dir_path = (r'C:\servsafe_zip')
all_files = glob.glob(dir_path + "\*.csv")

dfy = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

dfy = dfy.rename(columns={'2.8.2028.pdf': 'Zip File Name'})

# create a dataframe from the list
dfy['Zip File Name'] = dfy['Zip File Name'].str.replace('filenames','zip')

df_servsafe = dfy.merge(df_ss_c, left_on='File Name', right_on='File Name')

df_servsafe['Source System'] = 'Kronos'

df_servsafe = df_servsafe[['Worker ID',
'Source System',
'Zip File Name',
'File Name',
'Worker Type',
'File Type',
'File Content']]


df_docs = pd.concat([df_minor,df_servsafe])

write_to_csv(df_docs, 'worker_documents_082423.txt')

df_docs = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_worker_documents.csv")

