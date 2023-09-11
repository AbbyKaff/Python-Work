# -*- coding: utf-8 -*-
"""
Created on Wed May 31 09:04:46 2023

@author: akaff
"""

import pandas as pd
#import pysftp 
import os
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
from api_call2 import get_rpt
from apiposter import (get_impt_lst, postfile, poststatus)
import time
import paramiko
import datetime
from datetime import datetime
from datetime import timedelta
from datetime import date
import glob
import numpy as np




nphost = "sftp.talentreef.com"
npport = 22
npusr = "kbpbells"
nppwd = "k8p8311s"


def openstransport():
     # Open a transport
     transport = paramiko.Transport((nphost,npport))

     # Auth    
     transport.connect(None,npusr,nppwd)
     return transport


def opensftp(transport):
     # Go!    
     sftp = paramiko.SFTPClient.from_transport(transport)
     return sftp


def putsftp(sftp, localfile, filename):
     sftp.put(localfile, './' + filename)

def dirsftp(sftp):
     print(sftp.listdir('.'))


def closesftp(sftp, transport):
     # Close Connection
     sftp.close()
     transport.close()
     
     
import requests
import json
# import xmltodict
import io
from password_api import url, header, body


def bearer_token():
    # Retrieve Bearer Token
    r = requests.post(url, headers=header, data=json.dumps(body))
    data = r.json()
    b_token = "Bearer " + data["token"]
    return b_token


# Call reports endpoint to get list of reports that are saved to your profile
def get_impt_lst():
    url_report = 'https://secure4.saashr.com/ta/rest/v1/imports'

    headers_report = {"Accept": "application/json",
                      "Authentication": bearer_token(),
                      "Content-Type": "application/json"}

    parms_report = {"company": "6160324"}

    r = requests.get(url_report, headers=headers_report, params=parms_report)
    rcont = r.content
    rjson = json.loads(rcont)
    df = pd.json_normalize(rjson["Imports"])
    return df


def postfile(csv, importid):
    url_post = 'https://secure4.saashr.com/ta/rest/v1/imports' + importid

    headers_post = {"Accept": "application/json",
                    "Authentication": bearer_token()}

    files = {'upload_file': open(csv, 'rb')}
    r = requests.post(url_post, headers=headers_post, files=files)
    return r
     
     
import traceback
os.chdir(r'C:\Users\akaff')
import config_sftp as cn


def set_sftp_conn(host, port, username, password):
    """ set sftp connection to get the files, using config.py """
    # connect to sftp
    transport = paramiko.Transport((host, port))
    print("connecting to SFTP...")
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("connection established.")
    return sftp


remote_images_path = '/outbound/'
local_path = r'C:/Users/akaff/tr_sftp/'

sftp = set_sftp_conn(cn.host, cn.port, cn.username, cn.password)
mod_date = (datetime.now() + timedelta(days=-1)).date()  # set a date variable to yesterday
mod_date = mod_date.strftime("%Y%m%d")

files = [file for file in sftp.listdir(remote_images_path) if file.startswith('KBPBells_EmployeeTaxSettings_'+ mod_date)]

for file in files:
    file_remote = remote_images_path + file
    file_local = local_path + file

    print(file_remote + '>>>' + file_local)

    sftp.get(file_remote, file_local)

sftp.close()

os.chdir(r'C:/Users/akaff/tr_sftp/')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f,encoding="cp1251") for f in all_filenames ])

combined_csv_co = combined_csv[(combined_csv['State'] == 'CO')]

combined_csv_co['State Allowances_fix'] = combined_csv_co['State Additional Withholding']
combined_csv_co['Annual Withholding Allowance'] = combined_csv_co['State Allowances']
combined_csv_co.drop(columns=['State Additional Withholding','State Allowances'],inplace=True)

combined_csv_co = combined_csv_co.rename(columns={'State Allowances_fix': 'State Allowances'})

combined_csv_co['State Additional Withholding'] = ''

combined_csv_co = combined_csv_co[['SSN', 'EIN Tax Id', 'Federal Filing Status',
       '2020 and forward Form W-4', 'Federal Withhold',
       'Federal Multiple Jobs', 'Federal Claim Dependents',
       'Federal Other Income', 'Federal Deductions', 'Federal Allowances',
       'Federal Additional Withholding Type', 'Federal Additional Withholding',
       'State', 'State Withhold', 'Unemployment State',
       'Unemployment State Start Date', 'State Additional Withholding Type',
       'State Additional Withholding', 'State Allowances',
       'State Elected Percentage Rate','Annual Withholding Allowance']]

combined_csv = combined_csv[(combined_csv['State'] != 'CO')]
combined_csv['Annual Withholding Allowance'] = ''

combined_csv = pd.concat([combined_csv,combined_csv_co])

#combined_csv.loc[~(combined_csv['State Additional Withholding'].isnull(),'State Additional Withholding')] = combined_csv['State Additional Withholding'].astype(int)
                                                                                                                                                                               #combined_csv['State Additional Withholding'].astype(int)
#combined_csv['State Allowances'] = combined_csv['State Allowances'].astype(int)
#combined_csv['State Elected Percentage Rate'] = combined_csv['State Elected Percentage Rate'].astype(float)

#imports = get_impt_lst()
#150 - Employee Tax Settings

combined_csv.to_excel(r'C:\Users\akaff\tr_fix_tax_settings.xlsx',index=False)
combined_csv.to_csv(r'C:\Users\akaff\tr_fix_tax_settings.csv',index=False)

#postfile(r'C:\Users\akaff\tr_fix_tax_settings.xlsx','150')

###############################################################################
########################### MO Probation Period ###############################
###############################################################################

mopo = get_rpt('72834953')

mopo['Date Hired'] = pd.to_datetime(mopo['Date Hired'])
mopo['Date Re-Hired'] = pd.to_datetime(mopo['Date Re-Hired'])
mopo['masterhiredt'] = np.where(mopo['Date Re-Hired'] > mopo['Date Hired'],mopo['Date Re-Hired'],mopo['Date Hired'])
mopo['masterhiredt'] = pd.to_datetime(mopo['masterhiredt'], format='%m/%d/%Y')
today_date = datetime.now()
mopo['LOS'] = (today_date - mopo['masterhiredt']).dt.days
mopo = mopo[(mopo['LOS'] < 40)| (mopo['Probationary Code'].isna())]

mopo['Probationary Code'] = np.where(mopo['LOS'] < 28, 'Y','N')

mopo_c = mopo[['Employee Id','Employee EIN','Tax State/Province','Probationary Code']]
mopo_c = mopo_c.rename(columns={'Employee EIN':'EIN Name','Tax State/Province':'State'})
mopo_c.to_csv(r"C:\Users\akaff\OneDrive - KBP Investments\mo_pro.csv", index=False)

###############################################################################
########################### NJ Private Tax Plan ###############################
###############################################################################

njpp = get_rpt('72834954')
njpp = njpp[njpp['Private Plan Indicator'].isna()]

njpp['Private Plan Indicator'] = 'D'
njpp = njpp[['Employee Id','Employee EIN','Tax State/Province','Private Plan Indicator']]
njpp = njpp.rename(columns={'Employee EIN':'EIN Name','Tax State/Province':'State'})
njpp.to_csv(r"C:\Users\akaff\OneDrive - KBP Investments\nj_pp.csv",index=False)


