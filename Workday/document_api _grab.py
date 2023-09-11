# -*- coding: utf-8 -*-
"""
Created on Wed Aug  2 14:41:34 2023

@author: akaff
"""

import pandas as pd
import numpy as np
import os
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
from api_call2 import get_rpt, get_file
import datetime
from datetime import date
#import xlrd
import glob as glob

import requests
import json


os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")

#import xmltodict
import io
from password_api import url, header, body


def bearer_token():
    # Retrieve Bearer Token
    r = requests.post(url, headers=header, data=json.dumps(body))
    data = r.json()
    b_token = "Bearer " + data["token"]
    return b_token


def get_rpt(reportid):
    # Get Data from specific report ID
    # Employee - All Data: 71006709
    # GL - 71656724
    # Import FIle = 100
    url_report = 'https://secure4.saashr.com/ta/rest/v1/report/saved/' + reportid
    headers_report = {"Accept": "text/csv",
                      "Authentication": bearer_token(),
                      "Content-Type": "application/json"}
    parms_report = {"company": "6160324"}
    r_report = requests.get(url_report, headers=headers_report, params=parms_report)
    rpt_data = r_report.content
    rpt_csv = pd.read_csv(io.StringIO(rpt_data.decode('utf-8')), na_values=['–', '—'])
    return rpt_csv


def get_file(fileid):
    url_report = 'https://secure4.saashr.com/ta/rest/v2/companies/|6160324/ids/' + fileid
    headers_report = {"Accept": "application/json",
                      "Authentication": bearer_token(),
                      "Content-Type": "application/json"}
    parms_report = {"company": "6160324"}
    r_report = requests.get(url_report, headers=headers_report, params=parms_report)
    rpt_data = r_report.json()
    url = rpt_data['_links']['content_rw']
    name = rpt_data['display_name']
    print(url)
    #rpt_csv = pd.read_csv(io.StringIO(rpt_data.decode('utf-8')), na_values=['–', '—'])
    return url,name



files = get_rpt('71444864')
files = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\catchup_servsafe.csv")
files2 = files['Document Id'].values.tolist()

files2 = map(str, files2)
#files2 = files2.head(n=10)
get_file('79062778')

path = (r"C:\servsafe worker docs")

def download(url: str,filename: str,dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = filename.replace('/','_')
    
    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)
    if r.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))
        
for x in files2:
    download((get_file(x)[0]),(get_file(x)[1]),dest_folder=path)
    print(x)     


#72899450
files = get_rpt('72899450')
files.to_csv(r"C:\Users\akaff\OneDrive - KBP Investments\catchup_minors.csv")
files = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\catchupminor2.csv")
files2 = files['Document Id'].values.tolist()
files2 = map(str, files2)

#get_file('8661055857')

path = (r"C:\minor docs")

def download(url: str,filename: str,dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = filename.replace('/','_')
    
    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)
    if r.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))
        
for x in files2:
    download((get_file(x)[0]),(get_file(x)[1]),dest_folder=path)
    print(x)   
    
    

