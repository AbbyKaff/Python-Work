import requests
import json
import pandas as pd
import numpy as np
import os

os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")

#import xmltodict
import io
from password_api import url, header, body
#Password python file
#url = 'https://secure4.saashr.com/ta/rest/v1/login'

#header = {"Accept": "application/json",
#          "api-key": "8ush535utogu0ejdgthip154gue5n1pz",
#          "Content-Type": "application/json"}

#body = {"credentials": {"username": "",
#                        "password": ",
 #                       "company": ""}}



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
