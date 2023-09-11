# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 15:04:48 2023

@author: akaff
"""

##############################################################################
########################## Performance Reviews #################################
##############################################################################
import pandas as pd
import numpy as np
import datetime
import xlrd
import os
import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)

rev = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\talent_dc\perf_reviews.csv")

#------------------------------------------------------------------------------
cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
rev['Employee ID'] = rev['Employee ID'].astype(str)
rev = rev.loc[rev['Employee ID'].isin(cut_off_ees['Worker ID'].astype(str))]
#------------------------------------------------------------------------------



os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\talent_dc')
rev.to_csv('PROD_perf_reviews.txt',sep='|', encoding='utf-8', index=False)
