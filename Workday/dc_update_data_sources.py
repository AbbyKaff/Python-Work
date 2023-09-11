# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 09:00:55 2023

@author: akaff

PRIOR TO RUNNING PLEASE READ!!!!!!!!!!!!!!!
WHEN REPULL HAPPENS RE RUN TAX UPDATE AND CONTACT NAME UPDATE AND PROFILE UPDATES AND KBP CARES UPDATES
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"""

import pandas as pd
import numpy as np
import os
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
from api_call2 import get_rpt
import datetime
from datetime import date
from datetime import datetime
import xlrd
import glob as glob

os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion')

path = r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources'
os.chdir(path)

df_70602656 = get_rpt('70602656')
df_70602656.to_csv('70602656.csv')

#71457227
df_71457227 = get_rpt('71457227')
df_71457227.to_csv('71457227.csv')

#71567958
df_71567958 = get_rpt('71567958')
df_71567958.to_csv('71567958.csv')


#71877396
df_71877396 = get_rpt('71877396')
df_71877396.to_csv('71877396.csv')

#72604657
df_72604657 = get_rpt('72604657')
df_72604657.to_csv('72604657.csv')

#72866648
df_72866648 = get_rpt('72866648')
df_72866648.to_csv('72866648.csv')

#71877725
df_71877725 = get_rpt('71877725')
df_71877725.to_csv('71877725.csv')

#71250922
df_71250922 = get_rpt('71250922')
df_71250922.to_csv('71250922.csv')

#72578980
df_72578980 = get_rpt('72578980')
df_72578980.to_csv('72578980.csv')


#72584718
df_72584718 = get_rpt('72584718')
df_72584718.to_csv('72584718.csv')


#72593409
df_72593409 = get_rpt('72593409')
df_72593409.to_csv('72593409.csv')


#72593417
df_72593417 = get_rpt('72593417')
df_72593417.to_csv('72593417.csv')

#72593419
df_72593419 = get_rpt('72593419')
df_72593419.to_csv('72593419.csv')

#72593428
df_72593428 = get_rpt('72593428')
df_72593428.to_csv('72593428.csv')


#72596865
df_72596865 = get_rpt('72596865')
df_72596865.to_csv('72596865.csv')

#72869093
df_72869093 = get_rpt('72869093')
df_72869093.to_csv('72869093.csv')


#72869092
df_72869092 = get_rpt('72869092')
df_72869092.to_csv('72869092.csv')

#72596866
df_72596866 = get_rpt('72596866')
df_72596866.to_csv('72596866.csv')


#72879939
df_72879939 = get_rpt('72879939')
df_72879939.to_csv('72879939.csv')

#72879940
df_72879940 = get_rpt('72879940')
df_72879940.to_csv('72879940.csv')

#72603205
df_72603205 = get_rpt('72603205')
df_72603205.to_csv('72603205.csv')


#72603206
df_72603206 = get_rpt('72603206')
df_72603206.to_csv('72603206.csv')


#72603207
df_72603207 = get_rpt('72603207')
df_72603207.to_csv('72603207.csv')


#72603208
df_72603208 = get_rpt('72603208')
df_72603208.to_csv('72603208.csv')


#72604614
df_72604614 = get_rpt('72604614')
df_72604614.to_csv('72604614.csv')

#72604615
df_72604615 = get_rpt('72604615')
df_72604615.to_csv('72604615.csv')


#72604616
df_72604616 = get_rpt('72604616')
df_72604616.to_csv('72604616.csv')


#72604618
df_72604618 = get_rpt('72604618')
df_72604618.to_csv('72604618.csv')

#72604619
df_72604619 = get_rpt('72604619')
df_72604619.to_csv('72604619.csv')


#72744196
df_72744196 = get_rpt('72744196')
df_72744196.to_csv('72744196.csv')

#72830427
df_72830427 = get_rpt('72830427')
df_72830427.to_csv('72830427.csv')


#72830428
df_72830428 = get_rpt('72830428')
df_72830428.to_csv('72830428.csv')

#72867409
df_72867409 = get_rpt('72867409')
df_72867409.to_csv('72867409.csv')

#72867407
df_72867407 = get_rpt('72867407')
df_72867407.to_csv('72867407.csv')


#72860461
df_72860461 = get_rpt('72860461')
df_72860461.to_csv('72860461.csv')

#72864451 - deferred_comp_eligible
df_72864451 = get_rpt('72864451')
df_72864451.to_csv('deferred_comp_eligible.csv')

#71567958 - ScheduledDeductions-ProdAPI_Ded_401K
df_71567958 = get_rpt('71567958')
df_71567958.to_csv('71567958.csv')

df_72593419 = get_rpt('72593419')
df_72593419.to_csv('72593419.csv')
