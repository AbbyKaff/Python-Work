import pandas as pd
import pysftp 
import os
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\python_api")
from api_call2 import get_rpt
import time
import paramiko
import datetime
from datetime import date
from datetime import timedelta

adp = get_rpt('71787969')


######## Only need to run this to identify store names in the ADP system
#os.chdir(r'C:\Users\akaff\OneDrive - KBP Investments\ADP Files')
#contact = pd.read_csv(r'./store_name_numbers.csv')
#test = adp.merge(contact, left_on='LOCATION2', right_on='Store Number')
os.chdir(r"C:\Users\akaff\OneDrive - KBP Investments\ADP Files")
pilot_stores = ['G135015','G135271','G135285','G135286','G135287','G135288','G135289',
                'G135290','G135432','G135433','G135434','G135462','G135463','G135464',
                'G135480','G135526','G135527','G135528','G135529','G135541','G135692',
                'G135479','G135270','G135272','G135273','G135274','G135275','G135276',
                'G135277','G135777','G135778','G135779','G135780','G135781','G135895']
#adp = adp[adp['Store Number'].isin(pilot_stores)]
adp['Active From'] = pd.to_datetime(adp['Active From'], errors='coerce')
adp['Active From'] = adp['Active From'].dt.date
today = datetime.date.today()
day = datetime.timedelta(days = 1)
adp['Yesterday'] = today - day
adp = adp[((adp['Active From'] == adp['Yesterday']))]
#adp = adp[((adp['lastname'] == 'Barrett'))]
adp = adp.drop(['Store Number','Routing #','Active From','Yesterday'], axis=1)
adp.drop_duplicates(inplace=True)
#pad all zipcodes with 0 if they were read in as a int and lost their 0
adp['zipcode']  = adp['zipcode'].astype(str).str.pad(5,fillchar='0')
date = time.strftime('%Y%m%d')
adp['phone'] = ''

adp.to_csv('./kbpfoods1_ATM_P_'+date+'.csv',index=False)

nphost = "sdgcl.adp.com"
npport = 22
npusr = "kbpfoods1acct"



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
     
sftp = opensftp(openstransport())
#putsftp(sftp,'kbpfood1_ATM_T_'+date+'.csv', 'INBOUND')
sftp.put('kbpfoods1_ATM_P_'+date+'.csv', 'INBOUND/kbpfoods1_ATM_P_'+date+'.csv')
#test = sftp.get("OUTBOUND/kbpfoods1_ATM_T_20210322.csv", "kbpfoods1_ATM_T_20210322.csv")
closesftp(sftp,openstransport())
