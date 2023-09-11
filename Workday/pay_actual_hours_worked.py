##############################################################################
############################ Actual Hours Worked #############################
##############################################################################

import pandas as pd
import numpy as np
import datetime
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    hrs = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72604618.csv', dtype='object', encoding='cp1251')

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    hrs['Employee Id'] = hrs['Employee Id'].astype(str)
    hrs = hrs.loc[hrs['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    hrs = hrs.replace('^[-]$', '', regex=True)
    hrs = modify_amount(hrs, 'Total Work Hours')

    hrs = hrs.loc[hrs['Type'] == 'Regular']
    hrs = hrs.loc[hrs['Total Work Hours'].astype(float) > 0]

    hrs['Employee ID'] = hrs['Employee Id']
    hrs['Company'] = hrs['Cost Centers(Company Code)'].replace({'FQSR':'FQ'})
    hrs['Position'] = ''

    # Hardcode this
    hrs.loc[hrs['Employee ID'] == '97455', 'Company'] = 'FQ'

    # hrs['Period End Date'] = hrs['Pay Date']
    hrs['Payment Date'] = pd.to_datetime(hrs['Pay Date']).dt.strftime('%d-%b-%Y').str.upper()
    #hrs['Earning'] = hrs['Type'].replace({'Regular':'REG'})
    hrs['Earning'] = np.where(hrs['Pay Type'] == 'Hourly','REG','SREG')
    hrs['Related Calculation'] = np.where(hrs['Pay Type'] == 'Hourly', 'W_HRSU', 'W_HRSP')
    hrs['Hours Worked'] = hrs['Total Work Hours']

    pg_data = pd.read_csv(r"C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\files\PROD_pay_gp_assignments_redo_082523.txt",  sep='|')


    pg_datac = pg_data[['Employee ID', 'Source System', 'Effective Date', 'Pay Group ID']]
    # -----------------------------------------------------------------------------
    hrs['Employee ID'] = hrs['Employee ID'].astype(int)
    hrsc = hrs.merge(pg_datac, on='Employee ID')
    hrsc['Pay Group'] = hrsc['Pay Group ID']
    hrsc['Period End Date'] = pd.to_datetime(hrsc['Pay Period End']).dt.strftime("%d-%b-%Y").str.upper()
    # -----------------------------------------------------------------------------
    # Use Period End Date for each Pay Group
    # USA Bi-Weekly => 03/20/2023
    # USA TB Bi-Weekly => 03/21/2023
    # USA TB Weekly => 03/21/2023
    # USA Weekly => 03/20/2023
    # hrsc['Period End Date'] = pd.to_datetime(hrsc['Pay Date']).dt.strftime("%d-%b-%Y").str.upper()

    #hrsc.loc[hrsc['Pay Group'] == 'USA_Bi-Weekly', 'Period End Date'] = '11-JUL-2023'
    #hrsc.loc[hrsc['Pay Group'] == 'USA_TB_Bi-Weekly', 'Period End Date'] = '12-JUL-2023'
    #hrsc.loc[hrsc['Pay Group'] == 'USA_TB_Weekly', 'Period End Date'] = '12-JUL-2023'
    #hrsc.loc[hrsc['Pay Group'] == 'USA_Weekly', 'Period End Date'] = '11-JUL-2023'
    # -----------------------------------------------------------------------------
    
    hrsc = hrsc.merge(df_pm1, on='Employee ID', how='left')
    hrsc['Position'] = hrsc['Position ID']
    hrsc.loc[hrsc['Position'].isnull(), 'Position'] = "J-" + hrsc['Employee ID'].astype(str)
    hrsc['Source System'] = hrsc['Source System_x']
    hrsc = hrsc[['Employee ID', 'Source System', 'Company', 'Pay Group', 'Position',
                 'Period End Date', 'Payment Date', 'Earning', 'Related Calculation',
                 'Hours Worked']]

    write_to_csv(hrsc, 'actual_work_hrs_082923.txt')
