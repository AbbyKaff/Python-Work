##############################################################################
####################### Local Home City Tax Elections ########################
##############################################################################


import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    kro_tax1 = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72604614.csv', dtype='object', encoding='cp1251')
    kro_tax2 = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72879940.csv', dtype='object', encoding='cp1251')
    
    '''include_tax = ['Alexandria - OLF',
'Covington - OLF',
'Dry Ridge - OLF',
'Erlanger - OLF',
'Florence - OLF',
'Fort Thomas - OLF',
'Georgetown - OLF',
'Newport - OLF',
'Taylor Mill - OLF',
'Franklin, Township of - EIT - Gettysburg Area S D (013753)',
'Franklin, Township of - LST - Gettysburg Area S D (013753)',
'Gettysburg, Borough of - EIT - Gettysburg Area S D (013753)',
'Gettysburg, Borough of - LST - Gettysburg Area S D (013753)',
'Guilford, Township of - EIT - Chambersburg Area S D (281302)',
'Guilford, Township of - LST - Chambersburg Area S D (281302)',
'Lower Swatara, Township of - EIT - Middletown Area S D (226003)',
'Lower Swatara, Township of - LST - Middletown Area S D (226003)',
'Manheim, Township of - EIT - Manheim Twp S D (364503)',
'Manheim, Township of - LST - Manheim Twp S D (364503)',
'Washington, Township of - EIT - Waynesboro Area S D (289003)',
'Washington, Township of - LST - Waynesboro Area S D (289003)',
'Whitpain, Township of - EIT - Wissahickon S D (469303)',
'Whitpain, Township of - LST - Wissahickon S D (469303)',
'Paducah - OLF']'''
    #kro_tax2 = kro_tax2[kro_tax2['Company Tax Name'].isin(include_tax)]
    kro_tax = pd.concat([kro_tax1, kro_tax2])

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data sources\name_and_email.txt', sep='|')
    kro_tax['Employee Id'] = kro_tax['Employee Id'].astype(str)
    kro_tax = kro_tax.loc[kro_tax['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------
    #exclude = ['Work From Home','Home Office']
    #kro_tax = kro_tax[~(kro_tax['Location(1)'].isin(exclude))]
    work_del = pd.read_excel(config.PATH_WD_IMP + 'data files\\Kronos_Company_Taxes.xlsx')
    work_del_c = work_del
    kro_taxc = kro_tax.merge(work_del_c, left_on=['Company Tax Name'], right_on=['Company Tax Name'], how='left')
    #kro_taxc['Payroll Local City Tax Authority Code'] = kro_taxc['ID']
    #kro_taxc['Payroll Local City Tax Authority Code'] = kro_taxc['WD EE Code'].str[-7:]
    #kro_taxc['Payroll Local City Tax Authority Code'] = kro_taxc['Payroll Local City Tax Authority Code'].str.zfill(7)
    kro_taxc = kro_taxc[kro_taxc['WD EE Code'].str.startswith('W_CITYR') | kro_taxc['WD EE Code'].str.startswith('W_PNLST')]
    kro_taxc = kro_taxc[~kro_taxc['Payroll Local City Tax Authority Code'].isna()]
    
    kro_taxc['Employee ID'] = kro_taxc['Employee Id']
    kro_taxc['Source System'] = 'Kronos'
    kro_taxc['Company'] = kro_taxc['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})

    # Effective Date is before Hire Date
    # kro_tax['Effective As Of'] = np.where(pd.to_datetime(kro_tax['Created']) < '01/01/2023', '01/01/2023', kro_tax['Created'])
    kro_taxc['Effective As Of'] = pd.to_datetime(kro_taxc['Created']).dt.strftime('%d-%b-%Y').str.upper()

    kro_taxc['Number of Allowances'] = kro_taxc['Total Allowances - NYC']
    kro_taxc['Additional Amount'] = kro_taxc['Additional Allowances']
    kro_taxc['Exempt Indicator Pennsylvania'] = kro_taxc['Maryland State Tax Exemption for PA residents']
    kro_taxc['Constant Text'] = ''
    kro_taxc['Inactive'] = 'N'
    #kro_tax['Company Tax Name'] = kro_tax['Company Tax Name'].str.replace("City Tax", "")

    #kro_taxc = kro_tax.merge(work_del_c, left_on=['Company Tax Name'], right_on=['Company Tax Name'], how='left')
    
    #kro_tax_o = kro_taxc[kro_taxc['ID'].isna()]
    #kro_taxco = kro_tax_o.merge(work_del_c, left_on=['Tax State/Province','Company Tax Name'], right_on=['State','Authority'], how='left')
    #kro_taxco.to_csv(r'C:\Users\akaff\OneDrive - KBP Investments\Workday Implentation\Data Conversion\data files\test.csv')
    
    #kro_taxc['Effective As Of'] = pd.to_datetime(kro_taxc['Created']).dt.strftime('%d-%b-%Y').str.upper()
    city_tax = kro_taxc[['Employee ID', 'Source System', 'Company',
                         'Effective As Of', 'Payroll Local City Tax Authority Code',
                         'Number of Allowances', 'Additional Amount',
                         'Exempt Indicator Pennsylvania', 'Constant Text', 'Inactive']]

    city_tax['Employee ID'] = city_tax['Employee ID'].astype(int)
    write_to_csv(city_tax, 'city_tax_elections_home.txt')
