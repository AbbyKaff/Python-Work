##############################################################################
####################### Local Work City Tax Elections ########################
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
    
    include_tax = ['Alexandria - OLF',
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
'Paducah - OLF']
    kro_tax2 = kro_tax2[kro_tax2['Company Tax Name'].isin(include_tax)]
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
    kro_taxc['Payroll Local City Tax Authority Code'] = kro_taxc['Payroll Local City Tax Authority Code'].str.zfill(7)
    kro_tax = kro_taxc[kro_taxc['WD EE Code'].str.startswith('W_CITYW')]

    
    #work_del = pd.read_csv(config.PATH_WD_IMP + 'Workday Delivered\\Integration_IDs.csv', encoding='cp1251')
    #work_del_c = work_del[work_del['Type'] == 'Payroll_Local_City_Authority_Tax_Code']

    kro_tax['Employee ID'] = kro_tax['Employee Id']
    kro_tax['Source System'] = 'Kronos'
    kro_tax['Company'] = kro_tax['Cost Centers(Company Code)'].replace({'FQSR': 'FQ'})
    kro_tax['Effective As Of'] = pd.to_datetime(kro_tax['Created']).dt.strftime('%d-%b-%Y').str.upper()
    kro_tax['Number of Allowances'] = kro_tax['Total Allowances - NYC']
    kro_tax['Additional Amount'] = kro_tax['Additional Allowances']
    kro_tax['Exempt Indicator (Michigan, Pennsylvania)'] = kro_tax['Maryland State Tax Exemption for PA residents']
    kro_tax['Constant Text'] = ''
    kro_tax['Inactive'] = 'N'
    kro_tax['Currency Code'] = 'USD'
    kro_tax['Number of Allowances (Michigan)'] = ''
    kro_tax['Constant Percent (Michigan)'] = np.where(kro_tax['Tax State/Province_x'] == 'Michigan','1','')
    kro_tax['Constant Text'] = ''
    kro_tax['Previous Employer Deducted Amount (Pennsylvania)'] = ''
    kro_tax['Primary EIT Pennsylvania'] = ''
    kro_tax['Not Subject to EIT Pennsylvania'] = ''
    kro_tax['Low Income Threshold'] = ''
    #kro_tax['Company Tax Name'] = kro_tax['Company Tax Name'].str.replace("City Tax", "")

    

    city_tax2 = kro_tax[['Employee ID', 'Source System', 'Company',
                          'Effective As Of', 'Payroll Local City Tax Authority Code',
                          'Number of Allowances (Michigan)', 'Constant Percent (Michigan)',
                          'Exempt Indicator (Michigan, Pennsylvania)', 'Constant Text',
                          'Previous Employer Deducted Amount (Pennsylvania)',
                          'Primary EIT Pennsylvania', 'Not Subject to EIT Pennsylvania',
                          'Additional Amount', 'Low Income Threshold', 'Currency Code',
                          'Inactive']]

    write_to_csv(city_tax2, 'city_tax_elections_work.txt')
