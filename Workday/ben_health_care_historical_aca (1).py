##############################################################################
###################### Local Work County Tax Elections #######################
##############################################################################

import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import xlrd

import config
from common import (write_to_csv, active_workers, modify_amount, open_as_utf8, set_max_event_date)


# -----------------------------------------------------------------------------
if __name__ == '__main__':

    active_bene = pd.read_csv(config.PATH_WD_IMP + 'data sources\\72578980.csv', dtype='object', encoding='cp1251')
    # active_bene = active_bene[active_bene['Coverage Name'] != 'Waived']

    #------------------------------------------------------------------------------
    cut_off_ees = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\E2E_name_and_email.txt', sep="|")
    active_bene['Employee Id'] = active_bene['Employee Id'].astype(str)
    active_bene = active_bene.loc[active_bene['Employee Id'].isin(cut_off_ees['Worker ID'].astype(str))]
    #------------------------------------------------------------------------------

    active_benec = active_bene[active_bene['Benefit Type'].isin(['Medical'])]
    active_benec = active_benec.rename(columns={'Employee Id': 'Employee ID'})

    active_benec = set_max_event_date(active_benec)
    active_benec.loc[active_benec['Event Date'] > '27-MAR-2023', 'Event Date'] = '27-MAR-2023'

    active_benec['Benefit Event Type'] = 'BEN_CONVERSION_HEALTHCARE'
    active_benec['Health Care Coverage Plan'] = active_benec['Benefit Plan Name']

    dict_coverage_target = {'Employee Only': 'EE_Only',
                          'Employee + Spouse': 'EE+Spouse',
                          'Employee + Child': 'EE+Child(ren)',
                          'Employee + Children': 'EE+Child(ren)',
                          'Employee + Family': 'EE+Family',
                          'Waived': ''}

    active_benec['Health Care Coverage Target'] = active_benec['Coverage Name'].replace(dict_coverage_target)

    #merge dependent id from related person
    # dep_ff = pd.read_csv(config.PATH_WD_IMP + 'benefits_dc\\fixed_erp.csv')
    dep_ff = pd.read_csv(config.PATH_WD_IMP + 'payroll_dc\\E2E_emp_rel_per.txt', delimiter='|')
    erp_c = dep_ff[['Employee ID', 'Dependent ID']]
    erp_c = erp_c.loc[erp_c['Dependent ID'].str.contains('D', na=False)]
    active_benec['Employee ID'] = active_benec['Employee ID'].astype(str)
    erp_c['Employee ID'] = erp_c['Employee ID'].astype(str)
    active_benec = active_benec.merge(erp_c, on='Employee ID')

    # if coverage is waived, then remove dependents
    active_benec.loc[active_benec['Coverage Name'] == 'Waived', 'Dependent ID'] = ''

    active_benec = active_benec[['Employee ID', 'Event Date', 'Benefit Event Type',
                                 'Health Care Coverage Plan', 'Health Care Coverage Target',
                                 'Dependent ID', 'Coverage Name']]

    active_benec['Source System'] = 'Kronos'
    active_benec['Waived'] = np.where(active_benec['Coverage Name'] == 'Waived', 'Y', 'N')
    active_benec['Provider ID (Primary Physician)'] = ''
    active_benec['Provider ID (Primary Physician) - Dependent'] = ''
    # active_benec['Original Coverage Begin Date'] = ''
    active_benec['Deduction Begin Date'] = ''
    active_benec['Employee Cost PreTax'] = ''
    active_benec['Employee Cost PostTax'] = ''
    active_benec['Employer Cost NonTaxable'] = ''
    active_benec['Employer Cost Taxable'] = ''
    # active_benec['Enrollment Signature Date'] = ''
    # active_benec['Signing Worker'] = ''

    # hce_data = pd.read_excel(config.PATH_WD_IMP + 'templates\\USA_KBP_CNP_Benefits_Template_01122023 (1).xlsx',
    #                          sheet_name='Health Care Elections', skiprows=2, nrows=0)

    hce_data = active_benec[['Employee ID', 'Source System', 'Event Date', 'Waived',
                           'Health Care Coverage Plan', 'Health Care Coverage Target',
                           'Provider ID (Primary Physician)', 'Dependent ID',
                           'Provider ID (Primary Physician) - Dependent',
                           'Deduction Begin Date',
                           'Employee Cost PreTax', 'Employee Cost PostTax',
                           'Employer Cost NonTaxable', 'Employer Cost Taxable']]

    dict_coverage_plan = {'MEC Bronze': 'USA - Medical - American Worker (MEC) Plan Bronze',
                          'MEC Gold': 'USA - Medical - American Worker (MEC) Plan Gold',
                          'MEC Silver': 'USA - Medical - American Worker (MEC) Plan Silver',
                          'Medical A': 'USA - Medical - Surest/UHC Plan A',
                          'Medical A - Company Paid': 'USA - Medical - Surest/UHC Plan A',
                          'Medical B': 'USA - Medical - Surest/UHC Plan B'}

    hce_data['Health Care Coverage Plan'] = hce_data['Health Care Coverage Plan'].replace(dict_coverage_plan)

    write_to_csv(hce_data, 'historical_health_care.txt')
