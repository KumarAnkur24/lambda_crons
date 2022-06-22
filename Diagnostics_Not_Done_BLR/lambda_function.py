import pdb
import os
import pytz
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
import json
from service.base_functions import msg_to
from datetime import datetime, timedelta
from service.send_mail_client import send_email
import requests
from write_to_sheet import clear_and_write_to_sheet
import numpy as np

# Created By: Akchansh Kumar
# Description : pushing Diagnostics data of city BLR into sheets


logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {"diag_test": """select ldc.id as apptId,
                      leadId, 
                      date(appointmentDate) as appt_date,
                      appointmentCreationType,
                      consultationType,
                      ldc.consultationFee,
                      cp.customerNumber,
                      ldc.user,
                      aliasName , 
                      case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                            else 'No City' end as city,
                        ldc.additionalDetails->>'$.isDiagnosticsTestRecommended' as DiagnosticsRecomended
                    from

                lead_doctor_consultation ldc
                left join ayu_facility_profile afp on ldc.hospitalId = afp.facilityId
                join patient_case pc on pc.caseId = ldc.leadId
                join patient_profile pp on pp.id = pc.patientId
                join customer_profile cp on cp.customerId = pp.customerId
                left join ayu_cities pcp on afp.cityId = pcp.id

                where
                        date(appointmentDate) >= '2022-04-01'             
                        and doctorConsultationStatus = 3
                        and consultationType != 'DIAGNOSTICS'  
                        and afp.cityId in (2)
                        and ldc.tenantName = 'AYU'

                        """,

           'diag_done': '''select leadId , id as apptId , originConsultationId , date(appointmentDate) as diagnostics_date
                                from lead_doctor_consultation 
                                where 
                                    doctorConsultationStatus = 3
                                    and consultationType = 'DIAGNOSTICS'
                                    and leadId in  ({leadIds})  

            '''

           }


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        print(lookup)
        if lookup == 'diag_test':
            fetched_val[lookup] = fetch_record(conn, query)
            leadIds = [str(x['leadId']) for x in fetched_val[lookup]]
            # print(leadIds)
        elif lookup == 'diag_done':
            leadId = ','.join(leadIds)
            fetched_val[lookup] = fetch_record(conn, query.format(leadIds=leadId))

        else:
            fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val


def get_status(apptId, diag_done_dict):
    if apptId in diag_done_dict.keys():
        return 'Diagnostic Done', diag_done_dict[apptId]

    return 'Diagnostic Not Done', ''


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        diag_test = pd.DataFrame(dataSets['diag_test'])

        diag_done = pd.DataFrame(dataSets['diag_done'])

        print(len(diag_done))

        diag_done = diag_done.fillna(0)
        diag_done_dict = {}
        for index, val in diag_done.iterrows():

            originConsultationId = int(float(val['originConsultationId']))

            if originConsultationId not in diag_done_dict.keys():
                diag_done_dict[originConsultationId] = val['diagnostics_date']

        print(diag_done_dict)

        df = diag_test.apply(lambda x: get_status(x['apptId'], diag_done_dict), axis=1, result_type='expand')
        diag_test = pd.concat([diag_test, df], axis=1)
        diag_test = diag_test.rename(columns={0: 'Status', 1: 'diagnostic_Date'})

        print(diag_test.columns)

        data = [['leadId', 'appointmentCreationType', 'aliasName', 'DiagnosticsRecomended', 'agentName', 'appt_date',
                 'Status', 'diagnostic_Date']]

        for index, val in diag_test.iterrows():
            data.append([
                val['leadId'],
                val['appointmentCreationType'],
                val['aliasName'],
                val['DiagnosticsRecomended'],
                val['user'],
                str(val['appt_date']),
                val['Status'],
                str(val['diagnostic_Date'])
            ])

        clear_and_write_to_sheet('1tlx_CCZTVJ49RVOoFYLV4edWWE8NEQCStsJOp7iTLsg', 'formattedData', 'A1:H', data)


    except Exception as e:
        raise e


'''
['apptId', 'leadId', 'appt_date', 'appointmentCreationType',
'consultationType', 'consultationFee', 'customerNumber', 'user',
'aliasName', 'city', 'DiagnosticsRecomended', 'Status',
'diagnostic_Date']
'''