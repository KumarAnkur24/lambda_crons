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
from write_to_gsheet import clear_and_write_to_sheet

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {'appointment': '''select ldc.id , ldc.leadId , ldc.appointmentDate , month(ldc.appointmentDate) as appt_month , year(ldc.appointmentDate) as appt_Year,pc.leadSource,
                                    ldc.doctorConsultationStatus, ldc.appointmentCreationType , dp.name as DoctorName ,afp.aliasName as HospitalName , ayuMitraId ,
                                    pp.patientName , cp.customerNumber ,ldc.consultationType , ldc.consultationFee , pp.id as ppID ,
                            case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                            else 'No City' end as city

                            from lead_doctor_consultation ldc 
                            join patient_case pc on pc.caseId = ldc.leadId
                            join patient_profile pp on pp.id = pc.patientId
                            join customer_profile cp on cp.customerId = pp.customerId  
                            left join doctors_profile dp on dp.doctorProfileId = ldc.doctorId
                            left join ayu_facility_profile afp on afp.facilityId = ldc.hospitalId
                            left join ayu_cities pcp on pc.cityId = pcp.id
                            left join ayu_cities ppp on pp.cityId = ppp.id

                            where 
                                ldc.appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT')
                                and ldc.tenantName = 'AYU'
                                and date(appointmentDate) = curdate()  


''',
           'ayu_mitra': '''select personnelId , email 
                            from ayu_personnel_details
                            where 
                                personnelType = 'AYU_MITRA'
            '''

           }

DoctorConsultationStatus = {
    '0': 'OPEN',
    '1': 'BOOKED',
    '2': 'CONFIRMED',
    '3': 'DONE',
    '4': 'CANCELLED',
    None: 'None',
    '': 'None',
    '5': 'OPEN',
    '6': 'PATIENT_HAS_REACHED',
    '7': 'APPOINTMENT_HAS_STARTED'
}


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val


def get_ayuMitra(agent_id, ayu_dict):
    if agent_id in ayu_dict.keys():
        return ayu_dict[agent_id]

    return ''


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        appointment = pd.DataFrame(dataSets['appointment'])

        appointment['Doctor_Consultation_Status'] = appointment.apply(
            lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']], axis=1)

        ayu_mitra = dataSets['ayu_mitra']

        ayu_dict = {}
        for val in ayu_mitra:
            agent_id = val['personnelId']

            if agent_id not in ayu_dict.keys():
                ayu_dict[agent_id] = val['email']

        appointment['ayu_mitra'] = appointment.apply(lambda x: get_ayuMitra(x['ayuMitraId'], ayu_dict), axis=1)

        appointment_chd = appointment[appointment['city'] == 'CHD']

        print(appointment_chd.columns)

        data = [['Appt_id', 'leadId', 'appointmentDate', 'appt_month', 'appt_Year', 'leadSource', 'PatientName',
                 'CustomerNumber', 'ConsultationType', 'ConsultationFee',
                 'Doctor_Consultation_Status', 'appointmentCreationType', 'DoctorName', 'HospitalName',
                 'ayu_mitra', 'city', 'link']]

        for index, val in appointment_chd.iterrows():
            data.append([
                val['id'],
                val['leadId'],
                str(val['appointmentDate']),
                str(val['appt_month']),
                str(val['appt_Year']),
                val['leadSource'],
                val['patientName'],
                val['customerNumber'],
                val['consultationType'],
                val['consultationFee'],
                val['Doctor_Consultation_Status'],
                val['appointmentCreationType'],
                val['DoctorName'],
                val['HospitalName'],
                val['ayu_mitra'],
                val['city'],
                'https://amigos.ayu.health/patient/{0}/case/{1}'.format(val['ppID'], val['leadId'])
            ])

        clear_and_write_to_sheet('1ffoL_pwPF_oxbMLEtWQfwSQ2SiLd0LKumOKz2_wUwOo', 'New_Appointment_CHD', 'A1:Q', data)

        print(data)

    except Exception as e:
        Subject = 'appointments_data_CHD_to_sheet ERROR'
        email_recipient_list = ['analytics@ayu.health']
        # send_email(None, email_recipient_list, Subject, None, e , [])
        raise e