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

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {
    "exotel": """select * from exotel_response 
                        where leg_2_status = "completed" and call_created_on >= "2022-06-04"
                        """,

    "customer": """select pp.id, caseId,pc.createdOn, pc.specialityId, customerNumber, ldc.doctorConsultationStatus from patient_case pc
                        join patient_profile pp on pp.id = pc.patientId
                        join customer_profile cp on cp.customerId = pp.customerId
                        left join (select leadId, doctorConsultationStatus from lead_doctor_consultation 
                        where appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT') 
                        and doctorConsultationStatus = '3') ldc on ldc.leadId = pc.caseId
                        where date(date_add(pc.createdOn, INTERVAL '5:30' HOUR_MINUTE )) >= "2022-05-31"

                        """,

    "agent": """select name, phoneNumber, gender from customer_support_details """,

    "gender1": """select mc.gender, pmcs.patientProfileId from marketing_channels mc
                            join patient_marketing_channel_stats pmcs on mc.id = pmcs.marketingChannelId
                            """

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


def diversion(direction, to_no, from_no):
    cxNo = ""
    agent = ""
    if direction == 'inbound':
        cxNo = from_no
        agent = to_no

    else:
        cxNo = to_no
        agent = from_no

    return msg_to(cxNo), msg_to(agent)


def getgender(number, gender_dict):
    if number in gender_dict.keys():
        return gender_dict[number]

    return ''


def getdetails(cxNo, exotel_dict, case_created):
    if cxNo in exotel_dict.keys():

        dump = exotel_dict[cxNo]

        for i in dump:
            if i['call_created_on'] > case_created:
                return i['agentNo']

    return ''


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        exotel = pd.DataFrame(dataSets['exotel'])
        customer = pd.DataFrame(dataSets['customer'])
        agent = pd.DataFrame(dataSets['agent'])
        gender1 = pd.DataFrame(dataSets['gender1'])

        customer['Doctor_Consultation_Status'] = customer.apply(
            lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']], axis=1)

        df = exotel.apply(lambda x: diversion(x['direction'], x['to_no'], x['from_no']), axis=1, result_type='expand')
        exotel = pd.concat([exotel, df], axis=1)
        exotel = exotel.rename(columns={0: 'cxNo', 1: 'agentNo'})

        exotel_dict = {}
        for i, j in exotel.iterrows():
            if j['cxNo'] not in exotel_dict.keys():
                exotel_dict[j['cxNo']] = []
            exotel_dict[j['cxNo']].append(j)

        agent_dict = {}
        for i, j in agent.iterrows():
            agentNo = msg_to(j['phoneNumber'])
            gender = j['gender']
            if agentNo not in agent_dict.keys():
                agent_dict[agentNo] = gender

        gender_dict1 = {}
        for i, j in gender1.iterrows():
            ppid = j['patientProfileId']
            gender = j['gender']
            if ppid not in gender_dict1.keys():
                gender_dict1[ppid] = gender

        customer['agentNo'] = customer.apply(
            lambda x: getdetails(msg_to(x['customerNumber']), exotel_dict, x['createdOn']), axis=1)
        customer['agent_gender'] = customer.apply(lambda x: getgender(x['agentNo'], agent_dict), axis=1)
        customer['patient_gender'] = customer.apply(lambda x: getgender(x['id'], gender_dict1), axis=1)

        fpath = os.path.join('/tmp', 'required_data.csv')
        customer.to_csv(fpath)

        Subject = 'Data Required'
        email_recipient_list = ['nikunj.r@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None', None, [fpath])

    except Exception as e:
        raise e







