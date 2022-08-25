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
# from write_to_gsheet import write_to_sheet
import sys

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {"data": """select fpt.id, fpt.transactionId, fpt.amount as amount_paid_to_hospital,
                            fpt.user,fpt.paymentStatus,fpt.entityId, fpt.entityType,
                            date_add(fpt.createdOn, interval '5:30' HOUR_MINUTE) as payment_date,
                            case when ldc.id is null then 0 else ldc.id end as appId,ldc.appointmentDate,ldc.appointmentCreationType,
                         ldc.leadId,ldc.consultationType,ldc.doctorConsultationStatus,
                        ldc.consultationFee as ayu_price,ldc.hospitalPrice,
                        ldc.consultationType,afp.aliasName as hospital_name,fpt.utr,
                        ldc.additionalDetails->>'$.hospitalPriceBills' as hospitalPriceBills,

                        dp.name as doctor_name, pp.patientName, cp.customerNumber 
                        from facility_payment_transaction fpt
                        join lead_doctor_consultation ldc on ldc.id = fpt.entityId
                        left join ayu_facility_profile afp on afp.facilityId = ldc.hospitalId
                        left join doctors_profile dp on dp.doctorProfileId = ldc.doctorId
                        left join patient_case pc on pc.caseId = ldc.leadId
                        left join patient_profile pp on pp.id = pc.patientId
                        join customer_profile cp on cp.customerId = pp.customerId
                        where fpt.paymentStatus ="processed" and
                        fpt.entityType in ("APPOINTMENT","DIAGNOSTICS_APPOINTMENT")
                         and
                        date_add(fpt.createdOn, interval '5:30' HOUR_MINUTE) <= "{start}"
                        and date_add(fpt.createdOn, interval '5:30' HOUR_MINUTE) > "{end}"

                        """

           }

today = datetime.now() + timedelta(hours=5, minutes=30)
start = today.strftime("%Y-%m-%d %H:%M:%S")
date = today.date()
print(start)
start = datetime.now() + timedelta(hours=5, minutes=28)
end = start.strftime("%Y-%m-%d %H:%M:%S")
print(end)
month_start = datetime.now().replace(day=1).date()

DoctorConsultationStatus = {
    '0': 'OPEN',
    '1': 'BOOKED',
    '2': 'CONFIRMED',
    '3': 'DONE',
    '4': 'CANCELLED',
    '5': 'OPEN',
    '6': 'PATIENT_HAS_REACHED',
    '7': 'ONLINE_CONSULTATION_STARTED'
}
result_base = """<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr style="background-color:powderblue;"> 
        <td colspan = 9 width = \"600\"><b><center> CK Birla Hospital Gurugram  </center></b></td>
    </tr>

    <tr bgcolor=\"#EEFCF0\"> 
        <td colspan = 1  width = \"200\"><b><center> appId </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> payment_date </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> amount_paid_to_hospital </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> patientName </center></b></td>
        <td colspan = 1  width = \"200\"><b><center>  customerNumber </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> hospitalPriceBills </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> consultationType </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> doctor_name </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> utr </center></b></td>
    </tr>
"""


def itertable(data):
    table = [[0 for x in range(9)] for x in range(200)]
    payment = {}
    count = 0
    num = 1

    for i, j in data.iterrows():
        print(data.columns)
        if j['appId'] not in payment.keys():
            payment[j['appId']] = count
            table[count][0] = num
            table[count][1] = j['payment_date']
            table[count][2] = int(j['amount_paid_to_hospital'])
            table[count][3] = j['patientName']
            table[count][4] = j['customerNumber']
            table[count][5] = j['hospitalPriceBills']
            table[count][6] = j['consultationType']
            table[count][7] = j['doctor_name']
            table[count][8] = j['utr']

            count += 1
            num += 1

    result = result_base

    for i in range(0, count):
        result = result + "<tr>"
        for j in range(0, 9):
            if j == 0:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif table[i][j] == 0:
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"

    return result


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == "data":
            fetched_val[lookup] = fetch_record(conn, query.format(start=start, end=end))

    return fetched_val


def hospital1(dataSets):
    data = pd.DataFrame(dataSets['data'])
    # print(data.columns)
    if len(data):
        data['doctor_Consultation_Status'] = data.apply(
            lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']], axis=1)
        data = data.drop(['doctorConsultationStatus'], axis=1)
        data['amount_paid_to_hospital'] = data['amount_paid_to_hospital'] / 100

        data = data.loc[data['hospital_name'] == 'CK Birla Hospital Gurugram']

        df = data[
            ['appId', 'payment_date', 'amount_paid_to_hospital', 'patientName', 'customerNumber', 'hospitalPriceBills',
             'consultationType', 'doctor_name', 'utr', 'hospital_name']]
        df = df.reset_index(drop=True)

        # hospital_name_list = ['CK Birla Hospital Gurugram']
        # result = ""

        # for hospital_name in hospital_name_list:
        #     if len(data):
        #         data_final = data[data['hospital_name']==hospital_name]
        #         result += itertable(data_final,hospital_name)

        result = itertable(df)

        fpath = os.path.join('/tmp', 'facility_payment.csv')
        df.to_csv(fpath)

        Subject = 'IMPS payments to CK Birla Hospital Gurugram from Ayu Health |{date}'
        email_recipient_list = ['nikunj.r@ayu.health', 'aakash@ayu.health']
        send_email(None, email_recipient_list, Subject.format(date=date), '-PFA', result, [fpath])


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        # data = pd.DataFrame(dataSets['data'])
        # print(data.columns)
        hospital1(dataSets)


    except Exception as e:
        raise e





