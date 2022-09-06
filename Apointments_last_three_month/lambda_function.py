import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import pandas as pd
from datetime import datetime, timedelta
from service.base_functions import msg_to
from service.send_mail_client import send_email
import time
from datetime import datetime, timedelta 
from spreadsheet_write import clear_and_write_to_sheet,read_from_sheet
import boto3

import re
import csv
 
fpath = os.path.join('/tmp', 'ayuM_dash_dump.csv')

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SPREAD_SHEET_JSON = os.environ.get('SPREAD_SHEET_JSON', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')

yest = datetime.now() + timedelta(hours=5, minutes=30, days=-1)
month1 = yest.replace(day = 1)
month1 = month1 - timedelta(days=1)
month2 = month1.replace(day=1)
month2 = month2 - timedelta(days = 1)
month3 = month2.replace(day = 1)
yest = yest.strftime('%Y-%m-%d')
month3 = month3.strftime('%Y-%m-%d')

print(yest)
print(month3)

big_query_data={'big_query':""" select ldc.leadId as caseid,
                                ldc.id as apptId,ldc.doctorId as doctorid,
                                pc.patientId as patientId,
                                cp.customerNumber as number,
                                pp.patientName as patientName,
                                ldc.user as apptCreatedBy,
                                mitra.email as apptAssignedto,
                                DATE(ldc.appointmentdate) as  AppointmentDate,
                                ldc.consultationType,
                                ldc.doctorConsultationstatus as status,
                                aliasName as hospitalshortName,
                                dp.name as doctorName,
                                specialityname,
                                appointmentCreationType,
                                pc.leadSource as leadSource,
                                pc.symptoms as symptoms,
                                ldc.additionalDetails->>'$.appointmentConfirmCount' as appointmentConfirmCount,
                                ldc.additionalDetails->>'$.isDiagnosticsTestRecommended' as isDiagnosticsTestRecommended,
                                ldc.hospitalPrice, 
                                
                            case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                                else 'No City' end as city,
                            case when ldc.consultationFee is null then 0 else ldc.consultationFee end as 'consultationFee',
                            case when totalPaid is null then 0 else totalPaid end as 'amountPaid',
                            date_add(ldc.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
                            isSurgeryRecommended
                            from
                                lead_doctor_consultation ldc 
                                join patient_case pc on ldc.leadId = pc.caseId
                                join patient_profile pp on pc.patientId = pp.id
                                join customer_profile cp on pp.customerId = cp.customerId
                                left join ayu_cities pcp on pc.cityId = pcp.id
                                left join ayu_cities ppp on pp.cityId = ppp.id
                                left join ayu_personnel_details mitra on (ldc.ayuMitraId = mitra.personnelId and mitra.personnelType= 'AYU_MITRA')
                                left join ayu_facility_profile hos on ldc.hospitalId = hos.facilityId
                                left join doctors_profile dp on dp.doctorProfileId = ldc.doctorId
                                left join speciality_details sd on ldc.specialityId = sd.id 
                                left join (select entityId, sum(amount/100) as totalPaid
                                            from 
                                        online_payment_link
                                            where
                                             entityType = 'APPOINTMENT'
                                             and status = 'PAID'
                                        group by entityId) pay on ldc.id = pay.entityId
                                left join (select appointmentId,isSurgeryRecommended,surgeryOutcomeId from appointment_surgery_outcome 
                        where surgeryOutcomeId in 
                        (select max(surgeryOutcomeId) from appointment_surgery_outcome group by appointmentId)) aso on aso.appointmentId = ldc.id
                        where date(ldc.appointmentdate) >= '{start}' and date(ldc.appointmentdate) <= '{end}'
                            """ ,
                            
                'app_comments':""" Select leadId,
                                    text,
                                    user,
                                    commentType from lead_comments 
                            where commentType in ('APPOINTMENT_CANCELLED','APPOINTMENT_DONE')
                                
                            
                                and leadType in ('APPOINTMENT')
                                
                                """,
                
                
                'app_done': """select id, min(date_add(createdOn, INTERVAL '5:30' HOUR_MINUTE)) as appDoneTime 
                                    from lead_doctor_consultation_AUD
                                    where doctorConsultationstatus = 3
                                    group by id
                            """
    
}
DoctorConsultationStatus = { 
	'0': 'OPEN',
	'1': 'BOOKED',
	'2': 'CONFIRMED',
	'3': 'DONE',
	'4': 'CANCELLED',
	None : 'None',
	'' : 'None',
	'5': 'OPEN',
	'6': 'CONFIRMED',
	'7': 'CONFIRMED'
}
                        
def fetch_data(conn):
    fetched_val = {}
    
    for lookup, query in big_query_data.items():
        if lookup == 'big_query':
            fetched_val[lookup] = fetch_record(conn, query.format(start = month3,end = yest))
        else:
            fetched_val[lookup] = fetch_record(conn, query)
    return fetched_val

def getReason(bookedToCancelledCommentDump,apptId):
    
    if apptId in bookedToCancelledCommentDump.keys():
        return bookedToCancelledCommentDump[apptId]
    return ''


def upload_to_bq(bigdata):
    
    

    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("app_level_details_BQ")
     
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('caseid','STRING'),
        bigquery.SchemaField('apptId','STRING'),
        bigquery.SchemaField('doctorid','STRING'),
        bigquery.SchemaField('patientId','STRING'),
        bigquery.SchemaField('number','STRING'),
        bigquery.SchemaField('patientName','STRING'),
        bigquery.SchemaField('apptCreatedBy','STRING'),
        bigquery.SchemaField('apptAssignedto','STRING'),
        bigquery.SchemaField('AppointmentDate','DATE'),
        bigquery.SchemaField('consultationType','STRING'),
        bigquery.SchemaField('Appointment_Status','STRING'),
        bigquery.SchemaField('hospitalshortName','STRING'),
        bigquery.SchemaField('doctorName','STRING'),
        bigquery.SchemaField('specialityname','STRING'),
        bigquery.SchemaField('appointmentCreationType','STRING'),
        bigquery.SchemaField('leadSource','STRING'),
        bigquery.SchemaField('symptoms','STRING'),
        bigquery.SchemaField('Cancellation_Reason','STRING'),
        bigquery.SchemaField('crmlink','STRING') ,
        bigquery.SchemaField('city','STRING'),
        bigquery.SchemaField('appDoneTime','STRING'),
        bigquery.SchemaField('consultationFee','FLOAT'),
        bigquery.SchemaField('amountPaid','FLOAT'),
        bigquery.SchemaField('paymentStatus','STRING'),
        bigquery.SchemaField('createdOn','DATETIME'),
        bigquery.SchemaField('appointmentConfirmCount','STRING'),
        bigquery.SchemaField('hospitalPrice','STRING'),
        bigquery.SchemaField('isSurgeryRecommended','STRING'),
        bigquery.SchemaField('isDiagnosticsTestRecommended','STRING')
        ],
    write_disposition="WRITE_TRUNCATE",
         )  
         
    job = client.load_table_from_dataframe(
    bigdata, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()

def getDoneTime(apptId, appDoneDump):
    
    if apptId in appDoneDump.keys():
        return appDoneDump[apptId]

    return None
    
def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        appComments = pd.DataFrame(dataSets['app_comments'])  
        bigdata = pd.DataFrame(dataSets['big_query'])
        # print(bigdata['doctorId'])
        bookedToCancelledComment = appComments[appComments['commentType'] == 'APPOINTMENT_CANCELLED']
        appDone_Comments = appComments[appComments['commentType'] == 'APPOINTMENT_DONE'] 
        app_done = dataSets['app_done']
        
        appDoneDump = {}
        for val in app_done:
            if val['id'] not in appDoneDump.keys():
                appDoneDump[val['id']] = val['appDoneTime']
        
        bookedToCancelledCommentDump = {}
        for index, val in bookedToCancelledComment.iterrows():
            if val['leadId'] not in bookedToCancelledCommentDump.keys():
                text = val['text']
                b_ = text.find('Comments')
                c_ = text.find(', user : ')
                
                
                if b_!=-1:
                    
                    comment = text[b_+12:c_]
                    bookedToCancelledCommentDump[val['leadId']] = comment
                    
        appDone_markedBy = {}
        for index,val in appDone_Comments.iterrows():
            if val['leadId'] not in appDone_markedBy.keys():
                appDone_markedBy[val['leadId']] = val['user'] 
                
        bigdata['apptDoneMarkedBy'] = bigdata.apply(lambda x: getDoneTime(x['apptId'],appDone_markedBy),axis=1)
        
        bigdata['Cancellation_Reason'] = bigdata.apply(lambda x: getReason(bookedToCancelledCommentDump,x['apptId']),axis=1)
    
        bigdata['Appointment_Status'] = bigdata.apply(lambda x:DoctorConsultationStatus[x['status']],axis=1)
        bigdata['appDoneTime'] = bigdata.apply(lambda x: getDoneTime(x['apptId'], appDoneDump),axis=1)
        bigdata.drop('status',axis=1)
        emp_list=[]
        emp_list.append(['caseid','apptId','doctorid','patientId','number','patientName','apptCreatedBy',
                   'apptAssignedto',
                    'AppointmentDate','consultationType','Appointment_Status','hospitalshortName','doctorName',
                    'specialityname','appointmentCreationType','leadSource','symptoms','Cancellation_Reason','crmlink','city', 'appDoneTime',
                    'consultationFee', 'amountPaid', 'paymentStatus', 'createdOn','apptDoneMarkedBy','appointmentConfirmCount',
                    'hospitalPrice', 'isSurgeryRecommended','isDiagnosticsTestRecommended'])
        for index , val in bigdata.iterrows():
            
            paymentStatus = 'NOT_PAID'
            if float(val['amountPaid']) >= float(val['consultationFee']):
                paymentStatus = 'PAID'
            
            emp_list.append([
                str(val['caseid']),
                str(val['apptId']),
                str(val['doctorid']),
                str(val['patientId']), 
                str(val['number']),
                str(val['patientName']),
                str(val['apptCreatedBy']),
                str(val['apptAssignedto']),
                str(val['AppointmentDate']),
                str(val['consultationType']),
                str(val['Appointment_Status']),
                str(val['hospitalshortName']),
                str(val['doctorName']),
                str(val['specialityname']),
                str(val['appointmentCreationType']),
                str(val['leadSource']),
                str(val['symptoms']),
                str(val['Cancellation_Reason']), 
                "https://amigos.ayu.health/patient/{0}/case/{1}".format(val['patientId'],val['caseid']) ,
                str(val['city']),
                str(val['appDoneTime']),
                float(val['consultationFee']),
                float(val['amountPaid']),
                paymentStatus,
                str(val['createdOn']),
                val['apptDoneMarkedBy'],
                val['appointmentConfirmCount'],
                val['hospitalPrice'],
                val['isSurgeryRecommended'],
                val['isDiagnosticsTestRecommended']
        ]) 
                
        # empt1=pd.DataFrame(emp_list,columns=['caseid','apptId','doctorid','patientId','number','patientName','apptCreatedBy','apptAssignedto',
        #             'AppointmentDate','consultationType','Appointment_Status','hospitalshortName','doctorName',
        #             'specialityname','appointmentCreationType','leadSource','symptoms','Cancellation_Reason','crmlink','city', 'appDoneTime',
        #             'consultationFee', 'amountPaid', 'paymentStatus', 'createdOn','apptDoneMarkedBy','appointmentConfirmCount',
        #             'hospitalPrice', 'isSurgeryRecommended','isDiagnosticsTestRecommended'])
        
            
        
        clear_and_write_to_sheet('1uA5tU3Lwv-rcHxBbvIDXQse_YLeL4fkOfn5Y7DuXz5k','Appointments','A1:AD',emp_list)
        #print(empt1)
        
        
    except Exception as e:
        Subject = 'Apointments_last_three_month | Error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None,email_recipient_list,Subject,'None',e,[]) 
        raise e
                            