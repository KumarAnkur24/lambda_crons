import pdb 
import os
import pytz 
import re 
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
from service.base_functions import msg_to
from datetime import datetime , timedelta
from service.send_mail_client import send_email
# from s3_utils.utils import upload_to_s3
import numpy as np
from google.cloud import bigquery
import numpy as np 
import boto3
import json
 

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')


# Description : offline channel patient follow up (0,1,5)
# Author : Nisha Das


patient_case_query = """select caseId,patientLeadStatus,followUpdate,followUpTime,symptoms,pc.leadSource, 
                     reason,cp.customerNumber,   
                     case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                            else 'No City' end as city,
                            date_add(pc.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'caseCreatedOn',
                            month(date_add(pc.createdOn, INTERVAL '5:30' HOUR_MINUTE)) as 'caseMonth'
                            from patient_case pc
                    left join patient_profile pp on pp.id = pc.patientId
                    left join customer_profile cp on cp.customerId = pp.customerId
                    left join ayu_cities pcp on pc.cityId = pcp.id
                    left join ayu_cities ppp on pp.cityId = ppp.id
                     where pc.leadSource in ('Offline channel','VC Model')
                     and pc.tenantName = 'AYU' 
                     and date(pc.createdOn) >= '2022-04-01'"""
                     

patient_case_aud_query = """select distinct caseId
                         from patient_case_AUD 
                         where patientLeadStatus in (0,1,5)
                         and followUpdate is not null
                         and caseId in ({caseId})"""


lead_comments_query = """ select leadId,user
                            from lead_comments
                             where leadType='CASE' and commentType in ('CREATION')
                             and leadId in ({caseId})"""
                             
PatientLeadStatus = {
	'0' : 'OPENED',
	'1' : 'FOLLOWUP',
	'2' : 'APPOINTMENT_BOOKED',
	'3' : 'CANCELLED',
	'4' : 'REJECTED',
	'5' : 'INCOMPLETE',
	'6' : 'CLOSED',
	'7' : 'APPOINTMENT_CONFIRMED',
	None : None,
	'' : None
}
def getLeadStatus(patient):
    return PatientLeadStatus[patient]
    


def fetch_data(conn,queryNo,value = 0):
    if queryNo == 1:
        fetched_val = fetch_record(conn,patient_case_query)
    if queryNo == 2:
        fetched_val = fetch_record(conn,patient_case_aud_query.format(caseId = ','.join(value)))
    if queryNo == 3:
        fetched_val = fetch_record(conn,lead_comments_query.format(caseId = ','.join(value)))
        
    return fetched_val 
    


    
def upload_to_bq(df_list):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("offline_channel_followUp")
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('caseId','STRING'),
        bigquery.SchemaField('followUpdate','STRING'),
        bigquery.SchemaField('followUpTime','STRING'),
        bigquery.SchemaField('symptoms','STRING'),
        bigquery.SchemaField('leadSource','STRING'),
        bigquery.SchemaField('reason','STRING'),
        bigquery.SchemaField('customerNumber','STRING'),
        bigquery.SchemaField('city','STRING'),
        bigquery.SchemaField('createdBy','STRING'),
        bigquery.SchemaField('leadStatus','STRING'),
        bigquery.SchemaField('caseCreatedOn','DATETIME'),
        bigquery.SchemaField('caseMonth','INTEGER'),
        ],
    write_disposition="WRITE_TRUNCATE",
         )
         
    job = client.load_table_from_dataframe(
    df_list, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()
    
def getUser(caseId,lead):
    if caseId in lead.keys():
        return lead[caseId]
    else:
        return ""

def checkIfMovedToFollowUp(caseId,case_list):
    if caseId in case_list:
        return "Yes"
    else:
        return "No"
    

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj,1)
        patient_case = pd.DataFrame(dataSets)
        caseId2 = set((patient_case['caseId'].astype(str)))

        dataSets = fetch_data(read_connection_obj,2,caseId2)
        patient_case_AUD = pd.DataFrame(dataSets)
        caseId = list((patient_case_AUD['caseId'].astype(str)))
        patient_case['checkIfMovedToFollowUp'] = patient_case.apply(lambda x: 
            checkIfMovedToFollowUp(str(x['caseId']),caseId),axis = 1)
        patient_case = patient_case[patient_case['checkIfMovedToFollowUp'] == 'Yes']
        caseId3 = set((patient_case['caseId'].astype(str)))
        dataSets = fetch_data(read_connection_obj,3,caseId3)
        lead_comments = pd.DataFrame(dataSets)
        lead_comments_dict = {} 
        for key,value in lead_comments.iterrows():
            lead_comments_dict[value['leadId']] = value['user']
        
        patient_case['createdBy'] = patient_case.apply(lambda x: getUser(x['caseId'],lead_comments_dict),axis = 1)
        print(patient_case.columns)
        patient_case['leadStatus'] = patient_case.apply(lambda x: getLeadStatus(str(x['patientLeadStatus'])),axis = 1)
        print(patient_case.columns)
        patient_case.drop(['patientLeadStatus','checkIfMovedToFollowUp'],axis = 1)
        cols = ['caseId','patientLeadStatus','followUpdate','followUpTime','createdBy']
        patient_case[cols] = patient_case[cols].astype(str)
        upload_to_bq(patient_case)
        
        
        
       
    except Exception as e:
        Text = 'Error Running offline_channel_followUp , response Text: ' + str(e)
        Subject = "offline channel followUp | Error"
        email_recipient_list = ["analytics@ayu.health"]
        send_email(None, email_recipient_list, Subject, Text, None, [])
        raise e
 
 
 
 
 
 
    
'''
patient_case
Index(['caseId', 'hospitalId', 'doctorId', 'patientLeadStatus', 'followUpdate',
'followUpTime', 'specialityId', 'patientId', 'symptoms', 'assignedTo',
'createdOn', 'updatedOn', 'caseType', 'leadSource', 'reason',
'consultationFee', 'tenantName', 'additionalDetails', 'cityId',
'followUpdateNullCheck'],
dtype='object')
'''

'''
patient_case_AUD
Index(['caseId', 'hospitalId', 'doctorId', 'patientLeadStatus', 'followUpdate',
'followUpTime', 'specialityId', 'patientId', 'symptoms', 'assignedTo',
'createdOn', 'REVTYPE', 'REV', 'updatedOn', 'caseType', 'leadSource',
'reason', 'consultationFee', 'tenantName', 'additionalDetails',
'cityId'],
'''