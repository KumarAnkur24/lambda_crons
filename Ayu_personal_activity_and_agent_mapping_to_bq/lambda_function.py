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


yesterday = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days=1)
lastTenDay = yesterday - timedelta(days = 10)

yesterday = yesterday.date()
lastTenDay = lastTenDay.date()

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')

queries = {
    "ayu_personel_activity": """select ac.id,
                              ac.activity->>'$.newAvailabilityValue' as 'newValue',
                              ac.activity->>'$.perviousAvailabilityValue' as 'previousValue',
                              ac.activity->>'$.reason' as 'reason',
                              date_add(ac.createdOn,interval '5:30' HOUR_MINUTE) as 'createdOn',
                              lower(email) as 'assignedTo' 
                    from 
                        ayu_personnel_activity ac 
                        join customer_support_details cs on ac.ayuPersonnelId = cs.customerSupportId
                        where 
                            ayuPersonnelType = 'CUSTOMER_SUPPORT'
                        and date(date_add(ac.createdOn,interval '5:30' HOUR_MINUTE)) >= '{start}'
                        and date(date_add(ac.createdOn,interval '5:30' HOUR_MINUTE)) <= '{end}'
                        order by createdOn
                             """,
                             
                             
    "agent_source_mapping" : """
                            select * from agent_source_mapping_AUD
                            """
    
    
}

today = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days = 1)
start = today.date()
print(start)
def upload_to_bq(df_list):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("ayu_personnel_activity")
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('newValue','STRING'),
        bigquery.SchemaField('previousValue','STRING'),
        bigquery.SchemaField('reason','STRING'),
        bigquery.SchemaField('createdOn','STRING'),
        bigquery.SchemaField('assignedTo','STRING'),
        
        ],
    write_disposition="WRITE_TRUNCATE",
         )
         
    job = client.load_table_from_dataframe(
    df_list, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()
    

def upload_to_bq1(df_list):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("agent_source_mapping")
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('REVTYPE','STRING'),
        bigquery.SchemaField('REV','STRING'),
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('emailId','STRING'),
        bigquery.SchemaField('source','STRING'),
        bigquery.SchemaField('createdOn','STRING'),
        bigquery.SchemaField('cityId','STRING'),
        bigquery.SchemaField('updatedOn','STRING'),
        
        ],
    write_disposition="WRITE_TRUNCATE",
         )
         
    job = client.load_table_from_dataframe(
    df_list, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()     

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query.format(start=lastTenDay,end = yesterday))  
    
    return fetched_val 

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        dataSets = fetch_data(read_connection_obj)
        print(yesterday,lastTenDay) 
        
        
        data = pd.DataFrame(dataSets['ayu_personel_activity'])
        cols = ['id','newValue','previousValue','reason','createdOn','assignedTo']
        data[cols] = data[cols].astype(str)
        upload_to_bq(data)
        
        
        data = pd.DataFrame(dataSets['agent_source_mapping'])
        print(data.columns)
        cols = ['REVTYPE','REV','id','emailId','source','createdOn','cityId','updatedOn']
        data[cols] = data[cols].astype(str)
        upload_to_bq1(data)
        
    
        
    except Exception as e:
        raise e 
    
    
    