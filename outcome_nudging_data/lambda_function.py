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
from google.cloud import bigquery
import requests
import boto3

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')

queries = {
    'main_data': '''Select entityTagMapId,communicationDone,date_add(log.communicationDate,interval '5:30' HOUR_MINUTE) as msgSentOn,
         tag.*,pp.id as patientId,pp.patientName,cp.customerNumber,log.communicationType,log.assetId,
         case when tag.sourceEntityType = 'PATIENT_CASE' then 'CASE_CLOSED' else 'FOLLOWUP_NUDGE' end as 'type'
         from  
        user_communication_logs log 
        join ayu_assets aa on log.assetId = aa.id
        join entity_tag_map tag on log.entityTagMapId = tag.id
        left join patient_profile pp on (tag.entityId = pp.id and entityType = 'PATIENT_PROFILE')
        left join customer_profile cp on pp.customerId=cp.customerId
        join attribute_tags a on tag.tagId = a.tagId  

        where communicationDone = 1 
            and date(date_add(log.communicationDate,interval '5:30' HOUR_MINUTE))>= '2022-05-01'       
            and tag.tagId = 280  
    '''
}


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val


def upload_to_bq(exotel_summary_bq):
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("outcome_nudging_data")

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('entityTagMapId', 'STRING'),
            bigquery.SchemaField('communicationDone', 'STRING'),
            bigquery.SchemaField('msgSentOn', 'STRING'),
            bigquery.SchemaField('id', 'STRING'),
            bigquery.SchemaField('entityId', 'STRING'),
            bigquery.SchemaField('entityType', 'STRING'),
            bigquery.SchemaField('tagId', 'STRING'),
            bigquery.SchemaField('isEnabled', 'STRING'),
            bigquery.SchemaField('communicationStage', 'STRING'),
            bigquery.SchemaField('sourceEntityType', 'STRING'),
            bigquery.SchemaField('sourceEntityId', 'STRING'),
            bigquery.SchemaField('additionalDetails', 'STRING'),
            bigquery.SchemaField('patientId', 'STRING'),
            bigquery.SchemaField('patientName', 'STRING'),
            bigquery.SchemaField('customerNumber', 'STRING'),
            bigquery.SchemaField('communicationType', 'STRING'),
            bigquery.SchemaField('assetId', 'STRING'),
            bigquery.SchemaField('type', 'STRING')

        ], write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(
        exotel_summary_bq, table_ref, job_config=job_config
    )  # Make an API request.


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        main_data = pd.DataFrame(dataSets['main_data'])

        print(main_data.columns, main_data.dtypes)

        data = []

        for index, val in main_data.iterrows():
            data.append([
                str(val['entityTagMapId']),
                str(val['communicationDone']),
                str(val['msgSentOn']),
                str(val['id']),
                str(val['entityId']),
                str(val['entityType']),
                str(val['tagId']),
                str(val['isEnabled']),
                str(val['communicationStage']),
                str(val['sourceEntityType']),
                str(val['sourceEntityId']),
                str(val['additionalDetails']),
                str(val['patientId']),
                str(val['patientName']),
                str(val['customerNumber']),
                str(val['communicationType']),
                str(val['assetId']),
                str(val['type'])
            ])

        data_df = pd.DataFrame(data, columns=['entityTagMapId', 'communicationDone', 'msgSentOn', 'id', 'entityId',
                                              'entityType', 'tagId', 'isEnabled',
                                              'communicationStage', 'sourceEntityType', 'sourceEntityId',
                                              'additionalDetails', 'patientId', 'patientName', 'customerNumber',
                                              'communicationType', 'assetId', 'type'])

        print(data_df)

        upload_to_bq(data_df)

    except Exception as e:
        raise e


'''        
['entityTagMapId', 'communicationDone', 'msgSentOn', 'id', 'entityId',
'entityType', 'createdOn', 'updatedOn', 'tagId', 'isEnabled',
'communicationStage', 'sourceEntityType', 'sourceEntityId',
'additionalDetails', 'patientId', 'patientName', 'customerNumber',
'communicationType', 'assetId', 'type']
'''