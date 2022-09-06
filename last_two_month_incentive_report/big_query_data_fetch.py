from datetime import datetime, timedelta
from google.cloud import bigquery
import requests
import os
import sys
import csv
import boto3
 
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
BQ_SECRETS_FILE = os.environ.get('BQ_SECRETS_FILE', '')

boto3.client('s3').download_file(S3_BUCKET_NAME, BQ_SECRETS_FILE, '/tmp/secrets_bq.json')
bigquery_client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json")



def upload_to_bq(df_list):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, BQ_SECRETS_FILE, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("Monthly Appointments")
    
    
    
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('Appointment_date','STRING'),
        bigquery.SchemaField('caseId','STRING'),
        bigquery.SchemaField('Cstmr_Contact_No','STRING'),
        bigquery.SchemaField('Agent_Action','STRING'),
        bigquery.SchemaField('Team_Leader','STRING'),
        bigquery.SchemaField('Total_Call_Duration','STRING'),
        bigquery.SchemaField('Sequence','STRING'),
        bigquery.SchemaField('Appt_type','STRING'),
        bigquery.SchemaField('Consultation_Fee','STRING'),
        bigquery.SchemaField('Surgery_reco_Y_N','STRING'),
        bigquery.SchemaField('City','STRING'),
        bigquery.SchemaField('Other_hospital_doctor_follow_up_reason','STRING'),
        bigquery.SchemaField('Other_in_comment','STRING'),
        bigquery.SchemaField('Pitch_Charter_from_new_crm','STRING')
        
        ],
    write_disposition="WRITE_TRUNCATE",
         )
         
    job = client.load_table_from_dataframe(
    df_list, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()


def search_query(query):

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = bigquery_client.query(query, job_config=job_config)
    return query_job.result()


def main(query):

    data = search_query(query)

    return data