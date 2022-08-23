from google.cloud import bigquery
import requests
import os
import sys
import csv
import boto3

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')

boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
bigquery_client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json")


query = """ select * from `gmb-centralisation.tables_crons.exotel_summary_current_day` 
where call_created_on >= timestamp_add(current_datetime('Asia/Kolkata'),interval -20 minute)
and call_created_on < timestamp_add(current_datetime('Asia/Kolkata'),interval -10 minute)
and city = "Hyderabad"
"""


def search_query(query):

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = bigquery_client.query(query, job_config=job_config)
    return query_job.result().to_dataframe()


def main():

    data = search_query(query)
    return data