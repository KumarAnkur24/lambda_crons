from datetime import datetime, timedelta
from google.cloud import bigquery
import requests
import os
import sys
import boto3
import pytz
import pandas as pd
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE_BQ = os.environ.get('BQ_SECRETS_FILE', '')

boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
bigquery_client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json")

query = "SELECT * FROM `gmb-centralisation.tables_crons.website_submit_campaigns_ga`"

def search_query(query):

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = bigquery_client.query(query, job_config=job_config)
    return query_job.result().to_dataframe()


def main():

    data1 = search_query(query)
    data1 = data1[data1['eventLabel'] != '(not set)']
    data1['eventLabel'] = data1['eventLabel'].astype('int')
    websiteSubmitGA = {}
    for index, val in data1.iterrows():
        websiteSubmitGA[val['eventLabel']] = val['campaign']
        
    return websiteSubmitGA


