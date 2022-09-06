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


def search_query(query):

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = bigquery_client.query(query, job_config=job_config)
    return query_job.result().to_dataframe()


def main(query):

    data = search_query(query)

    return data