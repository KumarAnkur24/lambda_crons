from datetime import datetime, timedelta
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


query = '''SELECT
  *
FROM
  `gmb-centralisation.tables_crons.nps_feedback_consultations`
WHERE
  safe_CAST(Rating AS int) <=6
  OR safe_CAST(SupportfromAyuMitra AS int) <=3
  OR safe_CAST(Qualityofconsultationwithdoctors AS int) <=3
  OR safe_CAST(HospitalInfrastructure AS int) <=3
  OR safe_CAST(InteractionwithCS AS int) <=3
  OR safe_CAST(OverallExperience AS int)<=3 
        '''
query1 = """ SELECT
  *
FROM
  `gmb-centralisation.tables_crons.nps_feedback_surgery`
WHERE
  SAFE_CAST(SupportfromAyuMitra AS int)<=3
  OR SAFE_CAST(nursingSupport AS int) <=3
  OR SAFE_CAST(doctorBehaviour AS int) <=3
  OR SAFE_CAST(roomQuality AS int) <= 3
  OR SAFE_CAST(foodQuality AS int) <=3
  OR SAFE_CAST(billingandDischarge AS int) <=3
  OR SAFE_CAST(recommend AS int) <=6"""


def search_query(query):

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = bigquery_client.query(query, job_config=job_config)
    return query_job.result().to_dataframe()


def main():

    data = search_query(query)
    data1 = search_query(query1)
    return data, data1