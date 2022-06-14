import pdb
import logging 
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import pandas as pd

logger = logging.getLogger(__name__)

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')

def read_from_sheet(sheet_id,sheet_name):
    try:
        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
        
        
        client = gspread.authorize(creds)
        client_sp = client.open_by_key(sheet_id)
        sheet_obj = client_sp.worksheet(title=sheet_name)
        df = pd.DataFrame(data=sheet_obj.get_all_records())
        
        
        if len(df):
            return df
        
        
    except Exception as e:
        raise e
    
    


    

