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

def clear_and_write_to_sheet(sheet_id, sheet_name, sheet_range, data):
    try:
        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
        print(creds)
        client = gspread.authorize(creds)
        client_sp = client.open_by_key(sheet_id)
        sheet_obj = client_sp.worksheet(title=sheet_name)
        print("hello",sheet_obj)
        till_col_fd_temp = len(sheet_obj.col_values(1)) 
        print("hi ",till_col_fd_temp)
        
        sheet = sheet_range.format(till_col_fd_temp+1,len(data)+till_col_fd_temp) 
        
        
        
        sheet_obj.add_rows(rows=abs(len(data)))
        
        sheet_obj.batch_update([{'range': sheet, 'values': data}])
        
    except Exception as e:
        
        print(e)
        raise e