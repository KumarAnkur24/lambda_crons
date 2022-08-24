import pdb
import logging
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3

logger = logging.getLogger(__name__)

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')

def write_to_sheet(sheet_id, sheet_name, sheet_range, data):
    try:
        # pdb.set_trace()
        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
        client = gspread.authorize(creds)
        client_sp = client.open_by_key(sheet_id)
        sheet_obj = client_sp.worksheet(title=sheet_name)
        till_col_fd_temp = len(sheet_obj.col_values(1))
        sheet_obj.clear()
        
       
        # sheet_obj.resize(rows=len(data)+1+till_col_fd_temp)
        sheet_obj.resize(rows=len(data)+1)
        
        # print(till_col_fd_temp,len(data))
        # range_str = sheet_range.format(till_col_fd_temp+1,len(data)+till_col_fd_temp+1)
        range_str = sheet_range + str(len(data))
        # print(range_str)
        try:
            sheet_obj.batch_update([{'range': range_str, 'values': data}])
        except Exception as e:
            raise e
        
    except Exception as e:
        
        print(e)
        raise e