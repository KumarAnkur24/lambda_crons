import pdb
import logging
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3

logger = logging.getLogger(__name__)

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')

def clear_and_write_to_sheet(sheet_id, sheet_name, sheet_range, data):
    try:
        # pdb.set_trace()
        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
        client = gspread.authorize(creds)
        client_sp = client.open_by_key(sheet_id)
        sheet_obj = client_sp.worksheet(title=sheet_name)
        till_col_fd_temp = len(sheet_obj.col_values(1))
        # sheet_obj.clear()
        
        sheet_obj.resize(rows=len(data)+1+till_col_fd_temp)
        
        
        # sheet_obj.resize(rows=len(data)+1)
        
        
        
        print(till_col_fd_temp,len(data))
        # range_str = sheet_range + str(len(data))
        range_str = sheet_range.format(till_col_fd_temp+1,len(data)+till_col_fd_temp+1)
        try:
            sheet_obj.batch_update([{'range': range_str, 'values': data}])
        except Exception as e:
            raise e
        
    except Exception as e:
        
        print(e)
        raise e
        
'''
Reading a sheet

def read_sheet(SHEET_ID,READ_SHEET_NAME,client):

    try:
        client_sp = client.open_by_key(SHEET_ID)
        val = client_sp.worksheet(title=READ_SHEET_NAME)
        return val.get_all_values()
    
    except exception as E:
        raise E

# To be defined in lambda function 

scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
client = gspread.authorize(creds)
SHEET_ID = '1PvIBXZcLC3fPFf4SGUzMQg1ZFwlkD_nJQ3R7uvdtBwA'
READ_SHEET_NAME = 'TL_Mappings'
        
DATA = read_sheet(SHEET_ID,READ_SHEET_NAME,client)

'''