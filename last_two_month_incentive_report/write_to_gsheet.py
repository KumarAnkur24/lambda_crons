import pdb
import logging
import os
import gspread
from datetime import date, datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import boto3

logger = logging.getLogger(__name__)
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')

sheets = {
    '1GJGaj317eh51vOjtgUI6gMr6evjygrTflyqjE5D1dNY': 'Formatted Dump Incentive',
}

sheetId = '1GJGaj317eh51vOjtgUI6gMr6evjygrTflyqjE5D1dNY'

def update_sheets(client_sp, recs, sheetName, sheetLen):
    try:
        sheet_obj = client_sp.worksheet(title=sheetName)

        rows_present = len(sheet_obj.col_values(1))
        sheet_obj.clear()
        to_add_rows = len(recs) - rows_present
        if to_add_rows > 0:
            sheet_obj.add_rows(rows=abs(to_add_rows))

        range_str = sheetLen + str(len(recs))
        sheet_obj.batch_update([{'range': range_str, 'values': recs}])
        logger.info('For Leads Summary spreadsheet {sheet} worksheet Updated Successfully'.format(sheet=sheetName))
    except Exception as e:
        print(e)
        raise e

def contoller(recs, sheetName, sheetLen):
    try:
        print(sheetName)
        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
        client = gspread.authorize(creds)
        client_sp = client.open_by_key(sheetId)
        update_sheets(client_sp, recs, sheetName, sheetLen)

    except Exception as e:
        raise e