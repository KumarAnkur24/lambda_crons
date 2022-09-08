import pdb
import os
import pymysql as sql
import pytz
import pandas as pd
import traceback
import logging
from service.database import InitDatabaseConnetion, make_db_params, update_record
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

# DB_USER = os.environ.get('DB_USER', '')
# DB_PASSWORD = os.environ.get('DB_PASSWORD', '')
# DB_DOMAIN_URL = os.environ.get('DB_DOMAIN_URL', '')

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
# READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
READ_DB_DOMAIN_URL_BETA = os.environ.get('READ_DB_DOMAIN_URL_BETA', '')


def read_sheet(SHEET_ID, READ_SHEET_NAME, client):
    try:
        client_sp = client.open_by_key(SHEET_ID)
        val = client_sp.worksheet(title=READ_SHEET_NAME)
        return val.get_all_values()

    except exception as E:
        raise E


def record_update(records):
    update_stmt = """ Update medicine_delivery_orders

                        Set  orderStatus = '{0}'

                        where 
                            orderId = {1} 


    """

    to_append = ''
    for key, value in records.items():
        print(key, value)
        orderId = key
        orderStatus = value

        print(update_stmt.format(orderStatus, orderId))

        host, port, database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL_BETA})
        connection_obj = sql.connect(host=host,
                                     user=READ_DB_USER,
                                     password=READ_DB_PASSWORD,
                                     db=database,
                                     port=int(port),
                                     cursorclass=sql.cursors.DictCursor)

        update_record(connection_obj, update_stmt.format(orderStatus, orderId))

        # if agentEmail == None:
        #     continue

    #     to_app = (orderId ,orderStatus)

    #     to_append += str(to_app)
    #     to_append += ','

    # if to_append != '':
    #     print(update_stmt.format(recs=to_append[:-1]))
    #     update_record(conn, update_stmt.format(recs=to_append[:-1]))


def main():
    try:

        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
        client = gspread.authorize(creds)
        SHEET_ID = '1CQTHKJO3KU5QnJBuco__bDKjLY4cOGvxN-evSKG4dYA'
        READ_SHEET_NAME = 'Orders_beta'

        DATA = read_sheet(SHEET_ID, READ_SHEET_NAME, client)

        Sheet_data = pd.DataFrame(DATA)
        columns = Sheet_data.iloc[0]
        sheet_data = Sheet_data.rename(columns=columns)
        sheet_data = sheet_data.drop([0], axis=0)

        record = {}

        for index, val in sheet_data.iterrows():
            if val['orderStatus'] == 'OPEN':
                if val['Current Order Status'] in ('Cancelled', 'Completed'):
                    if val['orderId'] not in record.keys():
                        record[val['orderId']] = val['Current Order Status'].upper()
                        # record[val['orderId']]['entityId'] = val['entityId']
                        # record[val['orderId']]['entityType'] = val['entityType']
                        # record[val['orderId']]['orderStatus'] = val['Current Order Status']
                        # record[val['orderId']]['orderTrackingLink'] = val['orderTrackingLink']
                        # record[val['orderId']]['createdOn'] = val['createdOn']
                        # record[val['orderId']]['updatedOn'] = val['updatedOn']
                        # record[val['orderId']]['address'] = val['address']
                        # record[val['orderId']]['landmark'] = val['landmark']
                        # record[val['orderId']]['city'] = val['city']
                        # record[val['orderId']]['state'] = val['state']
                        # record[val['orderId']]['pincode'] = val['pincode']
                        # record[val['orderId']]['name'] = val['name']
                        # record[val['orderId']]['phoneNumber'] = val['phoneNumber']

        record_update(record)


    except Exception as e:
        raise e