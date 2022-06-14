import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record , update_record
from service.send_mail_client import send_email
import pandas as pd
import csv
import re
#from datetime import datetime
import datetime
from oauth2client.service_account import ServiceAccountCredentials
from s3_utils.utils import upload_to_s3
from datetime import datetime , timedelta
import boto3
from gSpreadPractice import clear_and_write_to_sheet
import requests 
import json

#Description - check the status for the question QUESTION -> Hi $(variable1), Thank you for choosing Ayu Health Hospitals for your treatment! Please let us know if you had a satisfactory experience with us.
#Author - Nisha Das

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
# READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL_BETA', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
fpath = os.path.join("/tmp","whatsapp.csv")
yesterday = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days=1)
yesterday = yesterday.date()
fpath = os.path.join("/tmp","whatsapp_message_response.csv")

whatsapp_query = """ select uuid,fromId,toMessage,message,status,
                    date_add(wmd.createdOn,interval '5:30' HOUR_MINUTE) 
                     as 'createdOn' from whatsapp_message_details wmd

                     where toMessage != fromId 
                     and date_add(wmd.createdOn,interval '5:30' HOUR_MINUTE) >= '{yesterday}'
 
""" 
# and date_add(createdOn,interval '5:30' HOUR_MINUTE) >= '{yesterday}'
def getQuestionStatus(question,regex):
    if  re.findall(regex,question):
        return "Yes"
    else:
        return "No"

def fetch_data(conn,queryNo):
    if queryNo == 1:
        fetched_val = fetch_record(conn, whatsapp_query.format(yesterday = yesterday))
    return fetched_val 
    
    
def getTable(question,yes_response,no_response):
    if question in yes_response:
        return "Yes"
    elif question in no_response:
        return "No"
    else:
        return " "
    
def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        dataSets = fetch_data(read_connection_obj,1)
        whatsapp = pd.DataFrame(dataSets)
        whatsapp.to_csv(fpath)
        
        
        question = "Hi.* Thank you for choosing Ayu Health Hospitals for your treatment! Please let us know if you had a satisfactory experience with us.*"
        whatsapp_question = whatsapp
        whatsapp_question['question'] = whatsapp_question.apply(lambda x: getQuestionStatus(x['message'],question),axis = 1)
        whatsapp_question = whatsapp_question[whatsapp_question['question'] == "Yes"]
        
        question = "Thank you for your feedback! Request you to rate us 5 stars on Google, click here.*"
        yes_response = whatsapp[['toMessage','message']]
        yes_response['response'] = yes_response.apply(lambda x: getQuestionStatus(x['message'],question),axis = 1)
        yes_response = yes_response[yes_response['response'] == "Yes"]
        
        
        question = "Sorry to hear that! Ayu Health strives to provide the best experience to the patients who approach us. Our officer will get in touch with you to understand the issue"
        no_response = whatsapp[['toMessage','message']]
        no_response['response'] = no_response.apply(lambda x: getQuestionStatus(x['message'],question),axis = 1)
        no_response = no_response[no_response['response'] == "Yes"]
        print(yes_response)
        print("-----------------")
        print(no_response)
        print("----------------")
        print(whatsapp_question)
        
        if(len(whatsapp_question)):
            whatsapp_question['response'] = whatsapp_question.apply(lambda x: getTable(x['toMessage'],list(yes_response['toMessage']),list(no_response['toMessage'])),axis = 1) 
            
            DATA = []
            for key,val in whatsapp_question.iterrows():
                DATA.append([
                        val['uuid'],
                        val['toMessage'],
                        val['message'],
                        val['status'], 
                        str(val['createdOn']),
                        val['response']
                        ])
            clear_and_write_to_sheet('1DNI2IAR9yUoB7azBD0Ri2FiHmg13jervuKdeU4vl7qo',
            'whatsapp_message_response','A{0}:F{1}',DATA)
            
        
        
    except Exception as e:
        Text = 'Error Running whatsapp_message_status , response Text: ' + str(e)
        Subject = "offline channel followUp | Error"
        # email_recipient_list = ["analytics@ayu.health"]
        # send_email(None, email_recipient_list, Subject, Text, None, [])
        raise e

'''
whatsapp_message_details
Index(['uuid', 'fromId', 'toMessage', 'message', 'attachment', 'epochCreated',
'epochUpdated', 'sendingFailed', 'status', 'firebaseId', 'createdOn',
'updatedOn'],
'''