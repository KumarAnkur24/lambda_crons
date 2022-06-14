import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
from service.base_functions import msg_to
from datetime import datetime , timedelta
from service.send_mail_client import send_email
from s3_utils.utils import upload_to_s3
import numpy as np
from google.cloud import bigquery
import numpy as np 
import boto3
import json
 

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
SECRETS_FILE_BQ = os.environ.get('SECRETS_FILE_BQ', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')

today = datetime.now() + timedelta(hours=5,minutes=30) 
today = today.date()
fpath = os.path.join("/tmp","ayu_credit_cash.csv")
fpath1 = os.path.join("/tmp","cash_in_wallet.csv")

# Description : Report to generate the amount present in patient's ayu cash wallet
# Author : Nisha Das



queries = {
    "ayu_cash": """select ac.ayuCashId,
                          ac.customerId,
                          ac.amount/100 as amount,
                          ac.validTill, 
                          date(date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE)) as 'createdOn',
                          ac.transactionType,
                          cp.customerName,cp.customerEmail,cp.customerNumber
                        from ayu_cash ac left join customer_profile cp on ac.customerId = cp.customerId
                            where 
                                date(date_add(ac.createdOn, INTERVAL '5:30' HOUR_MINUTE)) < curdate()
                        
                            """ 
}

# total 566710

# Index(['ayuCashId', 'customerId', 'amount', 'validTill', 'createdOn',
# 'transactionType', 'customerName', 'customerEmail', 'customerNumber',
# 'balance'],
# ayuCashId           int64
# customerId          int64
# amount             object
# validTill          object
# createdOn          object
# transactionType    object
# customerName       object
# customerEmail      object
# customerNumber     object
# balance            object

def upload_to_bq(df_list):
    
    boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE_BQ, '/tmp/secrets_bq.json')
    client = bigquery.Client.from_service_account_json("/tmp/secrets_bq.json", project='gmb-centralisation')
    table_ref = client.dataset("tables_crons").table("wallet_balance")
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('ayuCashId','STRING'),
        bigquery.SchemaField('customerId','STRING'),
        bigquery.SchemaField('amount','FLOAT'),
        bigquery.SchemaField('validTill','DATE'),
        bigquery.SchemaField('createdOn','STRING'),
        bigquery.SchemaField('transactionType','STRING'),
        bigquery.SchemaField('customerName','STRING'),
        bigquery.SchemaField('customerEmail','STRING'),
        bigquery.SchemaField('customerNumber','STRING'),
        bigquery.SchemaField('balance','FLOAT')
        ],
    write_disposition="WRITE_TRUNCATE",
         )
         
    job = client.load_table_from_dataframe(
    df_list, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()




def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)
    return fetched_val
    
    
def get_amount(cash):
    cash_dict = {}
    for key,value in cash.iterrows():
        if value['customerId'] not in cash_dict.keys():
            cash_dict[value['customerId']] = {'amount' : 0,'name':value['customerName']}
            
        if value['transactionType'] == 'CREDITED' and value['validTill'] >= today:
            cash_dict[value['customerId']]['amount'] += value['amount']
        elif value['transactionType'] == 'DEBITED':
            cash_dict[value['customerId']]['amount'] -= value['amount']
        
    return cash_dict
    
def resultant_table(cash):
    result = f"""<body width = 1200> 
    <table style=border-collapse:collapse border=1 >
    <tr bgcolor=#000000  style=color:#ffffff> 
    <td  width = 1600><b><center> Name </center></b></td>
    <td  width = 1600><b><center> Amount </center></b></td>
    </tr>"""
    
    for key,value in cash.items():
        result = result + f"""<tr  colspan = 13> 
                <td width = 1600><center>{value['name']}</center></td>
                <td width = 1600><center>{value['amount']}</center></td>
                </tr>"""
                
        result = result + """</table><br><br>"""
        
    return result

# to check whether credit is used 
def check_credit_used(credit,debit):
    debit_dictionary = {}
    for key,value in debit.iterrows():
        if value['customerId'] not in debit_dictionary.keys():
            debit_dictionary[value['customerId']] = {'amount': [],'date': [],'check': []} 
            
        debit_dictionary[value['customerId']]['amount'].append(value['amount'])
        debit_dictionary[value['customerId']]['date'].append(value['createdOn'])
        debit_dictionary[value['customerId']]['check'].append(False)
        
    
    credit = check_credit_expired(credit,debit_dictionary)
    return credit

# 6 scenarios----------------------------------------------------------------------------------------------------------------
# 1.credit is equal to the debit sum and credit expiry date is valid then in that case balance = 0
# 2.credit is expired but full amount is already debited in that case balance = 0
# 3.credit is expired and half amount is debited in that case balance = 0 and not credit - sum_of_debit,
# this balance needs to be rejected as it is expired
# 4.credit is not expired and half amount is debited than balance = credit - sum_of_debit
# 5.credit is not expired and nothing is debited than balance = credit
# 6.credit is expired and nothing is debited in that case balance = 0
# ------------------------------------------------------------------------------------------------------------------------------


def check_credit_expired(credit,debit):
    # calculate all subarrays
    for index,value in credit.iterrows():
        sum = 0
        if value['validTill'] < today and value['customerId'] in debit.keys():
            print('Inside 1st if block')
            s = np.array(debit[value['customerId']]['date'])
            sort_index = np.argsort(s)
            for sort_value in sort_index:
                if debit[value['customerId']]['date'][sort_value]<value['validTill']:
                    if debit[value['customerId']]['amount'][sort_value] >= value['amount']:
                        debit[value['customerId']]['amount'][sort_value] = (
                            debit[value['customerId']]['amount'][sort_value] - value['amount'])
                        print('1st', debit[value['customerId']]['amount'][sort_value])    
                        sum = sum + value['amount']
                    else:
                        print('2nd', debit[value['customerId']]['amount'][sort_value])
                        sum = sum + debit[value['customerId']]['amount'][sort_value]
                        debit[value['customerId']]['amount'][sort_value] = 0
                        
          
                print('Sum 1', sum)
                if sum >= value['amount']:
                    credit.at[index,'balance'] = 0
                    break
            
            if sum < value['amount']:
                credit.at[index,'balance'] = 0
            
            continue
        
        elif value['validTill'] >= today and value['customerId'] in debit.keys():
            print('Inside 2nd if block')
            s = np.array(debit[value['customerId']]['date'])
            sort_index = np.argsort(s)
            for sort_value in sort_index:
                #if debit[value['customerId']]['date'][sort_value]<value['validTill']:
                #sum = sum + debit[value['customerId']]['amount'][sort_value]
                #debit[value['customerId']]['check'][sort_value] = True
                if debit[value['customerId']]['amount'][sort_value] >= value['amount']:
                    debit[value['customerId']]['amount'][sort_value] = (
                        debit[value['customerId']]['amount'][sort_value] - value['amount'])
                    print('3rd',debit[value['customerId']]['amount'][sort_value])    
                    sum = sum + value['amount']
                else:
                    print('4th', debit[value['customerId']]['amount'][sort_value])
                    sum = sum + debit[value['customerId']]['amount'][sort_value]
                    debit[value['customerId']]['amount'][sort_value] = 0
                    
                
                print('Sum 2', sum)        
                if sum >= value['amount']:
                    credit.at[index,'balance'] = 0
                    break
            
            if sum < value['amount']:
                credit.at[index,'balance'] = value['amount'] - sum
                
        else:
            print('Inside 3rd if block')
            credit.at[index,'balance'] = value['amount']
                
    return credit            

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        ayu_cash = pd.DataFrame(dataSets['ayu_cash'])
        
        credit = ayu_cash[ayu_cash['transactionType'] == 'CREDITED']
        debit = ayu_cash[ayu_cash['transactionType'] == 'DEBITED']
        #print(credit[['customerId','transactionType','validTill','amount','createdOn']])
        print(debit[['customerId','transactionType','validTill','amount','createdOn']])
        #print(debit[['customerId','transactionType','validTill','amount','createdOn']])
        
    
        check = check_credit_used(credit,debit)
        cols = ['ayuCashId','customerId','createdOn','transactionType','customerName','customerEmail','customerNumber']
        check[cols] = check[cols].astype(str)
        check[['amount','balance']] = check[['amount','balance']].astype(float) 
        print(check.dtypes) 
        print(credit[['customerId','transactionType','validTill','amount','createdOn','balance']])
        upload_to_bq(check)
        
        
        # check.to_csv(fpath)
        # fileNamePath = 'reports/ayu_credit_cash.csv' 
        # upload_to_s3("crons-dumps", fileNamePath, fpath)
        
        
    except Exception as e:
        raise e
        
        
        
'''
ayu_cash
x(['ayuCashId', 'customerId', 'cashbackType', 'amount', 'issuedEntityType',
'issuedEntityId', 'validTill', 'additionalDetails', 'createdOn',
'updatedOn', 'transactionType'],
dtype='object')
'''
    
'''
patient_case
Index(['caseId', 'hospitalId', 'doctorId', 'patientLeadStatus', 'followUpdate',
'followUpTime', 'specialityId', 'patientId', 'symptoms', 'assignedTo',
'createdOn', 'updatedOn', 'caseType', 'leadSource', 'reason',
'consultationFee', 'tenantName', 'additionalDetails', 'cityId',
'followUpdateNullCheck'],
dtype='object')
'''

'''
customer_profile
Index(['customerId', 'customerName', 'customerEmail', 'gender', 'dateOfBirth',
'customerNumber', 'secondaryNumber', 'isSecondaryNumberWhatsappEnabled',
'referralCode', 'appToken', 'tenantName', 'prefLanguage', 'leadSource',
'customerLocation', 'additionalDetails', 'createdOn', 'updatedOn',
'customerType'],
dtype='object')
'''