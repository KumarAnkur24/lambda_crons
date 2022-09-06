import pdb
import os
import logging
import json
import pytz
from datetime import datetime, timedelta
from service.database import InitDatabaseConnetion,make_db_params,fetch_record 
from service.send_mail_client import send_email
import time
import pandas as pd
from service.base_functions import msg_to
import boto3 
import requests
import sys
logger = logging.getLogger(__name__)

# description --> send details of the new lead created for apollo hospital-Delhi in 10 min window
# author --> Nisha Das

time_19 = datetime.now() + timedelta(hours=5,minutes=19)
time_29 = datetime.now() + timedelta(hours=5,minutes=28)
time_19 = time_19.strftime('%Y-%m-%d %H:%M:00')
time_29 = time_29.strftime('%Y-%m-%d %H:%M:59')


# time_19 = datetime.now() + timedelta(hours=5,minutes=30)
# # time_19 = time_19.replace(day = 1)
# time_19 = time_19 - timedelta(days = 5)
# # time_19 = time_19.replace(day=1)
# # time_19 = time_19 - timedelta(days = 1)
# # time_19 = time_19.replace(day=1)
# time_29 = datetime.now() + timedelta(hours=5,minutes=30)
# time_29 = time_29 - timedelta(days = 1)
# time_19 = time_19.strftime('%Y-%m-%d %H:%M:00')
# time_29 = time_29.strftime('%Y-%m-%d %H:%M:59')
# print(time_19)
# print(time_29)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

fpath = os.path.join('/tmp','New Leads.csv')

queries = {
                'leads': """
                            SELECT pc.caseId,pc.hospitalId,pc.doctorId,pc.createdOn,
                            pp.age,pp.patientName,pp.customerId,
                            cp.customerNumber,
                            sd.specialityName,
                            dp.name as DoctorName
                            FROM patient_case pc
                            LEFT JOIN patient_profile pp on pp.id = pc.patientId
                            LEFT JOIN customer_profile cp on cp.customerId = pp.customerId
                            LEFT JOIN doctors_profile dp on pc.doctorId = dp.doctorProfileId
                            LEFT JOIN speciality_details sd on sd.id = pc.specialityId
                            WHERE pc.createdOn >= '{start}'
                            and pc.createdOn <= '{end}'
                            and pc.hospitalId = 199
                        """
}



def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query.format(start=time_19, end=time_29))
    return fetched_val
    
    
    
result_base= '''   
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        
        <tr bgcolor=\"#008080\" style=\"color:#ffffff\">  
            <td colspan = 2 width = \"1300\" height = \"30\" ><b><center> Case Created | Apollo Delhi </center></b></td>
        </tr> 
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Patient Name </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {0} </center></b></td>
        </tr>
        <tr > 
            <td colspan = 1 bgcolor=\"#DCDCDC\"  width = \"500\"><b><center> Patient Age </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {1} </center></b></td>
        </tr>
        <tr > 
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Date & time of lead creation </center></b></td>
            <td colspan = 1 width = \"800\" ><b><center> {2} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\" ><b><center> Mobile number </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {3} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\" ><b><center> Speciality</center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {4} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\" ><b><center> Doctor </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {5} </center></b></td>
        </tr>
        </table>
        <br>
    ''' 
    
def concat(doctorname,specialityName):
    return specialityName


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        
        new_leads = pd.DataFrame(dataSets['leads'])
        new_leads.to_csv(fpath)
        
        result = ''
        result_flag = False
        
        if len(new_leads):  
            
            new_leads['cxNo'] = new_leads.apply(lambda x: msg_to(x['customerNumber']),axis=1)
            new_leads['Specialty / Doctor | Emergency'] = new_leads.apply(lambda x: 
                concat(x['DoctorName'],x['specialityName']),axis = 1)
            
            for index , val in new_leads.iterrows():
                result = ''
                result = result_base.format(val['patientName'],val['age'],val['createdOn'],val['customerNumber'],
                val['specialityName'],val['DoctorName'])
                Subject = "Case Created | Apollo Delhi - {0}".format(val['caseId']) 
                email_recipient_list = ['doctorshelpdesk@apollohospitalsdelhi.com','ncr_ops@ayu.health',
                'kaveri@ayu.health','yashu@ayu.health']
                send_email(None, email_recipient_list, Subject, 'None',result,[fpath])     
                print("Mail Sent !!!!")  
                
                result_flag =True
                
                
        print("executed",len(new_leads))
        # if result_flag:
            
        #     Subject = "Real Time intimation to hospital | Apollo Delhi" 
        #     email_recipient_list = ['nisha@ayu.health']
        #     send_email(None, email_recipient_list, Subject, 'None',result,[fpath])     
        #     print("Mail Sent !!!!")    
        
    except Exception as e:
        raise e
    
    
