import json
import pdb 
import os
import pytz 
import re
import pandas as pd
import numpy as np  
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
from service.base_functions import msg_to
from datetime import datetime , timedelta
from service.send_mail_client import send_email

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '') 
# READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL_BETA', '')

yesterday = datetime.now() + timedelta(hours = 5,minutes = 30)
yesterday = yesterday.date() 

fpath = os.path.join("/tmp","Appointments.csv")  


queries = {'isPartiallyFilled':"""select  
                                apd.personnelId,apd.name,apd.phone,
                                ac.cityName,apd.email,ldc.leadId,ldc.id as appId,cp.customerNumber,
                                CASE WHEN
                                lc.agent_email IS NOT null then lc.agent_email
                                else apd.email 
                                end as agent_email
                                from lead_doctor_consultation ldc
                                join patient_case pc on ldc.leadId = pc.caseId
                                join patient_profile pp on pc.patientId = pp.id
                                join customer_profile cp on cp.customerId = pp.customerId
                                left join ayu_personnel_details apd on ldc.ayuMitraId = apd.personnelId 
                                left join ayu_cities ac on apd.cityId = ac.id
                                left join ayu_facility_profile afp on afp.facilityId = ldc.hospitalId
                                left join 
                                (select leadId,user as agent_email from lead_comments 
                                where commentType in ('APPOINTMENT_DONE') and leadType = 'APPOINTMENT')lc
                                on lc.leadId = ldc.id
                                
                                where ldc.additionalDetails->>'$.isPartiallyFilled' = 'true'
                                and date(ldc.appointmentDate) = '{yesterday}'
                                order by apd.email,ldc.leadId
                                """,
                                
        'lead_doctor_consultation': """select *,ldc.additionalDetails->>'$.isPartiallyFilled'  as isPartiallyFilled 
        from lead_doctor_consultation ldc where date(createdOn) >= '{yesterday}'"""                        
    
}



def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query.format(yesterday = yesterday))    
          
    return fetched_val
    
    
def getTable(df,city,result):
    print(df)
    result = result +  f"""<body width = 1200> 
    <table style= border-collapse:collapse border= 1 >
    <tr bgcolor=#3E00B3 height= 30px style=color:#ffffff> 
    <td colspan = 13 width = 1600><b><center> {city} </center></b></td>
    </tr>
    <tr bgcolor=#AC80FF colspan = 13 style=color:#ffffff> 
    <td width = 1600><b><center> ayuMitra </center></b></td>
    <td width = 1600><b><center> CaseId  </center></b></td>
    <td width = 1600><b><center> ApptId </center></b></td>
    <td width = 1600><b><center> customer Number </center></b></td>
    
    </tr>"""
    
    for key,value in df.iterrows(): 
        result = result + f"""<tr  colspan = 13>
        <td width = 1600><center>{value['agent_email']}</center></td>
        <td width = 1600><center>{value['leadId']}</center></td>
        <td width = 1600><center>{value['appId']}</center></td>
        <td width = 1600><center>{value['cx No']}</center></td>
        
        </tr>"""
        
    result = result + """</table><br><br>"""
    return result
    
    

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
                                                    
        dataSets = fetch_data(read_connection_obj)
        isPartiallyFilled = pd.DataFrame(dataSets['isPartiallyFilled'])
        lead_doctor_consultation = pd.DataFrame(dataSets['lead_doctor_consultation'])
        
        resultant = ""
        if len(isPartiallyFilled):
            isPartiallyFilled['cx No'] = isPartiallyFilled.apply(lambda x: msg_to(x['customerNumber']),axis=1)
            isPartiallyFilled.to_csv(fpath)
            city = ['Chandigarh','Bangalore','Jaipur','NCR','Hyderabad']
        
            for c in city:
                df = isPartiallyFilled[isPartiallyFilled['cityName'] == c]
                if len(df)!=0:
                    resultant = getTable(df,c,resultant) 
                
            print(isPartiallyFilled.columns)
            Subject = "Partially filled Appt done - {0}".format(yesterday) 
            email_recipient_list = ['aakash@ayu.health','arjit@ayu.health', 
            'anshul@ayu.health','jay@ayu.health','asif.jamil@ayu.health','abu@ayu.health',
            'krish@ayu.health','city-managers-leads@ayu.health','rohit.chahal@ayu.health','ncr_ops@ayu.health']
            # email_recipient_list = ['nisha@ayu.health']
            send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath])   
            
        else:
            Subject = "Partially filled Appt done - {0}".format(yesterday) 
            email_recipient_list = ['aakash@ayu.health','arjit@ayu.health', 
            'anshul@ayu.health','jay@ayu.health','asif.jamil@ayu.health','abu@ayu.health',
            'krish@ayu.health','city-managers-leads@ayu.health','rohit.chahal@ayu.health','ncr_ops@ayu.health']
            # email_recipient_list = ['nisha@ayu.health'] 
            text = "No such appt for today" 
            send_email(None, email_recipient_list, Subject, 'None',text,[])   
            
            
            
        
        
        
        
        
        
       
    except Exception as e:
        Subject = " isPartiallyFilled_true_ayu_mitra_level || Error" 
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
