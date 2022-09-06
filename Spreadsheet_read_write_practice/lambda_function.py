import pdb
import os
import logging
import json
import pytz
import gspread
from datetime import datetime, timedelta
from service.database import InitDatabaseConnetion,make_db_params,fetch_record 
from oauth2client.service_account import ServiceAccountCredentials
from service.send_mail_client import send_email
from gpreadPractice import clear_and_write_to_sheet,read_from_sheet
import time
import pandas as pd
from service.base_functions import msg_to
import boto3 
from gspread.models import Cell
import requests


logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

yesterday = datetime.now() - timedelta(days =1)
yesterday = yesterday.date()

today = datetime.now() + timedelta(hours=5,minutes = 30)
fpath = os.path.join("/tmp","cases.csv")



queries = {'surgery':'''select sd.*,sdl.specialityName,afp.aliasName,cp.customerNumber, 
                        date(date_add(dischargeDate,INTERVAL '5:30' HOUR_MINUTE)) as DischargeDate , 
                        admissionDate as AdmissionDate , pp.patientName , 
                        date_add(dischargeDate, INTERVAL '5:30' HOUR_MINUTE) as discharge_date ,
                                    
                        Month(date_add(dischargeDate, INTERVAL '5:30' HOUR_MINUTE)) as discharge_month , 
                        Year(date_add(dischargeDate, INTERVAL '5:30' HOUR_MINUTE)) as discharge_Year ,
                        pc.leadSource as caseLeadSource
                                    
                        ,case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                                else 'No City' end as city
                            
                    from patient_surgery_details sd
                    join patient_case pc on sd.caseId = pc.caseId
                    join patient_profile pp on pc.patientId = pp.id
                    left join ayu_cities pcp on pc.cityId = pcp.id
                    left join ayu_cities ppp on pp.cityId = ppp.id
                    join customer_profile cp on pp.customerId=cp.customerId
                    join ayu_facility_profile afp on sd.hospitalId = afp.facilityId
                    left join speciality_details sdl on pc.specialityId = sdl.id
                    where  
                    pc.leadSource != 'Hospital Insurance'
                    and pc.tenantName = 'AYU'
                    and date(date_add(admissionDate, INTERVAL '5:30' HOUR_MINUTE)) >= '2022-08-01'   
                    
                    
                    
                    
                    
                    
'''
# NOTE
#and afp.aliasName != 'Sapthagiri' 
# use id and not surgeryId column in patient_surgery_details to get surgeryId
    
}


# surgeryStatus = 'ADMISSION_DONE'


# surgeryStatus = 'ADMISSION_DONE'
#                     and pc.leadSource != 'Hospital Insurance'
#                     and pc.tenantName = 'AYU'

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)    
          
    return fetched_val 

def response_chk(timelist,responce):
    
    val = ''
    timelist = json.loads(timelist)
    
    if len(timelist) == 3:
        if responce == 'true':
            val = 'Patient_Responded'
        else:
            val= 'Patient_eligible_for_calling'
    elif len(timelist) == 2:
        if responce == 'true':
            val = 'Patient_Responded'
        else:
            val = '2nd stage'
    elif len(timelist) == 1:
        if responce == 'true':
            val = 'Patient_Responded'
        else:
            val = '1st stage'
    else:
        val = 'Patient_eligible_for_calling'
    
    return val 

def month_year(month,year):
    
    formatted_date = ''
    
    formatted_date = str(month) + '-' + str(year)
    
    return formatted_date




def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        
        surgery = pd.DataFrame(dataSets['surgery'])
                                  
        

        
        DATA = []
        read_excel = read_from_sheet('1S_w7t-HQJZ5upLxYvpGXKrAwwfe1bnJ-SbUammQ7Kqs','automated_sheet')
        surgery = surgery[~surgery.id.isin(read_excel)]
        
        
        # # code
        # surgery.to_csv(fpath)
        # Subject = "cases"
        # email_recipient_list = ['nisha@ayu.health'] 
        # send_email(None, email_recipient_list, Subject, 'None',"",[fpath])  
        # # 
        
       
        
        for index,val in surgery.iterrows():  
            #pass 
            
            
            #if val['aliasName'] == 'Fortis CngmRd' and val['caseLeadSource'] == 'Offline channel':
            #    continue
            
            # print(val['caseId'])
            date = val['DischargeDate']
            
            #delta = today - val['admissionDate'] 
            
            #if delta.days >= 1: 
            DATA.append([
                val['caseId'],
                val['patientName'],
                val['aliasName'],
                val['specialityName'],
                val['id'],
                str(val['admissionDate']),
                val['city'], 
                msg_to(val['customerNumber']),
                val['caseLeadSource']
                ]) 
            
        
    
        clear_and_write_to_sheet('1S_w7t-HQJZ5upLxYvpGXKrAwwfe1bnJ-SbUammQ7Kqs','automated_sheet','A{0}:I{1}',DATA)
        
        
        
      
        
    except Exception as e:
        Subject = "Admission Calling Sheet (spreadsheet read write practice) || ERROR" 
        email_recipient_list = ['Analytics@ayu.health','namitha@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e

'''
['id', 'caseId', 'createdOn', 'updatedOn', 'user', 'tenantName',
'treatmentType', 'admissionDate', 'dischargeDate', 'doctorId',
'hospitalId', 'ayuMitraId', 'surgeryPackageType', 'surgeryStatus',
'followUpDate', 'followUpTime', 'surgeryId', 'treatmentName',
'promisedPrice', 'tentativeDischargeDate', 'discount',
'surgeryDoneDate', 'isSurgeryApplicable', 'additionalDetails',
'surgeryTypeId', 'procedureSpecialityMapping', 'specialityName',
'aliasName', 'customerNumber', 'timelist', 'responce', 'DischargeDate',
'AdmissionDate', 'city', 'responce_phase']
'''