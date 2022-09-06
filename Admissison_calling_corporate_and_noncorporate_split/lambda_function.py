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
from spreadSheetRead import clear_and_write_to_sheet,read_from_sheet
import time
import pandas as pd
from service.base_functions import msg_to
import boto3 
from gspread.models import Cell 
import requests
import numpy as np


logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')


yesterday = datetime.now() + timedelta(hours=5,minutes=30,days = -1) 
yesterday = yesterday.date()
yesterday = yesterday.strftime("%d/%m/%Y")
current_time = datetime.now() + timedelta(hours=5,minutes=30) 
current_time = current_time.strftime("%H:%M")


fpath = os.path.join("/tmp","Brand Communication.csv")
fpath1 = os.path.join("/tmp","corporate_hospital.csv")
fpath2 = os.path.join("/tmp","loyalty_card.csv")


queries = {'loyalty_card':"""

                    select pc.caseId,cp.customerId,
                    CASE WHEN lc.status = 'done' then "Yes"
                    ELSE "No" end as status,date(lc.createdOn) as createdOn,
                    loyalty_paymentMode from patient_case pc
                    join patient_profile pp on pc.patientId = pp.id
                    join customer_profile cp on cp.customerId = pp.customerId
                    left join
                    (Select
                    c.customerId,
                    'done' as status, group_concat(l.createdOn) as createdOn,
                    group_concat(paymentMode) as loyalty_paymentMode  
                    from
                        lc_payment_details l  
                        join generated_mcards c on l.cardId = c.cardId
                        join customer_profile cp on c.customerId = cp.customerId
                        where
                            status = 'ACTIVE'
                        and cp.tenantName = 'AYU'
                        and amount = 0
                        group by 1,2) as lc on lc.customerId = cp.customerId
                        where pc.caseId in ('{caseId}')
"""
    
}


def fetch_data(conn,value):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query.format(caseId = ','.join(value)))
    return fetched_val
    



result_base = """<table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"rgb(56,118,29) \" style=\"color:#ffffff\"> 
        <td colspan = 13 width = \"1600\" height = \"30\"><b><center>{0}</center></b></td>
    </tr>
    <tr bgcolor=\" FFE599 \"> 
        <td colspan = 1  width = \"1600\"><b><center>Case Id</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Patient Name</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Contact Number</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Hospital Name</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Ayumitra Name</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>DOA</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Is contact details correct?</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Is Pt/Atendee aware about Ayuhealth?</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Did Ayumitra meet the patient?</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Is Patients aware about the benefits/services?</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Loyalty Card Given</center></b></td>
    </tr>
    """ 
    
    

def getTable(df,key):
    result = ""
    result = result + result_base.format(key)
    for key,value in df.iterrows():
        result = result + f"""<tr> 
            <td colspan = 1><center>{value['Case ID']}</center></td>
            <td colspan = 1><center>{value['Name']}</center></td>
            <td colspan = 1><center>{value['cxNo']}</center></td>
            <td colspan = 1><center>{value['Hospital']}</center></td>
            <td colspan = 1><center>{value['Ayumitra Name']}</center></td>
            <td colspan = 1><center>{value['DOA']}</center></td>
            <td colspan = 1><center>{value['Is contact details correct?']}</center></td>
            <td colspan = 1><center>{value['Offline case (Patients are aware about Ayuhealth)']}</center></td>
            <td colspan = 1><center>{value['Offline case (Did Ayumitra meet the patient)']}</center></td>
            <td colspan = 1><center>{value['Offline case (Patients are aware about Ayuhealth benefits/services)']}</center></td>
            <td colspan = 1><center>{value['loyalty_card_purchased']}</center></td>
            </tr>"""
            
    result = result + "</table><br><br>"
    
    return result
    
def getLoyaltyStatus(adm_status,caseId,date,loyalty_dict):
    if adm_status == 'ADMISSION_DONE':
            return "NOT DISCHARGED YET"
    if caseId in loyalty_dict.keys():
        if adm_status == 'DISCHARGE_DONE' and loyalty_dict[caseId][0] == 'No':
            return "No"
        elif adm_status == 'DISCHARGE_DONE' and loyalty_dict[caseId][0] == 'Yes' :
            return "Yes"
        else:
            return ""
    else:
        return ""




result_base2 = """<table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"rgb(56,118,29) \" style=\"color:#ffffff\"> 
        <td colspan = 13 width = \"1600\" height = \"30\"><b><center>{0}</center></b></td>
    </tr>
    <tr bgcolor=\" FFE599 \"> 
        <td colspan = 1  width = \"1600\"><b><center>Case ID</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Patient Name</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Hospital Name</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>AyuMitra Name</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Admission Date</center></b></td>
        <td colspan = 1  width = \"1600\"><b><center>Discharge Date</center></b></td>
    </tr>
    """ 



def getSecondTable(df,city):
    result = result_base2.format(city)
    for key,value in df.iterrows():
        result = result + f"""<tr> 
            <td colspan = 1><center>{value['Case ID']}</center></td>
            <td colspan = 1><center>{value['Name']}</center></td>
            <td colspan = 1><center>{value['Hospital']}</center></td>
            <td colspan = 1><center>{value['Ayumitra Name']}</center></td>
            <td colspan = 1><center>{value['DOA']}</center></td>
            <td colspan = 1><center>{value['Discharge_Date']}</center></td>
            </tr>"""
            
    result = result + "</table><br><br>"
    
    return result
        
    
    

def lambda_handler(event, context): 
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
        
        read = pd.DataFrame()
        read_excel = pd.DataFrame()
        read = read_from_sheet('1CuuDHqTr-AToxPjPHgrlGswp-r442T-VRXrO_nIg3G4','Work sheet')
        
        read_excel = read
        read_excel = read_excel[read_excel['Admission status'].isin(['ADMISSION_DONE','DISCHARGE_DONE'])]
        read_excel['Discharge_Date'] = pd.to_datetime(read_excel['Discharge_Date']) 
        read_excel = read_excel[(read_excel['Last Date of Call'] == yesterday) & (read_excel['Call status'] != 'Not Responded')]
        read_excel = read_excel[read_excel['leadSource'].isin(['Offline channel','VC Model'])]
        
        
        
        caseId = set(read_excel['Case ID'].astype(str))
        dataSets = fetch_data(read_connection_obj,caseId)
        loyalty_card = pd.DataFrame(dataSets['loyalty_card'])
        loyalty_card.to_csv(fpath2)
        
        read_excel.to_csv(fpath)
        
        loyalty_dict = {}
        for key,value in loyalty_card.iterrows():
            if value['caseId'] not in loyalty_dict.keys():
                loyalty_dict[value['caseId']] = [0,0]
                loyalty_dict[value['caseId']][0] = value['status']
                loyalty_dict[value['caseId']][1] = value['createdOn']
            
            
        
        c_hospital = ['Apollo Sheshadripuram','Fortis CngmRd','Aster RV Hospital','Manipal hospital','Apollo Hospital',
        'CK Birla Hospital Gurugram','Sarvodaya Hospital','Manas Hospital']
        nc_hospital = ['Mallige','GM Hospital','Kanva Sri Sai Hospital','Marvel Multispeciality Hospital',
        'Narayana Hospital','North Bangalore Hospital','Vasavi Hospital','Swastik','Best hospital','Santosh Hospital',
        'Felix Hospital','NIMS Medical College and Hospital'] 
        if len(read_excel):
            read_excel['loyalty_card_purchased'] = read_excel.apply(lambda x:getLoyaltyStatus(x['Admission status'],
            x['Case ID'],x['Discharge_Date'],loyalty_dict),axis = 1)
            read_excel = read_excel.replace('','Not Responded')
            read_excel['loyalty_card_purchased'] = read_excel['loyalty_card_purchased'].replace('Not Responded','')
            
        
        
          
        
        
        corporate_hospital = read_excel[read_excel['Hospital'].isin(c_hospital)]
        non_corporate_hospital = read_excel[read_excel['Hospital'].isin(nc_hospital)]
        resultant = getTable(corporate_hospital,"Corporate- Hospitals")
        resultant = resultant + getTable(non_corporate_hospital,"Non Corporate- Hospitals")
        corporate_hospital.to_csv(fpath1)
        
        
        
    
        read['Discharge_Date'] = read['Discharge_Date'].astype(str).replace({'NaT':''})
        
    
        
        
        report_second = read


        
        # report_second = report_second[report_second.Discharge_Date.isnull()] 
        # print("hello",report_second[['Case ID','Lead status','Discharge_Date']])
        # report_second = report_second[report_second['Lead status'] == 'Discharge Done']
        # report_second['Discharge_Date'] = report_second['Discharge_Date'].astype(str).replace({'NaT':''})
        
        report_second = report_second[(report_second['Discharge_Date'] == '') & 
        (report_second['Lead status'] == 'Discharge Done')]
        report_second['DOA'] = pd.to_datetime(report_second['DOA'])
        report_second['DOA'] = report_second['DOA'].dt.date 
        print(len(report_second))
        resultant2 = ""
        city = ['BLR','CHD','Jaipur','Hyderabad','NCR']
        for key in city:
            df = report_second[report_second['City '] == key]
            resultant2 = resultant2 + getSecondTable(df,key)  
            
        
        
             
        
        # if current_time == '20:00':      
        #     Subject = "Offline Brand Communication | {0}".format(yesterday)
        #     # email_recipient_list = ['nisha@ayu.health'] 
        #     email_recipient_list = ['city-managers-leads@ayu.health','himesh@ayu.health','karan@ayu.health',
        #     'namitha@ayu.health','jay@ayu.health','ravali@ayu.health','experience-team@ayu.health']
        #     send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath,fpath1,fpath2])   
        
        
        
    
        if current_time == '19:00':
            Subject = "Discharge Not Yet Initiated In CRM | {0}".format(yesterday)  
            # email_recipient_list = ['nisha@ayu.health'] 
            email_recipient_list = ['chandni@ayu.health','shahwaz@ayu.health','rohit.chahal@ayu.health',
            'vibhor@ayu.health','vishal.khobra@ayu.health','ncr_ops@ayu.health',
            'city-managers-leads@ayu.health','ashish@ayu.health','mallikharjun@ayu.health',
            'srikanth.k@ayu.health','lalit@ayu.health','ashish.nagar@ayu.health',
            'yash@ayu.health','upender@ayu.health',
            'manpreet@ayu.health','jay@ayu.health','pankaj@ayu.health','experience-team@ayu.health',
            'namitha@ayu.health','nisha@ayu.health']
            send_email(None, email_recipient_list, Subject, 'None',resultant2,[fpath])     
        
        
    except Exception as e: 
        Subject = 'Admissison_calling_corporate_and_noncorporate_split | Error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
