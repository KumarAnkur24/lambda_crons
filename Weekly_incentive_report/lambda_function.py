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
from read_spreadsheet import read_from_sheet
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

# Description: weekly incentive cs agent based on target,for bangalore,hyderabad and north
# Created By : Nisha Das

start_day = datetime.now() + timedelta(days=-datetime.now().weekday(), weeks=1 , hours =5 ,minutes =30) - timedelta(days=7)
start_day = start_day.date()

end_day = datetime.now() + timedelta(days=-datetime.now().weekday(), weeks=1 , hours =5 ,minutes =30) - timedelta(days=1)
end_day = end_day.date()

#customer suppport details
query = '''select csd.name,csd.email,ac.cityName from customer_support_details csd
            left join ayu_cities ac on ac.id = csd.cityId
            '''

def fetch_data(conn,queryNo):

    if queryNo == 1:
        fetched_val = fetch_record(conn, query)
    return fetched_val 

def resultant_table(city_wise_incentive,city,color1,color2):
    
    result = f"""<body width = 1200> 
    <table style= border-collapse:collapse border= 1 >
    <tr bgcolor={color1} height= 30px style=color:#ffffff> 
    <td colspan = 13 width = 1600><b><center> {city} </center></b></td>
    </tr>
    <tr bgcolor={color2} colspan = 13 height= 30px style=color:#ffffff> 
    <td width = 1600><b><center> Name  </center></b></td>
    <td width = 1600><b><center> City </center></b></td>
    <td width = 1600><b><center> Reporting TL </center></b></td>
    <td width = 1600><b><center>Apt Target </center></b></td>
    <td width = 1600><b><center> Cov Target</center></b></td>
    <td width = 1600><b><center> Target Achieved</center></b></td>
    <td width = 1600><b><center>Deft </center></b></td>
    <td width = 1600><b><center>Amount </center></b></td>
    </tr>"""
    
    for key,value in city_wise_incentive.items():
        target_achieved = value['online'] + value['offline']
        if value['target'] and target_achieved >= value['target']:
            amount = 200*value['offline'] + 100*value['online']
        else:
            amount = 0
        diff = target_achieved - value['target'] 
        result = result + f"""<tr  colspan = 13> 
        <td width = 1600 bgcolor=#d9d9d9><b><center>{key}</center></b></td> 
        <td width = 1600><center>{value['city']}</center></td>
        <td width = 1600><center>{value['Tl member']}</center></td>
        <td width = 1600><center>{value['target']}</center></td>
        <td width = 1600><center>{value['cov target']}</center></td>
        <td width = 1600><center>{target_achieved}</center></td>
        <td width = 1600><center>{diff}</center></td>
        <td width = 1600><center>{amount}</center></td>
        </tr>"""
        
    result = result + """</table><br><br>"""
    return result


def get_dictionary(incentive):
    bangalore = {}
    hyderabad = {}
    north = {}
    for index,value in incentive.iterrows():
        if value['City'] == 'Bangalore':
            if value['Email ID'] not in bangalore.keys():
                bangalore[value['Email ID']] = {'offline':0,'online':0,'Tl member':'','target':0,'cov target':'','city':''}
                bangalore[value['Email ID']]['Tl member'] = value['Reporting TL']
                bangalore[value['Email ID']]['target'] = value['New Target']
                bangalore[value['Email ID']]['cov target'] = value['New Cov Target']
                bangalore[value['Email ID']]['city'] = value['City']
                
            if value['Appt type'] == 'Online Consultation':
                bangalore[value['Email ID']]['online'] += 1
            else:
                bangalore[value['Email ID']]['offline'] += 1

            
        elif value['City'] == 'Hyderabad':
            if value['Email ID'] not in hyderabad.keys():
                hyderabad[value['Email ID']] = {'offline':0,'online':0,'Tl member':'','target':0,'cov target':'','city':''}
                hyderabad[value['Email ID']]['Tl member'] = value['Reporting TL']
                hyderabad[value['Email ID']]['target'] = value['New Target']
                hyderabad[value['Email ID']]['cov target'] = value['New Cov Target']
                hyderabad[value['Email ID']]['city'] = value['City']
                
            if value['Appt type'] == 'Online Consultation':
                hyderabad[value['Email ID']]['online'] += 1
            else:
                hyderabad[value['Email ID']]['offline'] += 1
                
        else:
            if value['Email ID'] not in north.keys():
                north[value['Email ID']] = {'offline':0,'online':0,'Tl member':'','target':0,'cov target':'','city':''}
                north[value['Email ID']]['Tl member'] = value['Reporting TL']
                north[value['Email ID']]['target'] = value['New Target']
                north[value['Email ID']]['cov target'] = value['New Cov Target']
                north[value['Email ID']]['city'] = value['City']
                
            if value['Appt type'] == 'Online Consultation':
                north[value['Email ID']]['online'] += 1
            else:
                north[value['Email ID']]['offline'] += 1
    
    return bangalore,hyderabad,north
    
 

def lambda_handler(event, context):
    
    read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
    read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
    
    read_weekly_report = read_from_sheet('1qENQOXUEuxghBaXvJvUdj71TGqMe0mMQMYlQ_JbqfiI','lastWeekDump')
    read_weekly_report = read_weekly_report[read_weekly_report['Sequence '].isin([1,2])]
    
    
    read_target = read_from_sheet('1qENQOXUEuxghBaXvJvUdj71TGqMe0mMQMYlQ_JbqfiI','Targets')
   
    dataSets = fetch_data(read_connection_obj,1)
    cs_agent_city = pd.DataFrame(dataSets)
    cs_agent_city.rename(columns = {'email':'Email ID','cityName':'City'},inplace = True)


    read_weekly_report.rename(columns = {'Agent Action':'Email ID'},inplace = True)
    read_weekly_report = pd.merge(read_weekly_report,read_target,on = 'Email ID',how = 'left') 
    read_weekly_report.drop("City", axis=1, inplace=True)
    read_weekly_report = pd.merge(read_weekly_report,cs_agent_city,on = 'Email ID',how = 'left') 
    
    
    
    bangalore,hyderabad,north = get_dictionary(read_weekly_report)
    
    resultant = ""
    resultant = resultant + resultant_table(bangalore,"BLR","#e68a00","#ffc266")
    resultant = resultant + resultant_table(hyderabad,"HYD","#330080","#a366ff")
    resultant = resultant + resultant_table(north,"North","#663300","#ff8000")
    
    Subject = "Last Week Incentive Report"
    email_recipient_list = ['ankur@ayu.health']
    send_email(None, email_recipient_list, Subject, 'None',resultant,None)  

    
    
    
    
    
    
    
'''
read_target = Index(['Email ID', 'Reporting TL', 'New Target', 'Region', 'New Cov Target'], dtype='object')

read_weekly_report = Index(['Appointment date', 'caseId', 'Cstmr Contact No', 'Agent Action',
'Team Leader', 'Total Call Duration', 'Appt type', 'Consultation Fee',
'Surgery reco(Y/N)', 'City', 'Other hospital/doctor follow up reason',
'Other in comment', 'Pitch Charter from new crm'],
dtype='object')
'''


'''
2022-05-16	852565	8553631777	gayathri@ayu.health	rohithm@ayu.health	292	Consultation	0	No	Hyderabad	No	No	No
gayathri is not there in target
'''
    
    
'''
customer_support_details
Index(['customerSupportId', 'name', 'email', 'phoneNumber', 'gender', 'token',
'isAvailable', 'createdOn', 'updatedOn', 'hasLeft', 'cityId',
'properties', 'isTestUserAccount'],
dtype='object')
'''

'''
ayu_cities
Index(['id', 'cityName', 'isActive', 'defaultAyuMitraId',
'isCsTeamSeparationEnabled'],
dtype='object')
'''