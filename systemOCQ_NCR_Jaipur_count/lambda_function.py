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

yesterday = datetime.now() + timedelta(hours=5,minutes=30) + timedelta(days=1)
yesterday = yesterday.date()

fpath = os.path.join("/tmp","systemOCQJaipur.csv")
fpath1 = os.path.join("/tmp","systemOCQNCR.csv")
fpath2 = os.path.join("/tmp","systemOCQAllData.csv")



query = '''select pc.caseId,pc.patientLeadStatus,pc.followUpdate,pc.followUpTime,pc.assignedTo,
           pc.reason,pc.cityId,pcp.cityName from patient_case pc 
           join patient_profile pp on pc.patientId = pp.id 
           join customer_profile cp on pp.customerId = cp.customerId 
           left join ayu_cities pcp on pc.cityId = pcp.id
           left join ayu_cities ppp on pp.cityId = ppp.id
           where pc.followUpdate = '{yst}' and pc.patientLeadStatus in ('0','1','5') and pc.assignedTo != 'system@ayu.health'
           and pc.tenantName = 'AYU'
           '''
           
     
        #   pc.assignedTo = "systemOCQueue@ayu.health
           
           
           
query2 = '''select * from patient_profile limit 3000'''
# cityName is in ayu_cities
           
#what is city id


def fetch_data(conn,queryNo,value):
    fetched_val = {}
    if queryNo == 1:
        fetched_val = fetch_record(conn, query.format(yst = yesterday))
    if queryNo == 2:
        fetched_val = fetch_record(conn,query2.format(yst = yesterday))
    # if queryNo == 3:
    #     fetched_val = fetch_record(conn,example.format(caseId = ','.join(value)))
    
    
    return fetched_val 
    
    
def fetchCityData(cityDataframe):
    resultCount = {}
    # data = cityDataframe['time'].unique()
    # print(len(data))
    for value in cityDataframe.index:
        resultCount[cityDataframe['time'][value]] = {}
    
        
    for value in cityDataframe.index:
        resultCount[cityDataframe['time'][value]]['follow up count of system OCQ'] = 0
        resultCount[cityDataframe['time'][value]]['lead in confirmation pending'] = 0
        resultCount[cityDataframe['time'][value]]['count of leads in MO bucket'] = 0
        resultCount[cityDataframe['time'][value]]['MO requires in the hour'] = 0
        
    for value in cityDataframe.index:
        if (cityDataframe['reason'][value] == 'Confirmation Pending') and (
            cityDataframe['assignedTo'][value] != 'systemOCQueue@ayu.health'):
            resultCount[cityDataframe['time'][value]]['lead in confirmation pending'] += 1  
            
        if cityDataframe['assignedTo'][value] == 'systemOCQueue@ayu.health': 
            resultCount[cityDataframe['time'][value]]['follow up count of system OCQ'] += 1

            
        
        if cityDataframe['assignedTo'][value] != 'systemOCQueue@ayu.health':
            resultCount[cityDataframe['time'][value]]['count of leads in MO bucket'] += 1
            
    
    for key, value in resultCount.items():
        resultCount[key]['MO requires in the hour'] = (value['follow up count of system OCQ'] + value['count of leads in MO bucket'])/25
        
    return resultCount
           
    
  
# time in different format , none value in time , min is not there    
def getTime(time):
    
    if time in [None,"None"]:
        return time
    
    if time not in [None,"None"] and time.find('M') == -1:
        listTime = time.split("-")
        str1 = ""
        
        if listTime[0].find(':') == -1:
            if len(listTime[0]) == 4:
                t1 = (int(listTime[0][0]))*10 + int(listTime[0][1])
            elif len(listTime[0]) == 3:
                t1 = int(listTime[0][0])
                
            if len(listTime[1]) == 4:
                t2 = (int(listTime[1][0]))*10 + int(listTime[1][1])
            elif len(listTime[1]) == 3:
                t2 = int(listTime[1][0])
                
        else:
            if len(listTime[0]) == 5:
                t1 = (int(listTime[0][0]))*10 + int(listTime[0][1])
            elif len(listTime[0]) == 4:
                t1 = int(listTime[0][0])
                
            if len(listTime[1]) == 5:
                t2 = (int(listTime[1][0]))*10 + int(listTime[1][1])
            elif len(listTime[1]) == 4:
                t2 = int(listTime[1][0])
            
        
    
        diff = 0
        if t1>12:
            diff = t1 - 12
            if t1<=9:
                str1 = str1 + '0' + str(diff) + "PM"
            else:
                str1 = str1 + str(t1-12) + "PM"
        elif t1 == 0 :
            str1 = str1 + "12PM"
        else:
            if t1<=9:
                 str1 = str1 + '0' + str(t1) + "AM"
            else:
                str1 = str1 + str(t1) + "AM"
                
 
            
            
        str1 = str1 + "-"
        if t2>12:
            diff = t2 -12
            if t1<=9:
                str1 = str1 + '0' + str(diff) + "PM"
            else:
                str1 = str1 + str(t2-12) + "PM"
        elif t2 == 0 :
            str1 = str1 + "12PM"
        else:
            if t2<=9:
                str1 = str1 + '0' + str(t2) + "AM"
            else:
                str1 = str1 + str(t2) + "AM"
                
        return str1
        
    else:
        listTime = time.split("-")
        str1 = ""
        if len(listTime[0]) == 3:
            str1 = str1 + '0' + listTime[0]
        else:
            str1 = str1 + listTime[0]
            
        str1 = str1 + "-"
        if len(listTime[1]) == 3:
            str1 = str1 + '0' + listTime[1]
        else:
            str1 = str1 + listTime[1]
       
        return str1
        
        
            
        
def resultantTable(city,countCity):
    result = '''<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#0066cc\"  style=\"color:#ffffff\"> 
    <td colspan = 13 width = \"1600\"><b><center> {0} </center></b></td>
    </tr>
    <tr bgcolor=\"#4da6ff\" colspan = 13  style=\"color:#ffffff\"> 
    <td width = \"1600\"><b><center> Time Of Follow Up  </center></b></td>
    <td width = \"1600\"><b><center> Follow Up Count Of System OCQ	 </center></b></td>
    <td width = \"1600\"><b><center> Leads In Confirmation Pending 	 </center></b></td>
    <td width = \"1600\"><b><center> Count Of Leads In MO Bucket	 </center></b></td>
    <td width = \"1600\"><b><center> MO Requires In The Hour</center></b></td>
    </tr>'''.format(city)
    
    
    
    timeList = ["12AM","01AM","02AM","03AM","04AM","05AM","06AM","07AM","08AM","09AM","10AM","11AM","12PM","01PM","02PM","03PM","04PM",
    "05PM","06PM","07PM","08PM","09PM","10PM","11PM","None",None]
    
    for time in timeList:
        for key,value in countCity.items():
            splitKey = key.split("-")
            if time == splitKey[0]:
                result = result + '''<tr  colspan = 13> 
                <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>{0}</center></b></td>
                <td width = \"1600\"><center>{1}</center></td>
                <td width = \"1600\"><center>{2}</center></td>
                <td width = \"1600\"><center>{3}</center></td>
                <td width = \"1600\"><center>{4}</center></td>
                </tr>'''.format(key,value['follow up count of system OCQ'],value['lead in confirmation pending'],
                value['count of leads in MO bucket'],value['MO requires in the hour'])
                
    
    # for summation of each column
    total = {'Total follow up count of system OCQ': 0,'Total lead in confirmation pending':0,
    'Total count of leads in MO bucket': 0,'Total MO requires in the hour': 0}
    
    for key,value in countCity.items():
        print(key,"  ",value['follow up count of system OCQ'])
        total['Total follow up count of system OCQ'] = total['Total follow up count of system OCQ'] + value['follow up count of system OCQ']
        total['Total lead in confirmation pending'] = total['Total lead in confirmation pending'] + value['lead in confirmation pending']
        total['Total count of leads in MO bucket'] = total['Total count of leads in MO bucket'] + value['count of leads in MO bucket']
        total['Total MO requires in the hour'] = total['Total MO requires in the hour'] + value['MO requires in the hour']
        total['Total MO requires in the hour'] = round(total['Total MO requires in the hour'],2)
        print()
        print("total= ",total['Total follow up count of system OCQ'])
        
    result = result + '''<tr  colspan = 13> 
                <td width = \"1600\" bgcolor=\"#4da6ff\"><b><center>{0}</center></b></td>
                <td width = \"1600\"><center>{1}</center></td>
                <td width = \"1600\"><center>{2}</center></td>
                <td width = \"1600\"><center>{3}</center></td>
                <td width = \"1600\"><center>{4}</center></td>
                </tr>'''.format("Total",total['Total follow up count of system OCQ'],total['Total lead in confirmation pending'],
                total['Total count of leads in MO bucket'],total['Total MO requires in the hour'])
                
    result = result + '''</table><br><br>'''
    
        
    print()
   
    print("city")
        
    
                
                
    return result
    
    


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        dataSets = fetch_data(read_connection_obj,1,"")
        systemOCQ = pd.DataFrame(dataSets)
        systemOCQ['time'] = systemOCQ.apply(lambda x : getTime(str(x['followUpTime'])),axis = 1) 
        
        
        
        recordsForJaipur = systemOCQ[systemOCQ['cityName'] == 'Jaipur']
        recordsForNCR = systemOCQ[systemOCQ['cityName'] == 'NCR']
        recordsForJaipur.to_csv(fpath)
        recordsForNCR.to_csv(fpath1)
        systemOCQ.to_csv(fpath2)
        jaipurData = fetchCityData(recordsForJaipur)
        NcrData = fetchCityData(recordsForNCR)
        resultant = resultantTable('Jaipur',jaipurData)
        resultant = resultant + resultantTable('NCR',NcrData)
        

        
        Subject = "Next Day System-OCQ lead Count | {0}".format(str(yesterday)) 
        email_recipient_list = ['tls@ayu.health']
        #email_recipient_list = ['ankur@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath,fpath1,fpath2])  
        
       
       
    except Exception as e:
        raise e
    
    
    
    
   
    