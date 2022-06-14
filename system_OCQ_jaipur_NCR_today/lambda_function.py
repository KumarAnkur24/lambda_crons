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

# description : system OCQ for today,MO for systemOCQ@ayu.health
# author : NISHA DAS

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

yesterday = datetime.now() + timedelta(hours=5,minutes=30) 
yesterday = yesterday.date()

fpath = os.path.join("/tmp","systemOCQJaipur.csv")
fpath1 = os.path.join("/tmp","systemOCQNCR.csv")
fpath2 = os.path.join("/tmp","enqueData.csv")
fpath3 = os.path.join("/tmp","queque_size.csv")

# 3 jaipur 4 ncr 




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
        
query3 = '''select * from agent_source_mapping  where cityId in (3,4) and source = "OUTBOUND_LOW_EXOTEL_TEAM"'''
query4 = '''select * from outbound_call_queue where status='ENQUEUED' and cityId in (3,4) 
          and date(scheduledOn) = curdate() '''
           
           
           
query2 = '''select email,name,cityId from customer_support_details'''

query5 = '''select cityId, count(1) as queque_size from outbound_call_queue where status='ENQUEUED' and 
scheduledOn < (now() + interval 5 hour) + interval 30 minute group by 1'''
# cityName is in ayu_cities
           
#what is city id


def fetch_data(conn,queryNo,value):
    fetched_val = {}
    if queryNo == 1:
        fetched_val = fetch_record(conn, query.format(yst = yesterday))
    if queryNo == 2:
        fetched_val = fetch_record(conn,query2)
    if queryNo == 3:
        fetched_val = fetch_record(conn,query3)
    if queryNo == 4:
        fetched_val = fetch_record(conn,query4)
    if queryNo == 5:
        fetched_val = fetch_record(conn,query5)
    
    return fetched_val 
    
    
def fetchCityData(cityDataframe,dictionary):
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
        resultCount[cityDataframe['time'][value]]['Scheduled Time'] = 0
        
    for value in cityDataframe.index:
        if cityDataframe['time'][value] in dictionary.keys():
            resultCount[cityDataframe['time'][value]]['Scheduled Time'] = dictionary[cityDataframe['time'][value]]
            
        
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
            
        
    
        # diff = 0
        # if t1>=12:
        #     diff = t1 - 12
        #     if t1<=9:
        #         str1 = str1 + '0' + str(diff) + "PM"
        #     elif t1 == 12:
        #         str1 = str1 + str(t1) + "PM"
        #     else:
        #         str1 = str1 + str(t1-12) + "PM"
        # elif t1 == 0 :
        #     str1 = str1 + "12AM"
        # else:
        #     if t1<=9:
        #          str1 = str1 + '0' + str(t1) + "AM"
        #     else:
        #         str1 = str1 + str(t1) + "AM"
        
        # # if(str1 == '12PM'):
        # #     print("time1 = ",t1)
        # #     print("time2 = ",t2)
                
 
            
            
        # str1 = str1 + "-"
        # if t2>=12:
        #     diff = t2 -12
        #     if t2<=9:
        #         str1 = str1 + '0' + str(diff) + "PM"
        #     elif t2 == 12:
        #         str1 = str1 + str(t2) + "PM"
        #     else:
        #         str1 = str1 + str(t2-12) + "PM"
        # elif t2 == 0 :
        #     str1 = str1 + "12AM"
        # else:
        #     if t2<=9:
        #         str1 = str1 + '0' + str(t2) + "AM"
        #     else:
        #         str1 = str1 + str(t2) + "AM"
                
        # if str1.find("12PM-1PM") != -1:
        #     print("yes")
        #     print(t1)
        #     print(t2)
        # # if(t1 == 0):
        # #     print("time format =",str1)
        # return str1
        
        
        diff = 0
        if t1>=12:
            diff = t1 - 12
            if t1<=9:
                str1 = str1  + str(diff) + "PM"
            elif t1 == 12:
                str1 = str1 + str(t1) + "PM"
            else:
                str1 = str1 + str(t1-12) + "PM"
        elif t1 == 0 :
            str1 = str1 + "12AM"
        else:
            if t1<=9:
                 str1 = str1 + str(t1) + "AM"
            else:
                str1 = str1 + str(t1) + "AM"
        
        # if(str1 == '12PM'):
        #     print("time1 = ",t1)
        #     print("time2 = ",t2)
                
 
            
            
        str1 = str1 + "-"
        if t2>=12:
            diff = t2 -12
            if t2<=9:
                str1 = str1 +  str(diff) + "PM"
            elif t2 == 12:
                str1 = str1 + str(t2) + "PM"
            else:
                str1 = str1 + str(t2-12) + "PM"
        elif t2 == 0 :
            str1 = str1 + "12AM"
        else:
            if t2<=9:
                str1 = str1 +  str(t2) + "AM"
            else:
                str1 = str1 + str(t2) + "AM"
                
        
        # if(t1 == 0):
        #     print("time format =",str1)
        return str1
        
    
        
    # else:
    #     listTime = time.split("-")
    #     str1 = ""
    #     if len(listTime[0]) == 3:
    #         str1 = str1 + '0' + listTime[0]
    #     else:
    #         str1 = str1 + listTime[0]
            
    #     str1 = str1 + "-"
    #     if len(listTime[1]) == 3:
    #         str1 = str1 + '0' + listTime[1]
    #     else:
    #         str1 = str1 + listTime[1]
            
    else:
        listTime = time.split("-")
        str1 = ""
        if len(listTime[0]) == 3:
            str1 = str1 +  listTime[0]
        else:
            str1 = str1 + listTime[0]
            
        str1 = str1 + "-"
        if len(listTime[1]) == 3:
            str1 = str1 +  listTime[1]
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
    <td width = \"1600\"><b><center> Enque</center></b></td>
    </tr>'''.format(city)
    
    
    
    timeList = ["12AM","1AM","2AM","3AM","4AM","5AM","6AM","7AM","8AM","9AM","10AM","11AM","12PM","1PM","2PM","3PM","4PM",
    "5PM","6PM","7PM","8PM","9PM","10PM","11PM","None",None] 
    
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
                <td width = \"1600\"><center>{5}</center></td>
                </tr>'''.format(key,value['follow up count of system OCQ'],value['lead in confirmation pending'],
                value['count of leads in MO bucket'],value['MO requires in the hour'],value['Scheduled Time'])
                
    
    # for summation of each column
    total = {'Total follow up count of system OCQ': 0,'Total lead in confirmation pending':0,
    'Total count of leads in MO bucket': 0,'Total MO requires in the hour': 0,'Total Scheduled': 0}
    
    for key,value in countCity.items():
        total['Total follow up count of system OCQ'] = total['Total follow up count of system OCQ'] + value['follow up count of system OCQ']
        total['Total lead in confirmation pending'] = total['Total lead in confirmation pending'] + value['lead in confirmation pending']
        total['Total count of leads in MO bucket'] = total['Total count of leads in MO bucket'] + value['count of leads in MO bucket']
        total['Total MO requires in the hour'] = total['Total MO requires in the hour'] + value['MO requires in the hour']
        total['Total MO requires in the hour'] = round(total['Total MO requires in the hour'],2)
        total['Total Scheduled'] = total['Total Scheduled'] + value['Scheduled Time']
        
        
    result = result + '''<tr  colspan = 13> 
                <td width = \"1600\" bgcolor=\"#4da6ff\"><b><center>{0}</center></b></td>
                <td width = \"1600\"><center>{1}</center></td>
                <td width = \"1600\"><center>{2}</center></td>
                <td width = \"1600\"><center>{3}</center></td>
                <td width = \"1600\"><center>{4}</center></td>
                <td width = \"1600\"><center>{5}</center></td>
                </tr>'''.format("Total",total['Total follow up count of system OCQ'],total['Total lead in confirmation pending'],
                total['Total count of leads in MO bucket'],total['Total MO requires in the hour'],total['Total Scheduled'])
                
    result = result + '''</table><br><br>'''
    
                
    return result
    
    
def resultantTable_agent(agent_dictionary):
    result = '''<table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#4da6ff\" style=\"color:#ffffff\"> 
    <td width = \"200\"><b><center> City  </center></b></td>
    <td width = \"200\"><b><center>Agents Available</center></b></td>
    <td width = \"200\"><b><center> # Agents Not From the same City </center></b></td>
    <td width = \"200\"><b><center> Agents Not From the same City </center></b></td>
    <td width = \"200\"><b><center> Current Queque Size </center></b></td>
    
    </tr>'''
    
    for key,value in agent_dictionary.items():
        result = result + '''<tr> 
                <td width = \"200\"><center>{0}</center></td>
                <td width = \"200\"><center>{1}</center></td>
                <td width = \"200\"><center>{2}</center></td>
                <td width = \"200\"><center>{3}</center></td>
                <td width = \"200\"><center>{4}</center></td>
                </tr>'''.format(key,value['Agents Available'],value['Agents Not Available'], value['Other City Agents'] , value['Queque Size'])
    result = result + '''</table><br><br>'''
    
    return result
        
    
    
def get_schedule_call_count(schedule_city):
    scheduled_count = {}
    for key,value in schedule_city.iterrows(): 
        time2 = value['scheduledOn'] + timedelta(hours=1)
        hour = '{0}-{1}'.format(value['scheduledOn'].strftime('%-I%p'),time2.strftime('%-I%p'))
        print(hour)
        
        if hour not in scheduled_count.keys():
            scheduled_count[hour] = 0
            
        scheduled_count[hour] += 1
        
    return scheduled_count


def getScheduleTime(timevalue,dictionary):
    if timevalue in dictionary.keys():
        return dictionary[timevalue]
    else:
        return 0
        
def getAgent(agent,cs_details,queque_size):
    agent_dictionary = {'Jaipur':{'Agents Available':0,'Agents Not Available':0,'Queque Size':0,'Other City Agents': []},
        'NCR':{'Agents Available':0,'Agents Not Available':0,'Queque Size':0,'Other City Agents': []}}
    

    # for jaipur
    agent_jaipur = agent[agent['cityId'] == 3]
    cs_agent_jaipur = cs_details
    agent_jaipur_list = agent_jaipur['emailId'][0].split(',')
    agent_dictionary['Jaipur']['Agents Available'] = len(agent_jaipur_list)
    value = cs_agent_jaipur[cs_agent_jaipur['email'].isin(agent_jaipur_list)]
    value = value[value['cityId'] != 3]
    agents_other_city = [x['email'] for index,x in value.iterrows()]
    agent_dictionary['Jaipur']['Agents Not Available'] = len(value)
    size = queque_size[queque_size['cityId'] == 3]
    agent_dictionary['Jaipur']['Queque Size'] = list(size['queque_size'])[0]
    agent_dictionary['Jaipur']['Other City Agents'] = ",".join(agents_other_city)
    
    # for ncr
    agent_ncr = agent[agent['cityId'] == 4]
    cs_agent_ncr = cs_details
    agent_ncr_list = agent_ncr['emailId'][1].split(',')
    agent_dictionary['NCR']['Agents Available'] = len(agent_ncr_list)
    value = cs_agent_ncr[cs_agent_ncr['email'].isin(agent_ncr_list)]
    value = value[value['cityId'] != 4]
    agents_other_city = [x['email'] for index,x in value.iterrows()]
    agent_dictionary['NCR']['Agents Not Available'] = len(value)
    size = queque_size[queque_size['cityId'] == 4]
    agent_dictionary['NCR']['Queque Size'] = list(size['queque_size'])[0]
    agent_dictionary['NCR']['Other City Agents'] = ",".join(agents_other_city)
    
    return agent_dictionary
    
    
    
    
        

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        dataSets = fetch_data(read_connection_obj,1,"")
        systemOCQ = pd.DataFrame(dataSets)
        systemOCQ['time'] = systemOCQ.apply(lambda x : getTime(str(x['followUpTime'])),axis = 1) 
        
        
        
        dataSets = fetch_data(read_connection_obj,4,"")
        scheduled_call = pd.DataFrame(dataSets)
        scheduled_call_jaipur = scheduled_call[scheduled_call['cityId'] == 3]
        scheduled_call_ncr = scheduled_call[scheduled_call['cityId'] == 4]
        scheduled_call_count_jaipur = get_schedule_call_count(scheduled_call_jaipur)
        scheduled_call_count_ncr = get_schedule_call_count(scheduled_call_ncr)
        
        
        
    
        recordsForJaipur = systemOCQ[systemOCQ['cityName'] == 'Jaipur']
        recordsForNCR = systemOCQ[systemOCQ['cityName'] == 'NCR']
        
        
        recordsForJaipur.to_csv(fpath)
        recordsForNCR.to_csv(fpath1)
        scheduled_call.to_csv(fpath2)
        jaipurData = fetchCityData(recordsForJaipur,scheduled_call_count_jaipur)
        NcrData = fetchCityData(recordsForNCR,scheduled_call_count_ncr)
        resultant = resultantTable('Jaipur',jaipurData)
        resultant = resultant + resultantTable('NCR',NcrData)
        
        dataSets = fetch_data(read_connection_obj,3,"")
        agent = pd.DataFrame(dataSets)
        
        dataSets = fetch_data(read_connection_obj,2,"")
        cs_details = pd.DataFrame(dataSets)
        
        dataSets = fetch_data(read_connection_obj,5,"")
        queque_size = pd.DataFrame(dataSets)
        queque_size.to_csv(fpath3)
        print(queque_size)
        print(queque_size['queque_size'][queque_size['cityId'] == 3])
        
        agent_dictionary = getAgent(agent,cs_details,queque_size)
        
        resultant = resultant + resultantTable_agent(agent_dictionary)
        
        

        
        Subject = "System-OCQ lead Count | {0}".format(str(yesterday)) 
        email_recipient_list = ['tls@ayu.health','arjit@ayu.health','yatharth@ayu.health','neha@ayu.health','shashank@ayu.health',
        'anshul@ayu.health','karan@ayu.health','ankur@ayu.health']
        #email_recipient_list = ['ankur@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath,fpath1,fpath2,fpath3])  
        
       
       
    except Exception as e:
        Subject = "System-OCQ lead Count | {0}".format(str(yesterday)) 
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',None,None)  
        raise e
    
    
    
'''
agent_source_mapping
Index(['id', 'emailId', 'source', 'createdOn', 'cityId', 'updatedOn'], dtype='object')
'''

'''
outbound_call_queue
Index(['queueId', 'entityId', 'entityType', 'patientNumber', 'callFlowType',
'status', 'cityId', 'languages', 'callSid', 'createdOn', 'updatedOn',
'priority', 'scheduledOn', 'additionalDetails'],
dtype='object')
'''
   
'''
customer_support_details
Index(['customerSupportId', 'name', 'email', 'phoneNumber', 'gender', 'token',
'isAvailable', 'createdOn', 'updatedOn', 'hasLeft', 'cityId',
'properties', 'isTestUserAccount'],
dtype='object')
'''
    