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

# description - conversion of total leads which are of diagnostic type or are coming from the diagnostics campaign (SH_Search_Diagnostics_BLR)
# author - Nisha Das

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

yesterday = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days=1)
week1 = yesterday - timedelta(days = 7)
week2 = week1 - timedelta(days = 7)
week3 = week2 - timedelta(days = 7)
week4 = week3 - timedelta(days = 7)
yesterday = yesterday.date()
week1 = week1.date()
week2 = week2.date()
week3 = week3.date()
week4 = week4.date()

yest = datetime.now() + timedelta(hours=5,minutes=30, days=-1)
yest = yest.strftime('%Y-%m-%d')

fpath = os.path.join("/tmp","diagnostic.csv")
fpath1 = os.path.join("/tmp","channel.csv")

query = """select caseId,patientLeadStatus,followUpdate,followUpTime,patientId, 
         date(date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE)) as createdOn , 
         caseType,reason from patient_case pc where caseType = 'Diagnostics' and 
         leadSource not in ('Offline channel','AYU_CAMP') and 
         date(date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE)) >= '{start}' 
         and date(date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE)) <= '{end}' and pc.tenantName='AYU' """ 
         
        
    # Diagnostics 
    # leadSource = Offline channel and consultation 
example = """select leadId,user from lead_comments lc join ayu_personnel_details csd 
          on (csd.email = lc.user and personnelType='AYU_MITRA') 
          where commentType = 'CREATION' and leadId in ({caseId}) and leadType = 'CASE'"""
          

         
query2 = """select pc.patientId,ldc.leadId,ldc.doctorConsultationStatus,ldc.consultationType, 
          date(ldc.appointmentDate) as date, 
          ldc.appointmentCreationType,  
          case when dd.name is null then 'N.A' 
          else dd.name 
          end as testName 
          from lead_doctor_consultation ldc  
          join patient_case pc on ldc.leadId = pc.caseId 
          left join appointment_treatment_details atd on ldc.id = atd.appointmentId 
          left join diagnostics_details dd on atd.treatmentId = dd.id 
          where ldc.doctorConsultationStatus = 3 and pc.leadSource not in ('Offline channel','AYU_CAMP') and 
          pc.caseType = 'Diagnostics' and date(ldc.appointmentDate) >= '{start}' 
          and date(ldc.appointmentDate) <= '{end}' and 
          ldc.appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT') 
          and pc.tenantName='AYU'"""
          

         
channel = """select pmcs.patientProfileId as patientId,
             ldc.leadId,ldc.doctorConsultationStatus,ldc.consultationType,
             date(date_add(pc.createdOn,INTERVAL '5:30' HOUR_MINUTE)) as CreatedOn, 
             channelName,pc.caseType,pmcs.patientLeadSource,pc.patientLeadStatus 
             from marketing_channels mc 
             join patient_marketing_channel_stats pmcs on mc.id = pmcs.marketingChannelId
             join patient_case pc on pc.patientId = pmcs.patientProfileId
             left join lead_doctor_consultation ldc on ldc.leadId = pc.caseId 
             where date(date_add(pc.createdOn,INTERVAL '5:30' HOUR_MINUTE)) >= '{start}' and 
             date(date_add(pc.createdOn,INTERVAL '5:30' HOUR_MINUTE))  <= '{end}'
          """  

         
# query3 = "select * from information_schema.columns where table_name = 'patient_case'"




def fetch_data(conn,queryNo,value):
    
    
    fetched_val = {}
    if queryNo == 1:
        fetched_val = fetch_record(conn, query.format(start = week4 , end = yesterday))
    if queryNo == 2:
        fetched_val = fetch_record(conn,query2.format(start = week4 , end = yesterday))
    if queryNo == 3:
        fetched_val = fetch_record(conn,example.format(caseId = ','.join(value)))
    if queryNo == 4:
        fetched_val = fetch_record(conn,channel.format(start = week4, end = yesterday ))
    

    return fetched_val 
    

def getCsUser(leadId,uniqueLeadId):
    if leadId in uniqueLeadId:
        return "no"
    else:
        return "yes"
        
def testDoneCount(diagnosticsDone):
    value = pd.DataFrame(diagnosticsDone['testName'])
    value = value.dropna()
    value['count'] = value.groupby('testName')['testName'].transform('count')
    
    value.drop_duplicates(inplace = True)
    return value


#clubing month and yesterday test count wrt test.    
def mergeDictionary(d1,choice):
    if choice == 1:
        mapColumn = 'reason'
    elif choice == 2:
        mapColumn = 'testName'
        
    
    columnNames = [mapColumn,'weeklyCount']
    newDict = pd.DataFrame(columns = columnNames)
    
    for key in d1.index:
        newDict = newDict.append({mapColumn: d1[mapColumn][key], 'weeklyCount': d1['count'][key]},ignore_index = True) 
 
    return newDict
    
    
def diagnostics_count(diagnostics,appointmentDone):
    count = {}
    count['confirmed'] = diagnostics[diagnostics['patientLeadStatus'] == '7']['createdOn'].count()
    count['booked'] = diagnostics[diagnostics['patientLeadStatus'] == '2']['createdOn'].count()
    count['cancelled'] = diagnostics[diagnostics['patientLeadStatus'] == '3']['createdOn'].count()
    count['total_leads_created'] = diagnostics['createdOn'].count()
    count['Diagnostics Appointment Done'] = appointmentDone
    count['conversion %'] = round((count['Diagnostics Appointment Done'] * 100)/(count['total_leads_created']),2)
    return count   
    
    
    
def resultantTable_channel(channel):
    result = f"""<table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#e68a00\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td colspan = 1 width = \"1600\"><b><center>   </center></b></td>
    <td colspan = 3 width = \"1600\"><b><center>Week1</center></b></td>
    <td colspan = 3 width = \"1600\"><b><center>Week2</center></b></td>
    <td colspan = 3 width = \"1600\"><b><center>Week3</center></b></td>
    <td colspan = 3 width = \"1600\"><b><center>Week4</center></b></td>
    </tr>
    
    <tr bgcolor=\"#ffb84d\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td width = \"1600\"><b><center>Channel Name</center></b></td>
    <td width = \"1600\"><b><center>Leads</center></b></td>
    <td width = \"1600\"><b><center>% Done</center></b></td>
    <td width = \"1600\"><b><center>% Contribution</center></b></td>
    <td width = \"1600\"><b><center>Leads</center></b></td>
    <td width = \"1600\"><b><center>% Done</center></b></td>
    <td width = \"1600\"><b><center>% Contribution</center></b></td>
    <td width = \"1600\"><b><center>Leads</center></b></td>
    <td width = \"1600\"><b><center>% Done</center></b></td>
    <td width = \"1600\"><b><center>% Contribution</center></b></td>
    <td width = \"1600\"><b><center>Leads</center></b></td>
    <td width = \"1600\"><b><center>% Done</center></b></td>
    <td width = \"1600\"><b><center>% Contribution</center></b></td>
    </tr>"""
    
    
    
    for key,value in channel.items():
        total_week1 = value['week1']['Confirmed'] + value['week1']['Booked'] + value['week1']['Cancelled'] + value['week1']['Open']
        total_week2 = value['week2']['Confirmed'] + value['week2']['Booked'] + value['week2']['Cancelled'] + value['week2']['Open']
        total_week3 = value['week3']['Confirmed'] + value['week3']['Booked'] + value['week3']['Cancelled'] + value['week3']['Open']
        total_week4 = value['week4']['Confirmed'] + value['week4']['Booked'] + value['week4']['Cancelled'] + value['week4']['Open']
        
        total_done = 0
        total_done = total_done+value['week1']['done %']+value['week2']['done %']+value['week3']['done %']+value['week4']['done %']
        if total_done == 0:
            contribution_week1 = 0.0
            contribution_week2 = 0.0
            contribution_week3 = 0.0
            contribution_week4 = 0.0
        else:
            contribution_week1 = round((value['week1']['done %']*100)/total_done,2)
            contribution_week2 = round((value['week2']['done %']*100)/total_done,2)
            contribution_week3 = round((value['week3']['done %']*100)/total_done,2)
            contribution_week4 = round((value['week4']['done %']*100)/total_done,2)
            
            
        
        
        result = result + f"""<tr  colspan = 13> 
        <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>{key}</center></b></td>
        <td width = \"1600\"><center>{total_week1}</center></td>
        <td width = \"1600\"><center>{value['week1']['done %']}</center></td>
        <td width = \"1600\"><center>{contribution_week1}</center></td>
        <td width = \"1600\"><center>{total_week2}</center></td>
        <td width = \"1600\"><center>{value['week2']['done %']}</center></td>
        <td width = \"1600\"><center>{contribution_week2}</center></td>
        <td width = \"1600\"><center>{total_week3}</center></td>
        <td width = \"1600\"><center>{value['week3']['done %']}</center></td>
        <td width = \"1600\"><center>{contribution_week3}</center></td>
        <td width = \"1600\"><center>{total_week4}</center></td>
        <td width = \"1600\"><center>{value['week4']['done %']}</center></td>
        <td width = \"1600\"><center>{contribution_week4}</center></td>
        </tr>"""
        
    result = result + '''</table><br><br>'''
    
    return result
    



def resultantTable(week1,week2,week3,week4):
    result = f"""<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#8a00e6\" style=\"color:#ffffff\" height=\"30px\" colspan = 13> 
    <td colspan = 13 width = \"1600\"><b><center>Overall Summary</center></b></td>
    </tr>
    <tr bgcolor=\"#cc80ff\"  style=\"color:#ffffff\" height=\"30px\" colspan = 13> 
    <td width = \"1600\"><b><center>     </center></b></td>
    <td width = \"1600\"><b><center>Week1</center></b></td>
    <td width = \"1600\"><b><center>Week2</center></b></td>
    <td width = \"1600\"><b><center>Week3</center></b></td>
    <td width = \"1600\"><b><center>Week4</center></b></td>
    </tr>
    <tr> 
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Diagnostic Confirmed</center></b></td>
    <td colspan = 1 ><center>{week1['confirmed']}</center></td>
    <td colspan = 1 ><center>{week2['confirmed']}</center></td>
    <td colspan = 1 ><center>{week3['confirmed']}</center></td>
    <td colspan = 1 ><center>{week4['confirmed']}</center></td>
    </tr>
    
    <tr > 
    <td  bgcolor=\"#d9d9d9\"><b><center>Diagnostic Booked </center></b></td>
    <td colspan = 1 ><center>{week1['booked']}</center></td>
    <td colspan = 1 ><center>{week2['booked']}</center></td>
    <td colspan = 1 ><center>{week3['booked']}</center></td>
    <td colspan = 1 ><center>{week4['booked']}</center></td>
    </tr>
    
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Cancelled</center></b></td>
    <td colspan = 1 ><center>{week1['cancelled']}</center></td>
    <td colspan = 1 ><center>{week2['cancelled']}</center></td>
    <td colspan = 1 ><center>{week3['cancelled']}</center></td>
    <td colspan = 1 ><center>{week4['cancelled']}</center></td>
    </tr>
    
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Diagnostics Appointment Done </center></b></td>
    <td colspan = 1 ><center>{week1['Diagnostics Appointment Done']}</center></td>
    <td colspan = 1 ><center>{week2['Diagnostics Appointment Done']}</center></td>
    <td colspan = 1 ><center>{week3['Diagnostics Appointment Done']}</center></td>
    <td colspan = 1 ><center>{week4['Diagnostics Appointment Done']}</center></td>
    </tr>
    
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Total Leads Created</center></b></td>
    <td colspan = 1><center>{week1['total_leads_created']}</center></td>
    <td colspan = 1><center>{week2['total_leads_created']}</center></td>
    <td colspan = 1><center>{week3['total_leads_created']}</center></td>
    <td colspan = 1><center>{week4['total_leads_created']}</center></td>
    </tr>
    
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Conversion %</center></b></td>
    <td colspan = 1><center>{week1['conversion %']}</center></td>
    <td colspan = 1><center>{week2['conversion %']}</center></td>
    <td colspan = 1><center>{week3['conversion %']}</center></td>
    <td colspan = 1><center>{week4['conversion %']}</center></td>
    </tr>
    
    </table><br><br>
    """
    
    return result
    
    
def resultantTableSH_Search(SH_search_dict):
    result = f""" <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#29a329\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td width = \"1600\"><b><center>   </center></b></td>
    <td width = \"1600\"><b><center>Leads</center></b></td>
    <td width = \"1600\"><b><center>Diagnostics Lead</center></b></td>
    <td width = \"1600\"><b><center>Diagnostics Appointment</center></b></td>
    <td width = \"1600\"><b><center>Consultations after Diagnostics App</center></b></td>
    </tr>
    
    <tr  colspan = 13> 
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>SH_Search_Diagnostics_BLR</center></b></td>
    <td width = \"1600\"><center>{SH_search_dict['leads']}</center></td>
    <td width = \"1600\"><center>{SH_search_dict['Diagnostics lead']}</center></td>
    <td width = \"1600\"><center>{SH_search_dict['Diagnostics Appointment']}</center></td>
    <td width = \"1600\"><center>{SH_search_dict['Consultations after Diagnostics App']}</center></td>
    </tr>
    </table><br><br>"""
    return result
    
    
def getChannelsCount(channel):
    channel_dict = {'Base Search Google Adwords':{'week1':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week2':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week3':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week4':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0}},
                                                 
                    'bangalore GMB':{'week1':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week2':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week3':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week4':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0}},
                                                 
                    'SH_Search_Base_BLR base - hospital direct':{'week1':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week2':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week3':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week4':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0}},
                                                 
                    'SH_Search_Base_BLR base - hospital direct':{'week1':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week2':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week3':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week4':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0}},
                    
                    'SH_Search_Diagnostics_BLR':{'week1':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week2':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week3':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week4':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0}},
                                                 
                    'Others':{'week1':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week2':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week3':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0},
                                                 'week4':{'Confirmed':0,'Booked':0,'Cancelled':0,'Open':0,'done %':0}}
                
                }
                
    for key,value in channel.iterrows():
        # assign dictionary key
        status = ""
        channelName = ""
        if value['patientLeadStatus'] == '7':
            status = "Confirmed"
        elif value['patientLeadStatus'] == '2':
            status = "Booked"
        elif value['patientLeadStatus'] == '3':
            status = "Cancelled"
        elif value['patientLeadStatus'] in ['0','1','5']:
            status = "Open"
        
        #assigning channel name to variable 
        if value['channelName'] == 'Base Search Google Adwords':
            channelName = 'Base Search Google Adwords'
        elif value['channelName'] == 'bangalore GMB':
            channelName = 'bangalore GMB'
        elif value['channelName'] == 'SH_Search_Base_BLR base - hospital direct':
            channelName = 'SH_Search_Base_BLR base - hospital direct'
        elif "SH_Search_Diagnostics_BLR" in value['channelName']:
            channelName = 'SH_Search_Diagnostics_BLR'
        else:
            channelName = 'Others'
        
    
        #weekly segregation for 4 weeks 
        if value['CreatedOn'] > week1 and value['CreatedOn'] <= yesterday:
            channel_dict[channelName]['week1'][status] += 1
        elif value['CreatedOn'] > week2 and value['CreatedOn'] <= week1:
            channel_dict[channelName]['week2'][status] += 1
        elif value['CreatedOn'] > week3 and value['CreatedOn'] <= week2:
            channel_dict[channelName]['week3'][status] += 1
        elif value['CreatedOn'] > week4 and value['CreatedOn'] <= week3:
            channel_dict[channelName]['week4'][status] += 1
            
    for key,value in channel_dict.items():
        total = value['week1']['Confirmed']+value['week1']['Booked']+value['week1']['Cancelled']+value['week1']['Open']
        if total != 0:
            value['week1']['done %'] = round((value['week1']['Confirmed']*100)/total,2)
        else:
            value['week1']['done %'] = 0.0
            
        
        
        total = value['week2']['Confirmed']+value['week2']['Booked']+value['week2']['Cancelled']+value['week2']['Open']
        if total != 0:
            value['week2']['done %'] = round((value['week2']['Confirmed']*100)/total,2)
        else:
            value['week2']['done %'] = 0.0
            
        
        
        total = value['week3']['Confirmed']+value['week3']['Booked']+value['week3']['Cancelled']+value['week3']['Open']
        if total != 0:
            value['week3']['done %'] = round((value['week3']['Confirmed']*100)/total,2)
        else:
            value['week3']['done %'] = 0.0
            
        
        
        total = value['week4']['Confirmed']+value['week4']['Booked']+value['week4']['Cancelled']+value['week4']['Open']
        if total != 0:
            value['week4']['done %'] = round((value['week4']['Confirmed']*100)/total,2)
        else:
            value['week4']['done %'] = 0.0
            
    return channel_dict
    
    

def getSHSearch(channels):
    SH_search_dict = {'leads':0,'Diagnostics lead':0,'Diagnostics Appointment':0,'Consultations after Diagnostics App':0}
    SH_search_dict['leads'] = channels['patientId'].count();
    SH_search_dict['Diagnostics lead'] = channels[channels['caseType'] == 'Diagnostics']['patientId'].count()
    SH_search_dict['Diagnostics Appointment'] = channels[channels['consultationType'] == 'Diagnostics']['patientId'].count()
    SH_search_dict['Consultations after Diagnostics App'] = channels[channels['consultationType'] == 'Consultation']['patientId'].count()
    print(SH_search_dict)
    return SH_search_dict

def getDiagnosticCount(diagnostics,diagnosticsDone):
    
    # seperate diagnostics dataframe into 4 weeks
    mask = (diagnostics['createdOn'] > week1) & (diagnostics['createdOn'] <= yesterday)
    diagnostic_week1 = diagnostics.loc[mask]
    
    mask = (diagnostics['createdOn'] > week2) & (diagnostics['createdOn'] <= week1)
    diagnostic_week2 = diagnostics.loc[mask]
    
    
    mask = (diagnostics['createdOn'] > week3) & (diagnostics['createdOn'] <= week2)
    diagnostic_week3 = diagnostics.loc[mask]
    
    
    mask = (diagnostics['createdOn'] > week4) & (diagnostics['createdOn'] <= week3)
    diagnostic_week4 = diagnostics.loc[mask]
    
    # seperating diagnosticsdone dataframe into 4 weeks.
    mask = (diagnosticsDone['date'] > week1) & (diagnosticsDone['date'] <= yesterday)
    diagnosticDone_week1 = diagnosticsDone.loc[mask]
    testDone = testDoneCount(diagnosticDone_week1)
    newTestDone_week1 = mergeDictionary(testDone,2)
    
    mask = (diagnosticsDone['date'] > week2) & (diagnosticsDone['date'] <= week1)
    diagnosticDone_week2 = diagnosticsDone.loc[mask]
    testDone = testDoneCount(diagnosticDone_week2)
    newTestDone_week2 = mergeDictionary(testDone,2)
    
    mask = (diagnosticsDone['date'] > week3) & (diagnosticsDone['date'] <= week2)
    diagnosticDone_week3 = diagnosticsDone.loc[mask]
    testDone = testDoneCount(diagnosticDone_week3)
    newTestDone_week3 = mergeDictionary(testDone,2)
    
    mask = (diagnosticsDone['date'] > week4) & (diagnosticsDone['date'] <= week3)
    diagnosticDone_week4 = diagnosticsDone.loc[mask]
    testDone = testDoneCount(diagnosticDone_week4)
    newTestDone_week4 = mergeDictionary(testDone,2)
    
    tota_appointment_done = [newTestDone_week1['weeklyCount'].sum(),newTestDone_week2['weeklyCount'].sum(),
    newTestDone_week3['weeklyCount'].sum(),newTestDone_week4['weeklyCount'].sum()]
        
    count_week1 = diagnostics_count(diagnostic_week1,tota_appointment_done[0])
    count_week2 = diagnostics_count(diagnostic_week2,tota_appointment_done[1]) 
    count_week3 = diagnostics_count(diagnostic_week3,tota_appointment_done[2]) 
    count_week4 = diagnostics_count(diagnostic_week4,tota_appointment_done[3]) 
    
    resultant = resultantTable(count_week1,count_week2,count_week3,count_week4)
    return resultant
    
    
    
    
    
    
    
    



def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
        
        
        # --------------------1st summary-----------------
        dataSets = fetch_data(read_connection_obj,1,"")
        diagnostics = pd.DataFrame(dataSets)
        caseId = set((diagnostics['caseId'].astype(str)))
        
        # diagnostic done
        dataSets = fetch_data(read_connection_obj,2,"")
        data = pd.DataFrame(dataSets)
        diagnosticsDone = data
        leadId = set((diagnosticsDone['leadId']).astype(str))
        caseId.update(leadId)
        
        
        exampleQuery = fetch_data(read_connection_obj,3,caseId)
        exampleQuery1 = pd.DataFrame(exampleQuery)
        
        
        uniqueLeadId = set()
        if len(exampleQuery):
            uniqueLeadId = set(exampleQuery1['leadId'].astype(str))
        
        diagnosticsDone['isCsUser'] = diagnosticsDone.apply(lambda x : getCsUser(str(x['leadId']),uniqueLeadId),axis = 1)     
        diagnostics['isCsUser'] = diagnostics.apply(lambda x : getCsUser(str(x['caseId']),uniqueLeadId),axis = 1)
        diagnostics = diagnostics[diagnostics['isCsUser'] == "yes"]
        diagnosticsDone = diagnosticsDone[diagnosticsDone['isCsUser'] == 'yes']
        
        diagnostics.to_csv(fpath)
        
        
        resultant = getDiagnosticCount(diagnostics,diagnosticsDone)
        
        # ----------------------------------------------------------
        
        
        
        # -------------------2nd summary-------------------------
        dataSets = fetch_data(read_connection_obj,4,"")
        channels = pd.DataFrame(dataSets)
        channels.to_csv(fpath1)
        channels_count_dict = getChannelsCount(channels)
        resultant = resultant + resultantTable_channel(channels_count_dict)
        # # ------------------------------------------------------------------
        
        
        
        # # ----------------------3rd summary---------------------------
        channels = channels[channels['channelName'].str.contains("SH_Search_Diagnostics_BLR")]
        # channels = pd.merge(channels, data, on='patientId')  
        SH_Search_dict = getSHSearch(channels)
        resultant = resultant + resultantTableSH_Search(SH_Search_dict)
        
        
        
        
        
        Subject = "Diagnostics Report {0} |".format(str(yest)) 
        email_recipient_list = ['nisha@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath,fpath1])
        
    except Exception as e:
        Subject = 'diagnostic done channel segregation | Error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
        raise e
    
