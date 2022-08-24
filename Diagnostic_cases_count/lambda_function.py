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

yesterday = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days=1)
mtd = yesterday.replace(day=1)
yesterday = yesterday.date()
mtd = mtd.date()
yest = datetime.now() + timedelta(hours=5,minutes=30, days=-1)
yest = yest.strftime('%Y-%m-%d')

fpath = os.path.join("/tmp","diagnostic.csv")
fpath1 = os.path.join("/tmp","testDone.csv")
fpath2 = os.path.join("/tmp","followuptestDone.csv")

query = "select caseId,patientLeadStatus,followUpdate,followUpTime,patientId, \
         date(date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE)) as createdOn , \
         caseType,reason from patient_case pc where caseType = 'Diagnostics' and \
         leadSource not in ('Offline channel','AYU_CAMP') and date(date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE)) >= '{start}' \
         and date(date_add(createdOn,INTERVAL '5:30' HOUR_MINUTE)) <= '{end}' and pc.tenantName='AYU' " 
         
        
    # Diagnostics 
    # leadSource = Offline channel and consultation 
example = "select leadId,user from lead_comments lc join ayu_personnel_details csd \
          on (csd.email = lc.user and personnelType='AYU_MITRA') \
          where commentType = 'CREATION' and leadId in ({caseId}) and leadType = 'CASE'" 
          

         
# query2 = "select ldc.leadId,ldc.doctorConsultationStatus,ldc.consultationType, \
#           date(ldc.appointmentDate) as date, \
#           ldc.appointmentCreationType, dd.name as testName \
#           from lead_doctor_consultation ldc  \
#           left join patient_case pc on ldc.leadId = pc.caseId \
#           left join appointment_treatment_details atd on ldc.id = atd.appointmentId \
#           left join diagnostics_details dd on atd.treatmentId = dd.id \
#           where ldc.doctorConsultationStatus = 3 and pc.leadSource <> 'Offline channel' and \
#           pc.caseType = 'Diagnostics' and date(ldc.appointmentDate) >= '{start}' and date(ldc.appointmentDate) <= '{end}' and \
#           ldc.appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT','FOLLOWUP_APPOINTMENT')"
          #doctorConsultationStatus = 3 createdon yesterday appointmentCreationType = NEW_APPOINTMENT' 'RESCHEDULED_APPOINTMENT
          #leadsource offline channel which is present in patient_case and add casetype
          
          
        #   in followup condition creationtype != diagnostic
query2 = "select ldc.leadId,ldc.doctorConsultationStatus,ldc.consultationType, \
          date(ldc.appointmentDate) as date, \
          ldc.appointmentCreationType,  \
          case when dd.name is null then 'N.A' \
          else dd.name \
          end as testName \
          from lead_doctor_consultation ldc  \
          join patient_case pc on ldc.leadId = pc.caseId \
          left join appointment_treatment_details atd on ldc.id = atd.appointmentId \
          left join diagnostics_details dd on atd.treatmentId = dd.id \
          where ldc.doctorConsultationStatus = 3 and pc.leadSource not in ('Offline channel','AYU_CAMP') and \
          pc.caseType = 'Diagnostics' and date(ldc.appointmentDate) >= '{start}' and date(ldc.appointmentDate) <= '{end}' and \
          ldc.appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT','FOLLOWUP_APPOINTMENT') and pc.tenantName='AYU'"
         
# query3 = "select * from information_schema.columns where table_name = 'patient_case'"




def fetch_data(conn,queryNo,value):
    
    
    fetched_val = {}
    if queryNo == 1:
        fetched_val = fetch_record(conn, query.format(start = yesterday , end = yesterday))
    if queryNo == 2:
        fetched_val = fetch_record(conn,query2.format(start = yesterday , end = yesterday))
    if queryNo == 3:
        fetched_val = fetch_record(conn,example.format(caseId = ','.join(value)))
    if queryNo == 4:
        fetched_val = fetch_record(conn,query.format(start = mtd , end = yesterday))
    if queryNo == 5:
        fetched_val = fetch_record(conn,query2.format(start = mtd , end = yesterday))
        
    return fetched_val 
    
    
def diagnostics_count(diagnostics,appointmentDone,followUpDone):
    count = {}
    count['confirmed'] = diagnostics[diagnostics['patientLeadStatus'] == '7']['createdOn'].count()
    count['booked'] = diagnostics[diagnostics['patientLeadStatus'] == '2']['createdOn'].count()
    count['followUp'] = diagnostics[diagnostics['patientLeadStatus'].isin(['1','0','5'])]['patientLeadStatus'].count()
    count['cancelled'] = diagnostics[diagnostics['patientLeadStatus'] == '3']['createdOn'].count()
    count['total_leads_created'] = diagnostics['createdOn'].count()
    count['Diagnostics Appointment Done'] = appointmentDone
    count['FollowUp Appointment Done'] = followUpDone
    return count
    
def followUp(diagnostics):
    value = pd.DataFrame(diagnostics['reason'])
    followUpCount = pd.DataFrame(diagnostics[diagnostics['patientLeadStatus'].isin(['1','0','5'])]['reason'])
    followUpCount = followUpCount.dropna()
    followUpCount['count'] = followUpCount.groupby('reason')['reason'].transform('count')
    
    followUpCount.drop_duplicates(inplace = True)
    return followUpCount
    
    
def cancelledCount(diagnostics):
    cancelledCallCount = pd.DataFrame(diagnostics[diagnostics['patientLeadStatus'] == '3']['reason'])
    cancelledCallCount = cancelledCallCount.dropna()
    cancelledCallCount['count'] = cancelledCallCount.groupby('reason')['reason'].transform('count')
    
    cancelledCallCount.drop_duplicates(inplace = True)
    return cancelledCallCount
    
def testDoneCount(diagnosticsDone):
   
    value = pd.DataFrame(diagnosticsDone['testName'])
    value = value.dropna()
    value['count'] = value.groupby('testName')['testName'].transform('count')
    
    value.drop_duplicates(inplace = True)
    return value
    
    
def getCsUser(leadId,uniqueLeadId):
    if leadId in uniqueLeadId:
        return "no"
    else:
        return "yes"
    
    
    
def resultTestCount(testDone,result,choice):
    if choice == 1:
        title = "NEW_APPOINTMENT"
        color1 = '#0066cc'
        color2 = '#66a3ff'
    elif choice == 2:
        title = "FOLLOWUP_APPOINTMENT"
        color1 = '#993333'
        color2 = '#d98c8c'
    
    result = result + '''<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"{1}\" height=\"30px\" style=\"color:#ffffff\"> 
    <td colspan = 13 width = \"1600\"><b><center> Diagnostic Done ({0}) </center></b></td>
    </tr>
    <tr bgcolor=\"{2}\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td width = \"1600\"><b><center> Test Name  </center></b></td>
    <td width = \"1600\"><b><center> Count(yesterday) </center></b></td>
    <td width = \"1600\"><b><center> Count(month) </center></b></td>
    </tr>'''.format(title,color1,color2)  
    
    for value in testDone.index:
        result = result + '''<tr  colspan = 13> 
        <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>{0}</center></b></td>
        <td width = \"1600\"><center>{1}</center></td>
        <td width = \"1600\"><center>{2}</center></td>
        </tr>'''.format(testDone['testName'][value],testDone['yesterdayCount'][value],testDone['monthCount'][value])
        
    result = result + '''</table><br><br>'''
    return result
    
    
    
    
    #count,followUpDistribution,cancelled,countMonth,followUpCallDistributionMonth,cancelledMonth 
def resultantTable(count,countMonth,followUpCallDistribution,cancelledCall):
    result ='''<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#8a00e6\" style=\"color:#ffffff\" height=\"30px\" colspan = 13> 
    <td colspan = 13 width = \"1600\"><b><center>Overall Summary</center></b></td>
    </tr>
    <tr bgcolor=\"#cc80ff\"  style=\"color:#ffffff\" height=\"30px\" colspan = 13> 
    <td width = \"1600\"><b><center>Date</center></b></td>
    <td width = \"1600\"><b><center> {0} </center></b></td>
    <td width = \"1600\"><b><center> {1} </center></b></td>
    </tr>
    <tr> 
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Diagnostic Confirmed</center></b></td>
    <td colspan = 1 ><center> {2}  </center></td>
    <td colspan = 1 ><center> {3}  </center></td>
            
    </tr>
    <tr > 
    <td  bgcolor=\"#d9d9d9\"><b><center>Diagnostic Booked </center></b></td>
    <td colspan = 1 ><center> {4}  </center></td>
    <td colspan = 1 ><center> {5}  </center></td>
    </tr>
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Follow Up</center></b></td>
    <td colspan = 1 ><center> {6}  </center></td>
    <td colspan = 1 ><center> {7}  </center></td>
    </tr>
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Cancelled</center></b></td>
    <td colspan = 1 ><center> {8}  </center></td>
    <td colspan = 1 ><center> {9}  </center></td>
    </tr>
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Diagnostics Appointment Done </center></b></td>
    <td colspan = 1 ><center> {10}  </center></td>
    <td colspan = 1 ><center> {11}  </center></td>
    </tr>
    </tr>
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>FollowUp Appointment Done</center></b></td>
    <td colspan = 1 ><center> {12}  </center></td>
    <td colspan = 1 ><center> {13}  </center></td>
    </tr>
    <tr >
    <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>Total Leads Created</center></b></td>
    <td colspan = 1><center> {14}  </center></td>
    <td colspan = 1><center> {15}  </center></td>
    </tr></table><br><br>
    '''.format(yesterday,str(mtd) + str("  to  ") + str(yesterday),count['confirmed'],countMonth['confirmed'],
    count['booked'],countMonth['booked'],count['followUp'],countMonth['followUp'],count['cancelled'],
    countMonth['cancelled'],count['Diagnostics Appointment Done'],countMonth['Diagnostics Appointment Done'],
    count['FollowUp Appointment Done'],countMonth['FollowUp Appointment Done'],
    count['total_leads_created'],countMonth['total_leads_created'])
    
    
    result = result + '''<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#e68a00\" height=\"30px\" style=\"color:#ffffff\"> 
    <td colspan = 13 width = \"1600\"><b><center> FollowUp Call Distribution </center></b></td>
    </tr>
    <tr bgcolor=\"#ffb84d\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td width = \"1600\"><b><center> Reason  </center></b></td>
    <td width = \"1600\"><b><center> Count(yesterday) </center></b></td>
    <td width = \"1600\"><b><center> Count(month) </center></b></td>
    </tr>'''
    
    
    for value in followUpCallDistribution.index:
        result = result + '''<tr  colspan = 13> 
        <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>{0}</center></b></td>
        <td width = \"1600\"><center>{1}</center></td>
        <td width = \"1600\"><center>{2}</center></td>
        </tr>'''.format(followUpCallDistribution['reason'][value],followUpCallDistribution['yesterdayCount'][value],
        followUpCallDistribution['monthCount'][value])
        
    result = result + '''</table><br><br>'''
    
    
    
    result = result + '''<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor=\"#29a329\" height=\"30px\" style=\"color:#ffffff\"> 
    <td colspan = 13 width = \"1600\"><b><center> Cancellation </center></b></td>
    </tr>
    <tr bgcolor=\"#70db70\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td width = \"1600\"><b><center> Reason  </center></b></td>
    <td width = \"1600\"><b><center> Count(yesterday) </center></b></td>
    <td width = \"1600\"><b><center> Count(month) </center></b></td>
    </tr>'''
    
    for value in cancelledCall.index:
        result = result + '''<tr  colspan = 13> 
        <td width = \"1600\" bgcolor=\"#d9d9d9\"><b><center>{0}</center></b></td>
        <td width = \"1600\"><center>{1}</center></td>
        <td width = \"1600\"><center>{2}</center></td>
        </tr>'''.format(cancelledCall['reason'][value],cancelledCall['yesterdayCount'][value],cancelledCall['monthCount'][value])
        
    result = result + '''</table><br><br>'''
    return result
    
    
    


    
def mergeDictionary(d1,d2,choice):
    if choice == 1:
        mapColumn = 'reason'
    elif choice == 2:
        mapColumn = 'testName'
        
    
    columnNames = [mapColumn,'yesterdayCount','monthCount']
    newDict = pd.DataFrame(columns = columnNames)
    
    for key in d1.index:
        newDict = newDict.append({mapColumn: d1[mapColumn][key], 'yesterdayCount': d1['count'][key], 'monthCount': 0},ignore_index = True) 
   
    unique = newDict[mapColumn].values
    for key in d2.index: 
        if d2[mapColumn][key] in unique:
            value = list(newDict.index[newDict[mapColumn] == d2[mapColumn][key]])[0]
            newDict['monthCount'][value] = d2['count'][key]
        else:
            newDict = newDict.append({mapColumn: d2[mapColumn][key], 'yesterdayCount': 0, 'monthCount': d2['count'][key]},ignore_index = True)
    
    
    return newDict
            
    
        

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        dataSets = fetch_data(read_connection_obj,1,"")
        diagnostics = pd.DataFrame(dataSets)
        caseId = set((diagnostics['caseId'].astype(str)))
        caseIdSecond = set((diagnostics['caseId'].astype(str)))
        
        # new line of code
        dataSets = fetch_data(read_connection_obj,4,"")
        diagnosticsMonth = pd.DataFrame(dataSets)
        caseIdMonth = set((diagnosticsMonth['caseId'].astype(str)))
        caseIdMonthSecond = set((diagnosticsMonth['caseId'].astype(str)))
        # 
        
        #  for yesterday new appointment
        dataSets = fetch_data(read_connection_obj,2,"")
        diagnosticsDone = pd.DataFrame(dataSets)
        diagnosticsDone = diagnosticsDone[diagnosticsDone['appointmentCreationType'] != 'FOLLOWUP_APPOINTMENT' ]
        leadId = set((diagnosticsDone['leadId']).astype(str))
        caseId.update(leadId)
        
        
        # for month new appointment
        dataSets = fetch_data(read_connection_obj,5,"")
        diagnosticsDoneMonth = pd.DataFrame(dataSets)
        diagnosticsDoneMonth = diagnosticsDoneMonth[diagnosticsDoneMonth['appointmentCreationType'] != 'FOLLOWUP_APPOINTMENT' ]
        leadIdMonth = set((diagnosticsDoneMonth['leadId']).astype(str))
        caseIdMonth.update(leadIdMonth)
        
        # for yesterday follow up appintment
        dataSets = fetch_data(read_connection_obj,2,"")
        followUpTest = pd.DataFrame(dataSets)
        followUpTest = followUpTest[followUpTest['appointmentCreationType'] == 'FOLLOWUP_APPOINTMENT'  ]
        followUpTest = followUpTest[followUpTest['consultationType'] != 'Diagnostics'  ]
        leadIdSecond = set((followUpTest['leadId']).astype(str))
        caseIdSecond.update(leadIdSecond)
        
        # for month follow up appointment
        dataSets = fetch_data(read_connection_obj,5,"")
        followUpTestMonth = pd.DataFrame(dataSets)
        followUpTestMonth = followUpTestMonth[followUpTestMonth['appointmentCreationType'] == 'FOLLOWUP_APPOINTMENT' ]
        followUpTestMonth = followUpTestMonth[followUpTestMonth['consultationType'] != 'Diagnostics' ]
        leadIdMonthSecond = set((followUpTestMonth['leadId']).astype(str))
        caseIdMonthSecond.update(leadIdMonthSecond)
        
        
        exampleQuery = fetch_data(read_connection_obj,3,caseId)
        exampleQuery1 = pd.DataFrame(exampleQuery)
        
        # new code
        exampleQueryMonth = fetch_data(read_connection_obj,3,caseIdMonth)
        exampleQueryMonth1 = pd.DataFrame(exampleQueryMonth)
        
        exampleQueryFollowp = fetch_data(read_connection_obj,3,caseIdSecond)
        exampleQueryFollowp1 = pd.DataFrame(exampleQueryFollowp)
        
        exampleQueryMonthFollowUp = fetch_data(read_connection_obj,3,caseIdMonthSecond)
        exampleQueryMonthFollowUp1 = pd.DataFrame(exampleQueryMonthFollowUp)
        # 
        
        uniqueLeadId = set()
        if len(exampleQuery):
            uniqueLeadId = set(exampleQuery1['leadId'].astype(str))
            
        # new code
        uniqueLeadIdMonth = set()
        if len(exampleQueryMonth):
            uniqueLeadIdMonth = set(exampleQueryMonth1['leadId'].astype(str))
        
        uniqueLeadIdFollwup = set()  
        if len(exampleQueryFollowp):
            uniqueLeadIdFollwup = set(exampleQueryFollowp1['leadId'].astype(str)) 
            
        uniqueLeadIdMonthFollowUp = set()
        if len(exampleQueryMonthFollowUp):
            uniqueLeadIdMonthFollowUp = set(exampleQueryMonthFollowUp1['leadId'].astype(str))
        
        # 
            
        diagnosticsDone['isCsUser'] = diagnosticsDone.apply(lambda x : getCsUser(str(x['leadId']),uniqueLeadId),axis = 1) 
        
        diagnostics['isCsUser'] = diagnostics.apply(lambda x : getCsUser(str(x['caseId']),uniqueLeadId),axis = 1)
        
        if len(followUpTest):
            followUpTest['isCsUser'] = followUpTest.apply(lambda x : getCsUser(str(x['leadId']),uniqueLeadIdFollwup),axis = 1)
            followUpTest = followUpTest[followUpTest['isCsUser'] == 'yes']
            
         
        
        
        
        # new code
        diagnosticsMonth['isCsUser'] = diagnosticsMonth.apply(lambda x : getCsUser(str(x['caseId']),uniqueLeadIdMonth),axis = 1)
        diagnosticsDoneMonth['isCsUser'] = diagnosticsDoneMonth.apply(lambda x : getCsUser(str(x['leadId']),uniqueLeadIdMonth),axis = 1)
        
        if len(followUpTestMonth):
            followUpTestMonth['isCsUser'] = followUpTestMonth.apply(lambda x : getCsUser(str(x['leadId']),uniqueLeadIdMonthFollowUp),axis = 1)
            followUpTestMonth = followUpTestMonth[followUpTestMonth['isCsUser'] == 'yes']
            
        
        
        
        # 
        
        diagnosticsMonth.to_csv(fpath)
        diagnosticsDoneMonth.to_csv(fpath1) 
        followUpTestMonth.to_csv(fpath2)
        diagnostics = diagnostics[diagnostics['isCsUser'] == "yes"]
        diagnosticsDone = diagnosticsDone[diagnosticsDone['isCsUser'] == 'yes']
        # followUpTest = followUpTest[followUpTest['isCsUser'] == 'yes']
        # new code
        diagnosticsMonth = diagnosticsMonth[diagnosticsMonth['isCsUser'] == 'yes']
        diagnosticsDoneMonth = diagnosticsDoneMonth[diagnosticsDoneMonth['isCsUser'] == 'yes']
        # followUpTestMonth = followUpTestMonth[followUpTestMonth['isCsUser'] == 'yes']
        
        
        
        testDone = testDoneCount(diagnosticsDone)
        testDoneMonth = testDoneCount(diagnosticsDoneMonth)
        newTestDone = mergeDictionary(testDone,testDoneMonth,2)
        tota_appointment_done = [newTestDone['yesterdayCount'].sum(),newTestDone['monthCount'].sum()]
        
        testDoneFollowUp = testDoneCount(followUpTest)
        testDoneMonthFollowUp = testDoneCount(followUpTestMonth)
        newTestDoneFollowUp = mergeDictionary(testDoneFollowUp,testDoneMonthFollowUp,2)
        total_followup_done = [newTestDoneFollowUp['yesterdayCount'].sum(),newTestDoneFollowUp['monthCount'].sum()]
        
        count = diagnostics_count(diagnostics,tota_appointment_done[0],total_followup_done[0]) 
        countMonth = diagnostics_count(diagnosticsMonth,tota_appointment_done[1],total_followup_done[1])
        followUpDistribution = followUp(diagnostics)
        followUpCallDistributionMonth = followUp(diagnosticsMonth)
        cancelled = cancelledCount(diagnostics)
        cancelledMonth = cancelledCount(diagnosticsMonth)
        
        # new code
        newFollowUpDistribution = mergeDictionary(followUpDistribution,followUpCallDistributionMonth,1)
        newCancelled = mergeDictionary(cancelled,cancelledMonth,1)
        # 
        
        
        
        resultant = resultantTable(count,countMonth,newFollowUpDistribution,newCancelled) 
        resultant = resultTestCount(newTestDone,resultant,1)
        
        # resultant = resultTestCount(newTestDoneFollowUp,resultant,2) 
        
        
        
        Subject = "Diagnostics Report {0} |".format(str(yest)) 
        # email_recipient_list = ['nisha@ayu.health']
        email_recipient_list = ['tls@ayu.health', 'ankur@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath,fpath1,fpath2])  
        
    except Exception as e:
        raise e
    
       
        
   
    