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
from oauth2client.service_account import ServiceAccountCredentials 
from gspread.models import Cell
from read_sheet import read_from_sheet
import requests


fpath = os.path.join("/tmp","doctorCosultation.csv")
fpath1 = os.path.join("/tmp","patientRefferal.csv")
fpath2 = os.path.join("/tmp","loyaltyCard.csv")
fpath3 = os.path.join("/tmp","prescription.csv")
fpath4 = os.path.join("/tmp","discharge.csv")
fpath5 = os.path.join("/tmp","testimonial.csv")
fpath6 = os.path.join("/tmp","ticket.csv")
fpath7 = os.path.join("/tmp","data.csv")

# Description: EOD reports on daily basis at 8:30 pm.
# Created By : Nisha Das


READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

yesterday = datetime.now() + timedelta(hours=5,minutes=30) 
yesterday = yesterday.date()

queries = {'doctorCosultation' : """select
                                ldc.id as appId,
                                ldc.leadId,
                                ldc.appointmentDate,  
                                ldc.ayuMitraId,
                                ldc.doctorConsultationStatus,
                                sub.commentType,
                                CASE 
                                WHEN sub.commentType IS NOT NULL THEN trim(lower(sub.user))
                                ELSE trim(lower(apd.email))
                                END AS 'AyuMitraEmail',
                                CASE 
                                WHEN sub.commentType IS NOT NULL THEN sub.cityId
                                ELSE apd.cityId
                                END AS AyuCity,
                                apd.email,
                                sub.user,
                                apd.cityId,
                                sub.cityId as latestCity,
                                ldc.appointmentCreationType,
                                ldc.originConsultationId,
                                case when pres.entityId is null then 'No' else 'Yes' end as 'prescriptionUploaded'
                                from lead_doctor_consultation ldc
                                left join ayu_personnel_details apd on apd.personnelId = ldc.ayuMitraId
                                left join (select entityId from medical_documents 
                                where 
                                documentType in ('PRESCRIPTION', 'E_PRESCRIPTION') 
                                and 
                                entityType='APPOINTMENT'
                                group by entityId) pres on pres.entityId = ldc.id
                                left join
                                (select leadId,user,cityId,commentType from lead_comments lc
                                left join ayu_personnel_details apd on apd.email = lc.user
                               where 
                               commentType in ('APPOINTMENT_DONE') 
                               and leadType = 'APPOINTMENT') sub on sub.leadId = ldc.id
                            where date(ldc.appointmentDate) = '{yesterday}'
                            """,                
                            

            'rescheduled' : """
                                select distinct originConsultationId as appId
                                                    from lead_doctor_consultation
            """,
                            
        
            'surgery': """
                            Select 
                                patientSurgeryId as surgeryId,
                                group_concat(distinct(case when surgeryDocumentType in ('DISCHARGE_SUMMARY') then documentLink  end)) as 'dischargeSummaryDoc'
                            from
                                patient_surgery_documents ps
                                join surgery_document_details sdd on  ps.id = sdd.surgeryDocId
                            where surgeryDocumentType in ('DISCHARGE_SUMMARY')
                                group by patientSurgeryId""", 
                                
                                
            'patient_surgery_details': """
                            select 
                                    psd.id as surgeryId,
                                    psd.caseId,
                                    date(date_add(dischargeDate,INTERVAL '5:30' HOUR_MINUTE)) as dischargeDate,
                                    psd.hospitalId,
                                    ayuMitraId,
                                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                                        when pcp.cityName = 'Bangalore' then 'BLR'
                                        when pcp.cityName is not null then pcp.cityName
                                        when ppp.cityName = 'Chandigarh' then 'CHD'
                                        when ppp.cityName = 'Bangalore' then 'BLR'
                                        when ppp.cityName is not null then ppp.cityName
                                    else 'No City' end as city,
                                    aliasName,
                                    trim(lower(apd.email)) as latestAyuM
                        from patient_surgery_details psd
                        join patient_case pc on psd.caseId = pc.caseId
                        join patient_profile pp on pc.patientId = pp.id
                        join customer_profile cp on cp.customerId = pp.customerId
                        left join ayu_cities pcp on pc.cityId = pcp.id
                        left join ayu_cities ppp on pp.cityId = ppp.id
                        left join ayu_facility_profile afp on psd.hospitalId = afp.facilityId
                        left join ayu_personnel_details apd on psd.ayuMitraId = apd.personnelId and apd.personnelType='AYU_MITRA'
                            where 
                                date(date_add(dischargeDate,INTERVAL '5:30' HOUR_MINUTE)) = '{yesterday}'
                                and apd.cityId = 1
                                       """,
                                       
            'prescription': """
                            select ldc.id as appId,
                                    leadId,
                                    ldc.hospitalId,
                                    date(appointmentDate) as appointmentDate, 
                                    doctorConsultationStatus,
                                    consultationType,
                                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                                        when pcp.cityName = 'Bangalore' then 'BLR'
                                        when pcp.cityName is not null then pcp.cityName
                                        when ppp.cityName = 'Chandigarh' then 'CHD'
                                        when ppp.cityName = 'Bangalore' then 'BLR'
                                        when ppp.cityName is not null then ppp.cityName
                                    else 'No City' end as city,
                                    appointmentCreationType,
                                    aliasName,
                                    trim(lower(apd.email)) as AyuMitraEmail,
                                    case when sub.entityId is null then 'No' else 'Yes' end as 'prescriptionUploaded'
                    from lead_doctor_consultation ldc 
                    join patient_case pc on ldc.leadId = pc.caseId
                    join patient_profile pp on pc.patientId = pp.id
                    join customer_profile cp on cp.customerId = pp.customerId
                    left join ayu_cities pcp on pc.cityId = pcp.id
                    left join ayu_cities ppp on pp.cityId = ppp.id
                    left join ayu_facility_profile afp on ldc.hospitalId = afp.facilityId
                    left join ayu_personnel_details apd on ldc.ayuMitraId = apd.personnelId and apd.personnelType='AYU_MITRA'
                    left join (select entityId from medical_documents 
                            where 
                            documentType in ('PRESCRIPTION', 'E_PRESCRIPTION') 
                            and 
                            entityType='APPOINTMENT'
                            group by entityId) sub on sub.entityId = ldc.id 
                        where
                          date(appointmentDate) = '{yesterday}'
                          and doctorConsultationStatus = 3
                         
                                       
                                      """,                                              
                            
                            
                            
            "patientRefferal": """select leadId,cp.leadSource,
                               date(date_add(lc.createdOn,INTERVAL '5:30' HOUR_MINUTE)) as 'commentDate',
                               date(date_add(pp.createdOn,INTERVAL '5:30' HOUR_MINUTE)) as 'createdOn',
                               trim(lower(lc.user)) as user 
                                    from
                                    patient_case pc
                                    join patient_profile pp on pc.patientId = pp.id
                                    join customer_profile cp on pp.customerId = cp.customerId
                                    join lead_comments lc on pc.caseId = lc.leadId
                                    left join ayu_personnel_details mitra on (lc.user = mitra.email and personnelType='AYU_MITRA')
                                    where
                                    commentType in ('CREATION')
                                    and leadType in ('CASE')
                                    and date(date_add(lc.createdOn,INTERVAL '5:30' HOUR_MINUTE)) = '{yesterday}'
                                    and date(date_add(pp.createdOn,INTERVAL '5:30' HOUR_MINUTE)) = '{yesterday}'
                                    and cp.leadSource = 'Patient Referral'
                                    and pc.tenantName = 'AYU'
                                   
                                """, 
                                
        
                                
            
             
                                
                           
            "loyalty_card_generated": """Select 
                                        l.cardId,
                                        trim(lower(l.collectedBy)) as collectedBy,
                                        l.paymentType,
                                        l.paymentStatus,
                                        l.amount,
                                        c.status,
                                        date_add(l.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'Date of Sale'
                                    from 
                                        
                                        generated_mcards c
                                        join lc_payment_details l on l.cardId = c.cardId
                                        left join ayu_personnel_details apd on trim(lower(apd.email)) = trim(lower(l.collectedBy)) 
                                    where
                                        l.paymentStatus = 'PAID'
                                        and c.status = 'ACTIVE'
                                        and apd.cityId = 1
                                        and date(l.createdOn) = '{yesterday}'
                                        """,
                                        
                                        
            "app_done": """select leadId as appId,trim(lower(user)) as email
                                from
                                lead_comments lc
                                where 
                                    commentType in ('APPOINTMENT_DONE') 
                                    and leadType = 'APPOINTMENT'
                                    and leadId in ({id})
                                order by commentId""",
                                
            
            "data": """select etm.entityId as caseId,td.ticketId,td.parentTicketId, td.createdBy, 
                            td.ticketType, td.assignedTo,
                            date_add(td.createdOn, INTERVAL '5:30' HOUR_MINUTE) as createdOn,
                            tc.categoryName,
                            case when ac.cityName = 'Chandigarh' then 'CHD' 
                                        when ac.cityName = 'Bangalore' then 'BLR'
                                        when ac.cityName is not null then ac.cityName
                                else 'No City' end as city
                                
                            from ticket_details td
                            left join ticket_category tc on tc.categoryId = td.categoryId
                            left join ayu_cities ac on ac.id = td.cityId
                            left join entity_ticket_mapping etm on td.ticketId = etm.ticketId
                            where
                            ac.cityName = 'Chandigarh'
                            
        """,
        
        "response": """ select ticketId, date_add(createdOn, INTERVAL '5:30' HOUR_MINUTE) as responsecreatedOn from
                        ticket_comments
                        where ticketId in ({ticketId})
                        and date(createdOn) = '{yesterday}'
                        order by createdOn """                     
                                           
           
                
                
                
            
    
}








logger = logging.getLogger(__name__)

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == 'prescription':
            fetched_val[lookup] = fetch_record(conn,query.format(yesterday=yesterday))
            leadId = [str(x['appId']) for x in fetched_val[lookup]]
        elif lookup == 'app_done':
            leadId = ','.join(leadId)
            fetched_val[lookup] = fetch_record(conn,query.format(id = leadId))
        
        elif lookup == "data":
            fetched_val[lookup] = fetch_record(conn, query)
            ticketId = [str(x['ticketId']) for x in fetched_val[lookup]]
            
        elif lookup == "response":
            tid = ",".join(ticketId)
            fetched_val[lookup] = fetch_record(conn,query.format(ticketId=tid,yesterday=yesterday)) 
            
        else:
            fetched_val[lookup] = fetch_record(conn,query.format(yesterday=yesterday))
            
    return fetched_val 
    
    
result_base = f"""<table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 9 width = \"1600\"><b><center>Daily Activity Report</center></b></td>
            <td colspan = 1 rowspan = 2 width = \"1600\"><b><center>Ticket Average ResponseTime</center></b></td>
            <td colspan = 2 width = \"1600\"><b><center>Prescription</center></b></td>
            <td colspan = 2 width = \"1600\"><b><center>Discharge Report</center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"1600\"><b><center>ayuM</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Done</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Follow Up</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>New</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Cancelled</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Rescheduled</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>pt Refferal</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Loyalty Card</center></b></td> 
            <td colspan = 1  width = \"1600\"><b><center>Testimonial</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>No</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Yes</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Done</center></b></td>
            <td colspan = 1  width = \"1600\"><b><center>Docs Uploaded</center></b></td>
            
            
        </tr>
"""
    
def resultant_table(result_base,agents):
    agents['Grand Total'] = [0]*14
    for key in agents.keys():
        agents['Grand Total'][0] += agents[key][0]
        agents['Grand Total'][1] += agents[key][1]
        agents['Grand Total'][2] += agents[key][2]
        agents['Grand Total'][3] += agents[key][3]
        agents['Grand Total'][4] += agents[key][4]
        agents['Grand Total'][5] += agents[key][5]
        agents['Grand Total'][6] += agents[key][6]
        agents['Grand Total'][7] += agents[key][7]
        agents['Grand Total'][9] += agents[key][9]
        agents['Grand Total'][10] += agents[key][10]
        agents['Grand Total'][11] += agents[key][11]
        agents['Grand Total'][12] += agents[key][12]
        agents['Grand Total'][13] += agents[key][13]
        
    print("total",agents['Grand Total'][13])
        
    for i in range(0,14):
        agents['Grand Total'][i] = agents['Grand Total'][i]//2
        
    agents['Grand Total'][8] = " "    
        
        
    
    result = result_base
    for key,value in agents.items():
        if key == 'Grand Total':
            color = '#d4ebf2'
        else:
            color = '#ffffff'
        result = result + f"""<tr bgcolor= {color}>
        
        <td colspan = 1 ><b><center>{key}</center></b></td>
        <td colspan = 1 ><center>{value[0]}</center></td>
        <td colspan = 1 ><center>{value[1]}</center></td>
        <td colspan = 1 ><center>{value[2]}</center></td>
        <td colspan = 1 ><center>{value[3]}</center></td>
        <td colspan = 1 ><center>{value[4]}</center></td>
        <td colspan = 1 ><center>{value[5]}</center></td>
        <td colspan = 1 ><center>{value[6]}</center></td>
        <td colspan = 1 ><center>{value[7]}</center></td>
        <td colspan = 1 ><center>{value[8]}</center></td>
        <td colspan = 1 ><center>{value[9]}</center></td>
        <td colspan = 1 ><center>{value[10]}</center></td>
        <td colspan = 1 ><center>{value[12]}</center></td>
        <td colspan = 1 ><center>{value[13]}</center></td>
        
        </tr>
        """
        
    result = result + "</table><br><br>"
    return result    
    
    
    
    
    
    
    
    
    
def getCount(doctorCosultation,rescheduled):
    agent_dict = {}
    rescheduled = list(rescheduled['appId'])
    for key,value in doctorCosultation.iterrows():
        if value['AyuMitraEmail'] not in agent_dict.keys() and value['doctorConsultationStatus']  == '3':
            agent_dict[value['AyuMitraEmail']] = [0]*14
        if value['doctorConsultationStatus']  == '3':
            agent_dict[value['AyuMitraEmail']][0] += 1
            
    for key,value in doctorCosultation.iterrows():
        print(value['AyuMitraEmail'])
        if value['AyuMitraEmail'] == 'Kulwinder.k@ayu.health':
            print("yes")
            
            
            
    for key,value in doctorCosultation.iterrows():
        if value['AyuMitraEmail'] in agent_dict.keys():
            if value['doctorConsultationStatus'] == '3' and value['appointmentCreationType'] == 'FOLLOWUP_APPOINTMENT':
                agent_dict[value['AyuMitraEmail']][1] += 1
            if value['doctorConsultationStatus'] == '3' and (value['appointmentCreationType'] == 'NEW_APPOINTMENT' or
            value['appointmentCreationType'] == 'RESCHEDULED_APPOINTMENT'):
                agent_dict[value['AyuMitraEmail']][2] += 1
                
            # print(value['AyuMitraEmail'])    
                
            if value['doctorConsultationStatus'] == '4' and value['appointmentCreationType'] == 'NEW_APPOINTMENT' and value['appId'] in rescheduled:
                agent_dict[value['AyuMitraEmail']][4] += 1
            elif value['doctorConsultationStatus'] == '4' and value['appointmentCreationType'] == 'NEW_APPOINTMENT':
                agent_dict[value['AyuMitraEmail']][3] += 1
          
            
    return agent_dict        
        
        
            
def getPatientReferralCount(patientRefferal,agent_dict):
    for key,value in patientRefferal.iterrows():
        # if value['user'] not in agent_dict.keys():
        #     agent_dict[value['user']] = [0]*14
        
        # agent_dict[value['user']][5] += 1
        
        if value['user'] in agent_dict.keys():
            agent_dict[value['user']][5] += 1
            
    return agent_dict    
        
 

def getDocUploaded(surgeryId, docsDump):
    
    print(surgeryId)
    if surgeryId in docsDump.keys():
        return 'Yes'
        
    return 'No'    
    
def getDischargeCount(surgery,agents):
    for key,value in surgery.iterrows():
        if value['latestAyuM'] in agents.keys():
            agents[value['latestAyuM']][12] += 1
            if value['docUploaded'] == "Yes":
                agents[value['latestAyuM']][13] += 1
    return agents
    
    
def getAyuMitra(latestAyuM, appId, commentsDump):
    
    if appId in commentsDump.keys():
        return commentsDump[appId]

    return latestAyuM        

def getPrescriptionCount(prescription,agents):
    for key,value in prescription.iterrows():
        if value['AyuMitraEmail'] in agents.keys():
            if value['doctorConsultationStatus'] == '3' and value['prescriptionUploaded'] == "Yes":
                agents[value['AyuMitraEmail']][10] += 1
            if value['doctorConsultationStatus'] == '3' and value['prescriptionUploaded'] == "No":
                agents[value['AyuMitraEmail']][9] += 1
                
    return agents            
            
def getTestimonialCount(testimonial,agents):
    print("yesterday",yesterday)
    for key,value in testimonial.iterrows():
        if value['Email address'] in agents.keys() and value['date'] == yesterday: 
            agents[value['Email address']][7] += 1
            
    return agents
    
def convert_to_hours(seconds):
    
    if seconds == 0:
        return ''
    hrs = '{:.0f}'.format(seconds // (60 * 60))
    mins = '{:.0f}'.format((seconds % 3600) // 60)
    secs = '{:.0f}'.format(((seconds % 3600) % 60))
    if hrs == '0':
        if mins == '0':
            return '{secs}sec'.format(secs=secs)
        else:
            if secs == '0':
                return '{mins}min'.format(mins=mins)
            else:
                return '{mins}min {secs}sec'.format(mins=mins, secs=secs)
                
    else:
        if mins == '0':
            if secs == '0':
                return '{hrs}hr'.format(hrs=hrs)
            else:
                return '{hrs}hr {secs}sec'.format(hrs=hrs, secs=secs)
        else:
            return '{hrs}hr {mins}m {secs}sec'.format(hrs=hrs, mins=mins,secs = secs)            

def delta(ticketId,createdOn, data):
    
    if ticketId in data.keys():
        y = data[ticketId] - createdOn
        print(y)
        print(convert_to_hours(y.seconds))
        
        
        return y.days*24*3600 + y.seconds, data[ticketId]
    return 'NA', ''

def getLoyaltyCount(loyalty,agents):
    for key,value in loyalty.iterrows():
        if value['collectedBy'] in agents.keys():
            agents[value['collectedBy']][6] += 1
            
    return agents        

def getTicketCount(data,agents):
    for key,value in data.iterrows():
        if value['assignedTo'] in agents.keys():
            agents[value['assignedTo']][8] = value['responseTime']
            
        for key,value in agents.items():
            if value[8] == 0:
                value[8] = "N/A"
            
    return agents        
    
def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
                                                    
        datasets = fetch_data(read_connection_obj)
        doctorCosultation = pd.DataFrame(datasets['doctorCosultation'])
        doctorCosultation = doctorCosultation[doctorCosultation['AyuCity'] == 1]
        patientRefferal = pd.DataFrame(datasets['patientRefferal'])
        rescheduled = pd.DataFrame(datasets['rescheduled'])
        loyalty_card = pd.DataFrame(datasets['loyalty_card_generated'])
        docsUpload = pd.DataFrame(datasets['surgery'])
        surgery = pd.DataFrame(datasets['patient_surgery_details'])
        prescription = doctorCosultation
        comments = pd.DataFrame(datasets['app_done'])
        data = pd.DataFrame(datasets['data'])
        ticket = pd.DataFrame(datasets['response'])
        doctorCosultation.to_csv(fpath)
        patientRefferal.to_csv(fpath1)
        loyalty_card.to_csv(fpath2)
        ticket.to_csv(fpath6)
        data.to_csv(fpath7)
        # prescription.to_csv(fpath3)
        daily_activity = getCount(doctorCosultation,rescheduled)
        print(yesterday)
        
        daily_activity = getPatientReferralCount(patientRefferal,daily_activity)
        testimonial = read_from_sheet('1hncnd6RrO4KlOkro07zsUxP_7MhCmKWyrkHGlBLMqew','Form responses 1')
        print(type(testimonial['Timestamp'][0]))
        testimonial['Timestamp'] = pd.to_datetime(testimonial['Timestamp'])
        testimonial['date'] = testimonial['Timestamp'].dt.date
        testimonial.to_csv(fpath5)
        daily_activity = getTestimonialCount(testimonial[testimonial['Location'] == 'Chandigarh'],daily_activity)
        daily_activity = getLoyaltyCount(loyalty_card,daily_activity)
        
        # commentsDump = {}
        # for key,val in comments.iterrows():
        #     apptId = str(val['appId'])
            
        #     if apptId not in commentsDump.keys():
        #         commentsDump[apptId] = val['email'] 
                
        # prescription['ayuMitra'] = prescription.apply(lambda x: getAyuMitra(x['latestAyuM'], str(x['appId']), commentsDump), axis=1)
        prescription.to_csv(fpath3)
        daily_activity = getPrescriptionCount(prescription,daily_activity)
        
        
        docsDump = {}
        for key,val in docsUpload.iterrows(): 
            surgeryId = str(val['surgeryId'])
            
            if surgeryId not in docsDump.keys():
                docsDump[surgeryId] = val['dischargeSummaryDoc']
                
        if len(surgery):
            surgery['docUploaded'] = surgery.apply(lambda x: getDocUploaded(str(x['surgeryId']), docsDump), axis=1)
            
        
        surgery.to_csv(fpath4)
        
        daily_activity = getDischargeCount(surgery,daily_activity)
        
        
        
        # ticket
        diff2 = {} 
        for i,j in ticket.iterrows():
            if j['ticketId'] not in diff2.keys():
                diff2[j['ticketId']] = j['responsecreatedOn']
                
        df = data.apply(lambda x : delta(x['ticketId'],x['createdOn'], diff2),result_type="expand", axis =1)
        data = pd.concat([data,df], axis=1)
        data = data.rename(columns = {0:"difference",1:"responsecreatedOn" })
        data = data[data['difference'] != 'NA']
        data = data[['assignedTo','difference']]
        data['difference'] = data['difference'].astype(int)
        data = data.groupby('assignedTo',as_index=False).mean()
        print(data.columns)
        data['responseTime'] = data.apply(lambda x: convert_to_hours((x['difference'])),axis = 1)
        daily_activity = getTicketCount(data,daily_activity)
        
        
        
        resultant = resultant_table(result_base,daily_activity)                                           
                                                   
                                                    
        Subject = "EOD Report Chandigarh | {0}".format(yesterday)
        email_recipient_list = ['shubham.sharma@ayu.health'] 
        # email_recipient_list = ['nisha@ayu.health'] 
        send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath,fpath1,fpath2,fpath3,fpath4,fpath5,fpath6,fpath7])                                            
        
    except Exception as e:
        Subject = "EOD Reports Automation Chandigarh"
        email_recipient_list = ['analytics@ayu.health'] 
        send_email(None, email_recipient_list, Subject, 'None',e, []) 
        raise e
