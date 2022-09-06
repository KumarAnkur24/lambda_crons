import pdb 
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
from service.send_mail_client import send_email 
import pandas as pd 
import csv
from datetime import datetime, timedelta  
import pytz
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread
from write_to_gsheet import contoller
from big_query_data_fetch import main as getdatafrombigq
from big_query_data_fetch import upload_to_bq
from service.base_functions import exotel_cxNo, msg_to, agent_name_from_email

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
which_time = str(os.environ.get('which_time', ''))
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')
#SPREAD_SHEET_JSON = '/home/ec2-user/Reports/script/src/google_secret/spreadsheet-integration-271313-14d43d1093d9.json'

def patient_lead_status(x):
    return {
        '0': 'OPENED',
        '1': 'FOLLOWUP',
        '2': 'APPOINTMENT_BOOKED',
        '3': 'CANCELLED',
        '4': 'REJECTED',
        '5': 'INCOMPLETE',
        '6': 'CLOSED',
        '7': 'APPOINTMENT_CONFIRMED',
        None: None,
        '': None
    }.get(x, 'NA')


def doctor_consultation_status(x):
    return {
        '0': 'OPEN',
        '1': 'BOOKED',
        '2': 'CONFIRMED',
        '3': 'DONE',
        '4': 'CANCELLED',
        None: 'None',
        '': 'None',
        '5': 'OPEN'
    }.get(x, 'NA')


queries = {
    "appointmentDone": """select 
                caseId,
                patientLeadStatus,
                pc.leadSource, 
                pc.reason,
                pc.patientId,
                patientName,
                customerNumber,
                rev_min.createdOn as 'createdOn',
                date(date_add(pc.createdOn,interval '5:30' HOUR_MINUTE)) as 'caseCreatedOn',
                doctorConsultationStatus,
                lda.id as appId,
                appointmentDate,
                consultationType,
                lda.consultationFee,
                case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                            else 'No City' end as city,
                pc.assignedTo,
                secondaryNumber
                                from 
                                lead_doctor_consultation_AUD lda 
                                join patient_case pc on lda.leadId = pc.caseId
                                join patient_profile pp on pc.patientId = pp.id
                                join customer_profile cp on pp.customerId=cp.customerId
                                left join ayu_cities pcp on pc.cityId = pcp.id
                                left join ayu_cities ppp on pp.cityId = ppp.id
                                left join (
                                select id, min(REV) as 'REV',min(createdOn) as createdOn from lead_doctor_consultation_AUD
                                where 
                                doctorConsultationStatus = 3
                                and tenantName = 'AYU'
                                and appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT')
                                group by id) rev_min on (lda.REV = rev_min.REV and lda.id = rev_min.id)
                                where 
                                    date(date_add(rev_min.createdOn,interval '5:30' HOUR_MINUTE)) >= '{start}'
                                    and date(date_add(rev_min.createdOn,interval '5:30' HOUR_MINUTE)) <= '{end}'
                                    and pc.leadSource not in ('Offline Channel')
                                """,
    #"followUp_comments": """select leadId,user,commentType,
    #                        date(date_add(createdOn,interval '5:30' HOUR_MINUTE)) as 'createdOn'
    #                        from lead_comments
    #                         where leadType='CASE' and commentType in ('FOLLOWUP_COMMENT') 
    #                         and text not like 'comment : Patient Call back reason: Patient Call back%'
    #                         and text not like 'comment : Whatsapp Follow up reason: Whatsapp Follow up%'
    #                         order by commentId
    #                         """,
    "exotel_response": """select *, date(call_created_on) as createdOn from exotel_response 
                            where 
                            call_created_on >= '{start}'
                            
                             """,
    'ipd_yes': "select ldc.leadId from appointment_surgery_outcome aso join lead_doctor_consultation ldc on aso.appointmentId = ldc.id where isSurgeryRecommended='YES' ",
    'other_enquiry_cases': """select 
                                    distinct caseId
                                from
                                    patient_case_AUD
                                where
                                    reason in ('Other hospital enquiry', 'Other doctor enquiry')
                                    and caseId in ({caseIds})
                            """,
    # "other_comments": """select 
    #                         distinct leadId
    #                     from
    #                         lead_comments
    #                     where
    #                                 commentType = 'APPOINTMENT_COMMENT'
    #                                 and leadType = 'APPOINTMENT'
    #                                 and upper(text) like '%OTHER%'
    #                                 and leadId in ({appIds}) 
    #                     """,
    "agents": """select email
                , phoneNumber 
                from customer_support_details csd
                
                    """,
    "firstAppointment": """select patientId, min(lda.id) as 'firstId' from
                                lead_doctor_consultation lda
                                join patient_case pc on lda.leadId = pc.caseId
                                join patient_profile pp on pc.patientId = pp.id
                                join customer_profile cp on pp.customerId=cp.customerId
                                where doctorConsultationStatus = 3
                                and pc.leadSource not in ('Offline Channel')
                                and pc.tenantName = 'AYU'
                                and appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT')
                                group by 1
                        """
}

change_to_indian_time = lambda x: x.astimezone(pytz.timezone('Asia/kolkata')).strftime('%Y-%m-%d') if type(
    x) == datetime else None 


today = datetime.now() + timedelta(hours=5,minutes=30) 
month = today.replace(day=1)

lastMonthEnd = month - timedelta(days = 1)
lastMonthStart = today - timedelta(days=30) #lastMonthEnd.replace(day = 1)
lastMonthStart_ = lastMonthStart.replace(hour=0,minute=0,second=0)

today = today.date()
month = month.date()
lastMonthEnd = lastMonthEnd.date()
lastMonthStart = lastMonthStart.date()


print(today)
print(month)
print(lastMonthStart)
print(lastMonthEnd)

SHEET_ID_AGENTS = '1PvIBXZcLC3fPFf4SGUzMQg1ZFwlkD_nJQ3R7uvdtBwA'
READ_SHEET_NAME = 'CsAgentMap'
scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
client = gspread.authorize(creds)
client_sp = client.open_by_key(SHEET_ID_AGENTS)
val = client_sp.worksheet(title=READ_SHEET_NAME)
agents = val.get_all_values()
i = 0
agent_nos = {}
for row in agents:

    if i == 0:
        i = i + 1
        continue
    agent_nos[row[0]] = row[2]
    agent_nos[row[0][1:]] = row[2]

val = client_sp.worksheet(title='TL_Mappings')
tl_maps = val.get_all_values()        
TMMapping = {}
Tls = set({})
i = 0
print(tl_maps)
for row in tl_maps:
        
    if i == 0:
        i = i + 1
        continue
    # print(row)
    tl = row[3].strip().lower() 
    tm = row[2].strip().lower()
    
    if tl == '':
        tl = 'NA'
    
    if tl in ['TL', 'Manager', 'NA']:
        Tls.add(tm)
        continue
    
    if tm not in TMMapping.keys():
        TMMapping[tm] = tl
    

# print(Tls)


def fetch_data(conn, k):
    fetched_val = {}
    
    start = lastMonthStart
    end = today
    for lookup, query in queries.items():
        print(lookup)
        if lookup == 'appointmentDone':
            fetched_val[lookup] = fetch_record(conn, query.format(start=start, end=end))
            caseIds = ['1']
            appIds = ['1']
            for row in fetched_val[lookup]:
                caseIds.append(str(row['caseId']))
                appIds.append(str(row['appId']))
        elif lookup == 'other_enquiry_cases':
            fetched_val[lookup] = fetch_record(conn, query.format(caseIds=",".join(caseIds)))
        elif lookup == 'other_comments':
            fetched_val[lookup] = fetch_record(conn, query.format(appIds=",".join(appIds)))
        elif lookup == 'exotel_response':
            fetched_val[lookup] = fetch_record(conn, query.format(start=lastMonthStart_))
        else:
            fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val


def getUser(caseId, dump):
    if caseId in dump.keys():
        return dump[caseId]
    return 'NA'


def exotel_cxNo(direction, from_no, to_no):
    if direction == 'inbound':
        return msg_to(from_no)
    else:
        return msg_to(to_no)


def agent_email(direction, from_no, to_no):
    if direction == 'inbound':
        if to_no in agent_nos.keys():
            return agent_nos[to_no]
        else:
            return 'No Mapping'
    else:
        if from_no in agent_nos.keys():
            return agent_nos[from_no]
        else:
            return 'No Mapping'


def msg_to(to):
    if to is None:
        return to
    if len(to) == 11:
        return to[1:]
    elif len(to) == 12:
        return to[2:]
    elif len(to) == 13:
        return to[3:]
    elif len(to) == 14:
        return to[4:]
    return to


def agent_call_duration(direction, leg2Status, leg2Duration, leg1Duration, leg1Status):
    if direction == 'inbound' and leg2Status == 'completed':
        return leg2Duration
    elif direction in ['outbound-api', 'outbound-dial'] and leg2Status == 'completed':
        return leg2Duration
    elif direction == 'normal-call' and leg1Status == 'completed':
        if leg1Duration > 30:
            return leg1Duration - 30
    return 0

def getInfo(caseId, dumpShared):

    if caseId in dumpShared.keys():
        return 'Yes'
    return 'No'

def checkIfValid(appId,patientId, firstAppointmentDump):
    
    if patientId in firstAppointmentDump.keys():
        apptId = firstAppointmentDump[patientId]
        
        if apptId == appId:
            return 'Yes'
            
    return 'No'

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        
    
        k = which_time
        dataSets = fetch_data(read_connection_obj, k)
        exotel_response = pd.DataFrame(dataSets['exotel_response'])
        appointmentDone = pd.DataFrame(dataSets['appointmentDone'])
        #followUp_comments = dataSets['followUp_comments']
        ipd_yes = dataSets['ipd_yes']
        other_enquiry_cases = dataSets['other_enquiry_cases']
        # other_comments = dataSets['other_comments']
        
        exotel_response = exotel_response[exotel_response['leg_1_status'] == 'completed']
        exotel_response = exotel_response.sort_values(by = ['call_created_on'],ascending=True)
        
        
        query = """select distinct caseId  
                from
            (SELECT event_name,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "category" ) AS category,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "label" ) AS caseId,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "value" ) AS value
            FROM 
                `amigos-ga.analytics_284143472.*`
            WHERE 
                event_name = 'pitch'
            )
            where 
            value = 'DoctorHospitalQuery'
                and category in ('screen_update', 'inbound_pitch')
            """
        
        data2 = getdatafrombigq(query)
        
        print('DB read done')

        pitchCharterCases = {}
        for line2 in data2:
            caseId = str(line2[0])
            pitchCharterCases[caseId] = 'Yes'
            
        surgeryReco = {}
        for value in ipd_yes:
            surgeryReco[value['leadId']] = 'Yes'
            
        otherEnquiryCases = {}
        for value in other_enquiry_cases:
            otherEnquiryCases[str(value['caseId'])] = 'Yes'
            
        otherApps = {}
        # for value in other_comments:
        #     otherApps[str(value['leadId'])] = 'Yes'
            
            
        print('Reco DUmp done')

        #for value in followUp_comments:
        #    user = value['user']
        #
        #    if value['leadId'] not in followUpCommentDump.keys():
        #        followUpCommentDump[value['leadId']] = []
        #
        #    followUpCommentDump[value['leadId']].append(value)

        agents = dataSets['agents']
        
        
        #agent_nos = {}
        for val in agents:
            agent = agent_name_from_email(val['email'].strip())
            agentNo = msg_to(val['phoneNumber'])
            
            if agentNo not in agent_nos.keys():
                agent_nos[agentNo] = agent
                
        if len(exotel_response):
            exotel_response['cxNo'] = exotel_response.apply(
                lambda x: exotel_cxNo(x['direction'], x['from_no'], x['to_no']), axis=1)
            exotel_response['agentEmail'] = exotel_response.apply(
                lambda x: agent_email(x['direction'], x['from_no'], x['to_no']), axis=1)
            exotel_response['callTime'] = exotel_response.apply(
                lambda x: agent_call_duration(x['direction'], x['leg_2_status'], x['leg_2_duration'],
                                              x['leg_1_duration'], x['leg_1_status']), axis=1)

        print('Exotel response applied')
        exotelDump = {}
        for index, value in exotel_response.iterrows():
            if value['agentEmail'] == 'No Mapping':
                continue

            if value['cxNo'] not in exotelDump.keys():
                exotelDump[value['cxNo']] = {}

            if value['agentEmail'] not in exotelDump[value['cxNo']].keys():
                exotelDump[value['cxNo']][value['agentEmail']] = []

            exotelDump[value['cxNo']][value['agentEmail']].append(value)

        print('Dump created')
        
        firstAppointmentDump = {}
        firstAppointment = dataSets['firstAppointment']
        for val in firstAppointment:
            if val['patientId'] not in firstAppointmentDump.keys():
                firstAppointmentDump[val['patientId']] = val['firstId']
        
        appointmentDoneValid = pd.DataFrame([])
        if len(appointmentDone):
            appointmentDone['ifValid'] = appointmentDone.apply(lambda x: checkIfValid(x['appId'],x['patientId'], firstAppointmentDump), axis=1)
            appointmentDoneValid = appointmentDone[appointmentDone['ifValid'] == 'Yes']
            
            if len(appointmentDoneValid)>0:    
                appointmentDoneValid['cxNo'] = appointmentDoneValid.apply(lambda x: msg_to(x['customerNumber']), axis=1)
                appointmentDoneValid['cxNo1'] = appointmentDoneValid.apply(lambda x: msg_to(x['secondaryNumber']), axis=1)
                
                appointmentDoneValid['surgeryReco'] = appointmentDoneValid.apply(lambda x: getInfo(x['caseId'], surgeryReco),
                                                                      axis=1)
                appointmentDoneValid['otherEnquiryFollowUp'] = appointmentDoneValid.apply(lambda x: getInfo(str(x['caseId']), otherEnquiryCases),
                                                                      axis=1)
                appointmentDoneValid['otherComments'] = appointmentDoneValid.apply(lambda x: getInfo(str(x['appId']), otherApps),
                                                                      axis=1)
                appointmentDoneValid['pitchCharter'] = appointmentDoneValid.apply(lambda x: getInfo(str(x['caseId']), pitchCharterCases),
                                                                      axis=1)

        columns = ['Appointment_date', 'caseId', 'Cstmr_Contact_No', 'Agent_Action', 'Team_Leader', 'Total_Call_Duration', 'Sequence','Appt_type',
                 'Consultation_Fee', 'Surgery_reco_Y_N', 'City', 'Other_hospital_doctor_follow_up_reason', 'Other_in_comment', 'Pitch_Charter_from_new_crm']
        recs = []
        for index, value in appointmentDoneValid.iterrows():
            appId = value['caseId']
            users = {value['assignedTo']: 0}
            #if appId in followUpCommentDump.keys():
            #    fDump = followUpCommentDump[appId]
            #    for rowVal in fDump:
            #        if rowVal['user'] in users.keys():
            #            continue

            #        if rowVal['createdOn'] > value['createdOn']:  # FollowUp after app done
            #            break

            #        users[rowVal['user']] = 0
            if value['cxNo'] in exotelDump.keys():
                agentsDump = exotelDump[value['cxNo']]
                for agent, eDump in agentsDump.items():
                    if agent not in users.keys():
                        users[agent] = 0

                    for row1 in eDump:
                        if row1['createdOn'] > value['createdOn']:
                            break
                        
                        if row1['createdOn'] < value['caseCreatedOn']:
                            delta = value['caseCreatedOn'] - row1['createdOn']
                            diff = delta.seconds + delta.days*24*3600
                            if diff <= 1800:
                                users[agent] += row1['callTime']
                        
                        if row1['createdOn'] >= value['caseCreatedOn']:
                            users[agent] += row1['callTime']
                            
            if value['cxNo1'] in exotelDump.keys():
                agentsDump = exotelDump[value['cxNo1']]
                for agent, eDump in agentsDump.items():
                    if agent not in users.keys():
                        users[agent] = 0

                    for row1 in eDump:
                        if row1['createdOn'] > value['createdOn']:
                            break
                        
                        if row1['createdOn'] < value['caseCreatedOn']:
                            delta = value['caseCreatedOn'] - row1['createdOn']
                            diff = delta.seconds + delta.days*24*3600
                            if diff <= 1800:
                                users[agent] += row1['callTime']
                        
                        if row1['createdOn'] >= value['caseCreatedOn']:
                            users[agent] += row1['callTime']
            
            users = dict(sorted(users.items(), key=lambda item: item[1],reverse=True))
            counter = 1
            for userId, callD in users.items():
                if userId not in agent_nos.values(): 
                    continue
                
                if userId.lower() in Tls: #Remove Tl's 
                    continue
                
                if userId.lower() in ['vasu@ayu.health', 'prashanth.k@ayu.health', 'saaleh@ayu.health', 'hethesh@ayu.health', 'mayank@ayu.health']: #Remove Other team callee's
                    continue
                
                if callD < 60: #Call Duration should be more than 60 secs
                    continue
                
                #if counter > 2:
                #    break
                
                teamLeader = 'NA'
                if userId.lower() in TMMapping.keys():
                    teamLeader = TMMapping[userId.lower()]

                
                recs.append([
                    str(value['createdOn']),
                    value['caseId'],
                    value['customerNumber'],
                    userId,
                    teamLeader,
                    callD,
                    counter,
                    value['consultationType'],
                    value['consultationFee'],
                    value['surgeryReco'],
                    value['city'],
                    value['otherEnquiryFollowUp'],
                    value['otherComments'],
                    value['pitchCharter']
                ])
                
                counter += 1
                
        df = pd.DataFrame(recs, columns = columns)
        df[columns] = df[columns].astype(str)
        print(df.dtypes)
        upload_to_bq(df)

        # if k == '0':
        #     contoller(recs, 'currentWeekDump', 'A1:N')
        # elif k == '1':
        #     contoller(recs, 'lastWeekDump', 'A1:N')
        #lastWeekDump
        #3rd_to_9th_Jan

    except Exception as e:
        raise e
