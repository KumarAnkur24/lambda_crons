import pdb
import os
import logging
from service.database import InitDatabaseConnetion,make_db_params,fetch_record
from service.send_mail_client import send_email
import pandas as pd
import csv
from datetime import datetime, timedelta

from service.base_functions import exotel_cxNo, msg_to, agent_name_from_email  

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')


fpath = os.path.join('/tmp', 'exotel.csv')
fpath1 = os.path.join('/tmp', 'followUp.csv')
fpath2 = os.path.join('/tmp', 'booked.csv')
fpath3 = os.path.join('/tmp', 'app_handled.csv')
fpath4 = os.path.join('/tmp', 'active_time.csv')
fpath5 = os.path.join('/tmp', 'normal_call.csv')

 
yest =  datetime.now() + timedelta(hours=5, minutes=30, days=-1)
yest = yest.strftime('%Y-%m-%d')   
today = datetime.now()
today = today.strftime('%Y-%m-%d')
queries = {
            "Booked_aud": """select lda.id , date(date_add(lda.createdOn, INTERVAL '5:30' HOUR_MINUTE)) as booked_date ,user as 'email' , appointmentCreationType,
                                doctorConsultationStatus
                            from lead_doctor_consultation lda 
                    where date(date_add(lda.createdOn, INTERVAL '5:30' HOUR_MINUTE)) = curdate() - interval 1 day
                    and user in (Select email
                                    from 
                                        ayu_personnel_details
                                where
                                email in ('nisar.a@ayu.health','ashwini.s@ayu.health','nihal@ayu.health','madhura@ayu.health')
                                )
                """,  
            "Done" : """select lc.user,date(appointmentDate),doctorConsultationStatus 
                                from
                            lead_doctor_consultation ldc
                                join
                            lead_comments lc on (ldc.id = lc.leadId and lc.leadType = 'APPOINTMENT' and lc.commentType = 'APPOINTMENT_DONE')
                            where
                                date(appointmentDate) = curdate() - interval 1 day
                            and 
                                doctorConsultationStatus = 3
                            and lc.user in 
                            (Select email
                                    from 
                                        ayu_personnel_details
                                where
                                email in ('nisar.a@ayu.health','ashwini.s@ayu.health','nihal@ayu.health','madhura@ayu.health')
                                )
                                """,
            
            "Lead_Aud":'''select ldc.id as appId,
                                 ldc.leadId, 
                                 case when date(ldc.followUpDate) >= curdate() or ldc.followUpDate is null
                                            or mitra.email != log.email
                                        then 'FollowUpDone'
                                        else 'FollowUpPending' end as 'followUpCheck',
                                date(ldc.followUpDate) as 'latestFollowUpDate',
                                date(lda.followUpDate) as 'followUpDate',
                                ldc.doctorConsultationStatus,
                                log.email as ayuMitraEmail
                            from 
                                lead_doctor_consultation_AUD lda 
                                    join
                                lead_doctor_consultation ldc on lda.id = ldc.id
                                    join
                                ayu_personnel_details mitra on (ldc.ayuMitraId = mitra.personnelId and mitra.personnelType='AYU_MITRA')
                                    join
                                (select email,lda.id, max(REV) as REV 
                                            from
                                        lead_doctor_consultation_AUD lda
                                            join
                                        ayu_personnel_details mitra on (lda.ayuMitraId = mitra.personnelId and mitra.personnelType='AYU_MITRA')
                                        where 
                                            date(lda.followUpDate) = curdate() - interval 1 day
                                        and
                                            email in (Select email
                                    from 
                                        ayu_personnel_details
                                where
                        
                                 email in ('nisar.a@ayu.health','ashwini.s@ayu.health','nihal@ayu.health','madhura@ayu.health')
                                )
                                            group by lda.id,email
                                            ) log on lda.REV = log.REV and lda.id = log.id
                                
                                ''', 
     
            "exotelTat": """select *
                        from exotel_response 
                        where 
                            date( call_created_on) = curdate() - interval 1 day
                            order by call_created_on """,
            "agent_active": """select ac.id,
                              ac.activity->>'$.newAvailabilityValue' as 'newValue',
                              ac.activity->>'$.perviousAvailabilityValue' as 'previousValue',
                              ac.activity->>'$.reason' as 'reason',
                              date_add(ac.createdOn,interval '5:30' HOUR_MINUTE) as 'createdOn',
                              lower(email) as 'assignedTo' 
                    from 
                        ayu_personnel_activity ac 
                        join ayu_personnel_details cc on ac.ayuPersonnelId = cc.personnelId
                        where 
                            ayuPersonnelType = 'AYU_MITRA'
        
                                and email in ('nisar.a@ayu.health','ashwini.s@ayu.health','nihal@ayu.health','madhura@ayu.health') 
                                and date(date_add(ac.createdOn,interval '5:30' HOUR_MINUTE)) = curdate() - interval 1 day
                        order by createdOn""",
            
            'AyuMitra_agents':'''Select *
                                    from 
                                        ayu_personnel_details
                                where
                              email in ('nisar.a@ayu.health','ashwini.s@ayu.health','nihal@ayu.health','madhura@ayu.health')
            ''',
            'example': ''' select * from ayu_personnel_details '''
}
 
def fetch_data(conn): 
    fetched_val = {}
    for lookup, query in queries.items():
        print(lookup)
        fetched_val[lookup] = fetch_record(conn,query)
    return fetched_val




result_base = '''
        
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 8 width = \"1200\"><b><center> Central Team </center></b></td> 
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1 rowspan=2  width = \"150\"><b><center> Agents </center></b></td>
            <td colspan = 1 rowspan=2  width = \"150\"><b><center> Follow Up Assigned </center></b></td>
            <td colspan = 1 rowspan=2  width = \"150\"><b><center> Follow Ups Done </center></b></td>
            <td colspan = 2  width = \"300\"><b><center> New Appointments </center></b></td>
            <td colspan = 2  width = \"300\"><b><center> Follow Up Appointments </center></b></td>
            <td colspan = 1  rowspan=2 width = \"150\"><b><center> Appointments Handled </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"150\"><b><center> Booked  </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Done  </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Booked  </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Done  </center></b></td>
            
        </tr>  '''
        

result_base1 = '''
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 9 width = \"1200\"><b><center> Exotel | {0} Calls </center></b></td> 
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"150\"><b><center> Agents </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> {1} </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Call {2} </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Busy/No Answer </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Failed </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Cancelled </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Pickup Rate </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Total Duration </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Avg Call Handling Time </center></b></td>
        </tr> 
    ''' 
    
result_base2 = '''
<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 4 width = \"1200\"><b><center> Yesterday Total Active Hours </center></b></td> 
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"300\"><b><center> Agents </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> Login Time </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> Total Active Hours </center></b></td>
            <td colspan = 1  width = \"300\"><b><center> Break Hours </center></b></td>
        </tr> 
    '''

result_base3 = '''
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        <tr bgcolor=\"#D1DCB2\"> 
           <td colspan = 9 width = \"1200\"><b><center> Exotel | {0} </center></b></td> 
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"150\"><b><center> Agents </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> {1} </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Call {2} </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Busy/No Answer </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Failed </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Cancelled </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Pickup Rate </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Total Duration </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Avg Call Handling Time </center></b></td>
        </tr> 
    '''
    
def agent_call_duration(direction, leg_2_status, leg_2_duration, leg_1_duration, leg_1_status):

    if direction == 'inbound' and leg_2_status == 'completed': 
        return leg_2_duration
    elif direction in ['outbound-api', 'outbound-dial'] and leg_1_status == 'completed':
        return leg_1_duration
    elif direction == 'normal-call' and leg_1_status == 'completed':
        return leg_1_duration



def eligible_call(direction, leg_2_status, leg_1_status):

    if direction == 'inbound' and leg_2_status== 'completed': 
        return 1
    elif direction in ['outbound-api', 'outbound-dial'] and leg_1_status=='completed':
        return 1
    elif direction == 'normal-call' and leg_1_status == 'completed':
        return 1
    return 0

def agent_number(direction, to_no, from_no):

    if direction == 'inbound':
        return msg_to(to_no)
    elif direction in ['outbound-api', 'outbound-dial']:
        return msg_to(from_no)
    elif direction =="normal-call":
        return msg_to(from_no)

def convert_to_hours(seconds):

    if seconds == 0:
        return '0 sec'
    hrs = '{:.0f}'.format(seconds // (60 * 60))
    mins = '{:.0f}'.format((seconds % 3600) // 60)
    secs = '{:.0f}'.format(((seconds % 3600) % 60))
    if hrs == '0':
        if mins == '0':
            return '{secs} sec'.format(secs=secs)
        return '{mins} min {secs} sec'.format(mins=mins, secs=secs)
    return '{hrs} hr {mins} min'.format(hrs=hrs, mins=mins)



def compute_result(row, col, result, tableD):
     
    for i in range(0, row): 
        result = result + "<tr>"
        for j in range(0, col):
            if j == 0:
                # print(i)
                # print("1st",tableD[i][0])
                result = result + "<td colspan = 1 ><center>" + str(tableD[i][j]) + "</center></td>"
            elif tableD[i][j] == 0:
                # print(i)
                # print("2nd",tableD[i][0])
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                # print(i)
                # print("3rd",tableD[i][0])
                result = result + "<td colspan = 1 ><center>" + str(tableD[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"
    return result

def select_data(exotelTat, Booked_aud, Done, AyuMitra_agents, Lead_Aud): 
    
    agents = {}
    DATA = []
    table = [[ 0 for x in range(8)]for x in range(50)]  
    table1 = [[ 0 for x in range(9)]for x in range(50)]  
    table2 = [[ 0 for x in range(9)]for x in range(50)]
    table3 = [[0 for x in range(9)] for x in range(50)]
    index_dict = {}
    counter = 0
    
    for index,val in AyuMitra_agents.iterrows():
        agent_no = msg_to(val['phone'])
        agent_email = val['email'].lower()
        
        if agent_no not in agents.keys():
            agents[agent_no] = agent_email
            
            index_dict[agent_email] = counter
            table[counter][0] = agent_email
            table1[counter][0] = agent_email
            table2[counter][0] = agent_email
            table3[counter][0] = agent_email
            counter += 1 
        
    for index,B_val in Booked_aud.iterrows():
        
        appt_cre_type = B_val['appointmentCreationType']
        agentEmail = B_val['email'].lower()
        
        if appt_cre_type == 'FOLLOWUP_APPOINTMENT':
            table[index_dict[agentEmail]][5] += 1
            if B_val['doctorConsultationStatus'] == '3':
                table[index_dict[agentEmail]][6] += 1
            #table[index_dict[agentEmail]][8] += 1
        else:
            table[index_dict[agentEmail]][3] += 1
            if B_val['doctorConsultationStatus'] == '3':
                table[index_dict[agentEmail]][4] += 1
            #table[index_dict[agentEmail]][8] += 1
    
    
    for index,D_val in Done.iterrows():
    
        agentEmail = D_val['user'].lower()
        table[index_dict[agentEmail]][7] += 1
        
    
            
    exotelTat['agentNumber'] = exotelTat.apply(lambda x: agent_number(x['direction'],x['to_no'],x['from_no']),axis=1)
    exotelTat['CallDuration'] = exotelTat.apply(lambda x: agent_call_duration(x['direction'],x['leg_2_status'],
                                x['leg_2_duration'],x['leg_1_duration'], x['leg_1_status']),axis=1) 
                                
    
    
    #exotelTat['Eligible_CAll'] =  exotelTat.apply(lambda x: eligible_call(x['direction'],x['leg_2_status'], x['leg_1_status']),axis=1)
    
    for index, exo_val in exotelTat.iterrows():
        if exo_val['agentNumber'] in agents.keys():
            agents_email = agents[exo_val['agentNumber']]

            #table[index_dict[agents_email]][3] += exo_val['Eligible_CAll']
            #table[index_dict[agents_email]][4] += exo_val['CallDuration']   
            #ringingDuration = (exo_val['duration'] - exo_val['leg_1_duration'])
            
            if exo_val['direction'] == 'inbound':
                table2[index_dict[agents_email]][1] += 1
                if exo_val['leg_2_status'] == 'completed':
                    table2[index_dict[agents_email]][2] += 1
                    table2[index_dict[agents_email]][7] += exo_val['CallDuration']
                    table2[index_dict[agents_email]][8] += 1
                    
                    
                elif exo_val['leg_2_status'] in ['busy', 'no-answer']:
                    table2[index_dict[agents_email]][3] += 1
                elif exo_val['leg_2_status'] in ['canceled']:
                    table2[index_dict[agents_email]][5] += 1
                elif exo_val['leg_2_status'] in ['failed']:
                    table2[index_dict[agents_email]][4] += 1
                else:
                    if exo_val['leg_1_status'] in ['busy', 'no-answer']:
                        table2[index_dict[agents_email]][3] += 1
                    else:
                        table2[index_dict[agents_email]][4] += 1
                
            elif exo_val['direction'] in ['outbound-api', 'outbound-dial']:
                
                table1[index_dict[agents_email]][1] += 1
            
                if exo_val['leg_1_status'] == 'completed':
                    table1[index_dict[agents_email]][7] += exo_val['CallDuration']
                    table1[index_dict[agents_email]][8] += 1
                    
                if exo_val['leg_2_status'] == 'completed':
                    table1[index_dict[agents_email]][2] += 1
                elif exo_val['leg_2_status'] in ['busy', 'no-answer']:
                    table1[index_dict[agents_email]][3] += 1
                elif exo_val['leg_2_status'] in ['canceled']:
                    table1[index_dict[agents_email]][5] += 1
                elif exo_val['leg_2_status'] in ['failed']:
                    table1[index_dict[agents_email]][4] += 1
                else:
                    if exo_val['leg_1_status'] in ['busy', 'no-answer']:
                        table1[index_dict[agents_email]][3] += 1
                    else:
                        table1[index_dict[agents_email]][4] += 1
                            
            else:
                table3[index_dict[agents_email]][1] += 1
               
                if exo_val['leg_1_status'] == 'completed':
                    if exo_val['leg_1_duration'] >= 60:
                        table3[index_dict[agents_email]][7] += exo_val['CallDuration']
                        table3[index_dict[agents_email]][8] += 1
                    
                if exo_val['leg_1_status'] == 'completed':
                    if exo_val['leg_1_duration'] >= 60:
                        table3[index_dict[agents_email]][2] += 1
                    else:
                        table3[index_dict[agents_email]][4] += 1
                elif exo_val['leg_2_status'] in ['busy', 'no-answer']:
                    table3[index_dict[agents_email]][3] += 1
                elif exo_val['leg_2_status'] in ['canceled']:
                    table3[index_dict[agents_email]][5] += 1
                elif exo_val['leg_2_status'] in ['failed']:
                    table3[index_dict[agents_email]][4] += 1
                else:
                    if exo_val['leg_1_status'] in ['busy', 'no-answer']:
                        table3[index_dict[agents_email]][3] += 1
                    else:
                        table3[index_dict[agents_email]][4] += 1
        
    
            
    for index,L_val in Lead_Aud.iterrows(): 
        agentEmail = L_val['ayuMitraEmail'].lower()
        if L_val['followUpCheck'] == 'FollowUpDone':  
            table[index_dict[agentEmail]][2] += 1  
        
        table[index_dict[agentEmail]][1] += 1
        
    
        
    n = len(index_dict)
    
    for i in range(0, n):
        table1[i][8] = (table1[i][7] // table1[i][8]) if table1[i][8] != 0 else 0
        table2[i][8] = (table2[i][7] // table2[i][8]) if table2[i][8] != 0 else 0
        table3[i][8] = (table3[i][7] // table3[i][8]) if table3[i][8] != 0 else 0
        table1[i][6] = round(table1[i][2] * 100 / table1[i][1],2) if table1[i][2] != 0 else 0
        table2[i][6] = round(table2[i][2] * 100 / table2[i][1],2) if table2[i][2] != 0 else 0
        table3[i][6] = round(table3[i][2] * 100 / table3[i][1],2) if table3[i][2] != 0 else 0
    
    
    for i in range(0, n):
        #table[i][4] = convert_to_hours(table[i][4])
        table1[i][7] = convert_to_hours(table1[i][7])
        table2[i][7] = convert_to_hours(table2[i][7])
        table3[i][7] = convert_to_hours(table3[i][7])
        table1[i][8] = convert_to_hours(table1[i][8])
        table2[i][8] = convert_to_hours(table2[i][8]) 
        table3[i][8] = convert_to_hours(table3[i][8])
    
    
    
    
    # result = result_base
    # result = compute_result(n, 8, result, table)
    
    result = result_base1.format('Outbound', 'Total Call Attempts', 'Connected')
    result = compute_result(n, 9, result, table1)
    
    result += result_base3.format('normal-call', 'Total Call Attempts', 'Connected')
    result = compute_result(n, 9, result, table3)
    
    # result += result_base1.format('Inbound', 'Total Incoming Calls', 'Picked')
    # result = compute_result(n, 9, result, table2)
    
    return result



def prepare_result(agentActive):
    
    table = [[ 0 for x in range(8)]for x in range(50)]  
    assignedToRow = {}
    rowIndex = 0
    firstFlag = {}
    for index, val in agentActive.iterrows():

        agent = val['assignedTo'].strip()
        delta = 0
        
        if agent not in assignedToRow.keys():
            assignedToRow[agent] = rowIndex
            table[rowIndex][0] = agent
            firstFlag[agent] = True
            rowIndex += 1
            
        if val['newValue'] == 'true' or val['reason'] == 'Going for a meeting':
            
            if firstFlag[agent]:
                table[assignedToRow[agent]][1] = val['createdOn'].strftime('%H:%M')
                firstFlag[agent] = False
                
            if pd.isna(val['newValue_lead']):
                diff = (datetime.now() + timedelta(hours=5, minutes=30, days=-1)).replace(hour=21, minute=0, second=0) - val['createdOn']
                #time_diff = diff.seconds
                delta = (diff.days * 24 * 3600) + diff.seconds
                
            else:
                diff = val['createdOn_lead'] - val['createdOn']
                delta = (diff.days * 24 * 3600) + diff.seconds
                
            table[assignedToRow[agent]][2] += delta
            
        
        breakHours = 0
        if val['newValue'] == 'false' and val['reason'] == 'General break' :
        
            if pd.isna(val['newValue_lead']):
                currentTime = (datetime.now() + timedelta(hours=5, minutes=30,days=-1))
                currentTime = currentTime.replace(hour = 21,minute = 0,second =0)
                
                diff = currentTime - val['createdOn']
                breakHours = (diff.days * 24 * 3600) + diff.seconds 
                
            else:
                diff = val['createdOn_lead'] - val['createdOn']
                breakHours = (diff.days * 24 * 3600) + diff.seconds 
                
            table[assignedToRow[agent]][3] += breakHours
            
    for j in (2, 3):
        for i in range(0, rowIndex):
            table[i][j] = convert_to_hours(table[i][j])
            
    result = result_base2
    result = compute_result(rowIndex, 4, result, table)
    
    return result
 




def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
                                                    
        
        dataSets = fetch_data(read_connection_obj)
        exotelTat = pd.DataFrame(dataSets['exotelTat'])
        Booked_aud = pd.DataFrame(dataSets['Booked_aud'])
        Done = pd.DataFrame(dataSets['Done'])
        Lead_Aud = pd.DataFrame(dataSets['Lead_Aud'])
        example = pd.DataFrame(dataSets['example'])
        
        
        AyuMitra_agents = pd.DataFrame(dataSets['AyuMitra_agents'])
        
        
        agent_active = pd.DataFrame(dataSets['agent_active'])
        print(len(agent_active))  
        if len(agent_active):
            agent_active['createdOn_lead'] = agent_active.groupby(['assignedTo'])['createdOn'].shift(-1)
            #agent_active['newValue_lag'] = agent_active.groupby(['assignedTo'])['newValue'].shift(1)
            agent_active['reason_lead'] = agent_active.groupby(['assignedTo'])['reason'].shift(-1)
            agent_active['newValue_lead'] = agent_active.groupby(['assignedTo'])['newValue'].shift(-1)
            #agent_active['agentCity'] = agent_active.apply(lambda x: getCity(x['assignedTo'], agentsDump), axis=1)
            
            
        agent_active.to_csv(fpath4)
        
        result = select_data(exotelTat,Booked_aud,Done,AyuMitra_agents,Lead_Aud) 
        
        resultA = prepare_result(agent_active)
        result_ = resultA + result
        
        normal_call = exotelTat[exotelTat['direction'] == 'normal-call'] 
        
        
        exotelTat.to_csv(fpath)
        Lead_Aud.to_csv(fpath1)
        Booked_aud.to_csv(fpath2)
        Done.to_csv(fpath3)
        normal_call.to_csv(fpath5) 
        
        Subject = "Experience Team Calling Report" 
        # email_recipient_list =['nikunj.r@ayu.health','ankur@ayu.health']
        email_recipient_list = ['akchansh@ayu.health','namitha@ayu.health','experience-team@ayu.health','ashwini.s@ayu.health','nisar.a@ayu.health','nihal@ayu.health','madhura@ayu.health']    
        send_email(None, email_recipient_list, Subject, 'None',result_,[fpath,fpath4,fpath5])   
        
    except Exception as e:
        Subject = "Experience Team Calling Report ERROR (automated_report_Central_Calling_Team_For_Few_Agents)" 
        email_recipient_list = ['analytics@ayu.health']     
        send_email(None, email_recipient_list, Subject, 'None',e,[]) 
        raise e
