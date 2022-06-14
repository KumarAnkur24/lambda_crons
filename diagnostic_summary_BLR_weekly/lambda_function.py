import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
import json
from service.base_functions import msg_to
from datetime import datetime , timedelta
from service.send_mail_client import send_email


logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

yesterday = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days=1)
mtd = yesterday.replace(day=1)
week = yesterday - timedelta(days = 7)


yesterday = yesterday.date()
mtd = mtd.date()
week = week.date()

start_day = datetime.now() + timedelta(days=-datetime.now().weekday(), weeks=1 , hours =5 ,minutes =30) - timedelta(days=7)
start_day = start_day.date()

end_day = datetime.now() + timedelta(days=-datetime.now().weekday(), weeks=1 , hours =5 ,minutes =30) - timedelta(days=1)
end_day = end_day.date()

queries = {'diagnostics':'''select ldc.id as apptId,
                      leadId,
                      date(appointmentDate) as appt_date,
                      appointmentCreationType,
                      consultationType,
                      ldc.consultationFee,
                      ldc.user,
                      doctorConsultationStatus,
                      month(appointmentDate) as 'month',
                      week(appointmentDate) as 'week',
                      aliasName,
                      case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                            else 'No City' end as city
                      
                    from
                      
                lead_doctor_consultation ldc
                left join ayu_facility_profile afp on ldc.hospitalId = afp.facilityId
                left join ayu_cities pcp on afp.cityId = pcp.id
                where
                        consultationType = 'DIAGNOSTICS'
                        and date(appointmentDate) >= '{start}'
                        and date(appointmentDate) <= '{end}'
                        and afp.cityId in (1,2,3,4)
                        and doctorConsultationStatus = 3 
''',

            'ayu_mitra':''' select email ,personnelId from ayu_personnel_details
                                where 
                                    personnelType = 'AYU_MITRA'
            ''',

            "diag_test": """select ldc.id as apptId,
                      leadId, 
                      date(appointmentDate) as appt_date,
                      appointmentCreationType,
                      consultationType,
                      ldc.consultationFee,
                      cp.customerNumber,
                      ldc.user,
                      aliasName , 
                      case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                            else 'No City' end as city,
                        ldc.additionalDetails->>'$.isDiagnosticsTestRecommended' as DiagnosticsRecomended
                    from
                     
                lead_doctor_consultation ldc
                left join ayu_facility_profile afp on ldc.hospitalId = afp.facilityId
                join patient_case pc on pc.caseId = ldc.leadId
                join patient_profile pp on pp.id = pc.patientId
                join customer_profile cp on cp.customerId = pp.customerId
                left join ayu_cities pcp on afp.cityId = pcp.id
                
                where
                                    
                        doctorConsultationStatus = 3
                        and date(ldc.appointmentDate)>= '{start}'
                        and date(ldc.appointmentDate)<= '{end}'
                        and consultationType != 'DIAGNOSTICS'
                        and afp.cityId in (1,2,3,4)
                        """,
    
            'diag_done':'''select 
                                leadId , id as apptId , originConsultationId
                                from lead_doctor_consultation
                                where 
                                    doctorConsultationStatus = 3
                                    and consultationType = 'DIAGNOSTICS'
                                    and leadId in ({leadIds})
            
            ''',
            
   #         'lead_doc_consult':'''select leadId , id as apptId , ayuMitraId
    #                                from lead_doctor_consultation_AUD
    #                                where  
    #                                     doctorConsultationStatus = 3
    #                                     and id in ({ids})
    #                                order by createdOn
    #        ''',
            
            "lead_doc_consult": """select leadId as appId,text,user
                        from
                        lead_comments lc
                        where 
                            commentType in ('APPOINTMENT_DONE') 
                            and leadType = 'APPOINTMENT'
                            and leadId in ({ids})
                        order by commentId
                        """
    
}


DoctorConsultationStatus = {
    '0': 'OPEN',
    '1': 'BOOKED',
    '2': 'CONFIRMED',
    '3': 'DONE',
    '4': 'CANCELLED',
    None: 'None',
    '': 'None',
    '5': 'OPEN',
    '6': 'CONFIRMED',
    '7': 'CONFIRMED'
}

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == 'diag_test':
            fetched_val[lookup] = fetch_record(conn, query.format(start = start_day,end = end_day))
            
            caseIds = [str(x['leadId']) for x in fetched_val[lookup]]
            apptId1 = [str(x['apptId']) for x in fetched_val[lookup]]
            appt_Id.extend(apptId1)
            
        elif lookup == 'diag_done':
            leadId = ','.join(caseIds)
            fetched_val[lookup] =  fetch_record(conn,query.format(leadIds = leadId))
            
        elif lookup == 'diagnostics':
            fetched_val[lookup] = fetch_record(conn, query.format(start = start_day,end = end_day))
            appt_Id = [str(x['apptId']) for x in fetched_val[lookup]]
            
            
        elif lookup == 'lead_doc_consult':
            Ids = ','.join(appt_Id)
            fetched_val[lookup] = fetch_record(conn,query.format(ids = Ids))
            
        else:
            fetched_val[lookup] = fetch_record(conn,query) 
            
        
        
    return fetched_val 
          

result_base = '''<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 13 width = \"1600\"><b><center> Diagnostic Report | Ayu Mitra </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1 rowspan=3  width = \"100\"><b><center> Ayu Mita </center></b></td>
            <td colspan = 6  width = \"750\"><b><center> Weekly </center></b></td>
            
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 3  width = \"375\"><b><center> Diagnostic Recommended  </center></b></td>
            <td colspan = 1 rowspan=2 width = \"125\"><b><center> Diagnostics Done  </center></b></td>
            <td colspan = 1 rowspan=2 width = \"125\"><b><center> Zero-Count  </center></b></td>
            <td colspan = 1 rowspan=2 width = \"125\"><b><center> Value  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\">
            <td colspan = 1  width = \"125\"><b><center> Yes </center></b></td>
            <td colspan = 1  width = \"125\"><b><center> Done </center></b></td>
            <td colspan = 1  width = \"125\"><b><center> Done %age </center></b></td>
        </tr>
'''

result_base2 = '''<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 13 width = \"1600\"><b><center> Diagnostic Report | Hospital </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1 rowspan=3  width = \"100\"><b><center> Ayu Mita </center></b></td>
            <td colspan = 6  width = \"750\"><b><center> Weekly </center></b></td>
            
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 3  width = \"375\"><b><center> Diagnostic Recommended  </center></b></td>
            <td colspan = 1 rowspan=2 width = \"125\"><b><center> Diagnostics Done  </center></b></td>
            <td colspan = 1 rowspan=2 width = \"125\"><b><center> Zero-Count  </center></b></td>
            <td colspan = 1 rowspan=2 width = \"125\"><b><center> Value </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\">
            <td colspan = 1  width = \"125\"><b><center> Yes </center></b></td>
            <td colspan = 1  width = \"125\"><b><center> Done </center></b></td>
            <td colspan = 1  width = \"125\"><b><center> Done %age </center></b></td>
        </tr>
'''

def agent_type(user,ayu_mitra_dump):
    
    if user in ayu_mitra_dump.keys():
        return ayu_mitra_dump[user]
        
    return 'CS Team'

def agentinfo(apptId,leads_dump):
    
    if apptId in leads_dump.keys():
        return leads_dump[apptId]
        
    return ''

def Diagnostics_done(apptId,diag_done_dump):
    
    if apptId in diag_done_dump.keys():
        return 'Yes'
        
    return 'No'
    
    
def select_data(diagnostics,diag_test):
    
    table = [[0 for x in range(13)]for x in range(100)]
    table1 = [[0 for x in range(13)]for x in range(100)]
    
    agent_dict = {}
    hospital_dict = {}
    
    appt = {}
    
    rindex = 0
    row = 0
    
    for index,val in diagnostics.iterrows():
        consultationFee = float(val['consultationFee'])
        if val['agent'] not in agent_dict.keys():
            agent_dict[val['agent']] = rindex
            table[rindex][0] = val['agent']
            table[rindex][1] = 0                                                    #Diagnostic Recommended Yes Count (WEEKLY) - AYUMITRA
            table[rindex][2] = 0                                                    #Diagnostic Recommended Done Count (WEEKLY) - AYUMITRA
            table[rindex][3] = 0                                                    # Done percentage
            table[rindex][4] = 0                                                    #Total Diagnostics Done (WEEKLY) - AYUMITRA
            table[rindex][5] = 0                                                    #Zero Value Count (WEEKLY) - AYUMITRA
            table[rindex][6] = 0                                                    #Amount for Diagnostic (WEEKLY) - AYUMITRA
            
            
            rindex += 1
            
        if val['aliasName'] not in hospital_dict.keys():
            hospital_dict[val['aliasName']] = row
            table1[row][0] = val['aliasName']
            table1[row][1] = 0
            table1[row][2] = 0
            table1[row][3] = 0
            table1[row][4] = 0
            table1[row][5] = 0
            table1[row][6] = 0
            
            row += 1
        
        
        if val['appt_date'] >= week and val['appt_date'] <= yesterday:
            if consultationFee != float(0):
                table[agent_dict[val['agent']]][4] += 1
                table[agent_dict[val['agent']]][6] += consultationFee
                
                table1[hospital_dict[val['aliasName']]][4] += 1
                table1[hospital_dict[val['aliasName']]][6] += consultationFee
                
            else:
                table[agent_dict[val['agent']]][4] += 1
                table[agent_dict[val['agent']]][5] += 1
                
                table1[hospital_dict[val['aliasName']]][4] += 1
                table1[hospital_dict[val['aliasName']]][5] += 1
        
            
    for index,val in diag_test.iterrows():
        if val['apptId'] in appt.keys():
            continue
        
        if val['agent'] not in agent_dict.keys():
            agent_dict[val['agent']] = rindex
            table[rindex][0] = val['agent']
            
            rindex += 1
            
        if val['aliasName'] not in hospital_dict.keys():
            hospital_dict[val['aliasName']] = row
            table1[row][0] = val['aliasName']
            
            row += 1
        
       
        
        if val['appt_date'] >= week and val['appt_date'] <= yesterday:  
            if val['DiagnosticsRecomended'] == 'YES':
                table[agent_dict[val['agent']]][1] += 1    
                
                table1[hospital_dict[val['aliasName']]][1] += 1
        
                
            if val['diagnostics_done'] == 'Yes':
                table[agent_dict[val['agent']]][2] += 1
                
                table1[hospital_dict[val['aliasName']]][2] += 1
    
        # Percentage done for agents
            
        if table[agent_dict[val['agent']]][1] != 0:    
            table[agent_dict[val['agent']]][3] = round((table[agent_dict[val['agent']]][2]*100)/table[agent_dict[val['agent']]][1],2)
        
        else:
            table[agent_dict[val['agent']]][3] = 0
        
        
        # Percentage done for hospitals
        
        if table1[hospital_dict[val['aliasName']]][1] != 0:
            table1[hospital_dict[val['aliasName']]][3] = round((table1[hospital_dict[val['aliasName']]][2]*100)/table1[hospital_dict[val['aliasName']]][1],2)
            
        else:
            table1[hospital_dict[val['aliasName']]][3] = 0
    
    result = result_base
    
    table[rindex][0] = 'Total'
    for i in range(0,rindex):
        for j in (1,2,3,4,5,6):
            table[rindex][j] += table[i][j] 
    
    table[rindex][3] = round((table[rindex][2]*100)/table[rindex][1],2)
    result = resultant_table(table,rindex+1,7,result)
    
    result_b = result_base2
    table1[row][0] = 'Total'
    for i in range(0,row): 
        for j in (1,2,4,5,6):
            table1[row][j] += table1[i][j] 
            
    table1[row][3] = round((table1[row][2]*100)/table1[row][1],2)
    
    result_b = resultant_table(table1,row+1,7,result_b) 
    
    Result = result + result_b
    
    return Result


def resultant_table(table,rows,columns,result):  
    for i in range(0, rows):
        result = result + "<tr>"
        for j in range(0, columns):
            if j == 0:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif j in (3,9):
                if table[i][j] == 0:
                    if table[i][j-2] > 0:
                        result = result + "<td colspan = 1 bgcolor=\"#F74035\"><center>" + str(table[i][j]) + "</center></td>"
                    else:
                        result = result + "<td colspan = 1 ><center> </center></td>"
                elif table[i][j] < float(50):
                    result = result + "<td colspan = 1 bgcolor=\"#F74035\"><center>" + str(table[i][j]) + "</center></td>"
                elif table[i][j] < float(70):
                    result = result + "<td colspan = 1 bgcolor=\"#FCC5C1\"><center>" + str(table[i][j]) + "</center></td>"
                else:
                    result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
                    
            elif table[i][j] == 0:
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"

    return result

fpath = os.path.join('/tmp','diagnostics.csv')
fpath1 = os.path.join('/tmp','diag_test.csv')

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        
        diagnostics = pd.DataFrame(dataSets['diagnostics'])
        
        print("yesterday",yesterday)
        print(diagnostics['appt_date'])
        
        diagnostics['Doctor_Consultation_Status'] = diagnostics.apply(lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']],axis=1)
        
        
        ayu_mitra = pd.DataFrame(dataSets['ayu_mitra']) 
        
        
        ayu_mitra_dump = {}
        for index,val in ayu_mitra.iterrows():
            if val['personnelId'] not in ayu_mitra_dump.keys():
                ayu_mitra_dump[val['personnelId']] = val['email']
        
        #diagnostics['agent'] = diagnostics.apply(lambda x: agent_type(x['ayuMitraId'],ayu_mitra_dump),axis=1) 
        
        diag_test = pd.DataFrame(dataSets['diag_test'])
        # print(diag_test.columns)
        
        #diag_test['agent'] = diag_test.apply(lambda x: agent_type(x['ayuMitraId'],ayu_mitra_dump),axis=1)
        
        diag_done = dataSets['diag_done']
        
        diag_done_dump = {}
        for val in diag_done:
            if val['originConsultationId'] == None or val['originConsultationId'] == '':
                continue
            originConsultationId = int(val['originConsultationId'])
            if originConsultationId not in diag_done_dump.keys():
                diag_done_dump[originConsultationId] = ''
    
        
        diag_test['diagnostics_done'] = diag_test.apply(lambda x: Diagnostics_done(x['apptId'],diag_done_dump),axis=1)
        
        #print(diag_test[diag_test['diagnostics_done']=='Yes'])
        
        lead_doc_consult = pd.DataFrame(dataSets['lead_doc_consult'])
        
        #lead_doc_consult['agent'] = lead_doc_consult.apply(lambda x: agent_type(x['ayuMitraId'],ayu_mitra_dump),axis=1)
        
        leads_dump = {}
        
        for index,val in lead_doc_consult.iterrows():
            if val['appId'] not in leads_dump.keys():
                leads_dump[val['appId']] = val['user']
                
        diagnostics['agent'] = diagnostics.apply(lambda x: agentinfo(x['apptId'],leads_dump),axis=1)
        
        diag_test['agent'] = diag_test.apply(lambda x: agentinfo(x['apptId'],leads_dump),axis = 1)
        
        diagnostics.to_csv(fpath)
        diag_test.to_csv(fpath1)
        
        today = datetime.now() + timedelta(hours=5,minutes=30)
        today = today.strftime('%Y-%m-%d')
        
        city_email_map = {
            'BLR' : ['city-managers-leads@ayu.health','arjit@ayu.health', 'karan@ayu.health'],
            'CHD' : ["upender@ayu.health", "jay@ayu.health", "rohit.chahal@ayu.health", "shahwaz@ayu.health", 'arjit@ayu.health', 'karan@ayu.health'],
            'Jaipur': ["jay@ayu.health", "rohit.chahal@ayu.health", "lalit@ayu.health", "chandni@ayu.health", 'arjit@ayu.health', 'karan@ayu.health'],
            'NCR': ["jay@ayu.health", "rohit.chahal@ayu.health", "karan@ayu.health", "vishal.khobra@ayu.health", "vibhor@ayu.health", "harshita@ayu.health", "shubham.mukherjee@ayu.health", 'arjit@ayu.health']
        }
        
        
        for city in ['BLR']:
            
            diagnostics_city = diagnostics[diagnostics['city'] == city]
            diag_test_city= diag_test[diag_test['city'] == city]
            Result = select_data(diagnostics_city,diag_test_city)
        
            Subject = "Diagnostics Report {0} |".format(city) + str(start_day) +'-'+ str(end_day) 
            # email_recipient_list = city_email_map[city]
            #print(email_recipient_list)
            email_recipient_list = ['city-managers-leads@ayu.health'] 
            #email_recipient_list = ['akchansh@ayu.health']
            send_email(None, email_recipient_list, Subject, 'None',Result,[fpath,fpath1])  
        
    except Exception as e:
        raise e
    