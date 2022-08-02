import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
from service.send_mail_client import send_email
from service.base_functions import msg_to
import pandas as pd
from datetime import datetime, timedelta
import pytz
import calendar
 
logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {"consultation" : """ select pc.patientId,ldc.leadId as caseId,ldc.id as apptId, pp.customerId,
                                date(ldc.appointmentDate) as appointmentDate,
                                date(ldc.createdOn) as caseCreatedOn,
                                ldc.consultationFee,pd.paymentMode,
                                case when ac.cityName = 'Chandigarh' then 'CHD'
                                when ac.cityName = 'Bangalore' then 'BLR'
                                when ac.cityName is not null then ac.cityName
                                else 'No City' end as city,
                                
                                week(date(ldc.appointmentDate)) as 'leadWeek',
                                month(date(ldc.appointmentDate)) as 'leadMonth',
                                year(date(ldc.appointmentDate)) as 'leadyear',pc.leadSource,
                                date(activationDate) as issuDate 
                                
                                
                                from lead_doctor_consultation ldc 
                                left join ayu_facility_profile afp on afp.facilityId = ldc.hospitalId
                                left join ayu_cities ac on afp.cityId = ac.id
                                left join patient_case pc on pc.caseId = ldc.leadId
                                left join patient_profile pp on pc.patientId = pp.id
                                left join customer_profile cp on pp.customerId = cp.customerId
                                left join online_payment_link pd on pd.entityId =ldc.leadId
                                left join (select customerId,max(activationDate) as activationDate from generated_mcards group by 1)
                                card on card.customerId = cp.customerId
                                
                                where appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT')
                                and ldc.hospitalId != '224'
                                and doctorConsultationStatus ='3'
                                and pc.leadSource not in ('Offline channel','VC Model')
                                and pc.caseType != 'Diagnostics' 
                                and ldc.user in (select email from customer_support_details) """,
                          
            
                                
            "card" :""" select entityId as lcappId,his.cardId,leadId,patientId,transactionStatus,his.units ,pp.customerId,
                        date_add(his.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'transactionOn'
                        from 
                        mcard_transaction_history his
                        join lead_doctor_consultation ldc on his.entityId = ldc.id
                        join patient_case pc on ldc.leadId = pc.caseId 
                        join patient_profile pp on pc.patientId = pp.id
                        join customer_profile cp on pp.customerId = cp.customerId
                        
                        where
                        transactionType = 'DEBIT'
                        and transactionStatus = 'CONFIRMED'
                        and entityType = 'APPOINTMENT'
                        order by his.createdOn""",
                        
            "new_users" :""" select customerId,date(activationDate) as issuDate
                            from 
                                generated_mcards
"""
} 
    
    # (DATEPART(week, @date_given) - DATEPART(week, DATEADD(day, 1, EOMONTH(@date_given, -1)))) + 1

today = datetime.now() + timedelta(hours = 5, minutes = 30)
today = today.date()
d1 = today-timedelta(days=1)
d2 = today-timedelta(days=2) 
d3 = today-timedelta(days=3)
d4 = today-timedelta(days=4)
yeard = today.year
# print(yeard)


month = d1.strftime('%m')
print(month)
month_start = datetime.now().replace(day=1).date()
# print(month_start)
month_days = calendar.monthrange(today.year, today.month)[1]
month_last = month_start - timedelta(days=1)
# print(month_last)



result_base = '''<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor = "\#99A3A4"> 
        <td colspan = 12 width = \"1300\"><b><center> {0} - OPD  </center></b></td>
    </tr>
    <tr bgcolor=\"#BDC3C7\"> 
        <td colspan = 1  width = \"300\"><b><center> Date </center></b></td>
        <td colspan = 2  width = \"200\"><b><center> D-Day </center></b></td>
        <td colspan = 2  width = \"200\"><b><center> D-1  </center></b></td>
        <td colspan = 2  width = \"200\"><b><center> D-2 </center></b></td>
        <td colspan = 2  width = \"200\"><b><center> D-3 </center></b></td>
        <td colspan = 2  width = \"200\"><b><center> MTD </center></b></td>
    </tr>
    
    <tr bgcolor=\"#A6ACAF \"> 
        <td colspan = 1  width = \"300\"><b><center> Fees </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
    </tr> '''
    
result_base1 = '''<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr bgcolor = "\#99A3A4"> 
        <td colspan = 13 width = \"1100\"><b><center> {0} - OPD  </center></b></td>
    </tr>
    <tr bgcolor=\"#BDC3C7\"> 
        <td colspan = 1  width = \"300\"><b><center> Date </center></b></td>
        <td colspan = 3  width = \"200\"><b><center> Week 4 </center></b></td>
        <td colspan = 3  width = \"200\"><b><center> Week 3  </center></b></td>
        <td colspan = 3  width = \"200\"><b><center> Week 2 </center></b></td>
        <td colspan = 3  width = \"200\"><b><center> Week 1 </center></b></td>
    </tr>
    
    <tr bgcolor=\"#A6ACAF \"> 
        <td colspan = 1  width = \"100\"><b><center> Fees </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan = 1  width = \"100\"><b><center> Avg  </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan = 1  width = \"100\"><b><center> Avg  </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan = 1  width = \"100\"><b><center> Avg  </center></b></td>
        <td colspan =1   width = \"100\"><b><center> Total Appt Done</center></b></td>
        <td colspan = 1  width = \"100\"><b><center> %age  </center></b></td>
        <td colspan = 1  width = \"100\"><b><center> Avg  </center></b></td>
    </tr> '''
    
def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)   
    
    return fetched_val

def bucket(consultationFee,paymentMode,apptId,caseCreatedOn,issuDate,lcappId):
    
    
    if consultationFee == 0:
        if paymentMode == 'AYU_CASH':
            return "AYU_CASH"
        if apptId in lcappId:
            return "Privilege Card"
        if caseCreatedOn == issuDate:
            return "Privilege Card"
        # if customerId in dict1.keys():
        #     return dict1[customerId]
        else: 
            return "0"  
    elif consultationFee>0 and consultationFee<=100: 
        return "1-100"
    elif consultationFee >100 and consultationFee <=200:
        return "101-200"
    elif consultationFee >200 and consultationFee <=300:
        return "201-300"
    elif consultationFee >300 and consultationFee<=400:
        return "301-400"
    else:
        return ">400"

def compute_table(row, col, result, tableD):
    for i in range(0, row): 
        result = result + "<tr>"
        for j in range(0, col):
            if j == 0:
                result = result + "<td colspan = 1 ><center>" + str(tableD[i][j]) + "</center></td>"
            elif tableD[i][j] == 0:
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                result = result + "<td colspan = 1 ><center>" + str(tableD[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"
    return result
 

    
def itertableD(data,city):
    table = [[0 for x in range(11)] for x in range(100)]
    # table1 = [[0 for x in range(10)] for x in range(100)]
    
    fees = {'0':0,'1-100':1,'101-200':2,'201-300':3,'301-400':4,'>400':5,'AYU_CASH':6,'Privilege Card':7}
    count = 0
    for i,j in fees.items():
        table[count][0] = i
        count +=1
    
    for i,j in data.iterrows():
        if j['bucket'] not in fees.keys():
            fees[j['bucket']] = count
            table[count][0] = j['bucket']
            count +=1
        # print("cur",j['appointmentDate'].month())
        if j['appointmentDate'] == d1:
            if j['consultationFee'] == 0:
                table[fees[j['bucket']]][1] +=1
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table[fees[j['bucket']]][1] +=1
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table[fees[j['bucket']]][1] +=1
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table[fees[j['bucket']]][1] +=1
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table[fees[j['bucket']]][1] +=1
            elif j['consultationFee'] >400:
                table[fees[j['bucket']]][1] +=1
                
        elif j['appointmentDate'] == d2:
            if j['consultationFee'] == 0:
                table[fees[j['bucket']]][3] +=1
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table[fees[j['bucket']]][3] +=1
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table[fees[j['bucket']]][3] +=1
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table[fees[j['bucket']]][3] +=1
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table[fees[j['bucket']]][3] +=1
            elif j['consultationFee'] >400:
                table[fees[j['bucket']]][3] +=1
                
        elif j['appointmentDate'] == d3:       
            if j['consultationFee'] == 0:
                table[fees[j['bucket']]][5] +=1
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table[fees[j['bucket']]][5] +=1
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table[fees[j['bucket']]][5] +=1
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table[fees[j['bucket']]][5] +=1
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table[fees[j['bucket']]][5] +=1
            elif j['consultationFee'] >400:
                table[fees[j['bucket']]][5] +=1
                
        elif j['appointmentDate'] == d4:        
            if j['consultationFee'] == 0:
                table[fees[j['bucket']]][7] +=1
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table[fees[j['bucket']]][7] +=1
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table[fees[j['bucket']]][7] +=1
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table[fees[j['bucket']]][7] +=1
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table[fees[j['bucket']]][7] +=1
            elif j['consultationFee'] >400:
                table[fees[j['bucket']]][7] +=1
             
        if j['appointmentDate'] <= month_start and j['appointmentDate'] > month_last:    
            if j['consultationFee'] == 0:
                table[fees[j['bucket']]][9] +=1
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table[fees[j['bucket']]][9] +=1
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table[fees[j['bucket']]][9] +=1
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table[fees[j['bucket']]][9] +=1
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table[fees[j['bucket']]][9] +=1
            elif j['consultationFee'] >400:
                table[fees[j['bucket']]][9] +=1

     
    table[count][0] = "Total"
    
    for i in range(count):
        table[count][1] += table[i][1]
        table[count][3] += table[i][3]
        table[count][5] += table[i][5]
        table[count][7] += table[i][7]
        table[count][9] += table[i][9]
    
    for i in range(count):
        try:
            table[i][10] = round((table[i][9]/table[count][9])*100,2) if table[count][9] != 0 else 0
            table[i][8] = round((table[i][7]/table[count][7])*100,2) if table[count][7] != 0 else 0
            table[i][6] = round((table[i][5]/table[count][5])*100,2) if table[count][5] != 0 else 0
            table[i][4] = round((table[i][3]/table[count][3])*100,2) if table[count][3] != 0 else 0
            table[i][2] = round((table[i][1]/table[count][1])*100,2) if table[count][1] != 0 else 0
        
            
        except:
            pass 
    
    n = len(fees)
    
    
    result = result_base.format(city)
    result = compute_table(n+1, 11, result, table)
    
    return result

date = datetime.now().strftime('%V')

date = int(date)
# print(date)
    
def itertableW(data,city):
    table1 = [[0 for x in range(13)] for x in range(100)]  
    fees1 = {'0':0,'1-100':1,'101-200':2,'201-300':3,'301-400':4,'>400':5,'AYU_CASH':6,'Privilege Card':7}
    count = 0
    for i,j in fees1.items():
        table1[count][0] = i
        count +=1 
    
    # ptId = j['patientId']
    
    for i,j in data.iterrows():
        
        if j['bucket'] not in fees1.keys():
            fees1[j['bucket']] = count
            table1[count][0] = j['bucket']
            count +=1
        # print("cur",j['createdOn'].date())
        if j['leadWeek']== date and j['leadyear'] == yeard:
            if j['consultationFee'] == 0:
                table1[fees1[j['bucket']]][10] +=1
                table1[fees1[j['bucket']]][12] +=j['consultationFee']
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table1[fees1[j['bucket']]][10] +=1
                table1[fees1[j['bucket']]][12] +=j['consultationFee']
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table1[fees1[j['bucket']]][10] +=1
                table1[fees1[j['bucket']]][12] +=j['consultationFee']
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table1[fees1[j['bucket']]][10] +=1
                table1[fees1[j['bucket']]][12] +=j['consultationFee']
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table1[fees1[j['bucket']]][10] +=1
                table1[fees1[j['bucket']]][12] +=j['consultationFee']
            elif j['consultationFee'] >400:
                table1[fees1[j['bucket']]][10] +=1
                table1[fees1[j['bucket']]][12] +=j['consultationFee']
        elif j['leadWeek']== date-int(1) and j['leadyear'] == yeard:
            if j['consultationFee'] == 0:
                table1[fees1[j['bucket']]][7] +=1
                table1[fees1[j['bucket']]][9] +=j['consultationFee']
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table1[fees1[j['bucket']]][7] +=1
                table1[fees1[j['bucket']]][9] +=j['consultationFee']
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table1[fees1[j['bucket']]][7] +=1
                table1[fees1[j['bucket']]][9] +=j['consultationFee']
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table1[fees1[j['bucket']]][7] +=1
                table1[fees1[j['bucket']]][9] +=j['consultationFee']
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table1[fees1[j['bucket']]][7] +=1
                table1[fees1[j['bucket']]][9] +=j['consultationFee']
            elif j['consultationFee'] >400:
                table1[fees1[j['bucket']]][7] +=1
                table1[fees1[j['bucket']]][9] +=j['consultationFee']
        
        elif j['leadWeek']== date-int(2) and j['leadyear'] == yeard:
            if j['consultationFee'] == 0:
                table1[fees1[j['bucket']]][4] +=1
                table1[fees1[j['bucket']]][6] +=j['consultationFee']
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table1[fees1[j['bucket']]][4] +=1
                table1[fees1[j['bucket']]][6] +=j['consultationFee']
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table1[fees1[j['bucket']]][4] +=1
                table1[fees1[j['bucket']]][6] +=j['consultationFee']
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table1[fees1[j['bucket']]][4] +=1
                table1[fees1[j['bucket']]][6] +=j['consultationFee']
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table1[fees1[j['bucket']]][4] +=1
                table1[fees1[j['bucket']]][6] +=j['consultationFee']
            elif j['consultationFee'] >400:
                table1[fees1[j['bucket']]][4] +=1
                table1[fees1[j['bucket']]][6] +=j['consultationFee']
        
        elif j['leadWeek']== date - int(3) and j['leadyear'] == yeard:
            if j['consultationFee'] == 0:
                table1[fees1[j['bucket']]][1] +=1
                table1[fees1[j['bucket']]][3] +=j['consultationFee']
            elif j['consultationFee'] >0 and j['consultationFee'] <=100:
                table1[fees1[j['bucket']]][1] +=1
                table1[fees1[j['bucket']]][3] +=j['consultationFee']
            elif j['consultationFee'] >100 and j['consultationFee'] <=200:
                table1[fees1[j['bucket']]][1] +=1
                table1[fees1[j['bucket']]][3] +=j['consultationFee']
            elif j['consultationFee'] >200 and j['consultationFee'] <=300:
                table1[fees1[j['bucket']]][1] +=1
                table1[fees1[j['bucket']]][3] +=j['consultationFee']
            elif j['consultationFee'] >300 and j['consultationFee'] <=400:
                table1[fees1[j['bucket']]][1] +=1
                table1[fees1[j['bucket']]][3] +=j['consultationFee']
            elif j['consultationFee'] >400:
                table1[fees1[j['bucket']]][1] +=1
                table1[fees1[j['bucket']]][3] +=j['consultationFee']
                
                
    table1[count][0] = "Total"
    
        
    for i in range(count):
        table1[count][10] += table1[i][10]
        table1[count][12] += table1[i][12]
        table1[count][7] += table1[i][7]
        table1[count][9] += table1[i][9]
        table1[count][4] += table1[i][4]
        table1[count][6] += table1[i][6]
        table1[count][1] += table1[i][1]
        table1[count][3] += table1[i][3]
        
    
    for i in range(count):
        try:
            
            table1[i][2] = round((table1[i][1]/table1[count][1])*100,2) if table1[count][1] != 0 else 0
            table1[i][5] = round((table1[i][4]/table1[count][4])*100,2) if table1[count][4] != 0 else 0
            table1[i][8] = round((table1[i][7]/table1[count][7])*100,2) if table1[count][7] != 0 else 0
            table1[i][11] = round((table1[i][10]/table1[count][10])*100,2) if table1[count][10] != 0 else 0
            
            
            
            
        except:
            pass 
        
    for i in range(count+1):
        table1[i][3] = round(table1[i][3]/table1[i][1],2) if table1[i][1] != 0 else 0
        table1[i][6] = round(table1[i][6]/table1[i][4],2) if table1[i][4] != 0 else 0
        table1[i][9] = round(table1[i][9]/table1[i][7],2) if table1[i][7] != 0 else 0
        table1[i][12] = round(table1[i][12]/table1[i][10],2) if table1[i][10] != 0 else 0
    
    n1 = len(fees1)
    result = result_base1.format(city)
    result = compute_table(n1+1, 13, result, table1)
    
    return result
        
    
def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
        
        
        dataSets = fetch_data(read_connection_obj)
        data = pd.DataFrame(dataSets['consultation'])
        card = pd.DataFrame(dataSets['card'])
        new_users = pd.DataFrame(dataSets['new_users'])
        
        data = data.sort_values(['patientId','apptId'], ascending = [True,True])
        # data = data.fillna('NA')
        
        data['consultationFee'] = data['consultationFee'].astype(float) 
        
        lc_appID_list = list(card['lcappId'])
        
        # dict1 = {}
        # for i,j in new_users.iterrows():
        #     if j['customerId'] not in dict1.keys():
        #         dict1[j['customerId']]=[]
        #     dict1[j['customerId']] = j['issuDate']
        
        # print(dict1)
        
        
        
        
        ptdict = {}
        for i,j in data.iterrows():
            if j['patientId'] not in ptdict.keys():
                ptdict[j['patientId']] = {}
            # ptdict[j['patientId']]['patientId'] = j['patientId'],j['apptId'],j['consultationFee'],j['paymentMode'],j['leadWeek'],j['city'],j['appointmentDate']])
                ptdict[j['patientId']]['patientId'] = j['patientId']
                ptdict[j['patientId']]['apptId'] = j['apptId']
                ptdict[j['patientId']]['consultationFee'] = j['consultationFee']
                ptdict[j['patientId']]['paymentMode'] = j['paymentMode']
                ptdict[j['patientId']]['leadWeek'] = j['leadWeek']
                ptdict[j['patientId']]['leadMonth'] = j['leadMonth'] 
                ptdict[j['patientId']]['leadyear'] = j['leadyear']
                ptdict[j['patientId']]['city'] = j['city']
                ptdict[j['patientId']]['appointmentDate'] = j['appointmentDate']
                ptdict[j['patientId']]['caseCreatedOn'] = j['caseCreatedOn']
                ptdict[j['patientId']]['issuDate'] = j['issuDate']
                
            
        # print(ptdict1.values())     
        list1=[]
        
        frame = pd.DataFrame(ptdict.values())
        # print(frame)  
        # for i,j in ptdict.items():
        #     print(type(i))
        #     list1.append([i,j])
            
        # frame = pd.DataFrame(list1,columns=['patientId','apptId','consultationFee','paymentMode','leadWeek','appointmentDate','city'])
 
        frame['bucket'] = frame.apply(lambda x : bucket(float(x['consultationFee']),x['paymentMode'],x['apptId'],x['caseCreatedOn'],x['issuDate'],lc_appID_list),axis=1) 
                
        # print(frame)
        
       
        
        citylist = ['CHD', 'BLR', 'Jaipur', 'NCR', 'Hyderabad']
        result = ""
        resultw = ""
        for city in citylist:
            data_final = frame[frame['city']==city]
            result += itertableD(data_final,city)
            resultw += itertableW(data_final,city)
        
        
        final = result + resultw
        
        fpath = os.path.join('/tmp','consultationFee.csv')
        frame.to_csv(fpath)
        
        Subject = 'Report on consultation fees | {today}'
        email_recipient_list = ['nikunj.r@ayu.health']
        # email_recipient_list = ['neha@ayu.health','shashank@ayu.health','tls@ayu.health']
        send_email(None, email_recipient_list, Subject.format(today=today), 'None',final,[fpath]) 
        
    except Exception as e:
        raise e


