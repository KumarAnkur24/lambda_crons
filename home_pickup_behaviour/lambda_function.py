import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
from service.send_mail_client import send_email
from service.base_functions import msg_to
import pandas as pd
from datetime import datetime, timedelta
import pytz
 
logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')


queries = {"data":"""select pc.patientId,pc.caseId,ldc.id as apptId , pc.leadSource, pc.caseType,pc.reason,
                        pp.patientName, pp.gender, cp.customerNumber,pc.createdOn,ldc.doctorConsultationStatus, 
                        ldc.appointmentDate, ldc.consultationType,ldc.ayuMitraId,ldc.appointmentCreationType,
                        ldc.consultationFee,ldc.hospitalId
                         
                    from lead_doctor_consultation ldc
                    left join patient_case pc on ldc.leadId = pc.caseId
                    left join patient_profile pp on pc.patientId = pp.id 
                    left join customer_profile cp on cp.customerId = pp.customerId
                    where patientId in (select pc.patientId from lead_doctor_consultation ldc
                        left join patient_case pc on pc.caseId = ldc.leadId where ldc.hospitalId = 142)
                        and doctorConsultationStatus = '3'
     
    
    
"""}
DoctorConsultationStatus = {
	'0': 'OPEN',
	'1': 'BOOKED',
	'2': 'CONFIRMED',
	'3': 'DONE',
	'4': 'CANCELLED',
	'5': 'OPEN',
	'6': 'PATIENT_HAS_REACHED', 
	'7': 'ONLINE_CONSULTATION_STARTED' 
}

def new(patientId,apptId,hospitalId, dict1):
    if patientId in dict1.keys():
        if dict1[patientId]['apptId'] == apptId:

            return "New Patient"
        else:
            return "Existing patient"
        
    
        
# def existing(apptId,hospitalId,first_diagnostic):
#     if apptId in first_diagnostic.values():
#         if hospitalId == 142:
#             return "Existing Patient and Took first Diagnostics from HOME PICKUP"
         
#     return "Existing Patient and Took first Diagnostics from Others"
    

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)   
    
    return fetched_val
    

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
        
        
        dataSets = fetch_data(read_connection_obj)
        data = pd.DataFrame(dataSets['data'])
        data = data.sort_values(['patientId', 'apptId'], ascending = [True, True])
        
        data['AppointmentStatus'] =data.apply(lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']], axis=1)
        data['rank'] = data.groupby('patientId')['apptId'].rank(ascending=True)
        dict1= {}
        for i,j in data.iterrows():
            if j['patientId'] not in dict1.keys():
                dict1[j['patientId']] = {}
                dict1[j['patientId']]['apptId'] = j['apptId']
                dict1[j['patientId']]['hospitalId'] = j['hospitalId'] 
                
        dataho = data[data['hospitalId'] == 142]
        print(dataho)
        
        dataho['New/Existing'] = dataho.apply(lambda x: new(x['patientId'],x['apptId'],x['hospitalId'],dict1), axis=1)
        
    
        
        fpath = os.path.join('/tmp','home_pick_up_patients_behaviour.csv')
        dataho.to_csv(fpath)
        
        Subject = 'home pick up patients behaviour'
        email_recipient_list = ['nikunj.r@ayu.health']
        
        send_email(None, email_recipient_list, Subject, 'None',None,[fpath]) 
        
    except Exception as e:
        raise e
