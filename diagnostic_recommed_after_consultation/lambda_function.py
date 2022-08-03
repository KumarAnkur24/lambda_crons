import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import json
from datetime import datetime , timedelta
from service.send_mail_client import send_email
import requests
import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
from service.send_mail_client import send_email
from service.base_functions import msg_to
import pandas as pd
from datetime import datetime, timedelta
import pytz

from bq import main
from write_to_gsheet import clear_and_write_to_sheet

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')


queries = {"data" :"""select appointmentId, group_concat(name) as testNamesRecommended 
                        from 
                    appointment_diagnostics_outcome ad
                    left join diagnostics_details dd on ad.diagnosticsId = dd.id
                    group by appointmentId
                    """
}

def abc(patientId,apptId,patientdict):
    
    if patientId in patientdict.keys():
        return patientdict[patientId]
        
def testreco(apptId,dict1):
    if apptId in dict1.keys():
        return dict1[apptId]

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
        test = pd.DataFrame(dataSets['data'])
        
        
                
        
        
        
        data = main()
        data = data.sort_values(['patientId','apptId'], ascending = [True,True])
        
        dict1 = {}
        for i,j in test.iterrows():
            if j['appointmentId'] not in dict1.keys(): 
                dict1[j['appointmentId']] = j['testNamesRecommended']
        data['test_name'] = data.apply(lambda x: testreco(int(x['apptId']),dict1),axis=1) 
        
        
        apptdict = {}
        patientdict = {}
        for i,j in data.iterrows():
            if j['patientId'] in apptdict.keys():
                continue
            if j['patientId'] not in patientdict.keys():
                if j['consultationType'] in ('Consultation','Online Consultation') and j['Appointment_Status'] =='DONE' and j['isDiagnosticsTestRecommended'] == 'YES':
                    patientdict[j['patientId']] ={}
                    patientdict[j['patientId']]['flag'] = True
                    patientdict[j['patientId']]['caseid'] = j['caseid']
                    patientdict[j['patientId']]['number'] = j['number']
                    patientdict[j['patientId']]['AppointmentDate'] = j['AppointmentDate']
                    patientdict[j['patientId']]['aaptid'] = j['apptId']
                    patientdict[j['patientId']]['test_name'] = j['test_name']
                    patientdict[j['patientId']]['aaptid2'] = ''
                    patientdict[j['patientId']]['AppointmentDate2'] = ''
                    
                    # previousApptID = j['apptId']
                    continue

                    
            if j['patientId'] in patientdict.keys():
                if patientdict[j['patientId']]['flag']:
                    if j['consultationType'] in ('Consultation','Online Consultation') and j['Appointment_Status'] =='DONE':
                        patientdict[j['patientId']]['caseid'] = j['caseid']
                        patientdict[j['patientId']]['number'] = j['number']
                        patientdict[j['patientId']]['AppointmentDate2'] = j['AppointmentDate']
                        patientdict[j['patientId']]['test_name'] = j['test_name']
                        patientdict[j['patientId']]['flag'] = False
                        patientdict[j['patientId']]['aaptid2'] = j['apptId']
                        
                        apptdict[j['patientId']] = ''
                        
        # print(patientdict)
        # data['Gone'] = data.apply(lambda x: abc(x['patientId'], patientdict), axis=1)
        list1 =[]
        for i,j in patientdict.items():
            list1.append([i,j['caseid'],j['number'],j['AppointmentDate'],j['aaptid'],j['aaptid2'],j['AppointmentDate2'],j['test_name']])
            
            
        final = pd.DataFrame(list1,columns=['Patient ID','Case ID','Patient phone number','AppointmentDate','Appt ID - Consultation','Appt ID-Diagnostic after consultation','Date of App after consultation','Test_names'])
        final = final.sort_values('Appt ID - Consultation', ascending=True) 
        sheet = [['Patient ID','Case ID','Patient phone number','AppointmentDate','Appt ID - Consultation','Appt ID-Diagnostic after consultation','Date of App after consultation','Test_names']]
        for index,val in final.iterrows():
            sheet.append([
                val['Patient ID'],
                val['Case ID'],
                val['Patient phone number'],
                str(val['AppointmentDate']),
                val['Appt ID - Consultation'],
                val['Appt ID-Diagnostic after consultation'],
                str(val['Date of App after consultation']),
                val['Test_names']
                ])
        clear_and_write_to_sheet('1fgZXjKhhZnEgb0swD8ScJtHQLz0XKtlt6RdimtNvElA','June onwards','A{0}:H{1}',sheet)
        
        
        # fpath = os.path.join('/tmp','final.csv')
        # final.to_csv(fpath) 
        
        # Subject = 'Diagnostics'
        # email_recipient_list = ['nikunj.r@ayu.health'] 
        # send_email(None, email_recipient_list, Subject, 'None',None,[fpath]) 
    
    
 
    except Exception as e:
        raise e
