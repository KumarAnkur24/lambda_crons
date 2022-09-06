import pdb
import os
import logging
import json
import pytz
from datetime import datetime, timedelta
from service.database import InitDatabaseConnetion,make_db_params,fetch_record 
from service.send_mail_client import send_email
from write_to_spreadsheet import clear_and_write_to_sheet,read_from_sheet
import time
import pandas as pd
from service.base_functions import msg_to
import boto3 
import requests
import sys
logger = logging.getLogger(__name__)

# description --> send details of the appointment which is booked in 10 min window
# author --> Nisha Das

time_19 = datetime.now() + timedelta(hours=5,minutes=19)
time_29 = datetime.now() + timedelta(hours=5,minutes=28)
time_19 = time_19.strftime('%Y-%m-%d %H:%M:00')
time_29 = time_29.strftime('%Y-%m-%d %H:%M:59') 

# time_19 = datetime.now() + timedelta(hours=5,minutes=30)
# time_19 = time_19.replace(day = 1)
# time_19 = time_19 - timedelta(days = 1)
# time_19 = time_19.replace(day=1)
# time_19 = time_19 - timedelta(days = 1)
# time_19 = time_19.replace(day=1)
# time_29 = datetime.now() + timedelta(hours=5,minutes=30)
# time_19 = time_19.strftime('%Y-%m-%d %H:%M:00')
# time_29 = time_29.strftime('%Y-%m-%d %H:%M:59')
# print(time_19)
# print(time_29)
fpath = os.path.join('/tmp','appointment_booked.csv')
fpath1 = os.path.join('/tmp','example.csv')

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')


queries = {'booked' : """
                    Select pc.caseId , ldc.doctorConsultationStatus ,  cp.customerNumber , atd.treatmentId,
                    cp.customerName , pc.hospitalId , h.aliasName ,atd.diagnostics_name,ldc.consultationFee,
                    cp.customerLocation->>'$.locationName' as locationName ,ldc.id as appt_Id,ldc.appointmentDate, 
                    ldc.hospitalPrice,h.aliasName as hospitalName,ldc.user,
                            ldc.appointmentStartTime , ldc.appointmentEndTime ,
                            case when pcp.cityName = 'Chandigarh' then 'CHD'
                                        when pcp.cityName = 'Bangalore' then 'BLR'
                                        when pcp.cityName is not null then pcp.cityName
                                        when ppp.cityName = 'Chandigarh' then 'CHD'
                                        when ppp.cityName = 'Bangalore' then 'BLR'
                                        when ppp.cityName is not null then ppp.cityName
                else 'No City' end as city ,    
                            ldc.additionalDetails->>'$.homePickUpAddress' as 'address',
                            confirmedOn,
                            secondaryNumber
                    from lead_doctor_consultation ldc
                    join patient_case pc on ldc.leadId = pc.caseId
                    join patient_profile pp on pc.patientId = pp.id
                    join customer_profile cp on pp.customerId = cp.customerId
                    left join ayu_cities pcp on pc.cityId = pcp.id
                    left join ayu_cities ppp on pp.cityId = ppp.id
                    join ayu_facility_profile h on ldc.hospitalId = h.facilityId
                    left join 
                    (select appointmentId, group_concat(at.treatmentId) as treatmentId,
                    group_concat(dd.name) as diagnostics_name,
                    group_concat(dd.price) as treatmentPrice
                    from appointment_treatment_details at
                    join diagnostics_details dd on at.treatmentId = dd.id
                    group by 1) atd on ldc.id = atd.appointmentId
                    
                    join
                        (select id, min(date_add(createdOn , INTERVAL '5:30' HOUR_MINUTE)) as 'confirmedOn'
                            from lead_doctor_consultation_AUD 
                            where 
                            consultationType = 'Diagnostics'
                            and doctorConsultationStatus in (1)
                            and appointmentCreationType != 'FOLLOWUP_APPOINTMENT'
                            group by id
                            ) as ldca on ldc.id = ldca.id 
                    where 
                    ldc.hospitalPrice > 1500 
                    and confirmedOn >= '{start}'
                    and confirmedOn <= '{end}'
                    
                    """,
        
        # 'booked_all_status': """

        # Select pc.caseId , ldc.doctorConsultationStatus ,  cp.customerNumber , atd.treatmentId,
        #             cp.customerName , pc.hospitalId , h.aliasName ,atd.diagnostics_name,ldc.consultationFee,
        #             cp.customerLocation->>'$.locationName' as locationName ,ldc.id as appt_Id,ldc.appointmentDate, 
        #             ldc.hospitalPrice,h.aliasName as hospitalName,ldc.user,
        #                     ldc.appointmentStartTime , ldc.appointmentEndTime ,
        #                     case when pcp.cityName = 'Chandigarh' then 'CHD'
        #                                 when pcp.cityName = 'Bangalore' then 'BLR'
        #                                 when pcp.cityName is not null then pcp.cityName
        #                                 when ppp.cityName = 'Chandigarh' then 'CHD'
        #                                 when ppp.cityName = 'Bangalore' then 'BLR'
        #                                 when ppp.cityName is not null then ppp.cityName
        #         else 'No City' end as city ,    
        #                     ldc.additionalDetails->>'$.homePickUpAddress' as 'address',
        #                     ldc.createdOn,
        #                     secondaryNumber
        #             from lead_doctor_consultation ldc
        #             join patient_case pc on ldc.leadId = pc.caseId
        #             join patient_profile pp on pc.patientId = pp.id
        #             join customer_profile cp on pp.customerId = cp.customerId
        #             left join ayu_cities pcp on pc.cityId = pcp.id
        #             left join ayu_cities ppp on pp.cityId = ppp.id
        #             join ayu_facility_profile h on ldc.hospitalId = h.facilityId
        #             left join 
        #             (select appointmentId, group_concat(at.treatmentId) as treatmentId,
        #             group_concat(dd.name) as diagnostics_name,
        #             group_concat(dd.price) as treatmentPrice
        #             from appointment_treatment_details at
        #             join diagnostics_details dd on at.treatmentId = dd.id
        #             group by 1) atd on ldc.id = atd.appointmentId
        #             where 
        #             ldc.hospitalPrice > 1500    
        #             and date(ldc.appointmentDate) >= '2022-07-28'
        #             order by ldc.createdOn
        # """

    
}

# and appointmentCreationType in ('NEW_APPOINTMENT','RESCHEDULED_APPOINTMENT')
# join
#                         (select id, min(date_add(createdOn , INTERVAL '5:30' HOUR_MINUTE)) as 'confirmedOn'
#                             from lead_doctor_consultation_AUD 
#                             where 
#                             consultationType = 'Diagnostics'
#                             group by id
#                             ) as ldca on ldc.id = ldca.id 


DoctorConsultationStatus = {
    '0': 'OPEN',
    '1': 'BOOKED',
    '2': 'CONFIRMED',
    '3': 'DONE',
    '4': 'CANCELLED',
    None: 'None',
    '': 'None',
    '5': 'OPEN',
    '6':'CONFIRMED',
    '7': 'CONFIRMED',
}


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == 'booked':
            fetched_val[lookup] = fetch_record(conn, query.format(start=time_19, end=time_29))
            
        elif lookup == 'example':
            fetched_val[lookup] = fetch_record(conn, query.format(start=time_19, end=time_29))      
        else:
            fetched_val[lookup] = fetch_record(conn, query)    
          
    return fetched_val
    
    
result_base= '''   
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        
        <tr bgcolor=\"#008080\" style=\"color:#ffffff\">  
            <td colspan = 2 width = \"1300\" height = \"30\" ><b><center> Appointment Details | BOOKED </center></b></td>
        </tr> 
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> AppId </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {10} </center></b></td>
        </tr>
        <tr > 
            <td colspan = 1 bgcolor=\"#DCDCDC\"  width = \"500\"><b><center> CaseId </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {0} </center></b></td>
        </tr>
        <tr > 
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Customer Number </center></b></td>
            <td colspan = 1 width = \"800\" ><b><center> {1} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\" ><b><center> Customer Name </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {2} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\" ><b><center> Location Name </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {3} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1  bgcolor=\"#DCDCDC\" width = \"500\" ><b><center> Appointment Date </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {4} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Diagnostics  </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {7} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Consultation Fees </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {11} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Ayu Mitra </center></b></td> 
            <td colspan = 1  width = \"800\"><b><center> {13} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Hospital Name </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {12} </center></b></td>
        </tr>
        <tr >     
            <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Hospital Price </center></b></td>
            <td colspan = 1  width = \"800\"><b><center> {9} </center></b></td>
        </tr>
        
        </table>
        <br>
    '''
    
    
# <tr >     
#             <td colspan = 1 bgcolor=\"#DCDCDC\" width = \"500\"><b><center> Address </center></b></td>
#             <td colspan = 1  width = \"800\"><b><center> {8} </center></b></td>
#         </tr>



def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        
        booked = pd.DataFrame(dataSets['booked'])
        #example = pd.DataFrame(dataSets['example'])
        #booked_all_status = pd.DataFrame(dataSets['booked_all_status'])
        booked.to_csv(fpath)
        #booked_all_status.to_csv(fpath1) 
        
        
        # if len(booked_all_status):
        #     booked_all_status['cxNo'] = booked_all_status.apply(lambda x: msg_to(x['customerNumber']),axis=1)
        #     booked_all_status['status'] = booked_all_status.apply(lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']],axis=1)
        #     # booked_all_status = booked_all_status[booked_all_status['status']=='BOOKED']
        #     # spreadsheet
        #     DATA = []
        #     DATA.append(['caseId','appId','customerName','customerNumber','doctorConsultationStatus','hospitalName',
        #     'hospitalPrice','diagnostics_name','consultationFee','locationName','confirmedOn','user','city',
        #     'address','appointmentDate','secondaryNumber'])
             
        #     for key,val in booked_all_status.iterrows():
        #         DATA.append([
        #             str(val['caseId']),
        #             str(val['appt_Id']),
        #             str(val['customerName']),
        #             str(val['cxNo']),
        #             str(val['status']),
        #             str(val['aliasName']),
        #             str(val['hospitalPrice']),
        #             val['diagnostics_name'], 
        #             str(val['consultationFee']),
        #             str(val['locationName']),
        #             str(val['createdOn']),
        #             str(val['user']),
        #             str(val['city']),
        #             val['address'],
        #             str(val['appointmentDate']),
        #             str(val['secondaryNumber']),
        #             ])
            
        #     print("writing to sheet")    
        #     clear_and_write_to_sheet('1_dWz8cD_USTjeqQg-cWOfRmiEoYXr4vW9JthmP5FjtE','appointmentStatus','A1:P',DATA) 
        
        
        

        
        
        
        
        
        result = ''
        result_flag = False
        
        if len(booked):
            
            booked['cxNo'] = booked.apply(lambda x: msg_to(x['customerNumber']),axis=1)
            
            booked['Doctor_Consultation_Status'] = booked.apply(lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']],axis=1)
            
            # booked_data = booked[booked['Doctor_Consultation_Status']=='BOOKED'] 
            for index , val in booked.iterrows():
                result += result_base.format(val['caseId'],val['cxNo'],val['customerName'],val['locationName'],val['appointmentDate'],
                                                val['appointmentStartTime'],val['appointmentEndTime'] , 
                                                val['diagnostics_name'], val['address'],val['hospitalPrice'],
                                                val['appt_Id'],val['consultationFee'],val['hospitalName'],val['user'])
                
                result_flag =True
                
                
        print("hello",result_flag) 
        if result_flag:
            
            Subject = "Appointment Booked | Diagnostics" 
            # email_recipient_list = ['nisha@ayu.health']
            # email_recipient_list = ['nisha@ayu.health','shoaib@ayu.health',
            # 'anushtha@ayu.health','jay@ayu.health','asif.jamil@ayu.health']
            email_recipient_list = ['anushtha@ayu.health','jay@ayu.health','ankur@ayu.health'] 
            send_email(None, email_recipient_list, Subject, 'None',result,[fpath])     
            print("Mail Sent !!!!")      
        
    except Exception as e:
        Subject = 'appointment_booked_today_send_details_mail | Error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None,email_recipient_list,Subject,'None',e,[]) 
        raise e
