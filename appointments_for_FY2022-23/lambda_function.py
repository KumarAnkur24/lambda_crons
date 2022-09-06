import pdb
import os
import logging
import json
import pytz
import gspread
from datetime import datetime, timedelta
from service.database import InitDatabaseConnetion,make_db_params,fetch_record 
from oauth2client.service_account import ServiceAccountCredentials
from service.send_mail_client import send_email
from spreadsheet import clear_and_write_to_sheet,read_from_sheet
import time
import pandas as pd
from service.base_functions import msg_to
import boto3 
from gspread.models import Cell
import requests

fpath = os.path.join("/tmp","reco.csv")


# Description --> appointment data for financial year 2022-23
#  Author --> Nisha Das 

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')


queries = { 'appointments':""" select ldc.id as appId,ldc.doctorId,ldc.hospitalId,ldc.leadId as caseId,
                                ldc.appointmentDate,
                                ldc.doctorConsultationStatus,ldc.consultationFee,ldc.hospitalPrice,
                                ldc.appointmentCreationType,
                                ldc.appointmentDate,ldc.doctorConsultationStatus,
                                CASE WHEN ldc.consultationType in 
                                ('Consultation','Online Consultation','Online Consulting')
                                THEN "OPD" ELSE "DIAGNOSTICS"
                                END AS Type_of_appt,
                               ldc.consultationType,ldc.appointmentComment,ldc.user as createdBy,
                               ldc.consultationFee,ldc.ayuMitraId,ldc.hospitalPrice,
                               pc.patientId,pp.patientName,pp.customerId,afp.aliasName as hospitalName,
                               case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                                else 'No City' end as city,
                                pc.leadSource as caseLeadSource,
                                cp.leadSource,
                                case when cp.leadSource in ('Offline channel', 'VC Model') then 1
                                when pc.leadSource in ('Offline channel', 'VC Model') then 1 else 0 end as isOffline,
                                case when cp.leadSource in ('Hospital Insurance') then 1
                                else 0 end as isReImbursementCase
                               ,dp.name as doctorName,
                               case when ldc.additionalDetails->>'$.discount' is null then 'NA' else ldc.additionalDetails->>'$.discount' end as 'discount',
                               concat(year(appointmentDate),"-",month(appointmentDate)) as 'yearMonth'
                               from lead_doctor_consultation ldc
                               join patient_case pc on pc.caseId = ldc.leadId
                               join patient_profile pp on pc.patientId = pp.id
                               join customer_profile cp on pp.customerId = cp.customerId
                               left join ayu_facility_profile afp on ldc.hospitalId = afp.facilityId
                               left join ayu_cities pcp on pc.cityId = pcp.id
                               left join ayu_cities ppp on pp.cityId = ppp.id
                               left join doctors_profile dp on dp.doctorProfileId = ldc.doctorId 
                               where ldc.appointmentDate >= '2022-04-01'
                               and ldc.tenantName = 'AYU'
                               order by pc.createdOn
                               
                            """,
                            
            'recos': """select fr.id as recoId,entityId as appId,
                        entityType,recoStatus,chargeableAmount,
                         nonChargeableAmount from finance_recos fr 
                         where entityType in ('APPOINTMENT', 'DIAGNOSTICS_APPOINTMENT') """,
                         
            'payment': """
                        select opl.entityId as appId,sum(amount)/100 as amount,
                        group_concat(paymentMode) 
                        as paymentMode,sum(case when paymentMode = 'PAY_AT_HOSPITAL' then amount/100 else 0 end) as collectedByHospital,
                        sum(case when paymentMode != 'PAY_AT_HOSPITAL' then amount/100 else 0 end) as collectedByAyu
                        from online_payment_link opl
                        where status = "PAID"
                        group by 1
                      """,
                       
                       
            "transactions": """Select entityId as appId,his.cardId as cardId,
                        transactionStatus,his.units ,
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
                        order by his.createdOn
                        """,
                        
            'loyalty_card' : """Select
                
                                c.customerId,
                                'done' as status, 
                                group_concat(distinct paymentMode) as loyalty_paymentMode
                            from
                                    lc_payment_details l  
                                    join generated_mcards c on l.cardId = c.cardId
                                    join customer_profile cp on c.customerId = cp.customerId
                                    join (select customerId,
                                id as patientId
                                    from
                                    patient_profile
                                    group by customerId
                                    ) pp on cp.customerId = pp.customerId
                                    where
                                        paymentStatus = 'PAID'
                                    and cp.tenantName = 'AYU'
                                    group by 1,2
               """,
    "payToHospital": """select fpt.entityId, sum(fpt.amount/100) as 'amount'
                                
                        from facility_payment_transaction fpt
                        where
                            fpt.paymentStatus ="processed"
                            and fpt.entityType in ("APPOINTMENT","DIAGNOSTICS_APPOINTMENT")
                            group by 1
                        
        """,
    "debit_credit_reco": """select entityId,amount/100 as 'amount',financeRecoPaymentType
        
            from
                finance_recos fr 
                join finance_reco_payments frp on fr.id = frp.recoId
                where
                    entityType in ('APPOINTMENT', 'DIAGNOSTICS_APPOINTMENT')
             """
            
            
            # "example": """
            #             select * from lead_doctor_consultation where appointmentDate >= '2022-04-01'
            # """,
            # "example2": """
            #      select ldc.*,pc.*,pp.*,cp.*,afp.* from lead_doctor_consultation ldc
            #      join patient_case pc on pc.caseId = ldc.leadId 
            #      join patient_profile pp on pc.patientId = pp.id
            #      join customer_profile cp on pp.customerId = cp.customerId
            #      left join ayu_facility_profile afp on ldc.hospitalId = afp.facilityId
            #      left join ayu_cities ac on ac.id = pp.cityId
            #      where appointmentDate >= '2022-07-01'
            # """
    
}

# 'payment': """
#                         select entityId as appId,sum(amount)/100 as amount,collectedBy,
#                         group_concat(paymentMode) 
#                         as paymentMode
#                         from online_payment_link
#                         where status = "PAID"
#                         group by 1
#                       """,

doctorConsultationStatus = {
    '1':'BOOKED',
    '2':'CONFIRMED',
    '3':'DONE',
    '4':'CANCELLED',
    '6':'PATIENT_HAS_REACHED',
    '7':'ONLINE_CONSULTATION_STARTED'
}


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)    
          
    return fetched_val
    
def getRecoStatus(appId,reco):
    print(reco)
    if appId in reco.keys():
        return reco[appId][0],reco[appId][1],reco[appId][2]


# reco 3 user with duplicate

def getInt(value):
    if value:
        return int(value)
        
def getSoldStatus(status):
    print(status)
    if status!='done':
        return "NO"
    else:
        return "YES"  
        
def f(value):
    if value == '':
        return 0
    else:
        return int(float(value)) 
        
def getPaidAmount(appId, payToHospitalDump):
    
    if appId in payToHospitalDump.keys():
        return payToHospitalDump[appId]
        
    return 0
    
def getDebitCreditAmount(appId, debitCreditEntryDump):
    
    if appId in debitCreditEntryDump.keys():
        return debitCreditEntryDump[appId]['debit'], debitCreditEntryDump[appId]['credit']
        
    return 0, 0
    
        

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        appointments = pd.DataFrame(dataSets['appointments'])
        reco = pd.DataFrame(dataSets['recos'])
        payment = pd.DataFrame(dataSets['payment'])
        loyalty_card = pd.DataFrame(dataSets['loyalty_card'])
        payToHospital = dataSets['payToHospital']
        debit_credit_reco = dataSets['debit_credit_reco']
        
        payToHospitalDump = {}
        for val in payToHospital:
            if val['entityId'] not in payToHospitalDump.keys():
                payToHospitalDump[val['entityId']] = val['amount']
                
        debitCreditEntryDump = {}
        for val in debit_credit_reco:
            if val['entityId'] not in debitCreditEntryDump.keys():
                debitCreditEntryDump[val['entityId']] = {}
                debitCreditEntryDump[val['entityId']]['debit'] = 0
                debitCreditEntryDump[val['entityId']]['credit'] = 0
                
            if val['financeRecoPaymentType'] == 'DEBIT':
                debitCreditEntryDump[val['entityId']]['debit'] += val['amount']
            else:
                debitCreditEntryDump[val['entityId']]['credit'] += val['amount']
        
        payment.to_csv(fpath)
        
        customerId = list(appointments['customerId'])
        loyalty_card = loyalty_card[loyalty_card['customerId'].isin(customerId)]
        
    
        print(len(appointments))
        
        
        
        appointments['appointmentStatus'] = appointments.apply(lambda x: 
            doctorConsultationStatus[x['doctorConsultationStatus']],axis=1)
        print(len(appointments))
        
        appointments = appointments.merge(reco,on = 'appId',how='left')
        print("reco",len(appointments))
        appointments = appointments.merge(payment,on = 'appId',how='left')
        print(len(appointments))
        appointments = appointments.merge(loyalty_card,on='customerId',how='left')
        print(len(appointments))
        appointments['LC Sold'] = appointments.apply(lambda x: getSoldStatus(x['status']),axis = 1)
        
        appointments['Paid To Hospital'] = appointments.apply(lambda x: getPaidAmount(x['appId'], payToHospitalDump),axis = 1)
        df = appointments.apply(lambda x: getDebitCreditAmount(x['appId'], debitCreditEntryDump),axis = 1, result_type='expand')
        appointments = pd.concat([appointments,df], axis=1)
        appointments = appointments.rename(columns={0: 'debit', 1: 'credit'})
        
    

        DATA = []
        DATA.append(['Type_of_appt','appId','caseId','patientId','patientName','hospitalName','hospitalPrice',
        'city','createdBy','doctorName','consultationFee','appointmentDate','appointmentStatus','reco Status',
        'chargeableAmount','nonChargeableAmount','amount','collectedByAyu','collectedByHospital',
        'paymentMode','appointmentCreationType','loyaltyCard', 'caseLeadSource', 'leadSource', 'isOffline', 'isReImbursementCase',
        'discount', 'yearMonth', 'Paid To Hospital', 'Debit Amount', 'Credited Amount'])
        
        appointments.fillna('', inplace=True)
        # cols = ['hospitalPrice','consultationFee','chargeableAmount','nonChargeableAmount','amount']
        # appointments[[cols]] = appointments[[cols]].replace('',0)
         
        
        
        
        for index,val in appointments.iterrows():
            
            hospitalPrice = f(val['hospitalPrice']) 
            consultationFee = f(val['consultationFee'])
            
            if hospitalPrice is None:
                hospitalPrice = 0
                
            if consultationFee is None:
                consultationFee = 0
            
            discount = None
            if val['discount'] != 'NA':
                discount = float(val['discount'])
                
            if discount is None:
                discount =  float(hospitalPrice - consultationFee)
            
            DATA.append([
                val['Type_of_appt'],
                str(val['appId']),
                str(val['caseId']),
                str(val['patientId']), 
                str(val['patientName']),
                str(val['hospitalName']),
                f(val['hospitalPrice']),
                val['city'],
                str(val['createdBy']),
                str(val['doctorName']),
                f(val['consultationFee']),
                str(val['appointmentDate']),
                str(val['appointmentStatus']),
                str(val['recoStatus']),
                f(val['chargeableAmount']),
                f(val['nonChargeableAmount']),
                f(val['amount']),
                f(val['collectedByAyu']),
                f(val['collectedByHospital']),
                str(val['paymentMode']),
                str(val['appointmentCreationType']),
                val['LC Sold'] ,
                val['caseLeadSource'],
                val['leadSource'],
                val['isOffline'],
                val['isReImbursementCase'],
                discount,
                val['yearMonth'],
                f(val['Paid To Hospital']),
                f(val['debit']),
                f(val['credit'])
                ])
        
        clear_and_write_to_sheet('1tcq_nCsouLiwHeXhB4A_D1BysZiOnIfDnh81eRz9WB0','Appointments','A1:AE',DATA)        
        
                
        # Subject = "reco"
        # email_recipient_list = ['nisha@ayu.health'] 
        # send_email(None, email_recipient_list, Subject, 'None',None,[fpath])        

        
    except Exception as e:
        
        raise e
    
