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

fpath = os.path.join("/tmp","surgery.csv")


# Description --> discharge and prescription complete data starting from july 2022 in separate spreadsheet
#  Author --> Nisha Das 

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')


queries = {
    
    'lead_doctor_consultation': """
                            select ldc.id as appId,
                                    leadId,
                                    ldc.hospitalId,
                                    date(appointmentDate) as appointmentDate, 
                                    doctorConsultationStatus,
                                    patientName,
                                    afp.aliasName as hospitalName,
                                    
                                    consultationType,ldc.consultationFee,hospitalPrice, 
                                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                                        when pcp.cityName = 'Bangalore' then 'BLR'
                                        when pcp.cityName is not null then pcp.cityName
                                        when ppp.cityName = 'Chandigarh' then 'CHD'
                                        when ppp.cityName = 'Bangalore' then 'BLR'
                                        when ppp.cityName is not null then ppp.cityName
                                    else 'No City' end as city,
                                    appointmentCreationType,
                                    aliasName as hospitalName,
                                    apd.email as latestAyuM,
                                    dp.name as doctorName,
                                    pd.description,pd.submittedBy,pd.documentName,
                                    case when pd.entityId is null then 'No' else 'Yes' end as 'prescriptionUploaded',
                                    prescriptions,
                                    pd.documentName
                    from lead_doctor_consultation ldc 
                    join patient_case pc on ldc.leadId = pc.caseId
                    join patient_profile pp on pc.patientId = pp.id
                    join customer_profile cp on cp.customerId = pp.customerId
                    left join ayu_cities pcp on pc.cityId = pcp.id
                    left join ayu_cities ppp on pp.cityId = ppp.id
                    left join ayu_facility_profile afp on ldc.hospitalId = afp.facilityId
                    left join doctors_profile dp on ldc.doctorId = dp.doctorProfileId
                    left join ayu_personnel_details apd on ldc.ayuMitraId = apd.personnelId and apd.personnelType='AYU_MITRA'
                    left join (select entityId,entityType,group_concat(distinct dd.documentLink) as prescriptions,
                    group_concat(distinct dd.description) as description,group_concat(distinct dd.submittedBy) as submittedBy,
                    group_concat(distinct dd.documentName) as documentName
                    from medical_documents md
                    left join documents_details dd on md.medicalDocsId = dd.medicalDocsId
                            where 
                            documentType in ('PRESCRIPTION', 'E_PRESCRIPTION') 
                            and 
                            entityType='APPOINTMENT'
                            group by entityId) pd on ldc.id = pd.entityId
                        where
                           date(appointmentDate) >= '2022-07-01'
                           and doctorConsultationStatus = 3
                           and consultationType != 'Diagnostics' 
                                       
                                       """,
                            
        'surgery': """
                            Select 
                                patientSurgeryId as surgeryId,group_concat(distinct(documentName)) as documentName,
                                group_concat(distinct(submittedBy)) as submittedBy,
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
                                    psd.createdOn,psd.treatmentType,psd.admissionDate,psd.dischargeDate,
                                    psd.surgeryPackageType,treatmentName,psd.promisedPrice,
                                    date(date_add(dischargeDate,INTERVAL '5:30' HOUR_MINUTE)) as dischargeDate,
                                    psd.hospitalId,
                                    ayuMitraId,
                                    dp.name as doctorName,
                                    afp.aliasName as hospitalName,
                                    apd.email as ayuMitraEmail,
                                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                                        when pcp.cityName = 'Bangalore' then 'BLR'
                                        when pcp.cityName is not null then pcp.cityName
                                        when ppp.cityName = 'Chandigarh' then 'CHD'
                                        when ppp.cityName = 'Bangalore' then 'BLR'
                                        when ppp.cityName is not null then ppp.cityName
                                    else 'No City' end as city,
                                    aliasName,
                                    apd.email as latestAyuM
                        from patient_surgery_details psd
                        join patient_case pc on psd.caseId = pc.caseId
                        join patient_profile pp on pc.patientId = pp.id
                        join customer_profile cp on cp.customerId = pp.customerId
                        left join ayu_cities pcp on pc.cityId = pcp.id
                        left join ayu_cities ppp on pp.cityId = ppp.id
                        left join ayu_facility_profile afp on psd.hospitalId = afp.facilityId
                        left join doctors_profile dp on psd.doctorId = dp.doctorProfileId
                        left join ayu_personnel_details apd on psd.ayuMitraId = apd.personnelId and apd.personnelType='AYU_MITRA'
                            where 
                                date(date_add(dischargeDate,INTERVAL '5:30' HOUR_MINUTE)) >= '2022-07-01'
                                       """,
                                       
        "app_done": """select leadId as appId,user as email
                                from
                                lead_comments lc
                                where 
                                    commentType in ('APPOINTMENT_DONE') 
                                    and leadType = 'APPOINTMENT'
                                    and leadId in ({id})
                                order by commentId"""                               
}

logger = logging.getLogger(__name__)

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == 'lead_doctor_consultation':
            fetched_val[lookup] =  fetch_record(conn,query.format())
            leadId = [str(x['appId']) for x in fetched_val[lookup]]
        elif lookup == 'app_done':
            leadId = ','.join(leadId)
            fetched_val[lookup] =  fetch_record(conn,query.format(id = leadId))
        else:
            fetched_val[lookup] = fetch_record(conn,query.format()) 
            
    return fetched_val
    
    
def getDocUploaded(surgeryId, docsDump):
    
    if surgeryId in docsDump.keys():
        return 'Yes'
        
    return 'No'
    
def getAyuMitra(latestAyuM, appId, commentsDump):
    
    if appId in commentsDump.keys():
        return commentsDump[appId]

    return latestAyuM        

def lambda_handler(event, context):
    try:
        
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
                                                    
        datasets = fetch_data(read_connection_obj)
        docsUpload = pd.DataFrame(datasets['surgery'])
        surgery = pd.DataFrame(datasets['patient_surgery_details'])
        surgery = surgery[surgery['city'] == 'BLR']
        prescription = pd.DataFrame(datasets['lead_doctor_consultation'])
        prescription = prescription[prescription['city'] == 'BLR']
        app_done = pd.DataFrame(datasets['app_done'])
        
        
        # discharge
        docsDump = {}
        for key,val in docsUpload.iterrows(): 
            surgeryId = str(val['surgeryId'])
            
            if surgeryId not in docsDump.keys():
                docsDump[surgeryId] = val['dischargeSummaryDoc']
        surgery['docUploaded'] = surgery.apply(lambda x: getDocUploaded(str(x['surgeryId']), docsDump), axis=1)
        surgery = surgery.merge(docsUpload,on='surgeryId',how='left')
        
        # update sheet for discharge
        DATA = []
        DATA.append(['caseId','surgeryId','createdOn','treatmentType','admissionDate','dischargeDate',
        'surgeryPackageType','treatmentName','promisedPrice','city','latestAyuM','hospitalName','doctorName',
        'docUploaded'
        ,'submittedBy','documentName','dischargeSummaryDoc'])
         
        surgery.fillna('',inplace = True) 
        for key,val in surgery.iterrows():
            DATA.append([
                str(val['caseId']),
                str(val['surgeryId']),
                str(val['createdOn']),
                val['treatmentType'],
                str(val['admissionDate']),
                str(val['dischargeDate']),
                val['surgeryPackageType'], 
                val['treatmentName'],
                str(val['promisedPrice']),
                val['city'],
                val['latestAyuM'],
                val['hospitalName'],
                val['doctorName'],
                val['docUploaded'],
                val['submittedBy'],
                str(val['documentName']),
                str(val['dischargeSummaryDoc'])
                ])
                
        clear_and_write_to_sheet('1AClT2oxk-dYLzI-NKs95HV4-GNUSME7Bf-Lr3fICke4','Discharge','A1:Q',DATA) 
        
        
        # prescription
        prescription.to_csv(fpath)
        
        commentsDump = {}
        for key,val in app_done.iterrows():
            apptId = str(val['appId'])
            
            if apptId not in commentsDump.keys():
                commentsDump[val['appId']] = val['email']
                
        prescription['ayuMitra'] = prescription.apply(lambda x: getAyuMitra(x['latestAyuM'], str(x['appId']), commentsDump), axis=1)
        
        
        
        
        	
        DATA = []	
        DATA.append(['appId','caseId','appointmentDate','patientName','hospitalName','consultationType',
        'consultationFee','hospitalPrice','city','ayuMitra','doctorName','prescriptionUploaded',
        'description','submittedBy','prescriptions','documentName'])	
        prescription.fillna('',inplace = True) 
        for key,val in prescription.iterrows():
            DATA.append([
                str(val['appId']),
                str(val['leadId']),
                str(val['appointmentDate']),
                val['patientName'],
                str(val['hospitalName']),
                str(val['consultationType']),
                val['consultationFee'], 
                val['hospitalPrice'],
                str(val['city']),
                val['ayuMitra'],
                val['doctorName'],
                val['prescriptionUploaded'],
                val['description'],
                val['submittedBy'],
                val['prescriptions'],
                val['documentName']
                ])
                
        clear_and_write_to_sheet('1AClT2oxk-dYLzI-NKs95HV4-GNUSME7Bf-Lr3fICke4','Prescription','A1:P',DATA)         
        
        
            
        
    
        
        
        
        
    except Exception as e:
        raise e
