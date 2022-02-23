import pdb
import os
import logging
import json
import pytz
import gspread
from datetime import datetime, timedelta
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
from oauth2client.service_account import ServiceAccountCredentials
from service.send_mail_client import send_email
import time
import pandas as pd
import boto3
from gspread.models import Cell
import requests

# Description: NPS_Feedback citywise
# Created By : Akchansh Kumar

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
SECRETS_FILE = os.environ.get('SECRETS_FILE', '')

timestamp = datetime.now() - timedelta(hours=1)
now = timestamp.strftime('%d/%m/%Y %H:59:59')
timestamp = timestamp.strftime('%d/%m/%Y %H:00:00')

queries = {
    'Patient': '''
              select pc.caseId , cp.customerNumber , pc.patientId , 
              case when ldc.id is null then 0 else ldc.id end as appId
              ,  


            case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                            else 'NOCITY' end as CITY


              from patient_case as pc
              inner join patient_profile as pp on pp.id = pc.patientId
              inner join customer_profile as cp on pp.customerId = cp.customerId
              left join lead_doctor_consultation as ldc on ldc.leadId = pc.caseId 
              left join ayu_cities pcp on pc.cityId = pcp.id
            left join ayu_cities ppp on pp.cityId = ppp.id
              ''',
    'cities': '''select 
                            case when cityName = 'Chandigarh' then 'CHD'
                            when cityName = 'Bangalore' then 'BLR'
                            when cityName is not null then cityName
                            else 'NOCITY' end as city
                            from ayu_cities

              '''
}


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val


def read_sheet(SHEET_ID, READ_SHEET_NAME, client):
    try:
        client_sp = client.open_by_key(SHEET_ID)
        val = client_sp.worksheet(title=READ_SHEET_NAME)
        return val.get_all_values()

    except Exception as E:
        raise E


def get_time(Time):
    time_val = datetime.strptime(Time, '%d/%m/%Y %H:%M:%S')
    last_hour = datetime.strptime(timestamp, '%d/%m/%Y %H:%M:%S')
    present = datetime.strptime(now, '%d/%m/%Y %H:%M:%S')
    print(time_val, last_hour, present)
    if time_val >= last_hour and time_val <= present:
        return 'True'

    return 'False'


result_base1 = '''   
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        <tr bgcolor=\"#EEFCF0\">  
            <td colspan = 16 width = \"1600\"><b><center> NPS report |{0} </center></b></td>
        </tr> 
        <tr > 
            <td colspan = 1  width = \"100\"><b><center> Thanks for trusting Ayu Health with your health. How likely are you to recommend us to your friends and family? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {1} </center></b></td>
        </tr>
        <tr>
            <td colspan = 1  width = \"100\"><b><center> Thank you for your response! Would you be able to answer a few more questions that allow us to further improve our services? This will not take more than a minute, we promise! </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {2} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Support from Ayu Mitra </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {3} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Quality of consultation with doctors </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {4} </center></b></td>
        </tr>
        <tr >    
            <td colspan = 1  width = \"100\"><b><center> Hospital infrastructure (waiting area, washrooms, cafe, etc.) </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {5} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Interaction with Ayu Health customer support </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {6} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Overall Experience </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {7} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Is this the first time you have visited an Ayu Health hospital? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {8} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Could you tell us about how you discovered Ayu Health? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {9} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> What made you consider Ayu Health? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {10} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Please share phone numbers of friends and family you would want to refer to Ayu Health? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {11} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Any suggestions for us?  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {12} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Appointment Id </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {13} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> customerNumber </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {14} </center></b></td>
        </tr>
        <tr>    
            <td colspan = 1  width = \"100\"><b><center> Submitted At </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {15} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Amigos Url </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {16} </center></b></td>
        </tr>
        </table> 
        <br>  
    '''
result_base2 = '''   
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        <tr bgcolor=\"#EEFCF0\">  
            <td colspan = 13 width = \"1300\"><b><center> NPS report | {0} </center></b></td>
        </tr> 
        <tr> 
            <td colspan = 1  width = \"100\"><b><center> Support from Ayu Mitra </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {1} </center></b></td>
        </tr>
        <tr> 
            <td colspan = 1  width = \"100\"><b><center> Doctor Behaviour </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {2} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"300\"><b><center> Room / ward quality (if applicable) </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {3} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Quality of Food (if applicable) </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {4} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Billing and Discharge </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {5} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Please tell us if you feel relieved from symptoms that you were experiencing before the procedure? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {6} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> What made you consider hospitalisation at Ayu Health? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {7} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"150\"><b><center> Thanks, and how likely are you to recommend us to your friends and family? </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {8} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Any suggestions for us?  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {9} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Caseid </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {10} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> customerNumber </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {11} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Submitted At </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {12} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Amigos Url </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {13} </center></b></td>
        </tr>
        </table>
        <br>
    '''


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)
        patient = pd.DataFrame(dataSets['Patient'])
        patient_case = {}
        appointment_Id = {}
        for index, val in patient.iterrows():
            if str(val['caseId']) not in patient_case.keys():
                patient_case[str(val['caseId'])] = {}
                patient_case[str(val['caseId'])]['customerNumber'] = val['customerNumber']
                patient_case[str(val['caseId'])]['CITY'] = val['CITY']
                patient_case[str(val['caseId'])]['patientId'] = val['patientId']

            appId = str(int(val['appId']))
            if appId not in appointment_Id.keys():
                appointment_Id[appId] = {}
                appointment_Id[appId]['customerNumber'] = val['customerNumber']
                appointment_Id[appId]['CITY'] = val['CITY']
                appointment_Id[appId]['patientId'] = val['patientId']
                appointment_Id[appId]['caseId'] = val['caseId']
        print(appointment_Id)

        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        boto3.client('s3').download_file(S3_BUCKET_NAME, SECRETS_FILE, '/tmp/secrets.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/secrets.json", scope)
        client = gspread.authorize(creds)
        SHEET_ID = '1OJhaPaTQ2o9-MM-ve4HoUrsNt5tiF1FtXXFbqPRuHLQ'
        READ_SHEET_NAME = 'Ayu Health Feedback Survey (Post Consultation)'
        READ_SHEET_NAME2 = 'Ayu Health Feedback Survey (Post Procedure)'

        'copy sheet :1OJhaPaTQ2o9-MM-ve4HoUrsNt5tiF1FtXXFbqPRuHLQ'
        'original sheet: 1Br1rUtd50gUZxb-ilzc2K27kNCxyYxxNUjUYzeqOAhs'

        consultation_data = read_sheet(SHEET_ID, READ_SHEET_NAME, client)
        consultation_data = pd.DataFrame(consultation_data)

        Surgery_data = read_sheet(SHEET_ID, READ_SHEET_NAME2, client)
        Surgery_data = pd.DataFrame(Surgery_data)

        consultation_data.columns = consultation_data.iloc[0]
        consultation_data = consultation_data.drop([0], axis=0)
        consultation_data['TimeFlag'] = consultation_data.apply(lambda x: get_time(x['Submitted At']), axis=1)

        cities = dataSets['cities']

        city_list = [x['city'] for x in cities]

        city_list.append('NOCITY')

        print(city_list)

        In_range = (consultation_data[consultation_data['TimeFlag'] == 'True'])
        # print(In_range)

        result = {}

        result_surgery = {}

        for city in city_list:

            if city not in result.keys():
                result[city] = {}
                result[city]['flag'] = False
                result[city]['result'] = '<body>'

            if city not in result_surgery.keys():
                result_surgery[city] = {}
                result_surgery[city]['flag'] = False
                result_surgery[city]['result'] = '<body>'

        if len(In_range) > 0:

            for index, val in In_range.iterrows():
                submitted_at = datetime.strptime(val['Submitted At'], '%d/%m/%Y %H:%M:%S') + timedelta(hours=5,
                                                                                                       minutes=30)
                submitted_at = submitted_at.strftime('%d/%m/%Y %H:%M:%S')
                if str(val['caseid']) in appointment_Id.keys():
                    patientId = appointment_Id[str(val['caseid'])]['patientId']
                    customerNumber = appointment_Id[str(val['caseid'])]['customerNumber']
                    caseId = appointment_Id[str(val['caseid'])]['caseId']
                    amigos_url = 'https://amigos.ayu.health/patient/{patientId}/case/{caseId}'
                    city = appointment_Id[str(val['caseid'])]['CITY']

                    result[city]['result'] += result_base1.format(city, val[
                        'Thanks for trusting Ayu Health with your health. How likely are you to recommend us to your friends and family?'],
                                                                  val[
                                                                      'Thank you for your response! Would you be able to answer a few more questions that allow us to further improve our services? This will not take more than a minute, we promise!'],
                                                                  val['Support from Ayu Mitra'],
                                                                  val['Quality of consultation with doctors'],
                                                                  val[
                                                                      'Hospital infrastructure (waiting area, washrooms, cafe, etc.)'],
                                                                  val['Interaction with Ayu Health customer support'],
                                                                  val['Overall Experience'],
                                                                  val[
                                                                      'Is this the first time you have visited an Ayu Health hospital?'],
                                                                  val[
                                                                      'Could you tell us about how you discovered Ayu Health?'],
                                                                  val['What made you consider Ayu Health?'],
                                                                  val['''Please share phone numbers of friends and family you would want to refer to Ayu Health?
(Each succesful referral gets Paytm cashback upto Rs. 500)'''],
                                                                  val['Any suggestions for us? '],
                                                                  val['caseid'], customerNumber, submitted_at,
                                                                  amigos_url.format(patientId=patientId, caseId=caseId))

                    result[city]['flag'] = True


                else:
                    result['NOCITY']['result'] += result_base1.format('', val[
                        'Thanks for trusting Ayu Health with your health. How likely are you to recommend us to your friends and family?'],
                                                                      val[
                                                                          'Thank you for your response! Would you be able to answer a few more questions that allow us to further improve our services? This will not take more than a minute, we promise!'],
                                                                      val['Support from Ayu Mitra'],
                                                                      val['Quality of consultation with doctors'],
                                                                      val[
                                                                          'Hospital infrastructure (waiting area, washrooms, cafe, etc.)'],
                                                                      val[
                                                                          'Interaction with Ayu Health customer support'],
                                                                      val['Overall Experience'],
                                                                      val[
                                                                          'Is this the first time you have visited an Ayu Health hospital?'],
                                                                      val[
                                                                          'Could you tell us about how you discovered Ayu Health?'],
                                                                      val['What made you consider Ayu Health?'],
                                                                      val['''Please share phone numbers of friends and family you would want to refer to Ayu Health?
(Each succesful referral gets Paytm cashback upto Rs. 500)'''],
                                                                      val['Any suggestions for us? '],
                                                                      val['caseid'], '', submitted_at, '')

                    result['NOCITY']['flag'] = True

        Surgery_data.columns = Surgery_data.iloc[0]
        Surgery_data = Surgery_data.drop([0], axis=0)
        Surgery_data['TimeFlag'] = Surgery_data.apply(lambda x: get_time(x['Submitted At']), axis=1)

        In_range1 = (Surgery_data[Surgery_data['TimeFlag'] == 'True'])
        # print(In_range1)

        if len(In_range1) > 0:
            result_chd_surgery = '<body>'
            result_blr_surgery = '<body>'
            result_Nocity_surgery = '<body>'
            for index, val1 in In_range1.iterrows():
                if val1['caseid'] in patient_case.keys():
                    patientId = patient_case[val['caseid']]['patientId']
                    customerNumber = patient_case[val['caseid']]['customerNumber']
                    amigos_url = 'https://amigos.ayu.health/patient/{patientId}/case/{caseId}'
                    submitted_at = datetime.strptime(val1['Submitted At'], '%d/%m/%Y %H:%M:%S') + timedelta(hours=5,
                                                                                                            minutes=30)
                    submitted_at = submitted_at.strftime('%d/%m/%Y %H:%M:%S')
                    city = patient_case[val1['caseid']]['CITY']

                    result_surgery[city]['result'] += result_base2.format(city, val1['Support from Ayu Mitra'],
                                                                          val1['Doctor Behaviour'],
                                                                          val1['Room / ward quality (if applicable)'],
                                                                          val1['Quality of Food (if applicable)'],
                                                                          val1['Billing and Discharge'], val1[
                                                                              'Please tell us if you feel relieved from symptoms that you were experiencing before the procedure?'],
                                                                          val1[
                                                                              'What made you consider hospitalisation at Ayu Health?'],
                                                                          val1[
                                                                              'Thanks, and how likely are you to recommend us to your friends and family?'],
                                                                          val1['Any suggestions for us? '],
                                                                          val1['caseid'], customerNumber, submitted_at,
                                                                          amigos_url.format(patientId=patientId,
                                                                                            caseId=val1['caseid']))

                    result[city]['flag'] = True



                else:
                    result_surgery['NOCITY']['result'] += result_base2.format('NOCITY', val1['Support from Ayu Mitra'],
                                                                              val1['Doctor Behaviour'],
                                                                              val1[
                                                                                  'Room / ward quality (if applicable)'],
                                                                              val1['Quality of Food (if applicable)'],
                                                                              val1['Billing and Discharge'], val1[
                                                                                  'Please tell us if you feel relieved from symptoms that you were experiencing before the procedure?'],
                                                                              val1[
                                                                                  'What made you consider hospitalisation at Ayu Health?'],
                                                                              val1[
                                                                                  'Thanks, and how likely are you to recommend us to your friends and family?'],
                                                                              val1['Any suggestions for us? '],
                                                                              val1['caseid'], '', submitted_at, '')
                    result_surgery['NOCITY'][flag] = True

        def email_dict(city):
            return {'CHD': 'nps_chandigarh@ayu.health',
                    'BLR': 'nps_bangalore@ayu.health',
                    'Jaipur': 'nps_chandigarh@ayu.health',
                    'NCR': 'nps_chandigarh@ayu.health'

                    }.get(city, 'nps_all@ayu.health')

        for city, val in result.items():

            if val['flag']:
                Subject = "NPS Feedback  | Consultation | " + city
                email_recipient_list = email_dict(city)
                # email_recipient_list = ['akchansh@ayu.health']
                send_email(None, email_recipient_list, Subject, 'None', val['result'], [])

        for city, value in result_surgery.items():

            if value['flag']:
                Subject = "NPS Feedback  | Surgery | " + city
                email_recipient_list = email_dict(city)
                # email_recipient_list = ['akchansh@ayu.health']
                send_email(None, email_recipient_list, Subject, 'None', value['result'], [])



    except Exception as E:
        Subject = 'NPS Feedback Alarm'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None', E, [])
        raise E

