import pdb
import os
import pytz
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import json
from datetime import datetime, timedelta
from service.send_mail_client import send_email
import requests

from bq import main
from write import clear_and_write_to_sheet


def lambda_handler(event, context):
    try:
        data, data1 = main()
        print(data1.columns)

        data_to_send = [['Rating', 'otherQuestionsflag', 'SupportfromAyuMitra',
                         'Qualityofconsultationwithdoctors', 'HospitalInfrastructure',
                         'InteractionwithCS', 'OverallExperience', 'isfirsttimevisit', 'source',
                         'reasonforConsidering', 'referralcontacts', 'suggestions', 'apptId',
                         'SubmittedAt', 'Token', 'caseId', 'contactNumber', 'city',
                         'submittedDate']]

        for i, j in data.iterrows():
            data_to_send.append([
                j['Rating'], j['otherQuestionsflag'], j['SupportfromAyuMitra'],
                j['Qualityofconsultationwithdoctors'], j['HospitalInfrastructure'],
                j['InteractionwithCS'], j['OverallExperience'], j['isfirsttimevisit'], j['source'],
                j['reasonforConsidering'], j['referralcontacts'], j['suggestions'], j['apptId'],
                str(j['SubmittedAt']), j['Token'], j['caseId'], j['contactNumber'], j['city'],
                str(j['submittedDate'])
            ])
        data_to_send_1 = [['SupportfromAyuMitra', 'nursingSupport', 'doctorBehaviour',
                           'roomQuality', 'foodQuality', 'billingandDischarge', 'review',
                           'reasonforConsidering', 'recommend', 'suggestions', 'caseid',
                           'SubmittedAt', 'Token', 'contactNumber', 'city', 'submittedDate']]

        for i, j in data1.iterrows():
            data_to_send_1.append([
                j['SupportfromAyuMitra'], j['nursingSupport'], j['doctorBehaviour'],
                j['roomQuality'], j['foodQuality'], j['billingandDischarge'], j['review'],
                j['reasonforConsidering'], j['recommend'], j['suggestions'], j['caseid'],
                str(j['SubmittedAt']), j['Token'], j['contactNumber'], j['city'], str(j['submittedDate'])
            ])
        clear_and_write_to_sheet('nps_feedback', 'A1:S', data_to_send)
        clear_and_write_to_sheet('nps_surgery', 'A1:P', data_to_send_1)




    except Exception as e:
        Subject = "nps_feedback_data_bq_to_sheet error"
        email_recipient_list = ['Analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None', e, [])
        raise e



