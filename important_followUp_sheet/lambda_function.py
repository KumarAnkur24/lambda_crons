import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import pandas as pd
# from write_to_gsheet import clear_and_write_to_sheet
from write_to_gsheet import write_to_sheet
import requests
import json
from datetime import datetime, timedelta
from service.send_mail_client import send_email

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {
    "main": """Select 
                    mitra.email as ayuMitraEmail,
                    case when ac.cityName = 'Chandigarh' then 'CHD'
                                when ac.cityName = 'Bangalore' then 'BLR'
                                when ac.cityName is not null then ac.cityName
                                else 'No City' end as city
                from 
                    ayu_personnel_details mitra 
                    left join ayu_cities ac on ac.id = mitra.cityId
                where

                        personnelType = 'AYU_MITRA'
                        and isAvailable = 1



                    """
}

date = datetime.now()
print(date)


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)
    return fetched_val


def getCashBalance(ayuMitra):
    url = 'http://backend.cron.api.ayu.health/ayu-mitra/{ayuMitraEmail}/importantFollowUps'

    response = requests.get(url.format(ayuMitraEmail=ayuMitra))

    return response


def bucket(second):
    if second <= 600:
        return "less than 10_Mins"
    elif second > 600 and second <= 1800:
        return "10 - 30_Mins"
    elif second > 1800 and second <= 3600:
        return "30 - 60_Mins"
    elif second > 3600 and second <= 7200:
        return "1 - 2_hours"
    elif second > 7200 and second <= 36000:
        return "2 - 10_hours"
    else:
        return "more than 10_hours"


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        mainData = dataSets['main']
        mainData_ = pd.DataFrame(dataSets['main'])

        # print(mainData)

        main = pd.DataFrame([])
        for val in mainData:

            city = val['city']
            ayuMitra = val['ayuMitraEmail']
            response = getCashBalance(ayuMitra)
            # print(response.status_code)
            if response.status_code == 200:

                data = json.loads(response.text)
                if len(data) == 0:
                    continue
                df = pd.DataFrame(data)

                df['creted'] = df.apply(lambda x: datetime.fromtimestamp(x['createdOn'] / 1000), axis=1)
                df['diff'] = df.apply(lambda x: (date - x['creted']), axis=1)
                df['second'] = df.apply(lambda x: x['diff'].total_seconds(), axis=1)

                df['bucket_'] = df.apply(lambda x: bucket(x['second']), axis=1)
                df['email'] = df.apply(lambda x: ayuMitra, axis=1)
                df['city'] = df.apply(lambda x: city, axis=1)
                main = pd.concat([main, df])

        # final = pd.concat([mainData_,df], axis=1)

        print(main)
        finaldata = [['caseId', 'patientId', 'entityId',
                      'entityType', 'followUpReason', 'followUpDate', 'followUpTime',
                      'createdOn', 'creted', 'diff', 'second', 'bucket_', 'email', 'city']]

        for index, val in main.iterrows():
            finaldata.append([

                val['caseId'],
                val['patientId'],
                val['entityId'],
                val['entityType'],
                val['followUpReason'],
                str(val['followUpDate']),
                str(val['followUpTime']),
                str(val['createdOn']),
                str(val['creted']),
                str(val['diff']),
                val['second'],
                val['bucket_'],
                val['email'],
                val['city']
            ])

        write_to_sheet('16Bkb9GEJfLMvGFy78PEeRrutwPYRkH6V-PyLJFbCo4Q', 'data', 'A1:Z', finaldata)

        # Subject = 'important_FollowUp_sheet'
        # email_recipient_list = ['nikunj.r@ayu.health']
        # send_email(None, email_recipient_list, Subject, 'None',None,[])


    except Exception as e:

        raise e

