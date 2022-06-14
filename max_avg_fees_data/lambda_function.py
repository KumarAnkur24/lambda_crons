import pdb
import os
import pytz
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
import json
from service.base_functions import msg_to
from datetime import datetime, timedelta
from service.send_mail_client import send_email
import requests

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {"Data": """ select ldc.doctorID,dp.name,  ldc.hospitalID, afp.aliasName, ldc.appointmentDate, ldc.consultationType, 
                    ldc.consultationFee, ldc.tenantName, ldc.appointmentCreationType,
                    case when ac.cityName = 'Chandigarh' then 'CHD'
                            when ac.cityName = 'Bangalore' then 'BLR'
                            when ac.cityName is not null then ac.cityName
                            else 'No City' end as city
                    from lead_doctor_consultation ldc 
                    left join doctors_profile dp on dp.doctorProfileID = ldc.doctorID
                    left join ayu_facility_profile afp on afp.facilityId = ldc.hospitalID
                    left join ayu_cities ac on afp.cityId = ac.id
                    where ldc.appointmentDate > "2022-03-0" 
                    and ldc.tenantName = "AYU" 

                    """
           }


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
        data = pd.DataFrame(dataSets['Data'])

        fpath = os.path.join('/tmp', 'required_data.csv')
        data.to_csv(fpath)

        Subject = 'Fees data'
        email_recipient_list = ['nikunj.r@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None', None, [fpath])

    except Exception as e:
        raise e


