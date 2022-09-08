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
from write_to_gsheet import clear_and_write_to_sheet
from read_and_update import main as main2

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
# READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')
READ_DB_DOMAIN_URL_BETA = os.environ.get('READ_DB_DOMAIN_URL_BETA', '')

queries = {
    'main': """ select mdo.*, date_add(mdo.createdOn,INTERVAL '5:30' HOUR_MINUTE) as order_createdOn  ,opm.prescriptions ,
                customerNumber
                from medicine_delivery_orders mdo
                left join (select orderId,group_concat(prescription) as prescriptions

                from
                order_prescription_mappings 
                group by 1)

                opm on mdo.orderId = opm.orderId
                join customer_profile cp on cp.customerId = mdo.entityId
                where entityType = 'CUSTOMER_PROFILE'

                order by mdo.orderId 
    """
}


# ,(amount/100) as ayu_amount

def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val


result_base2 = '''   
        <table style=\"border-collapse:collapse\" border=\"1\" > 
        <tr bgcolor=\"#EEFCF0\">  
            <td colspan = 13 width = \"1300\"><b><center> Medicine Delivery report </center></b></td>
        </tr> 
        <tr> 
            <td colspan = 1  width = \"100\"><b><center> Patient Name </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {0} </center></b></td>
        </tr>
        <tr> 
            <td colspan = 1  width = \"100\"><b><center> Order Creation Date and Time </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {1} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"300\"><b><center> Patient's Adress </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {2} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Pincode </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {3} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Contact No </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {4} </center></b></td>
        </tr>
        <tr>     
            <td colspan = 1  width = \"100\"><b><center> Prescription </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> {5} </center></b></td>
        </tr>
        </table>
        <br>
    '''


# <tr>
#             <td colspan = 1  width = \"100\"><b><center> Ayu Cash Used </center></b></td>
#             <td colspan = 1  width = \"100\"><b><center> {6} </center></b></td>
#         </tr>


# <tr>
#             <td colspan = 1  width = \"100\"><b><center> Ayu Cash Available </center></b></td>
#             <td colspan = 1  width = \"100\"><b><center> {13} </center></b></td>
#         </tr>

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(
            **{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL_BETA})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        main = pd.DataFrame(dataSets['main'])

        result = '<body>'

        print(main.columns)

        data_to_send = []
        if len(main):

            # main['ayu_amount'] = main['ayu_amount'].fillna(0)

            for index, val in main.iterrows():
                # if val['orderStatus'] == 'BOOKED':

                result = result_base2.format(val['name'], val['order_createdOn'], val['address'], val['pincode'],
                                             val['phoneNumber'], val['prescriptions'], '')

                data_to_send.append([
                    val['orderId'],
                    val['entityId'],
                    val['entityType'],
                    val['orderStatus'],
                    val['orderTrackingLink'],
                    str(val['createdOn']),
                    val['address'],
                    val['landmark'],
                    val['city'],
                    val['state'],
                    val['pincode'],
                    val['name'],
                    val['phoneNumber'],
                    val['prescriptions'],
                    val['customerNumber']
                ])

            clear_and_write_to_sheet('1CQTHKJO3KU5QnJBuco__bDKjLY4cOGvxN-evSKG4dYA', 'Orders_beta', 'A2:O{0}',
                                     data_to_send)

            main2()

            Subject = "Medicine Delivery Customer App"
            email_recipient_list = ['akchansh@ayu.health']
            # send_email(None, email_recipient_list, Subject, 'None',result,[])

    except Exception as e:
        Subject = "Medicine Delivery Customer App | ERROR"
        email_recipient_list = ['akchansh@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None', e, [])
        raise e