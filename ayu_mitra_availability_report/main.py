import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
from service.send_mail_client import send_email
import pandas as pd

logger = logging.getLogger(__name__)

# Description: AyuMitra Availability in hospitals city wise
# Created By : Ankur Kumar

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {
    "main": """Select 
                    mitra.email as ayuMitraEmail,

                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                else 'NA' end as city,
                    group_concat(distinct aliasName) as 'hospitals'


                    from 
                    ayu_personnel_details mitra
                    join ayu_mitra_hospital_mapping mapp on mitra.personnelId = mapp.ayuMitraId
                    join ayu_facility_profile hos on mapp.facilityId = hos.facilityId
                    left join ayu_cities pcp on mitra.cityId = pcp.id
                    where
                        isAvailable = 1
                        and mapp.isValid = 1
                        and mapp.tenantName = 'AYU'
                        and personnelType = 'AYU_MITRA'
                        and mappingType = 'AYUMITRA'
                        and hasLeft = 0
                        group by 1,2
                    """
}


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        fetched_val[lookup] = fetch_record(conn, query)
    return fetched_val


result_base = """
        <body width = \"800\">
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 2  width = \"800\"><b><center> Availability - {0} </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"200\"><b><center> Ayu Mitra </center></b></td>
            <td colspan = 1  width = \"600\"><b><center> Hospital </center></b></td>
        </tr>
"""


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        mainData = dataSets['main']

        result_chd = result_base.format('Chandigarh')
        result_blr = result_base.format('Bengaluru')
        result_jp = result_base.format('Jaipur')
        result_ncr = result_base.format('NCR')
        for val in mainData:
            if val['city'] == 'CHD':
                result_chd += """<tr> 
                            <td colspan = 1  width = \"200\"><center> {0} </center></td>
                            <td colspan = 1  width = \"600\"><center> {1} </center></td>
                        </tr>
                       """.format(val['ayuMitraEmail'], val['hospitals'])
            elif val['city'] == 'BLR':
                result_blr += """<tr> 
                            <td colspan = 1  width = \"200\"><center> {0} </center></td>
                            <td colspan = 1  width = \"600\"><center> {1} </center></td>
                        </tr>
                       """.format(val['ayuMitraEmail'], val['hospitals'])
            elif val['city'] == 'Jaipur':
                result_jp += """<tr> 
                            <td colspan = 1  width = \"200\"><center> {0} </center></td>
                            <td colspan = 1  width = \"600\"><center> {1} </center></td>
                        </tr>
                       """.format(val['ayuMitraEmail'], val['hospitals'])
            elif val['city'] == 'NCR':
                result_ncr += """<tr> 
                            <td colspan = 1  width = \"200\"><center> {0} </center></td>
                            <td colspan = 1  width = \"600\"><center> {1} </center></td>
                        </tr>
                       """.format(val['ayuMitraEmail'], val['hospitals'])

        result = result_chd + "</table><br><br>" + result_blr + "</table><br><br>" + result_jp + "</table><br><br>" + result_ncr + "</table></body>"

        Subject = "Ayu Mitra Availability | Hospitals"
        email_recipient_list = ["chd-offline@ayu.health", "managers-and-TLs@ayu.health"]
        # email_recipient_list = ['ankur@ayu.health']
        send_email(None, email_recipient_list, Subject, None, result, [])

    except Exception as e:
        raise e

