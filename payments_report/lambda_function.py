import json
import pdb
import os
import logging
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import pandas as pd
from datetime import datetime, timedelta
# from write_to_gsheet import contoller
from service.send_mail_client import send_email

# from write_to_gsheet import contoller

# Description : Online Payment VS Other Mode Of Payment %
# Created By: Akchansh Kumar

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

queries = {"loyalty": '''select  l.cardId ,paymentMode, paymentStatus , l.createdOn, 
                        case 	
                             when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                            else 'No City' end as city
                        from
                            lc_payment_details l join generated_mcards c on l.cardId = c.cardId
                        inner join (select cp.customerId, cityId from  customer_profile cp 
                        inner join  patient_profile pp on cp.customerId = pp.customerId ) ci on  c.customerId = ci.customerId
                        left join ayu_cities ppp on ci.cityId = ppp.id

                        where
                            PaymentStatus = 'PAID' and
                            date(date_add(l.createdOn, INTERVAL '5:30' HOUR_MINUTE)) >= '{startDate}' and
                            date(date_add(l.createdOn, INTERVAL '5:30' HOUR_MINUTE)) <= '{endDate}'  

    ''',
           "appointment": '''
                            select paymentMode ,consultationType, ldc.Id , leadId, Status,
                            case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                            else 'No City' end as city
                            from lead_doctor_consultation ldc
                            inner join online_payment_link opl on (opl.entityType ='APPOINTMENT' and  ldc.id = opl.entityId)
                            join patient_case pc on ldc.leadId = pc.caseId
                            join patient_profile pp on pc.patientId = pp.id
                            join customer_profile cp on pp.customerId = cp.customerId
                            left join ayu_cities pcp on pc.cityId = pcp.id
                            left join ayu_cities ppp on pp.cityId = ppp.id

                            where
                                paymentMode != 'ZERO_FEE' and
                                Status = 'PAID' 
                                and date(date_add(ldc.createdOn, INTERVAL '5:30' HOUR_MINUTE)) >= '{startDate}'
                                and date(date_add(ldc.createdOn, INTERVAL '5:30' HOUR_MINUTE)) <= '{endDate}'

    ''',
           "IPD": '''
                    select psd.*,pspd.*,
                    case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName is not null then ppp.cityName
                            else 'No City' end as city 
                    from 
                        patient_surgery_details psd
                        join patient_surgery_payment_details pspd on (psd.id = pspd.patientSurgeryId)
                        join patient_case pc on psd.caseId = pc.caseId
                        join patient_profile pp on pc.patientId = pp.id
                        join customer_profile cp on pp.customerId = cp.customerId
                        left join ayu_cities pcp on pc.cityId = pcp.id
                        left join ayu_cities ppp on pp.cityId = ppp.id

                    where
                        pspd.paymentStatus = 'PAID'
                        and date(date_add(pspd.createdOn, INTERVAL '5:30' HOUR_MINUTE)) >= '{startDate}'
                        and date(date_add(pspd.createdOn, INTERVAL '5:30' HOUR_MINUTE)) <= '{endDate}'

    ''',
           'cities': '''select
                    case when cityName = 'Chandigarh' then 'CHD'
                    when cityName = 'Bangalore' then 'BLR'
                    when cityName is not null then cityName
                    else 'No City' end as city
                    from ayu_cities
    '''
           }

yest = datetime.now() + timedelta(hours=5, minutes=30, days=-1)
mtd = yest.replace(day=1)
currentMonth = mtd.strftime('%B-%Y')
lmEnd = mtd - timedelta(days=1)
lmStart = lmEnd.replace(day=1)

lm2End = lmStart - timedelta(days=1)
lm2Start = lm2End.replace(day=1)
yest = yest.strftime('%Y-%m-%d')
mtd = mtd.strftime('%Y-%m-%d')
lmEnd = lmEnd.strftime('%Y-%m-%d')
lmStart = lmStart.strftime('%Y-%m-%d')
lm2End = lm2End.strftime('%Y-%m-%d')
lm2Start = lm2Start.strftime('%Y-%m-%d')

today = datetime.now()
thisWeekStart = today - timedelta(today.weekday())
thisWeekEnd = thisWeekStart + timedelta(days=7)
lastWeekStart = thisWeekStart - timedelta(days=7)
lastWeekStart = lastWeekStart.strftime('%Y-%m-%d')
lastWeekEnd = thisWeekStart - timedelta(days=1)
lastWeekEnd = lastWeekEnd.strftime('%Y-%m-%d')
thisWeekStart = thisWeekStart.strftime('%Y-%m-%d')


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == 'cities':
            fetched_val[lookup] = fetch_record(conn, query)
        else:
            fetched_val[lookup + '_mtd'] = fetch_record(conn, query.format(startDate=mtd, endDate=yest))
            fetched_val[lookup + '_lm'] = fetch_record(conn, query.format(startDate=lmStart, endDate=lmEnd))
            fetched_val[lookup + '_lw'] = fetch_record(conn, query.format(startDate=lastWeekStart, endDate=lastWeekEnd))
            fetched_val[lookup + '_tw'] = fetch_record(conn, query.format(startDate=thisWeekStart, endDate=yest))
        # print(query.format(startDate = lmStart, endDate = lmEnd))
    return fetched_val


result_base_1 = '''<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 13 width = \"1200\"><b><center> Monthly Payment report | {0}  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1 rowspan=2  width = \"150\"><b><center> Service </center></b></td>
            <td colspan = 6  width = \"300\"><b><center> MTD </center></b></td>
            <td colspan = 6  width = \"300\"><b><center> Last Month </center></b></td>


        </tr>
        <tr bgcolor=\"#F5EEF8\"> 
            <td colspan = 1  width = \"100\"><b><center> Razorpay  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Upi  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Other  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Insurance  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Cash  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> KTKSWIPE  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Razorpay  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Upi  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Other </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Insurance </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Cash  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> KTKSWIPE  </center></b></td>
'''

result_base_2 = '''<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#D1DCB2\"> 
            <td colspan = 13 width = \"1200\"><b><center> Weekly Payment report | {0}  </center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1 rowspan=2  width = \"150\"><b><center> Service </center></b></td>
            <td colspan = 6 width = \"300\"><b><center> Last Week </center></b></td>
            <td colspan = 6 width = \"300\"><b><center> This Week </center></b></td>


        </tr>
        <tr bgcolor=\"#F5EEF8\"> 
            <td colspan = 1  width = \"100\"><b><center> Razorpay  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Upi  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Other  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Insurance  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Cash  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> KTKSWIPE  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Razorpay  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Upi  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Other </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Insurance </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Cash  </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> KTKSWIPE  </center></b></td>

'''

service = {'Online Consultation': 0, 'Consultation': 1, 'Diagnostics': 2, 'Loyalty': 3, 'IPD': 4}


def payment_option(x):
    return {'RAZOR_PAY': 1, 'UPI': 2, 'CASH': 5, 'RAZOR_PAY_UPI_PAID': 1, 'INSURANCE': 4, 'GOOGLE_PAY': 2,
            'KTKSWIPE': 6}.get(x, 3)


def prepare_result_monthly(IPD_lm, IPD_mtd, appointment_lm, appointment_mtd, loyalty_lm, loyalty_mtd, city, result):
    table1 = [[0 for x in range(13)] for x in range(5)]
    offset = 6

    for index, val in appointment_lm.iterrows():
        paymentMode = val['paymentMode']
        consultationType = val['consultationType']
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode) + offset
        table1[rownumber][colnumber] += 1

    for index, val in appointment_mtd.iterrows():
        paymentMode = val['paymentMode']
        consultationType = val['consultationType']
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode)
        table1[rownumber][colnumber] += 1

    for index, val in IPD_lm.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'IPD'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode) + offset
        table1[rownumber][colnumber] += 1

    for index, val in IPD_mtd.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'IPD'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode)
        table1[rownumber][colnumber] += 1

    for index, val in loyalty_lm.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'Loyalty'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode) + offset
        table1[rownumber][colnumber] += 1

    for index, val in loyalty_mtd.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'Loyalty'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode)
        table1[rownumber][colnumber] += 1

    table1[0][0] = 'Online Consultation'
    table1[1][0] = 'Consultation'
    table1[2][0] = 'Diagnostics'
    table1[3][0] = 'Loyalty'
    table1[4][0] = 'IPD'

    table2 = [[0 for x in range(13)] for x in range(5)]
    for i in range(5):
        for j in range(13):
            table2[i][j] = table1[i][j]

    for i in range(0, 5):
        for j in range(1, 13):
            total = 0
            if j in range(1, 7):
                total = table2[i][1] + table2[i][2] + table2[i][3] + table2[i][4] + table2[i][5] + table2[i][6]

            else:
                total = table2[i][7] + table2[i][8] + table2[i][9] + table2[i][10] + table2[i][11] + table2[i][12]

            table1[i][j] = round((table1[i][j] / total) * 100, 2) if total != 0 else 0
    result += result_base_1.format(city)
    result = resultant_table(table1, 5, 13, result)

    return result


def resultant_table(table, rows, columns, result):
    for i in range(0, rows):
        result = result + "<tr>"
        for j in range(0, columns):
            if j == 0:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif table[i][j] == 0:
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"

    return result


#
def prepare_result_weekly(appointment_lw, appointment_tw, IPD_lw, IPD_tw, loyalty_lw, loyalty_tw, result, city):
    table1 = [[0 for x in range(13)] for x in range(5)]
    onset = 6

    for index, val in appointment_lw.iterrows():
        paymentMode = val['paymentMode']
        consultationType = val['consultationType']
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode)
        table1[rownumber][colnumber] += 1

    for index, val in appointment_tw.iterrows():
        paymentMode = val['paymentMode']
        consultationType = val['consultationType']
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode) + onset
        table1[rownumber][colnumber] += 1

    for index, val in IPD_lw.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'IPD'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode)
        table1[rownumber][colnumber] += 1

    for index, val in IPD_tw.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'IPD'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode) + onset
        table1[rownumber][colnumber] += 1

    for index, val in loyalty_lw.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'Loyalty'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode) + onset
        table1[rownumber][colnumber] += 1

    for index, val in loyalty_tw.iterrows():
        paymentMode = val['paymentMode']
        consultationType = 'Loyalty'
        rownumber = service[consultationType]
        colnumber = payment_option(paymentMode)
        table1[rownumber][colnumber] += 1

    table1[0][0] = 'Online Consultation'
    table1[1][0] = 'Consultation'
    table1[2][0] = 'Diagnostics'
    table1[3][0] = 'Loyalty'
    table1[4][0] = 'IPD'

    table2 = [[0 for x in range(13)] for x in range(5)]
    for i in range(5):
        for j in range(13):
            table2[i][j] = table1[i][j]

    for i in range(0, 5):
        for j in range(1, 13):
            total = 0
            if j in range(1, 7):
                total = table2[i][1] + table2[i][2] + table2[i][3] + table2[i][4] + table2[i][5] + table2[i][6]
            else:
                total = table2[i][7] + table2[i][8] + table2[i][9] + table2[i][10] + table2[i][11] + table2[i][12]

            table1[i][j] = round((table1[i][j] / total) * 100, 2) if total != 0 else 0

    result += result_base_2.format(city)
    result = resultant_table(table1, 5, 13, result)

    return result


fpath = os.path.join('/tmp', 'payment_report.xlsx')
buffer_obj = open(fpath, 'wb+')


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        loyalty_mtd = pd.DataFrame(dataSets['loyalty_mtd'])
        loyalty_lm = pd.DataFrame(dataSets['loyalty_lm'])

        appointment_mtd = pd.DataFrame(dataSets['appointment_mtd'])
        appointment_lm = pd.DataFrame(dataSets['appointment_lm'])

        IPD_mtd = pd.DataFrame(dataSets['IPD_mtd'])
        IPD_lm = pd.DataFrame(dataSets['IPD_lm'])

        loyalty_lw = pd.DataFrame(dataSets['loyalty_lw'])
        loyalty_tw = pd.DataFrame(dataSets['loyalty_tw'])

        appointment_lw = pd.DataFrame(dataSets['appointment_lw'])
        appointment_tw = pd.DataFrame(dataSets['appointment_tw'])

        IPD_lw = pd.DataFrame(dataSets['IPD_lw'])
        IPD_tw = pd.DataFrame(dataSets['IPD_tw'])

        cities = pd.DataFrame(dataSets['cities'])

        with pd.ExcelWriter(buffer_obj, mode='wb') as writer:
            appointment_lm.to_excel(writer, index=False, header=True, sheet_name='appointment_lm', engine='xlsxwriter')
            loyalty_lm.to_excel(writer, index=False, header=True, sheet_name='loyalty_lm', engine='xlsxwriter')
            IPD_lm.to_excel(writer, index=False, header=True, sheet_name='IPD_lm', engine='xlsxwriter')
            appointment_mtd.to_excel(writer, index=False, header=True, sheet_name='appointment_tm', engine='xlsxwriter')
            loyalty_mtd.to_excel(writer, index=False, header=True, sheet_name='loyalty_tm', engine='xlsxwriter')
            IPD_mtd.to_excel(writer, index=False, header=True, sheet_name='IPD_tm', engine='xlsxwriter')
        writer.save()
        writer.close()

        city_list = pd.unique(cities['city'])

        IPD_lm_City = pd.DataFrame([])
        IPD_mtd_City = pd.DataFrame([])
        IPD_lw_City = pd.DataFrame([])
        IPD_tw_City = pd.DataFrame([])

        appointment_lm_City = pd.DataFrame([])
        appointment_mtd_City = pd.DataFrame([])
        appointment_lw_City = pd.DataFrame([])
        appointment_tw_City = pd.DataFrame([])

        loyalty_lm_City = pd.DataFrame([])
        loyalty_mtd_City = pd.DataFrame([])
        loyalty_lw_City = pd.DataFrame([])
        loyalty_tw_City = pd.DataFrame([])

        Result = ''

        for city in city_list:
            if len(IPD_lm):
                IPD_lm_City = IPD_lm[IPD_lm['city'] == city]
                # IPD_lm_CHD =IPD_lm[IPD_lm['city'] == 'CHD']
                # IPD_lm_NOCITY =IPD_lm[IPD_lm['city'] == 'No City']

            if len(IPD_mtd):
                IPD_mtd_City = IPD_mtd[IPD_mtd['city'] == city]
                # IPD_mtd_CHD = IPD_mtd[IPD_mtd['city'] == 'CHD']
                # IPD_mtd_NOCITY = IPD_mtd[IPD_mtd['city'] == 'No City']

            if len(IPD_lw):
                IPD_lw_City = IPD_lw[IPD_lw['city'] == city]
                # IPD_lw_CHD = IPD_lw[IPD_lw['city'] == 'CHD']
                # IPD_lw_NOCITY = IPD_lw[IPD_lw['city'] == 'No City']

            if len(IPD_tw):
                IPD_tw_City = IPD_tw[IPD_tw['city'] == city]
                # IPD_tw_CHD = IPD_tw[IPD_tw['city'] == 'CHD']
                # IPD_tw_NOCITY = IPD_tw[IPD_tw['city'] == 'No City']

            if len(appointment_lm):
                appointment_lm_City = appointment_lm[appointment_lm['city'] == city]

            if len(appointment_mtd):
                appointment_mtd_City = appointment_mtd[appointment_mtd['city'] == city]

            if len(appointment_lw):
                appointment_lw_City = appointment_lw[appointment_lw['city'] == city]

            if len(appointment_tw):
                appointment_tw_City = appointment_tw[appointment_tw['city'] == city]

            if len(loyalty_lm):
                loyalty_lm_City = loyalty_lm[loyalty_lm['city'] == city]

            if len(loyalty_mtd):
                loyalty_mtd_City = loyalty_mtd[loyalty_mtd['city'] == city]

            if len(loyalty_lw):
                loyalty_lw_City = loyalty_lw[loyalty_lw['city'] == city]

            if len(loyalty_tw):
                loyalty_tw_City = loyalty_tw[loyalty_tw['city'] == city]

            Result = prepare_result_monthly(IPD_lm_City, IPD_mtd_City, appointment_lm_City, appointment_mtd_City,
                                            loyalty_lm_City, loyalty_mtd_City, city, Result)
            Result = prepare_result_weekly(appointment_lw_City, appointment_tw_City, IPD_lw_City, IPD_tw_City,
                                           loyalty_lw_City, loyalty_tw_City, Result, city)



        Subject = "Online Payment VS Other Mode Of Payment % "
        email_recipient_list = ['arjit@ayu.health', 'anshul@ayu.health', 'shahwaz@ayu.health',
                                'rohit.chahal@ayu.health', 'asif.jamil@ayu.health', 'abu@ayu.health']
        # email_recipient_list = ['akchansh@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None', Result, [fpath])

        # print(loyalty_lm)
    except Exception as e:
        raise e