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

#Created By: Akchansh Kumar
#Description : Appointment Tag summary for date range 1-7,8-14,15-21,21-last_day


logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

fpath = os.path.join('/tmp', 'ayuM_app_track_confirmed.csv')
# fpath1 = os.path.join('/tmp', 'ayuM_app_track_patient_reached.csv')

yesterday = datetime.now() + timedelta(hours=5, minutes=30, days=-1)

today_date = datetime.now() + timedelta(hours=5, minutes=30)
month = today_date.month
today = today_date.day

if month in (1, 3, 5, 7, 8, 10, 12):
    if today == 7:
        end_day = yesterday.replace(day=7)
        start_day = yesterday.replace(day=1)

    elif today == 14:
        end_day = yesterday.replace(day=14)
        start_day = yesterday.replace(day=8)

    elif today == 21:
        end_day = yesterday.replace(day=21)
        start_day = yesterday.replace(day=15)

    elif today == 31:
        end_day = yesterday.replace(day=31)
        start_day = yesterday.replace(day=22)

elif month in (4, 6, 9, 11):
    if today == 7:
        end_day = yesterday.replace(day=7)
        start_day = yesterday.replace(day=1)

    elif today == 14:
        end_day = yesterday.replace(day=14)
        start_day = yesterday.replace(day=8)

    elif today == 21:
        end_day = yesterday.replace(day=21)
        start_day = yesterday.replace(day=15)

    elif today == 31:
        end_day = yesterday.replace(day=30)
        start_day = yesterday.replace(day=22)

else:
    if today == 7:
        end_day = yesterday.replace(day=7)
        start_day = yesterday.replace(day=1)

    elif today == 14:
        end_day = yesterday.replace(day=14)
        start_day = yesterday.replace(day=8)

    elif today == 21:
        end_day = yesterday.replace(day=21)
        start_day = yesterday.replace(day=15)

    elif today == 31:
        end_day = yesterday.replace(day=28)
        start_day = yesterday.replace(day=22)

queries = {
    'ayu_mitra': '''select personnelId ,email from ayu_personnel_details
                                where 
                                personnelType = 'AYU_MITRA'
            ''',
    'confirmed_cases': '''select ldc.* ,zone, pc.leadSource ,
                                    case 
                                        when afp.cityId = 1 then 'CHD'
                                        when afp.cityId = 2 then 'BLR'
                                        when afp.cityId = 3 then 'Jaipur'
                                    end as city
                                from lead_doctor_consultation ldc
                            join ayu_facility_profile  afp on ldc.hospitalId = afp.facilityId  
                            join patient_case pc on pc.caseId = ldc.leadId 
                            where
                            ldc.id in (select distinct id 
                                    from lead_doctor_consultation_AUD
                                    where 
                                        date(appointmentDate) >= '{start}'
                                        and date(appointmentDate) <= '{end}' 
                                        and doctorConsultationStatus = 2) 
                            and afp.cityId in (2)
                            order by ldc.id desc
            ''',
    'appointment': '''select ldc.id, date_add(createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
                                doctorConsultationStatus,
                                '' as tagName,
                                ayuMitraId
                            from lead_doctor_consultation_AUD ldc
                                where 
                                    ldc.id in ({id}) 
                                order by ldc.createdOn 
            ''',
    "app_tags": """select e.entityId as 'id',a.tagName as tagName,
                            date_add(e.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
                            '' as doctorConsultationStatus,
                            '' as ayuMitraId
                    from entity_tag_map e
                    join attribute_tags a on e.tagId = a.tagId
                where e.entityType = 'APPOINTMENT'
                    and tagCategory in ('APPOINTMENT_STAGE', 'AYU_MITRA') 
                    and e.entityId in ({id})
                order by e.createdOn
                    """,
    "app_done": """select leadId as appId,text,user
                        from
                        lead_comments lc
                        where 
                            commentType in ('APPOINTMENT_DONE') 
                            and leadType = 'APPOINTMENT'
                            and leadId in ({id})
                        order by commentId"""
}

DoctorConsultationStatus = {
    '0': 'OPEN',
    '1': 'BOOKED',
    '2': 'CONFIRMED',
    '3': 'DONE',
    '4': 'CANCELLED',
    None: 'None',
    '': 'None',
    '5': 'OPEN',
    '6': 'PATIENT_HAS_REACHED',
    '7': 'APPOINTMENT_STARTED'
}

start_day = start_day.strftime('%Y-%m-%d')
end_day = end_day.strftime('%Y-%m-%d')

print(start_day, end_day)


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == 'confirmed_cases':

            fetched_val[lookup] = fetch_record(conn, query.format(start=start_day, end=end_day))
            caseIds = [str(x['id']) for x in fetched_val[lookup]]

            print(caseIds)

        elif lookup in ('appointment', 'app_tags', 'app_done'):

            # caseIds.extend(caseIds1)
            ids = ",".join(caseIds)
            fetched_val[lookup] = fetch_record(conn, query.format(id=ids))

        else:

            fetched_val[lookup] = fetch_record(conn, query)

    return fetched_val


def get_ayumitra(ayuMitraId, ayu_dict, app_done_dump, appointmentDump, Doctor_Consultation_Status, appId):
    if Doctor_Consultation_Status == 'DONE':
        if appId in app_done_dump.keys():
            return app_done_dump[appId]

    if appId in appointmentDump.keys():
        dump = appointmentDump[appId]
        for row in dump:
            if DoctorConsultationStatus[row['doctorConsultationStatus']] == Doctor_Consultation_Status:
                if row['ayuMitraId'] in ayu_dict.keys():
                    return ayu_dict[row['ayuMitraId']]

    if ayuMitraId in ayu_dict.keys():
        return ayu_dict[ayuMitraId]

    return ''


result_base = '''<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#A6DBFE\"> 
            <td colspan = 8 width = \"1100\"><b><center> New FollowUp Report - Ayu M Tag Update Tracking : Confirmed Cases | BLR  | {0}</center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"200\"><b><center> Ayu Mitra </center></b></td>
            <td colspan = 1  width = \"200\"><b><center> Zones </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Total Appointments </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated till t+5 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated between t+5 to t+15 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated between t+15 to t+30 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated between t+30 to t+60 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated after t+60 mins </center></b></td>
        </tr>

'''

result_base1 = '''<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#A6DBFE\"> 
            <td colspan = 9 width = \"1300\"><b><center> New FollowUp Report - Ayu M Tag Update Tracking : Confirmed Cases | BLR  | {0}</center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"200\"><b><center> Ayu Mitra </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Zones </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Total Appointments </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Updated till t+5 mins </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Updated between t+5 to t+15 mins </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Updated between t+15 to t+30 mins </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Updated between t+30 to t+60 mins </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> Updated after t+60 mins </center></b></td>
            <td colspan = 1  width = \"150\"><b><center> No Update </center></b></td>
        </tr>
'''


def resultant_table(table, rows, columns, result):
    for i in range(0, rows):
        result = result + "<tr>"
        for j in range(0, columns):
            if j == 0:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif table[i][j] == 0:
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                if j == 8:
                    result = result + "<td colspan = 1 bgcolor=\"#FAB4C2\"><center>" + str(
                        table[i][j]) + "</center></td>"
                else:
                    result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"

    return result


def resultant_table1(table, rows, columns, result):
    for i in range(0, rows):
        result = result + "<tr>"
        for j in range(0, columns):
            if j == 0:
                result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif table[i][j] == 0:
                result = result + "<td colspan = 1 ><center> </center></td>"
            else:
                if j == 6:
                    result = result + "<td colspan = 1 bgcolor=\"#FAB4C2\"><center>" + str(
                        table[i][j]) + "</center></td>"
                else:
                    result = result + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result = result + "</tr>"
    result = result + "</table><br>"

    return result


def getStartTime(appointmentStartTime, appointmentDate):
    if appointmentStartTime is None:
        appointmentStartTime_formatted = datetime.strptime(appointmentDate, "%Y-%m-%d")
        appointmentStartTime_formatted = appointmentStartTime_formatted.replace(hour=9)

        return appointmentStartTime_formatted

    try:
        hour, minute = int(appointmentStartTime[0:2]), int(appointmentStartTime[2:4])

        appointmentStartTime_formatted = datetime.strptime(appointmentDate, "%Y-%m-%d")
        appointmentStartTime_formatted = appointmentStartTime_formatted.replace(hour=hour, minute=minute)

        return appointmentStartTime_formatted

    except Exception as e:
        appointmentStartTime_formatted = datetime.strptime(appointmentDate, "%Y-%m-%d")
        appointmentStartTime_formatted = appointmentStartTime_formatted.replace(hour=9)

        return appointmentStartTime_formatted


# 'Patient will reach on App time' CS add this tag


def select_data1(confirmed_cases, tag):
    table = [[0 for x in range(4)] for x in range(200)]
    row = 0

    agent_dict = {}
    # alreadyConsidered = {}
    for index, val in confirmed_cases.iterrows():

        # if val['leadId'] in alreadyConsidered.keys():
        #    continue
        # alreadyConsidered[val['leadId']] = ''

        if val['ayu_mitra_email'] not in agent_dict.keys():
            agent_dict[val['ayu_mitra_email']] = row
            table[row][0] = val['ayu_mitra_email']
            table[row][1] = set([])
            table[row][2] = 0
            table[row][3] = 0

            row += 1

        table[agent_dict[val['ayu_mitra_email']]][2] += 1
        table[agent_dict[val['ayu_mitra_email']]][1].add(val['zone'])

        table[agent_dict[val['ayu_mitra_email']]][3] += 1

    result = result_base1.format(tag)
    table[row][0] = 'Grand Total'
    table[row][1] = ''
    for i in range(0, row):
        for j in range(2, 4):
            table[row][j] += table[i][j]

    for i in range(0, row):
        for j in range(1, 2):
            table[i][j] = ",".join(map(str, table[i][j]))

    result = resultant_table(table, row + 1, 4, result)

    return result


def select_data(confirmed_cases):
    summaries = ['Overall', 'Patient Reached', 'AyuM connected with Patient', 'Patient Callback', 'Reschedule request',
                 'App Cancellation Request', 'Reschedule as doctor slot is not available',
                 'Pt. not responding/not reachable', 'Pt. on the way', 'In coordination with Pt.']
    table = {}
    row = {}
    agent_dict = {}
    for summary in summaries:
        table[summary] = [[0 for x in range(9)] for x in range(200)]
        agent_dict[summary] = {}
        row[summary] = 0

    overall_app_counts = {}
    for index, val in confirmed_cases.iterrows():

        tag = val['Status_Changed_To']
        if tag == 'Patient not responding':
            tag = 'Pt. not responding/not reachable'

        elif tag == 'Patient on the way':
            tag = 'Pt. on the way'

        # Overall
        if val['ayu_mitra_email'] not in agent_dict['Overall'].keys():
            agent_dict['Overall'][val['ayu_mitra_email']] = row['Overall']
            table['Overall'][row['Overall']][0] = val['ayu_mitra_email']
            table['Overall'][row['Overall']][1] = set([])
            table['Overall'][row['Overall']][2] = 0
            table['Overall'][row['Overall']][3] = 0
            table['Overall'][row['Overall']][4] = 0
            table['Overall'][row['Overall']][5] = 0
            table['Overall'][row['Overall']][6] = 0
            table['Overall'][row['Overall']][7] = 0
            table['Overall'][row['Overall']][8] = 0
            overall_app_counts[val['ayu_mitra_email']] = 0

            row['Overall'] += 1

        table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][2] += 1
        overall_app_counts[val['ayu_mitra_email']] += 1
        table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][1].add(val['zone'])

        if val['bucket'] == 'In T+5 mins':
            table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][3] += 1

        elif val['bucket'] == 'Inbetween T+5 to T+15 mins':
            table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][4] += 1

        elif val['bucket'] == 'Inbetween T+15 to T+30 mins':
            table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][5] += 1

        elif val['bucket'] == 'Inbetween T+30 to T+60 mins':
            table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][6] += 1

        elif val['bucket'] == 'After T+60 mins':
            table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][7] += 1

        else:
            table['Overall'][agent_dict['Overall'][val['ayu_mitra_email']]][8] += 1

        if tag in ['No Tag Added', 'DONE', 'APPOINTMENT_STARTED', 'CANCELLED', 'BOOKED', 'Guide patient for location',
                   'AyuM Call Patient Again', 'Consultation Start', 'Patient will reach on App time']:
            continue

        # For Tags
        if val['ayu_mitra_email'] not in agent_dict[tag].keys():
            agent_dict[tag][val['ayu_mitra_email']] = row[tag]
            table[tag][row[tag]][0] = val['ayu_mitra_email']
            table[tag][row[tag]][1] = set([])
            table[tag][row[tag]][2] = 0
            table[tag][row[tag]][3] = 0
            table[tag][row[tag]][4] = 0
            table[tag][row[tag]][5] = 0
            table[tag][row[tag]][6] = 0
            table[tag][row[tag]][7] = 0

            row[tag] += 1

        # table[tag][agent_dict[tag][val['ayu_mitra_email']]][2] += 1
        table[tag][agent_dict[tag][val['ayu_mitra_email']]][1].add(val['zone'])

        if val['bucket'] == 'In T+5 mins':
            table[tag][agent_dict[tag][val['ayu_mitra_email']]][3] += 1

        elif val['bucket'] == 'Inbetween T+5 to T+15 mins':
            table[tag][agent_dict[tag][val['ayu_mitra_email']]][4] += 1

        elif val['bucket'] == 'Inbetween T+15 to T+30 mins':
            table[tag][agent_dict[tag][val['ayu_mitra_email']]][5] += 1

        elif val['bucket'] == 'Inbetween T+30 to T+60 mins':
            table[tag][agent_dict[tag][val['ayu_mitra_email']]][6] += 1

        elif val['bucket'] == 'After T+60 mins':
            table[tag][agent_dict[tag][val['ayu_mitra_email']]][7] += 1

    result = ''
    for summary in summaries:
        if summary == 'Overall':
            result += result_base1.format(summary)

            table['Overall'][row['Overall']][0] = 'Grand Total'
            table['Overall'][row['Overall']][1] = ''
            for i in range(0, row['Overall']):
                for j in range(2, 9):
                    table['Overall'][row['Overall']][j] += table['Overall'][i][j]

            for i in range(0, row['Overall']):
                for j in range(1, 2):
                    table['Overall'][i][j] = ",".join(map(str, table['Overall'][i][j]))

            result = resultant_table(table[summary], row[summary] + 1, 9, result)

        else:
            result += result_base.format(summary)

            table[summary][row[summary]][0] = 'Grand Total'
            table[summary][row[summary]][1] = ''
            for i in range(0, row[summary]):
                for j in range(2, 8):
                    table[summary][row[summary]][j] += table[summary][i][j]

            for i in range(0, row[summary]):
                table[summary][i][1] = ",".join(map(str, table[summary][i][1]))
                table[summary][i][2] = overall_app_counts[table[summary][i][0]]

            result = resultant_table(table[summary], row[summary] + 1, 8, result)

    return result


def getDelta(createdOn, appointmentStartTime_formatted):
    if createdOn <= appointmentStartTime_formatted:
        return 'In T+5 mins'

    delta = createdOn - appointmentStartTime_formatted

    noOfHoursTaken = ((delta.days) * 24 * 3600 + delta.seconds) // 60

    if noOfHoursTaken >= 5 and noOfHoursTaken < 15:
        return 'Inbetween T+5 to T+15 mins'

    elif noOfHoursTaken >= 15 and noOfHoursTaken < 30:
        return 'Inbetween T+15 to T+30 mins'

    elif noOfHoursTaken >= 30 and noOfHoursTaken < 60:
        return 'Inbetween T+30 to T+60 mins'

    elif noOfHoursTaken >= 60:
        return 'After T+60 mins'

    return 'In T+5 mins'


def getTagBucket(appointmentStartTime_formatted, appid, appTagsDump):
    confirmedFlag = False
    if appid in appTagsDump.keys():
        dump = appTagsDump[appid]

        for row in dump:
            if row['doctorConsultationStatus'] == '2':
                confirmedFlag = True

            if confirmedFlag:
                if row['tagName'] in ['Patient Callback', 'Guide patient for location', 'Patient on the way',
                                      'Patient Reached', 'AyuM Call Patient Again', 'Patient not responding',
                                      'AyuM connected with Patient', 'Consultation Start', 'Reschedule request',
                                      'App Cancellation Request', 'Reschedule as doctor slot is not available',
                                      'Pt. not responding/not reachable', 'Pt. on the way', 'In coordination with Pt.']:
                    tag = getDelta(row['createdOn'], appointmentStartTime_formatted)
                    return tag, row['createdOn'], row['tagName']

                elif row['doctorConsultationStatus'] == '6':
                    tag = getDelta(row['createdOn'], appointmentStartTime_formatted)
                    return tag, row['createdOn'], 'Patient Reached'

                elif row['doctorConsultationStatus'] in ['7', '3', '4', '1']:
                    tag = getDelta(row['createdOn'], appointmentStartTime_formatted)
                    return tag, row['createdOn'], DoctorConsultationStatus[row['doctorConsultationStatus']]

    return '', '', 'No Tag Added'


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        appointment = pd.DataFrame(dataSets['appointment'])
        appointment_phr = appointment[appointment['doctorConsultationStatus'] == '6']
        ayu_mitra = dataSets['ayu_mitra']
        confirmed_cases = pd.DataFrame(dataSets['confirmed_cases'])
        app_done = pd.DataFrame(dataSets['app_done'])
        app_tags = pd.DataFrame(dataSets['app_tags'])

        newDf = pd.concat([appointment, app_tags])
        newDf = newDf.sort_values(by=['createdOn'], ascending=True)

        ayu_dict = {}
        for val in ayu_mitra:
            if val['personnelId'] not in ayu_dict.keys():
                ayu_dict[val['personnelId']] = val['email']

        app_done_dump = {}
        for index, val in app_done.iterrows():
            if val['appId'] not in app_done_dump.keys():
                app_done_dump[val['appId']] = val['user']

        appointmentDump = {}
        for index, val in appointment.iterrows():
            if val['id'] not in appointmentDump.keys():
                appointmentDump[val['id']] = []

            appointmentDump[val['id']].append(val)

        appTagsDump = {}
        for index, val in newDf.iterrows():
            if val['id'] not in appTagsDump.keys():
                appTagsDump[val['id']] = []

            appTagsDump[val['id']].append(val)

        if len(confirmed_cases):
            confirmed_cases['Doctor_Consultation_Status'] = confirmed_cases.apply(
                lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']], axis=1)
            confirmed_cases['ayu_mitra_email'] = confirmed_cases.apply(
                lambda x: get_ayumitra(x['ayuMitraId'], ayu_dict, app_done_dump, appointmentDump,
                                       x['Doctor_Consultation_Status'], x['id']), axis=1)
            confirmed_cases['appointmentStartTime_formatted'] = confirmed_cases.apply(
                lambda x: getStartTime(x['appointmentStartTime'], x['appointmentDate']), axis=1)

            df = confirmed_cases.apply(
                lambda x: getTagBucket(x['appointmentStartTime_formatted'], x['id'], appTagsDump), axis=1,
                result_type='expand')
            confirmed_cases = pd.concat([confirmed_cases, df], axis=1)
            confirmed_cases = confirmed_cases.rename(columns={0: 'bucket', 1: 'Update timing', 2: 'Status_Changed_To'})

            # confirmed_cases_pc = confirmed_cases[confirmed_cases['Status_Changed_To'] == 'Patient Callback']
            # confirmed_cases_acwp = confirmed_cases[confirmed_cases['Status_Changed_To'] == 'AyuM connected with Patient']
            # confirmed_cases_pr = confirmed_cases[confirmed_cases['Status_Changed_To'] == 'Patient Reached']
            # confirmed_cases_nu = confirmed_cases[confirmed_cases['Status_Changed_To'] == 'No Tag Added']

            result_1 = select_data(confirmed_cases)
            # result_2 = select_data(confirmed_cases_acwp, 'AyuM connected with Patient')
            # result_3 = select_data(confirmed_cases_pr, 'Patient Reached')
            # result_4 = select_data1(confirmed_cases_nu, 'No Tag Added')
            # result_1_BLR = select_data(confirmed_cases_new_BLR, 'New Appointments')

            confirmed_cases.to_csv(fpath)

        print(today_date.strftime('%Y-%m-%d') == end_day)

        if today_date.strftime('%Y-%m-%d') == end_day:
            # BLR
            Result = result_1
            Subject = "BLR | Ayu M Tags Add Tracking | Confirmed Cases | <{0}>-<{1}>".format(start_day, end_day)
            email_recipient_list = ["city-managers-leads@ayu.health"]
            # email_recipient_list = ['akchansh@ayu.health']
            send_email(None, email_recipient_list, Subject, None, Result, [fpath])

    except Exception as e:
        Subject = 'confirmed_appts_tags_summary ERROR'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, None, e, [])
        raise e