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

logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

fpath = os.path.join('/tmp', 'ayuM_app_track_confirmed.csv')
fpath1 = os.path.join('/tmp', 'ayuM_app_track_patient_reached.csv')

yesterday = datetime.now() + timedelta(hours=5, minutes=30, days=-1)
mtd = yesterday.replace(day=1)

yesterday = yesterday.strftime('%Y-%m-%d')
mtd = mtd.strftime('%Y-%m-%d')

print(mtd, yesterday)

# anddate(appointmentDate) = '{end}'
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
                                        when afp.cityId = 4 then 'NCR'
                                        when afp.cityId = 5 then 'Hyderabad'
                                    end as city
                                from lead_doctor_consultation ldc
                            join patient_case pc on pc.caseId = ldc.leadId    
                            join ayu_facility_profile  afp on ldc.hospitalId = afp.facilityId  
                            where
                            ldc.id in (select distinct id 
                                    from lead_doctor_consultation_AUD
                                    where 
                                        date(appointmentDate) >= '{start}'
                                        and date(appointmentDate) <= '{end}'
                                        and doctorConsultationStatus = 2)
                            and afp.cityId in (1,2,3,4,5)
                            order by ldc.id desc
            ''',
    'patient_reached_cases': '''select ldc.* ,zone,patientReachedCreatedOn, pc.leadSource ,
                                        case 
                                        when afp.cityId = 1 then 'CHD'
                                        when afp.cityId = 2 then 'BLR'
                                        when afp.cityId = 3 then 'Jaipur'
                                        when afp.cityId = 4 then 'NCR'
                                        when afp.cityId = 5 then 'Hyderabad'
                                    end as city 
                                from lead_doctor_consultation ldc
                            join patient_case pc on pc.caseId = ldc.leadId 
                            join ayu_facility_profile  afp on ldc.hospitalId = afp.facilityId  
                            join
                            (select id, min(date_add(createdOn, INTERVAL '5:30' HOUR_MINUTE)) as 'patientReachedCreatedOn'
                                    from lead_doctor_consultation_AUD
                                    where 
                                        date(appointmentDate) >= '{start}'
                                        and date(appointmentDate) <= '{end}'
                                        and doctorConsultationStatus = 6
                                        group by 1
                                        ) as prc on prc.id = ldc.id
                            where afp.cityId in (1,2,3,4,5)
                        order by ldc.id desc
            ''',
    'appointment': '''select ldc.id, date_add(createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
                                doctorConsultationStatus,
                                '' as tagName,
                                ayuMitraId
                            from lead_doctor_consultation_AUD ldc
                                where 
                                    ldc.id in ({id}) 
                                    order by createdOn

            ''',
    "app_tags": """select e.entityId as 'id',a.tagName as tagName,
                            date_add(e.createdOn, INTERVAL '5:30' HOUR_MINUTE) as 'createdOn',
                            '' as doctorConsultationStatus,
                            '' as ayuMitraId,
                            tagCategory
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


def fetch_data(conn):
    fetched_val = {}
    for lookup, query in queries.items():
        if lookup == 'confirmed_cases':

            fetched_val[lookup] = fetch_record(conn, query.format(start=mtd, end=yesterday))
            caseIds = [str(x['id']) for x in fetched_val[lookup]]

        elif lookup == 'patient_reached_cases':

            fetched_val[lookup] = fetch_record(conn, query.format(start=mtd, end=yesterday))
            caseIds1 = [str(x['id']) for x in fetched_val[lookup]]

        elif lookup in ('appointment', 'app_tags', 'app_done'):

            caseIds.extend(caseIds1)
            ids = ",".join(caseIds)
            print(ids)
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


result_base = '''<body width = \"1500\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#A6DBFE\"> 
            <td colspan = 12 width = \"1400\"><b><center> New FollowUp Report - Ayu M App Update Tracking : Confirmed Cases | {0}</center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"200\"><b><center> Ayu Mitra </center></b></td>
            <td colspan = 1  width = \"200\"><b><center> Zones </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Total Appointments </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated before t-30 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated between t-30 to t-15 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated till t-15 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated till t+5 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated between t+5 to t+15 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated between t+15 to t+30 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated between t+30 to t+60 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Updated after t+60 mins </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> No Update </center></b></td>
        </tr>

'''

result_base1 = '''<body width = \"1200\"> 
        <table style=\"border-collapse:collapse\" border=\"1\" >
        <tr bgcolor=\"#A6DBFE\"> 
            <td colspan = 8 width = \"1300\"><b><center> New FollowUp Report - Ayu M App Update Tracking : Patient Reached Cases | {0}</center></b></td>
        </tr>
        <tr bgcolor=\"#EEFCF0\"> 
            <td colspan = 1  width = \"200\"><b><center> Ayu Mitra </center></b></td>
            <td colspan = 1  width = \"200\"><b><center> Zones </center></b></td>
            <td colspan = 1  width = \"200\"><b><center> Total Appointments </center></b></td>
            <td colspan = 1  width = \"200\"><b><center> Updated till t+2 Hrs </center></b></td>
            <td colspan = 1  width = \"200\"><b><center> Updated between t+2 and t+4 Hrs </center></b></td>
            <td colspan = 1  width = \"200\"><b><center> Updated after t+4 Hrs </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> No Update </center></b></td>
            <td colspan = 1  width = \"100\"><b><center> Appt Not Done </center></b></td>
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
                if j == 11:
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
    print(appointmentDate)
    if len(appointmentDate) > 4:

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


def getActionBucketConfirmed(appointmentStartTime_formatted, appId, appointmentDump):
    confirmedFlag = False
    statusChanged = False
    if appId in appointmentDump.keys():
        dump = appointmentDump[appId]
        for row in dump:
            print(row['createdOn'], appointmentStartTime_formatted)
            if row['doctorConsultationStatus'] == '2':
                confirmedFlag = True

            if confirmedFlag:
                if row['doctorConsultationStatus'] in ['6', '7', '3', '4', '1']:
                    statusUpdated = DoctorConsultationStatus[row['doctorConsultationStatus']]
                    statusChanged = True
                elif row['tagName'] in ['Patient Callback', 'Guide patient for location', 'Patient on the way',
                                        'Patient Reached', 'AyuM Call Patient Again', 'Patient not responding',
                                        'AyuM connected with Patient', 'Consultation Start', 'Reschedule request',
                                        'App Cancellation Request', 'Reschedule as doctor slot is not available',
                                        'Patient Reached', 'AyuM connected with Patient', 'Patient Callback',
                                        'Reschedule request', 'App Cancellation Request',
                                        'Reschedule as doctor slot is not available',
                                        'Pt. not responding/not reachable', 'Pt. on the way',
                                        'In coordination with Pt.', 'Patient will reach on App time']:
                    statusUpdated = row['tagName']
                    statusChanged = True

                if appointmentStartTime_formatted != None:
                    # print('appointmentStartTime_formatted',appointmentStartTime_formatted)
                    if row[
                        'createdOn'] >= appointmentStartTime_formatted:  # previous logic  call after appt_start_time
                        # if row['createdOn'] <= appointmentStartTime_formatted:    # new logic  call before appt_start_time
                        if statusChanged:
                            delta = row['createdOn'] - appointmentStartTime_formatted

                            # delta = appointmentStartTime_formatted - row['createdOn']

                            # print('Status Changed after Start Time')
                            noOfHoursTaken = ((delta.days) * 24 * 3600 + delta.seconds) // 60

                            if noOfHoursTaken >= 5 and noOfHoursTaken < 15:
                                return 'Inbetween T+5 to T+15 mins', row['createdOn'], statusUpdated

                            elif noOfHoursTaken >= 15 and noOfHoursTaken < 30:
                                return 'Inbetween T+15 to T+30 mins', row['createdOn'], statusUpdated

                            elif noOfHoursTaken >= 30 and noOfHoursTaken < 60:
                                return 'Inbetween T+30 to T+60 mins', row['createdOn'], statusUpdated

                            elif noOfHoursTaken >= 60:
                                return 'After T+60 mins', row['createdOn'], statusUpdated

                            else:
                                return 'In T+15 mins', row['createdOn'], statusUpdated

                if statusChanged:
                    # print('Status Changed before Start Time')
                    return 'In T+15 mins', row['createdOn'], statusUpdated

        if confirmedFlag:
            return 'No Change', '', 'CONFIRMED'

    return 'NA', '', ''


def getActionBucketConfirmed_before_appt(appointmentStartTime_formatted, appId, appointmentDump):
    confirmedFlag = False
    statusChanged = False
    if appId in appointmentDump.keys():
        dump = appointmentDump[appId]
        for row in dump:
            print(row['createdOn'], appointmentStartTime_formatted)
            if row['doctorConsultationStatus'] == '2':
                confirmedFlag = True

            if confirmedFlag:
                if row['doctorConsultationStatus'] in ['6', '7', '3', '4', '1']:
                    statusUpdated = DoctorConsultationStatus[row['doctorConsultationStatus']]
                    statusChanged = True
                elif row['tagName'] in ['Patient Callback', 'Guide patient for location', 'Patient on the way',
                                        'Patient Reached', 'AyuM Call Patient Again', 'Patient not responding',
                                        'AyuM connected with Patient', 'Consultation Start', 'Reschedule request',
                                        'App Cancellation Request', 'Reschedule as doctor slot is not available',
                                        'Patient Reached', 'AyuM connected with Patient', 'Patient Callback',
                                        'Reschedule request', 'App Cancellation Request',
                                        'Reschedule as doctor slot is not available',
                                        'Pt. not responding/not reachable', 'Pt. on the way',
                                        'In coordination with Pt.', 'Patient will reach on App time']:
                    statusUpdated = row['tagName']
                    statusChanged = True

                if appointmentStartTime_formatted != None:
                    # print('appointmentStartTime_formatted',appointmentStartTime_formatted)
                    # if row['createdOn'] >= appointmentStartTime_formatted:   #previous logic  call after appt_start_time
                    if row['createdOn'] <= appointmentStartTime_formatted:  # new logic  call before appt_start_time
                        if statusChanged:
                            # delta = row['createdOn'] - appointmentStartTime_formatted

                            delta = appointmentStartTime_formatted - row['createdOn']

                            # print('Status Changed after Start Time')
                            noOfHoursTaken = ((delta.days) * 24 * 3600 + delta.seconds) // 60

                            if noOfHoursTaken >= 0 and noOfHoursTaken < 15:
                                return 'In T-15 mins', row['createdOn'], statusUpdated

                            elif noOfHoursTaken >= 15 and noOfHoursTaken < 30:
                                return 'Inbetween T-15 to T-30 mins', row['createdOn'], statusUpdated

                            # elif noOfHoursTaken >= 30 and noOfHoursTaken < 60:
                            #     return 'Inbetween T+30 to T+60 mins' , row['createdOn'] , statusUpdated

                            # elif noOfHoursTaken >= 60 and noOfHoursTaken < 120:
                            #     return 'Inbetween T+60 mins to T+120 mins' ,row['createdOn'] , statusUpdated

                            else:
                                return 'Before T-30 mins', row['createdOn'], statusUpdated

                if statusChanged:
                    # print('Status Changed before Start Time')
                    return 'In T-15 mins', row['createdOn'], statusUpdated

        if confirmedFlag:
            return 'No Change', '', 'CONFIRMED'

    return 'NA', '', ''


def getActionBucketPatientReached(patientReachedCreatedOn, appId, appointmentDump):
    patientReachedFlag = False
    statusChanged = False
    if appId in appointmentDump.keys():
        dump = appointmentDump[appId]
        for row in dump:

            if row['doctorConsultationStatus'] == '6':
                patientReachedFlag = True

            if patientReachedFlag:
                if row['doctorConsultationStatus'] in ['7', '3', '4', '1', '2']:
                    statusUpdated = DoctorConsultationStatus[row['doctorConsultationStatus']]
                    statusChanged = True
                elif row['tagName'] in ['Guide patient for location', 'Patient on the way', 'Patient Reached',
                                        'AyuM Call Patient Again', 'Patient not responding',
                                        'AyuM connected with Patient', 'Consultation Start',
                                        'Patient will reach on App time', 'Reschedule request',
                                        'App Cancellation Request', 'Reschedule as doctor slot is not available',
                                        'Patient Callback', 'Pt. not responding/not reachable', 'Pt. on the way',
                                        'In coordination with Pt.']:
                    statusUpdated = row['tagName']
                    statusChanged = True

                if row['createdOn'] >= patientReachedCreatedOn:
                    if statusChanged:
                        delta = row['createdOn'] - patientReachedCreatedOn
                        print('Status Changed after Start Time')
                        noOfHoursTaken = ((delta.days) * 24 * 3600 + delta.seconds) // 3600

                        if noOfHoursTaken < 2:
                            return 'In T+2 hours', row['createdOn'], statusUpdated
                        elif noOfHoursTaken >= 2 and noOfHoursTaken <= 4:
                            return 'In T+2 to T+4 hours', row['createdOn'], statusUpdated

                        return 'After T+4 hours', row['createdOn'], statusUpdated

                if statusChanged:
                    print('Status Changed before Start Time')
                    return 'In T+2 hours', row['createdOn'], statusUpdated

        if patientReachedFlag:
            return 'No Change', '', 'PATIENT_HAS_REACHED'

    return 'NA', '', ''


def select_data(confirmed_cases, tag):
    table = [[0 for x in range(12)] for x in range(200)]
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
            table[row][4] = 0
            table[row][5] = 0
            table[row][6] = 0
            table[row][7] = 0
            table[row][8] = 0
            table[row][9] = 0
            table[row][10] = 0
            table[row][11] = 0

            row += 1

        table[agent_dict[val['ayu_mitra_email']]][2] += 1
        if val['zone'] is not None:
            table[agent_dict[val['ayu_mitra_email']]][1].add(val['zone'])

        if val['bucket'] == 'In T+5 mins':
            table[agent_dict[val['ayu_mitra_email']]][6] += 1

        elif val['bucket'] == 'Inbetween T+5 to T+15 mins':
            table[agent_dict[val['ayu_mitra_email']]][7] += 1

        elif val['bucket'] == 'Inbetween T+15 to T+30 mins':
            table[agent_dict[val['ayu_mitra_email']]][8] += 1

        elif val['bucket'] == 'Inbetween T+30 to T+60 mins':
            table[agent_dict[val['ayu_mitra_email']]][9] += 1

        elif val['bucket'] == 'After T+60 mins':
            table[agent_dict[val['ayu_mitra_email']]][10] += 1

        elif val['bucket_2'] == 'In T-15 mins':
            table[agent_dict[val['ayu_mitra_email']]][5] += 1

        elif val['bucket_2'] == 'Inbetween T-15 to T-30 mins':
            table[agent_dict[val['ayu_mitra_email']]][4] += 1

        elif val['bucket_2'] == 'Before T-30 mins':
            table[agent_dict[val['ayu_mitra_email']]][3] += 1

        else:
            table[agent_dict[val['ayu_mitra_email']]][11] += 1

    result = result_base.format(tag)
    table[row][0] = 'Grand Total'
    table[row][1] = ''
    for i in range(0, row):
        for j in range(2, 12):
            table[row][j] += table[i][j]

    for i in range(0, row):
        for j in range(1, 2):
            table[i][j] = ",".join(map(str, table[i][j]))

    result = resultant_table(table, row + 1, 12, result)

    return result




def select_data2(patient_reached_cases, tag):
    table1 = [[0 for x in range(8)] for x in range(200)]
    rowindex = 0

    agent_dict = {}
    # alreadyConsidered = {}
    for index, val in patient_reached_cases.iterrows():
        # if val['leadId'] in alreadyConsidered.keys():
        #    continue
        # alreadyConsidered[val['leadId']] = ''

        if val['ayu_mitra_email'] not in agent_dict.keys():
            agent_dict[val['ayu_mitra_email']] = rowindex
            table1[rowindex][0] = val['ayu_mitra_email']
            table1[rowindex][1] = set([])
            table1[rowindex][2] = 0
            table1[rowindex][3] = 0
            table1[rowindex][4] = 0
            table1[rowindex][5] = 0
            table1[rowindex][6] = 0
            table1[rowindex][7] = 0

            rowindex += 1

        if val['zone'] is not None:
            table1[agent_dict[val['ayu_mitra_email']]][1].add(val['zone'])

        table1[agent_dict[val['ayu_mitra_email']]][2] += 1

        if val['Doctor_Consultation_Status'] == 'CONFIRMED':
            table1[agent_dict[val['ayu_mitra_email']]][6] += 1

        elif val['Doctor_Consultation_Status'] == 'DONE':
            if val['bucket'] == 'In T+2 hours':
                table1[agent_dict[val['ayu_mitra_email']]][3] += 1

            elif val['bucket'] == 'In T+2 to T+4 hours':
                table1[agent_dict[val['ayu_mitra_email']]][4] += 1

            elif val['bucket'] == 'After T+4 hours':
                table1[agent_dict[val['ayu_mitra_email']]][5] += 1

        else:
            table1[agent_dict[val['ayu_mitra_email']]][7] += 1

    result = result_base1.format(tag)

    table1[rowindex][0] = 'Grand Total'
    table1[rowindex][1] = ''
    for i in range(0, rowindex):
        for j in range(2, 8):
            table1[rowindex][j] += table1[i][j]

    for i in range(0, rowindex):
        for j in range(1, 2):
            table1[i][j] = ",".join(table1[i][j])

    result = resultant_table1(table1, rowindex + 1, 8, result)

    return result


def getDelta(createdOn, appointmentStartTime_formatted):
    # if appointmentStartTime_formatted != None:
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


def getTagBucket(appointmentStartTime_formatted, appid, appTagsDump):
    patientReachedTag = ''
    ayuMConnectedWithPatientTag = ''
    patientOnWayTag = ''
    patientCallbackTag = ''
    patientReachedFlag = False
    ayuMConnectedWithPatientFlag = False
    patientOnWayFlag = False
    patientCallbackFlag = False
    if appid in appTagsDump.keys():
        dump = appTagsDump[appid]

        for row in dump:
            if row['tagName'] == 'Patient Callback':
                if not patientCallbackFlag:
                    patientCallbackTag = getDelta(row['createdOn'], appointmentStartTime_formatted)
                    patientCallbackFlag = True

            if row['tagName'] == 'AyuM connected with Patient':
                if not ayuMConnectedWithPatientFlag:
                    ayuMConnectedWithPatientTag = getDelta(row['createdOn'], appointmentStartTime_formatted)
                    ayuMConnectedWithPatientFlag = True

            if row['tagName'] == 'Patient on the way':
                if not patientOnWayFlag:
                    patientOnWayTag = getDelta(row['createdOn'], appointmentStartTime_formatted)
                    patientOnWayFlag = True

            if row['tagName'] == 'Patient Reached':
                if not patientReachedFlag:
                    patientReachedTag = getDelta(row['createdOn'], appointmentStartTime_formatted)
                    patientReachedFlag = True

    return patientReachedTag, patientCallbackTag, patientOnWayTag, ayuMConnectedWithPatientTag


def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)

        dataSets = fetch_data(read_connection_obj)

        appointment = pd.DataFrame(dataSets['appointment'])
        ayu_mitra = dataSets['ayu_mitra']
        confirmed_cases = pd.DataFrame(dataSets['confirmed_cases'])
        patient_reached_cases = pd.DataFrame(dataSets['patient_reached_cases'])
        app_tags = pd.DataFrame(dataSets['app_tags'])
        app_done = pd.DataFrame(dataSets['app_done'])

        newDf = pd.concat([appointment, app_tags])
        newDf = newDf.sort_values(by=['createdOn'], ascending=True)

        app_done_dump = {}
        for index, val in app_done.iterrows():
            if val['appId'] not in app_done_dump.keys():
                app_done_dump[val['appId']] = val['user']

        appointmentDump_1 = {}
        for index, val in appointment.iterrows():
            if val['id'] not in appointmentDump_1.keys():
                appointmentDump_1[val['id']] = []

            appointmentDump_1[val['id']].append(val)

        ayu_dict = {}
        for val in ayu_mitra:
            if val['personnelId'] not in ayu_dict.keys():
                ayu_dict[val['personnelId']] = val['email']

        appointmentDump = {}
        for index, val in newDf.iterrows():
            if val['id'] not in appointmentDump.keys():
                appointmentDump[val['id']] = []

            appointmentDump[val['id']].append(val)

        appTagsDump = {}
        for index, val in app_tags.iterrows():
            if val['id'] not in appTagsDump.keys():
                appTagsDump[val['id']] = []

            appTagsDump[val['id']].append(val)

        if len(confirmed_cases):
            confirmed_cases['Doctor_Consultation_Status'] = confirmed_cases.apply(
                lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']], axis=1)
            # confirmed_cases['ayu_mitra_email'] = confirmed_cases.apply(lambda x: get_ayumitra(x['ayuMitraId'],ayu_dict),axis=1)
            confirmed_cases['ayu_mitra_email'] = confirmed_cases.apply(
                lambda x: get_ayumitra(x['ayuMitraId'], ayu_dict, app_done_dump, appointmentDump,
                                       x['Doctor_Consultation_Status'], x['id']), axis=1)
            confirmed_cases['appointmentStartTime_formatted'] = confirmed_cases.apply(
                lambda x: getStartTime(x['appointmentStartTime'], x['appointmentDate']), axis=1)

            df = confirmed_cases.apply(
                lambda x: getActionBucketConfirmed(x['appointmentStartTime_formatted'], x['id'], appointmentDump),
                axis=1, result_type='expand')
            confirmed_cases = pd.concat([confirmed_cases, df], axis=1)
            confirmed_cases = confirmed_cases.rename(columns={0: 'bucket', 1: 'Update timing', 2: 'Status_Changed_To'})

            df = confirmed_cases.apply(
                lambda x: getTagBucket(x['appointmentStartTime_formatted'], x['id'], appTagsDump), axis=1,
                result_type='expand')
            confirmed_cases = pd.concat([confirmed_cases, df], axis=1)
            confirmed_cases = confirmed_cases.rename(
                columns={0: 'Patient Reached Tag', 1: 'Patient Callback Tag', 2: 'Patient on the way Tag',
                         3: 'AyuM connected with Patient Tag'})

            df = confirmed_cases.apply(
                lambda x: getActionBucketConfirmed_before_appt(x['appointmentStartTime_formatted'], x['id'],
                                                               appointmentDump), axis=1, result_type='expand')
            confirmed_cases = pd.concat([confirmed_cases, df], axis=1)
            confirmed_cases = confirmed_cases.rename(
                columns={0: 'bucket_2', 1: 'Update timing before appt', 2: 'before_appt_Status_Changed_To'})

            confirmed_cases_yest = confirmed_cases[confirmed_cases['appointmentDate'] == yesterday]

            confirmed_cases_new = confirmed_cases_yest[
                confirmed_cases_yest['appointmentCreationType'].isin(['NEW_APPOINTMENT', 'RESCHEDULED_APPOINTMENT'])]

            confirmed_cases_BLR = confirmed_cases_yest[confirmed_cases_yest['city'] == 'BLR']
            confirmed_cases_CHD = confirmed_cases_yest[confirmed_cases_yest['city'] == 'CHD']
            confirmed_cases_Jaipur = confirmed_cases_yest[confirmed_cases_yest['city'] == 'Jaipur']
            confirmed_cases_NCR = confirmed_cases_yest[confirmed_cases_yest['city'] == 'NCR']
            confirmed_cases_HYD = confirmed_cases_yest[confirmed_cases_yest['city'] == 'Hyderabad']

            confirmed_cases_new_BLR = confirmed_cases_new[confirmed_cases_new['city'] == 'BLR']
            # confirmed_cases_new_CHD = confirmed_cases_new[confirmed_cases_new['city'] == 'CHD']
            # confirmed_cases_new_Jaipur = confirmed_cases_new[confirmed_cases_new['city'] == 'Jaipur']

            result_BLR = select_data(confirmed_cases_BLR, 'Overall')
            result_1_BLR = select_data(confirmed_cases_new_BLR, 'New Appointments')

            result_CHD = select_data(confirmed_cases_CHD, 'Overall')
            # result_1_CHD = select_data(confirmed_cases_new_CHD, 'New Appointments')

            result_Jaipur = select_data(confirmed_cases_Jaipur, 'Overall')
            # result_1_Jaipur = select_data(confirmed_cases_new_Jaipur, 'New Appointments')

            result_NCR = select_data(confirmed_cases_NCR, 'Overall')

            result_HYD = select_data(confirmed_cases_HYD, 'Overall')

            confirmed_cases.to_csv(fpath)

        if len(patient_reached_cases):
            patient_reached_cases['Doctor_Consultation_Status'] = patient_reached_cases.apply(
                lambda x: DoctorConsultationStatus[x['doctorConsultationStatus']], axis=1)
            # patient_reached_cases['ayu_mitra_email'] = patient_reached_cases.apply(lambda x: get_ayumitra(x['ayuMitraId'],ayu_dict),axis=1)
            patient_reached_cases['ayu_mitra_email'] = patient_reached_cases.apply(
                lambda x: get_ayumitra(x['ayuMitraId'], ayu_dict, app_done_dump, appointmentDump,
                                       x['Doctor_Consultation_Status'], x['id']), axis=1)
            df = patient_reached_cases.apply(
                lambda x: getActionBucketPatientReached(x['patientReachedCreatedOn'], x['id'], appointmentDump), axis=1,
                result_type='expand')
            patient_reached_cases = pd.concat([patient_reached_cases, df], axis=1)
            patient_reached_cases = patient_reached_cases.rename(
                columns={0: 'bucket', 1: 'Update timing', 2: 'Status_Changed_To'})

            # df = patient_reached_cases.apply(lambda x: getTagBucket(x['patientReachedCreatedOn'],x['id'], appTagsDump),axis=1,result_type='expand')
            # patient_reached_cases = pd.concat([patient_reached_cases,df],axis=1)
            # patient_reached_cases = patient_reached_cases.rename(columns={0:'Patient Reached Tag',1:'Patient Callback Tag',2:'Patient on the way Tag', 3:'AyuM connected with Patient Tag'})

            patient_reached_cases_yest = patient_reached_cases[patient_reached_cases['appointmentDate'] == yesterday]

            patient_reached_cases_new = patient_reached_cases_yest[
                patient_reached_cases_yest['appointmentCreationType'].isin(['NEW_APPOINTMENT', ''])]

            patient_reached_cases_BLR = patient_reached_cases_yest[patient_reached_cases_yest['city'] == 'BLR']
            patient_reached_cases_CHD = patient_reached_cases_yest[patient_reached_cases_yest['city'] == 'CHD']
            patient_reached_cases_Jaipur = patient_reached_cases_yest[patient_reached_cases_yest['city'] == 'Jaipur']
            patient_reached_cases_NCR = patient_reached_cases_yest[patient_reached_cases_yest['city'] == 'NCR']
            patient_reached_cases_HYD = patient_reached_cases_yest[patient_reached_cases_yest['city'] == 'Hyderabad']

            patient_reached_cases_new_BLR = patient_reached_cases_new[patient_reached_cases_new['city'] == 'BLR']
            # patient_reached_cases_new_CHD = patient_reached_cases_new[patient_reached_cases_new['city'] == 'CHD']
            # patient_reached_cases_new_Jaipur = patient_reached_cases_new[patient_reached_cases_new['city'] == 'Jaipur']

            result_2_BLR = select_data2(patient_reached_cases_BLR, 'Overall')
            result_3_BLR = select_data2(patient_reached_cases_new_BLR, 'New Appointments')

            result_2_CHD = select_data2(patient_reached_cases_CHD, 'Overall')
            # result_3_CHD = select_data2(patient_reached_cases_new_CHD, 'New Appointments')

            result_2_Jaipur = select_data2(patient_reached_cases_Jaipur, 'Overall')
            # result_3_Jaipur = select_data2(patient_reached_cases_new_Jaipur, 'New Appointments')

            result_2_NCR = select_data2(patient_reached_cases_NCR, 'Overall')

            result_2_HYD = select_data2(patient_reached_cases_HYD, 'Overall')

            patient_reached_cases.to_csv(fpath1)

            # BLR
        Result = result_BLR + result_1_BLR + result_2_BLR + result_3_BLR
        Subject = "BLR | Ayu M App Update Tracking | Confirmed Cases | Patient Reached Cases | {0}".format(yesterday)
        # email_recipient_list = ["city-managers-leads@ayu.health", "abu@ayu.health"]
        email_recipient_list = ['akchansh@ayu.health']
        send_email(None, email_recipient_list, Subject, None, Result, [fpath, fpath1])

        # CHD
        Result = result_CHD + result_2_CHD
        Subject = "CHD | Ayu M App Update Tracking | Confirmed Cases | Patient Reached Cases | {0}".format(yesterday)
        email_recipient_list = ["upender@ayu.health", "jay@ayu.health", "rohit.chahal@ayu.health", "shahwaz@ayu.health",
                                "manpreet@ayu.health", "shubham.sharma@ayu.health"]
        # email_recipient_list = ['akchansh@ayu.health']
        # send_email(None, email_recipient_list, Subject, None, Result, [fpath,fpath1])

        # Jaipur
        Result = result_Jaipur + result_2_Jaipur
        Subject = "Jaipur | Ayu M App Update Tracking | Confirmed Cases | Patient Reached Cases | {0}".format(yesterday)
        email_recipient_list = ["jay@ayu.health", "rohit.chahal@ayu.health", "lalit@ayu.health", "chandni@ayu.health",
                                "yash@ayu.health", "ashish.nagar@ayu.health", 'ayu-mitra-jaipur@ayu.health']
        # email_recipient_list = ['akchansh@ayu.health']
        # send_email(None, email_recipient_list, Subject, None, Result, [fpath,fpath1])

        # NCR
        Result = result_NCR + result_2_NCR
        Subject = "NCR | Ayu M App Update Tracking | Confirmed Cases | Patient Reached Cases | {0}".format(yesterday)
        email_recipient_list = ["jay@ayu.health", "rohit.chahal@ayu.health", "karan@ayu.health",
                                "vishal.khobra@ayu.health", "vibhor@ayu.health", "harshita@ayu.health",
                                "shubham.mukherjee@ayu.health", "shahwaz@ayu.health", "chandni@ayu.health",
                                "ncr_ops@ayu.health"]
        # email_recipient_list = ['akchansh@ayu.health']
        # send_email(None, email_recipient_list, Subject, None, Result, [fpath,fpath1])

        # HYD
        Result = result_HYD + result_2_HYD
        Subject = "HYD | Ayu M App Update Tracking | Confirmed Cases | Patient Reached Cases | {0}".format(yesterday)
        email_recipient_list = ["jay@ayu.health", "karan@ayu.health", "ashish@ayu.health", "pankaj@ayu.health",
                                'hyderabad_ops@ayu.health', 'hyd_ops_report@ayu.health']
        # email_recipient_list = ['akchansh@ayu.health']
        # send_email(None, email_recipient_list, Subject, None, Result, [fpath,fpath1])

    except Exception as e:
        Subject = 'New_followUps_report_BLR_App_Update_Tracking ERROR'
        email_recipient_list = ['Analytics@ayu.health']
        # send_email(None, email_recipient_list, Subject, None, e , [])
        raise e