import pdb
import os
import logging
import json

logger = logging.getLogger(__name__)


def payment_details(appId, paymentData):
    for val in paymentData:
        if val['appointmentId'] == appId and val['paymentId'] not in ['CANCELLED']:
            return val['paymentId']
    return ''


def hospital_profile(profileId, profileData):
    if profileId is None:
        return ''
    for val in profileData:
        if val['hospitalId'] == profileId:
            return val['hospitalName']
    return ''


def doctor_profile(profileId, profileData):
    if profileId is None:
        return ''
    for val in profileData:
        if val['doctorProfileId'] == profileId:
            return val['doctorName']
    return ''


def patient_profile(profileId, propertyName, profileData):
    if profileId is None:
        return ''
    for val in profileData:
        if val['patientId'] == profileId:
            return val[propertyName]
    return ''


def speciality_profile(profileId, profileData):
    if profileId is None:
        return ''
    for val in profileData:
        if val['specialityId'] == profileId:
            return val['specialityName']
    return ''


def ayu_mitra_profile(profileId, propertyName, profileData):
    for val in profileData:
        if str(val['ayuMitraId']) == str(profileId):
            return val[propertyName]
    return ''


def comments(caseId, leadType, commentsData):
    if caseId is None:
        return ''
    if leadType == 'APPOINTMENT':
        comment = 'Appointment Comments: \n '
    elif leadType == 'CASE':
        comment = 'User Comments: \n '

    for val in commentsData:
        if val['leadId'] == caseId and val['leadType'] == leadType:
            date = str(val['createdOn']) if val['createdOn'] is not None else ''
            text = val['comment'] if val['comment'] is not None else ''
            if text == '':
                continue
            comment += date + ' : ' + text + ' \n'
    return comment


def surgery_outcome_recomendations_comment(surgeryOutComeId, Data):
    for val in Data:
        comment = '\n Surgery Comments: \n'
        if val['surgeryOutcomeId'] == surgeryOutComeId:
            jsonify = json.loads(val['additionalDetails'])
            if jsonify is None:
                continue
            if 'comments' in jsonify.keys():
                comment += jsonify['comments'] + ' \n'
    return comment


def surgery_outcome_details(appId, propertyName, surgeryData):
    for val in surgeryData:
        if val['appointmentId'] == appId:
            if propertyName == 'additionalDetails':
                additionalDetails = val['additionalDetails']
                if additionalDetails != 'null' and additionalDetails != '{}' and additionalDetails and additionalDetails != '':
                    jsonify = json.loads(additionalDetails)
                    if 'reasonForNotTakingSurgery' in jsonify.keys():
                        reasonNotTakeSurgery = jsonify['reasonForNotTakingSurgery']
                        return reasonNotTakeSurgery
                    return ''
            return val[propertyName]
    return ''


def get_jsonify_key(propertyName, Data):
    if Data != 'null' and Data != '{}' and Data and Data != '':
        jsonify = json.loads(Data)
        if propertyName in jsonify.keys():
            return jsonify[propertyName]
        return ''
    return ''


def comments_split(comment):
    if comment != 'null' and comment != '{}' and comment and comment != '':
        comment = comment.replace("\n", ",")
        comment = comment.replace("\\n", ",")
        return comment
    return ''


def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


def msg_to(to):
    if to is None:
        return to
    if len(to) == 11:
        return to[1:]
    elif len(to) == 12:
        return to[2:]
    elif len(to) == 13:
        return to[3:]
    elif len(to) == 14:
        return to[4:]
    return to


def msg_type(fromId):
    fromId = '' if fromId is None else fromId
    if '@ayu.health' in fromId or fromId in ['AUTO', '', 'NOT_ASSIGNED', 'RAZOR_PAY_WEBHOOK_API', 'pankaj.ptain@gmail.com', 'sahilsaiwal7@gmail.com', 'INACTIVE_USER_HANDLING_SERVICE']:
        return 'OUTBOUND'
    return 'INBOUND'


def get_hsm_json_file():
    HSM_JSON_FILE_PATH = '/home/ec2-user/Reports/script/src/service/whats_app_hsm.json'
    with open(HSM_JSON_FILE_PATH, 'r') as file:
        data = file.read()
        values_hsm = json.loads(data)
    return values_hsm


def get_hsm_msg_info(msg, values_hsm):
    if msg is None:
        return ''

    for an_rec, k in values_hsm.items():
        if msg.startswith(k['text'][0:5]):
            return an_rec
    return ''


def agent_name_from_email(email):
    if email is None or email in ('NOT_ASSIGNED', ''):
        return email
    find_a = email.find('@')
    if find_a == -1:
        return email
    return email[:find_a].capitalize()


def exotel_cxNo(direction, from_no, to_no):
    if direction == 'inbound':
        return msg_to(from_no)
    else:
        return msg_to(to_no)

def exotel_agentNo(direction, from_no, to_no):
    if direction == 'inbound':
        return msg_to(to_no)
    else:
        return msg_to(from_no)


def agent_exotel_nos(x):
    return {
        '06366367979': 'saaleh',
        '08899111898': 'saaleh',
        '06366465757': 'Amishek',
        '06366367474': 'Mansi',
        '06366367733': 'Namitha',
        '08984607019': 'Tushar',
        '09840738160': 'Janesh',
        '09036547872': 'Hossana',
        '06380482902': 'Janesh 2',
        '07008624572': 'Ashu',
        '08317495739': 'Amreen',
        '06202466945': 'Mayank',
        '07022566535': 'Nitya',
        '08553745949': 'Shweta',
        '08754406283': 'Gautham',
        '09779536543': 'Shweta',
        '09779818277': 'Vijay',
        '08310565395': 'Anamika',
        '09872167857': 'Durga',
        '09872165847': 'Mayank',
        '09035330934': 'Anjul',
        '08910154616': 'Aneeket'
    }.get(x, 'NA')

def agent_exotel_email(x):
    return {
        '06366367979': 'saaleh@ayu.health',
        '08899111898': 'saaleh@ayu.health',
        '06366465757': 'amishek@ayu.health',
        '06366367474': 'mansi@ayu.health',
        '06366367733': 'namitha@ayu.health',
        '08984607019': 'tushar@ayu.health',
        '09840738160': 'janesh@ayu.health',
        '09036547872': 'hossana@ayu.health',
        '06380482902': 'janesh@ayu.health',
        '07008624572': 'ashu@ayu.health',
        '08317495739': 'amreen@ayu.health',
        '06202466945': 'mayank@ayu.health',
        '07022566535': 'nitya@ayu.health',
        '08553745949': 'shweta@ayu.health',
        '08754406283': 'gautham@ayu.health',
        '09779536543': 'shweta@ayu.health',
        '09779818277': 'vijay@ayu.health',
        '08310565395': 'anamika@ayu.health',
        '09872167857': 'durga@ayu.health',
        '09872165847': 'mayank@ayu.health',
        '09035330934': 'anjul@ayu.health',
        '08910154616': 'aneeket@ayu.health'
    }.get(x, 'NA')

def patient_lead_status(x):
    return {
	'0' : 'OPENED',
	'1' : 'FOLLOWUP',
	'2' : 'APPOINTMENT_BOOKED',
	'3' : 'CANCELLED',
	'4' : 'REJECTED',
	'5' : 'INCOMPLETE',
	'6' : 'CLOSED',
	'7' : 'APPOINTMENT_CONFIRMED',
	None : None,
	'' : None
}.get(x, 'NA')

def doctor_consultation_status(x):
    return {
	'0': 'OPEN',
	'1': 'BOOKED',
	'2': 'CONFIRMED',
	'3': 'DONE',
	'4': 'CANCELLED',
	None : 'None',
	'' : 'None',
	'5': 'OPEN'
}.get(x, 'NA')

def tags_on_caseId(caseId, tagDump):

    for val in tagDump:
        if val['caseId'] == caseId:
            return val['tagName']
    return ''

def treatments_on_caseId(caseId, treatmentsDump):

    for val in treatmentsDump:
        if val['caseId'] == caseId:
            return val['treatments']
    return ''

def latest_followup_reasons(appId, commentsData):
    """
    @type appId: int
    @type commentsData: DataFrame (Need to be in descending : commentId)
    @return text: str (This function return latest followup reason based on appointment comments)
    """
    for index, val in commentsData.iterrows():
        if appId == val['leadId']:
            text = val['comment'] if val['comment'] is not None else ''
            if text != '':
                find_a = text.find(',')
                if find_a == -1:
                    return text
                return text[:find_a]
            return text
    return ''

def day_suffix(day):
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]

    return str(day) + suffix

def channel_name(patientId, channelDump):

    for val in channelDump:
        if val['patientProfileId'] == patientId:
            return val['channelName']
    return ''


def whatsapp_cxNo(fromId, toNo):
    fromId = '' if fromId is None else fromId
    if '@ayu.health' in fromId or fromId in ['AUTO', '', 'NOT_ASSIGNED', 'RAZOR_PAY_WEBHOOK_API', 'pankaj.ptain@gmail.com', 'sahilsaiwal7@gmail.com', 'INACTIVE_USER_HANDLING_SERVICE']:
        return msg_to(toNo)
    return msg_to(fromId)

def channel_bucket(x):
    return {
        'Google Base Broad Match Core Campaign': 'High',
        'Not Tracked': 'Medium',
        'Google-Search-Ads': 'Low',
        '1011 Google': 'High',
        'Google Diagnostics': 'Medium',
        '2021 Google': 'High',
        '2010 Google': 'Medium',
        'Google Base Campaign': 'Low',
        '1010 Google': 'High',
        '1110 Google': 'High',
        '2021 Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        '4011 Google': 'High',
        'Offline': 'Medium',
        '1012 Google': 'Medium',
        '1010 Whatsapp-price': 'All Other(Whatsapp, Website, Video)',
        '1411 WebsiteSubmit': 'All Other(Whatsapp, Website, Video)',
        'Website Submit': 'All Other(Whatsapp, Website, Video)',
        '1311 Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        'Google Ads': 'Medium',
        '1311 WebsiteSubmit': 'All Other(Whatsapp, Website, Video)',
        '1013 Google': 'Low',
        'Default Google': 'Medium',
        '1013 Whatsapp-other': 'All Other(Whatsapp, Website, Video)',
        '1410 Google': 'Low',
        '2021 Whatsapp-price': 'All Other(Whatsapp, Website, Video)',
        '1011 WebsiteSubmit': 'All Other(Whatsapp, Website, Video)',
        '1011 Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        '1411 Whatsapp-price': 'All Other(Whatsapp, Website, Video)',
        'Default Whatsapp-other': 'All Other(Whatsapp, Website, Video)',
        '1011 Whatsapp-price': 'All Other(Whatsapp, Website, Video)',
        '2010 Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        '1411 Google': 'High',
        'Default Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        '1411 Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        'Dr. exp': 'Medium',
        '1013 Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        '2011 Whatsapp-main': 'All Other(Whatsapp, Website, Video)',
        '1013 WebsiteSubmit': 'All Other(Whatsapp, Website, Video)',
        '2011 Whatsapp-price': 'All Other(Whatsapp, Website, Video)',
        '3010 Google': 'Low',
        'Default Whatsapp-price': 'All Other(Whatsapp, Website, Video)',
        '1010 WebsiteSubmit': 'All Other(Whatsapp, Website, Video)',
        'Facebook Form': 'Facebook Form',
        '1410 WebsiteSubmit': 'All Other(Whatsapp, Website, Video)',
        'Newspaper TV': 'All Other(Whatsapp, Website, Video)',
        '202 whatsapp': 'All Other(Whatsapp, Website, Video)',
        'Facebook Videos': 'All Other(Whatsapp, Website, Video)',
        '1011 Website Submit': 'All Other(Whatsapp, Website, Video)',
        '1011 Whatsapp-other': 'All Other(Whatsapp, Website, Video)',
        'Website Direct': 'All Other(Whatsapp, Website, Video)',
        '2021 Website Submit': 'All Other(Whatsapp, Website, Video)',
        '1010 Website Submit': 'All Other(Whatsapp, Website, Video)',
        '1411 Website Submit': 'All Other(Whatsapp, Website, Video)',
    }.get(x, 'No Channel Info')

def formatCurrency(price):
    if price:
    	price = locale.currency(long(price), symbol=False, grouping=True)
    else:
	    price = '0.00'
    if price.endswith('.00'):
        price = price[:-3]
    return price