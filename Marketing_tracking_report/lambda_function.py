import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
import json
from service.base_functions import msg_to
from datetime import datetime , timedelta
from service.send_mail_client import send_email
from read_from_bq import main as readbqData


logger = logging.getLogger(__name__)

READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

yesterday = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days=1)
week = yesterday - timedelta(days = 7)

yesterday = yesterday.date()
week = week.date()
fpath = os.path.join("/tmp","markeeting.csv")


query = '''select pc.caseId,pc.cityId,pp.cityId,pp.id, cp.customerNumber,pp.createdOn,mc.channelName as channelName2,
          cp.leadSource,
          case when pcp.cityName = 'Chandigarh' then 'CHD'
                                when pcp.cityName = 'Bangalore' then 'BLR'
                                when pcp.cityName = 'Jaipur' then 'JPR'
                                when pcp.cityName = 'NCR' then 'NCR'
                                when pcp.cityName = 'Hyderabad' then 'HYD'
                                when pcp.cityName is not null then pcp.cityName
                                when ppp.cityName = 'Chandigarh' then 'CHD'
                                when ppp.cityName = 'Bangalore' then 'BLR'
                                when ppp.cityName = 'Jaipur' then 'JPR'
                                when ppp.cityName = 'NCR' then 'NCR'
                                when ppp.cityName = 'Hyderabad' then 'HYD'
                                when ppp.cityName is not null then ppp.cityName
                            else 'No City' end as city,
          date(date_add(pp.createdOn,INTERVAL '5:30' HOUR_MINUTE)) as date from patient_case pc
          join patient_profile pp on pc.patientId = pp.id
          join customer_profile cp on pp.customerId = cp.customerId
          left join ayu_cities pcp on pc.cityId = pcp.id
          left join ayu_cities ppp on pp.cityId = ppp.id
          left join patient_marketing_channel_stats pmcs on pp.id = pmcs.patientProfileId
          left join marketing_channels mc on mc.id = pmcs.marketingChannelId 
          where date(date_add(pp.createdOn,INTERVAL '5:30' HOUR_MINUTE)) >= '{start}' and 
          date(date_add(pp.createdOn,INTERVAL '5:30' HOUR_MINUTE)) <= '{end}' and
          cp.leadSource in ('Exotel','Whatsapp','Website Submit')
          '''
          

query3 = '''select * from ayu_cities limit 5000'''
           
# query3 = '''select * from patient_case limit 5000'''
# Index(['caseId', 'hospitalId', 'doctorId', 'patientLeadStatus', 'followUpdate',
# 'followUpTime', 'specialityId', 'patientId', 'symptoms', 'assignedTo',
# 'createdOn', 'updatedOn', 'caseType', 'leadSource', 'reason',
# 'consultationFee', 'tenantName', 'additionalDetails', 'cityId',
# 'followUpdateNullCheck'],
# dtype='object')

           
# query2 = '''select * from patient_profile limit 5000'''
# Index(['id', 'age', 'dateOfBirth', 'gender', 'isInsuranceAvailable',
# 'patientName', 'createdOn', 'updatedOn', 'relation', 'customerId',
# 'cityId'],
# dtype='object')
# query3 = '''select * from customer_profile limit 10000'''

'''
# Index(['customerId', 'customerName', 'customerEmail', 'gender', 'dateOfBirth',
# 'customerNumber', 'secondaryNumber', 'isSecondaryNumberWhatsappEnabled',
# 'referralCode', 'appToken', 'tenantName', 'prefLanguage', 'leadSource',
# 'customerLocation', 'additionalDetails', 'createdOn', 'updatedOn',
# 'customerType'],
# dtype='object')

# leadSource = ['' None 'Exotel' 'Whatsapp' 'Website Submit' 'Google' 'Facebook'
# 'Facebook form' 'Inbound Call' 'Justdial' 'Google Ads' 'Offline channel'
# "Didn't Share" 'Patient Referral' 'Offline']
'''

#  QUERY TO WRITE
# join patient_profile,customer_profile table by customerId 
# Where LeadSource = 'Exotel' 'Whatsapp' 'Website Submit' from customer_profile

# channel': 'select pmcs.patientProfileId,channelName from marketing_channels mc, 
# patient_marketing_channel_stats pmcs where mc.id = pmcs.marketingChannelId',


# query2 = "select * from marketing_channels "

'''
Index(['id', 'marketingChannelId', 'createdOn', 'updatedOn', 'isActive',
'channelName', 'googleCallToNumber', 'regex', 'cityId',
'exotelImportance', 'surgeryId', 'specialityId'],
'''
# query3 = '''select * from patient_marketing_channel_stats limit 20000'''
'''
Index(['id', 'createdOn', 'updatedOn', 'patientProfileId',
'marketingChannelId', 'patientLeadSource', 'whatsappMessageType'],
dtype='object')
'''



def fetch_data(conn,queryNo):
    fetched_val = {}
    if queryNo == 1:
        fetched_val = fetch_record(conn, query.format(start = week,end = yesterday))
    if queryNo == 2:
        fetched_val = fetch_record(conn,query2.format(start = week,end = yesterday))
    if queryNo == 3:
        fetched_val = fetch_record(conn,query3)
    return fetched_val 
    
    
    
def count_leadsource(data,city_list):
    result = ''
    for key,value in city_list.items():
        markeeting = data[data['city'] == key]
        total_market = markeeting[~markeeting['channelName'].isin([None , ''])]
        total_leads_whatsapp = len(total_market[total_market['leadSource'] == 'Whatsapp'])
        total_leads_exotel = len(total_market[total_market['leadSource'] == 'Exotel'])
        total_leads_website = len(total_market[total_market['leadSource'] == 'Website Submit'])
        
        leadsource_dictionary = {'Exotel':{'count':0,'%':0},'Whatsapp': {'count':0,'%':0},'Website Submit':{'count':0,'%':0}}
        leadsource_dictionary['Exotel']['count'] = len(markeeting[markeeting['leadSource'] == 'Exotel'])
        leadsource_dictionary['Whatsapp']['count'] = len(markeeting[markeeting['leadSource'] == 'Whatsapp'])
        leadsource_dictionary['Website Submit']['count'] = len(markeeting[markeeting['leadSource'] == 'Website Submit'])
        
            
        leadsource_dictionary['Exotel']['%'] = round((total_leads_exotel*100)/leadsource_dictionary['Exotel']['count'],2)
        leadsource_dictionary['Whatsapp']['%'] = round((total_leads_whatsapp*100)/leadsource_dictionary['Whatsapp']['count'],2)
        leadsource_dictionary['Website Submit']['%'] = round((total_leads_website*100)/leadsource_dictionary['Website Submit']['count'],2)
        
        result = result + resultant_table(leadsource_dictionary,value)
    
    return result
        

def getChannelName(channelName2,leadSource,data,caseId):
    if leadSource in ('Exotel','Whatsapp'):
        return channelName2
    else:
        if caseId in data.keys():
            return data[caseId]
        else:
            return ''
        
    
    
def resultant_table(markeeting_count,value):
    result = '''
    <table style=\"border-collapse:collapse\" border=\"1\"  >
    <tr bgcolor=\"#002db3\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td width = \"1600\" colspan = 13><b><center> {0}  </center></b></td>
    </tr>
    <tr bgcolor=\"#668cff\" colspan = 13 height=\"30px\" style=\"color:#ffffff\"> 
    <td width = \"1600\"><b><center> LeadSource  </center></b></td>
    <td width = \"1600\"><b><center> Total Leads </center></b></td>
    <td width = \"1600\"><b><center> % of leads having channel name </center></b></td>
    </tr>'''.format(value)
    
    for key,value in markeeting_count.items():
        result = result + '''<tr  colspan = 13> 
        <td width = \"1600\" bgcolor=\"#ccd9ff\" height=\"40px\"><b><center>{0}</center></b></td>
        <td width = \"1600\"><center>{1}</center></td>
        <td width = \"1600\"><center>{2}</center></td>
        </tr>'''.format(key,value['count'],value['%'])
        
    result = result + '''</table><br><br>'''
    return result

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
        # dataSets = fetch_data(read_connection_obj,3)
        # example = pd.DataFrame(dataSets)
        # print("hi")
        # print(example['cityName'].unique())
                                                    
        
        dataSets = fetch_data(read_connection_obj,1)
        marketing = pd.DataFrame(dataSets)
        print(marketing['city'].unique())
        data = readbqData()
        eventLabel_list = list(data.keys())
        
        marketing['channelName'] = marketing.apply(lambda x : getChannelName(x['channelName2'],x['leadSource'],data,x['caseId']),axis = 1) 
        
        

        # for index in marketing.index:
        #     print("caseid = ",marketing['caseId'][index])
        #     print()
        #     print("index =" , index)
        #     if marketing['leadSource'][index] == 'Website Submit' and  marketing['caseId'][index] in eventLabel_list:
        #         print(marketing['channelName'][index])
        #         marketing['channelName'][index] = data[marketing['channelName'][index]]
    
                
        marketing.to_csv(fpath)
            
        
        city_list = {'BLR':'Bangalore','CHD':'Chandigarh','JPR':'Jaipur','NCR':'NCR','HYD':'Hyderabad'}
        resultant = count_leadsource(marketing,city_list) 
        
           
        
        Subject = "Marketing Tracking Last Week" 
        email_recipient_list = ['ankur@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath])  
        
        
        
    except Exception as e:
        Subject = 'Marketing Tracking Last Week'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
    
    
