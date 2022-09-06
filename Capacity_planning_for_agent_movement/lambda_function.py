import json
import pdb 
import os  
import pytz    
import re      
import pandas as pd   
import numpy as np  
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import logging
from service.base_functions import msg_to 
from datetime import datetime , timedelta   
from service.send_mail_client import send_email
from big_query_data_fetch import main as getdatafrombigq  
import requests
import sys
  
fpath = os.path.join("/tmp","leadSource.csv")
fpath1 = os.path.join("/tmp","exotel_outbound.csv")
fpath2 = os.path.join("/tmp","agent_conversion.csv")
fpath3 = os.path.join("/tmp","exotel_response.csv")
# fpath4 = os.path.join("/tmp","agents.csv")



logger = logging.getLogger(__name__)
URL_POST = 'http://backend.cron.api.ayu.health/patient-profile/agent'

# Description: 1. List all the agents for the city who are going to take exotel calls and order by conversion 
# 2. from past data (last 30 days call data ) figure out avg calls coming in that 10 min window and add that number of agents to inbound calls. 
# 3. Calculate missed calls and website and just dial for the same time window add so many agents to outbound high 
# 4. Add rest of agent to follow up ( if the capacity is less for that window send email to TLs ) 

# Created By : Nisha Das


READ_DB_USER = os.environ.get('READ_DB_USER', '')
READ_DB_PASSWORD = os.environ.get('READ_DB_PASSWORD', '')
READ_DB_DOMAIN_URL = os.environ.get('READ_DB_DOMAIN_URL', '')

yesterday = datetime.now() + timedelta(hours=5,minutes=30) - timedelta(days=1)
last_30_days = yesterday - timedelta(days = 7)

time1 = datetime.now() + timedelta(hours=5,minutes=30)
time1 = time1.replace(second=0)
time_1 = (time1 + timedelta(minutes=2)).time()
time_2 = (time1 + timedelta(minutes=4)).time()
time_3 = (time1 + timedelta(minutes=6)).time()
time_4 = (time1 + timedelta(minutes=8)).time()
time1 = time1.time()
print(time1,time_1,time_2,time_3,time_4)
#sys.exit(1)
# time1 = datetime.now() + timedelta(hours=5,minutes=40)
# time1 = time1.time()
strTime1 = time1.strftime("%H:%M")

time2 = datetime.now() + timedelta(hours=5,minutes=33)
time2 = time2.time()
strTime2 = time2.strftime("%H:%M")


yesterday = yesterday.date()
last_30_days = last_30_days.date()

print(yesterday)
print(last_30_days)
print(time1)
print(time2)





queries = {'cs_agent':"""
                        select trim(lower(email)) as Agent_Action,
                        CASE  
                        when cityName = 'Bangalore' THEN 'BLR'
                        when cityName = 'Chandigarh' THEN 'CHD'
                        ELSE cityName
                        END as City
                        from customer_support_details csd
                        left join ayu_cities ac on ac.id = csd.cityId
                        where hasLeft = 0
                        and isAvailable = 1
                       
                        """,
                        
            'cs_agent_not_included':"""
             select emailId from agent_source_mapping
             where cityId=5 
             and source in ('WHATSAPP') 
            """,
            
            'cs_agent_not_included_second':"""
            select email
            from customer_support_details csd
            where isTestUserAccount = 1
            """
            
            
            
                        
                        
                        
            # 'patient_callback': """
            # select REV,caseId,
            # CASE
            # WHEN cityName = 'Bangalore' THEN 'BLR'
            # WHEN cityName = 'Chandigarh' THEN 'CHD'
            # ELSE cityName
            # END as City
            # from patient_case_AUD pc
            # left join ayu_cities ac on ac.id = pc.cityId 
            #             where
            #                 leadSource = 'Exotel'
            #                 and REV in (select min(REV) from patient_case_AUD
            #                             where
            #                             caseId in ({ids}) 
            #                             group by caseId) 
            #                 and reason = 'Patient Call back'
            # """

}



leadsource = """
select caseId,DATE(createdOn) as createdOn,city,leadSource, time(datetime(createdOn)) as time from 
`gmb-centralisation.tables_crons.crm_dump`
where DATE(createdOn) >= '{start}'
and DATE(createdOn) <= '{end}'
and time(datetime(createdOn)) >= '{time1}' 
and time(datetime(createdOn)) <= '{time2}'
and leadSource in ('Justdial','Website Submit','Exotel')

"""

query_for_monthly_incentive = """
select DATE(Appointment_date) as date,caseId,trim(Agent_Action) as Agent_Action,
Team_Leader,Total_Call_Duration,Sequence,Appt_type from
`gmb-centralisation.tables_crons.Monthly Appointments` 
where DATE(Appointment_date) >= '{start}'
"""



# exotel_summary = """
# select * from (select sid,date(call_created_on) as date,CAST(call_created_on AS time) as time,
#                               direction,city,
#                               leg_2_status,
#                               case when duration < 10 then TIMESTAMP_ADD(call_created_on , INTERVAL 3 MINUTE) 
#                               else call_created_on end as new_time
#                                 from `gmb-centralisation.tables_crons.exotel_summary` 
#                               where date(call_created_on) > '{start}' 
#                               and date(call_created_on) <= '{end}' 
#                               and CAST(call_created_on AS time) >= '{time1}' 
#                               and CAST(call_created_on AS time) <= '{time2}'
#                               and direction = 'inbound'
#                               and ayuPersonnelType = 'CUSTOMER_SUPPORT'
#                               )
#                               where
#                               CAST(new_time AS time) >= '{time1}' 
#                               and CAST(new_time AS time) <= '{time2}'

# """

# exotel_summary = """select city, ceil(avg(max_total)) as average from 
# (select city,date,max(total) as max_total
# from (select city,
# date,
# bucket,
# count(*) as total
# from
# (select *,
# case when CAST(new_time AS time) < '{t1}' then 1
# when CAST(new_time AS time) < '{t2}' then 2
# when CAST(new_time AS time) < '{t3}' then 3
# when CAST(new_time AS time) < '{t4}' then 4
# else 5 end as bucket
#  from (select sid,date(call_created_on) as date,CAST(call_created_on AS time) as time,
#                               direction,city,
#                               leg_2_status,
#                               case when leg_2_status != 'completed' then TIMESTAMP_ADD(call_created_on , INTERVAL 4 MINUTE) 
#                               else TIMESTAMP_ADD(call_created_on , INTERVAL cast(safe_cast(leg_2_duration as integer)/60 as integer)+1 MINUTE)  end as new_time,call_created_on
#                                 from `gmb-centralisation.tables_crons.exotel_summary` 
#                               where date(call_created_on) > '{start}' 
#                               and date(call_created_on) <= '{end}' 
#                               and CAST(call_created_on AS time) >= '{time1}' 
#                               and CAST(call_created_on AS time) <= '{time2}'
#                               and direction = 'inbound'
#                               and ayuPersonnelType = 'CUSTOMER_SUPPORT'
                                
#                               )
#                               where
#                                 CAST(new_time AS time) >= '{time1}' 
#                                 and CAST(new_time AS time) <= '{time2}'
#                               )
#                               group by 1,2,3) group by 1,2)
#                               group by 1
                               
# """ 

exotel_summary = """
select 
city, 
ceil(avg(total)) as average
from
(
    select city,
  date,
count(*) as total
from
   (select sid,date(call_created_on) as date,CAST(call_created_on AS time) as time,
                              direction,city,
                              leg_2_status,
                              case when leg_2_status != 'completed' then TIMESTAMP_ADD(call_created_on , INTERVAL 2 MINUTE) 
                              when cast(leg_2_duration as decimal) <= 5 then TIMESTAMP_ADD(call_created_on , INTERVAL 2 MINUTE) 
                              else TIMESTAMP_ADD(call_created_on , INTERVAL safe_cast(cast(duration as decimal) as integer)+60 SECOND)  end as new_time,call_created_on
                                from `gmb-centralisation.tables_crons.exotel_summary` 
                              where date(call_created_on) >= '{start}' 
                              and date(call_created_on) <= '{end}' 
                              and direction = 'inbound'
                              and ayuPersonnelType = 'CUSTOMER_SUPPORT'
                              )
                              where
                                CAST(new_time AS time) >= '{time1}' 
                                and CAST(new_time AS time) <= '{time2}' 
                              
                              group by 1,2
                              )
                              group by 1

"""

exotel_outbound = """select city, ceil(avg(max_total)) as average from 
(select city,date,max(total) as max_total
from (select city,
date,
bucket,
count(*) as total
from
(select *,
case when CAST(new_time AS time) < '{t1}' then 1
when CAST(new_time AS time) < '{t2}' then 2
when CAST(new_time AS time) < '{t3}' then 3
when CAST(new_time AS time) < '{t4}' then 4
else 5 end as bucket
 from (
select * from (select sid,date(call_created_on) as date,CAST(call_created_on AS time) as time,
                              direction,AgentCity as city,
                              leg_2_status,
                              case when cast(leg_2_duration as decimal) < 20 then TIMESTAMP_ADD(call_created_on , INTERVAL 4 MINUTE) 
                              else TIMESTAMP_ADD(call_created_on , INTERVAL cast(safe_cast(leg_2_duration as integer)/60 as integer)+1 MINUTE)  end as new_time,call_created_on, callSid
                                from `gmb-centralisation.tables_crons.exotel_outbound_summary` outbound
                                left join `gmb-centralisation.tables_crons.system_ocq_calls` ocq on outbound.sid=ocq.callSid
                              where date(call_created_on) > '{start}' 
                              and date(call_created_on) <= '{end}' 
                             and CAST(call_created_on AS time) >= '{time1}' 
                               and CAST(call_created_on AS time) <= '{time2}'
                               and AyuPersonnelType = 'CUSTOMER_SUPPORT'
                               and leg_2_status = 'completed') bb
                               where callSid is null
                                                       
                              )
                              where
                                CAST(new_time AS time) >= '{time1}' 
                                and CAST(new_time AS time) <= '{time2}'
                              )
                              group by 1,2,3) group by 1,2)
                              group by 1
"""


patient_callback_query = """
                   select cd.caseId,DATE(createdOn) as createdOn,city,leadSource,
                   case when pca.reason IS NULL then 'N.A'
                   else pca.reason
                   end as reason
                   , time(datetime(createdOn)) as time from 
`gmb-centralisation.tables_crons.crm_dump` cd
left join (select caseId,REV,reason from (select caseId,reason,REV,
ROW_NUMBER() OVER(partition by caseId order by REV) AS row_no
                                from `gmb-centralisation.tables_crons.patient_callback_AUD` 
                                )
                                where row_no = 1 
                                and reason = 'Patient Call back')pca on cd.caseId = cast(pca.caseId as string)
where DATE(createdOn) >= '{start}'
and DATE(createdOn) <= '{end}'
and time(datetime(createdOn)) >= '{time1}' 
and time(datetime(createdOn)) <= '{time2}'
and leadSource in ('Justdial','Website Submit','Exotel')
                   """





def getDataFromBigQ(query):
    
    data2 = getdatafrombigq(query.format(start = last_30_days,end = yesterday,time1 = time1 , time2 = time2))
    return data2


def getDataFromBigQ_ex(query):
    
    data2 = getdatafrombigq(query.format(start = last_30_days,end = yesterday,time1 = time1 , time2 = time2)) # , t1=time_1, t2=time_2, t3=time_3, t4=time_4))
    return data2
    



def fetch_data(conn,caseId = ""):
    fetched_val = {}
    print(','.join(caseId))
    for lookup, query in queries.items():
            fetched_val[lookup] = fetch_record(conn,query.format(start = last_30_days , end = yesterday , time1 =
            time1 , time2 = time2,ids = ','.join(caseId)))  
            
    return fetched_val 
 
def conversion_count(city):
    city = city.groupby(['Agent_Action'])['Sequence'].count().reset_index(name = 'conversion')
    city = city.sort_values(by=['conversion'], ascending=False)
    # print(city)
    # print("----------------------------------")
    return city
    
    
def getCityWiseConversion(agents):
    ncr = agents[agents['City'] == 'NCR']
    bangalore = agents[agents['City'] == 'BLR']
    jaipur = agents[agents['City'] == 'Jaipur']
    chd = agents[agents['City'] == 'CHD']
    hyderabad = agents[agents['City'] == 'Hyderabad']
    city = [ncr,bangalore,jaipur,chd,hyderabad]
    citywise_sorted_agents = []
    for i in city:
        citywise_sorted_agents.append(conversion_count(i))
        
    return citywise_sorted_agents


def getTotal(city):
    return int(len(city)/30)


def getCountCityWiseLeadSource(city_leadsource):
    ncr = city_leadsource[city_leadsource['city'] == 'NCR']
    bangalore = city_leadsource[city_leadsource['city'] == 'BLR']
    jaipur = city_leadsource[city_leadsource['city'] == 'Jaipur']
    chd = city_leadsource[city_leadsource['city'] == 'CHD']
    hyderabad = city_leadsource[city_leadsource['city'] == 'Hyderabad']
    city = [ncr,bangalore,jaipur,chd,hyderabad]
    city1 = ['NCR','BLR','Jaipur','CHD','Hyderabad']
    city_wise_total_leadsource = []
    for i in range(len(city)):
        city_wise_total_leadsource.append(getTotal(city[i]))
    
    return city_wise_total_leadsource


def getAverage(city):
    # city = city.groupby(["date"])["sid"].count().reset_index(name="count")
    # length = len(city)
    # return int(city['count'].sum()/length)
    df_ = 0
    for index, val in city.iterrows():
        df_ = val['average']
    
    return df_
    

def getCityWiseAverageCalls(calls):
    print(len(calls))
    print(calls['city'].unique())
    ncr = calls[calls['city'] == 'NCR']
    bangalore = calls[calls['city'] == 'BLR']
    jaipur = calls[calls['city'] == 'Jaipur']
    chd = calls[calls['city'] == 'CHD']
    hyderabad = calls[calls['city'] == 'Hyderabad']
    city = [ncr,bangalore,jaipur,chd,hyderabad]
    average = []
    for i in city:
        average.append(getAverage(i))
        
    return average
    

# elif count >= average and count < average + total:
#             data['Outbound High Group'].append(value['Agent_Action'])  
#         else:
#             if total == 0 and count >= average and count < average + 1:
#                 data['Outbound High Group'].append(value['Agent_Action'])
#             else:
#                 data['FollowUp'].append(value['Agent_Action'])
            
#         count = count + 1;

def getInboundOutboundFollow(agents,average,total,city):
    data = {'Inbound Call Group': [],'Outbound High Group':[],'FollowUp':[]}
    count = 0 
    current_hour = datetime.now() + timedelta(hours=5,minutes=30)
    current_hour = current_hour.hour
    # if current_hour in (8,17,18,19):
    #     average = average + 1
        
    for key,value in agents.iterrows():
        if count < average:
            data['Inbound Call Group'].append(value['Agent_Action'])
        else:
            if total > 0:
                if len(data['Outbound High Group']) < 2 and total != len(data['Outbound High Group']): 
                    data['Outbound High Group'].append(value['Agent_Action'])
                else:
                    data['FollowUp'].append(value['Agent_Action'])
            elif total == 0:
                if len(data['Outbound High Group']) < 1:
                    data['Outbound High Group'].append(value['Agent_Action'])
                else:
                    data['FollowUp'].append(value['Agent_Action'])
            else:
                data['FollowUp'].append(value['Agent_Action'])
                
        # elif total >= 2 and count >= average and count < average + 2:
        #     data['Outbound High Group'].append(value['Agent_Action'])
        # else:
        #     if (total == 0 or total == 1) and count >= average and count < average + 1:
        #         data['Outbound High Group'].append(value['Agent_Action'])
        #     else:
        #         data['FollowUp'].append(value['Agent_Action'])
             
        count = count + 1;
    
    print("city = ",city)
    print("total agent = ",len(agents))
    print("average = ",average)
    print("inbound = ",len(data['Inbound Call Group']))
    print("total = ",total)
    print("half = ",total//2)
    print("outbound",len(data['Outbound High Group']))
    print("followup",len(data['FollowUp']))
    print("---------------------------------------------------")
    return data;
        
    
    
    
    

def getDictionary(agents,average,total):
    city = ['NCR','BLR','Jaipur','CHD','Hyderabad']
    city_wise_dictionary = {}
    
    for i in range(len(city)):
        city_wise_dictionary[city[i]] = getInboundOutboundFollow(agents[i],average[i],total[i],city[i])
        
    return city_wise_dictionary
    
    
    
def getStringConcat(data):
    str = ""
    for value in data:
        str = str + value + ","
    print(str[0:len(str) - 1])    
    return str[0:len(str) - 1]


def resultant_table(data,average,total): 
    result = ""
    result = f"""<body width = 1200>
        <table style= border-collapse:collapse border= 1px>"""
        
    result_1 = ""
    result_1 = f"""<body width = 1200>
        <table style= border-collapse:collapse border= 1px>
        <tr>
            <td colspan = 4 height= 30px  bgcolor= #3498db style=color:#ffffff><b><center> Summary </center></b></td>
        </tr>
        	<tr height= 30px bgcolor= #2cc16a style=color:#ffffff>
        	<td width = 1600><b><center> MO </center></b></td>
        	<td width = 1600><b><center>Inbound Call Group</center></b></td>
        	<td width = 1600><b><center>Outbound High Group</center></b></td>
        	<td width = 1600><b><center>FollowUp</b></center></td>
        </tr>
        """
        
    
    increment = 0	
    for key,value in data.items():
        if key == 'Hyderabad':
            print(key) 
            half = total[increment]
            
                
                
            result = result + f""" 
            <tr>
            <td colspan = 3 height= 30px  bgcolor= #3498db style=color:#ffffff><b><center>{key}</center></b></td>
        	</tr>
        	<tr height= 30px bgcolor= #2cc16a style=color:#ffffff>
        	<td width = 1600><b><center>Inbound Call Group({average[increment]})</center></b></td>
        	<td width = 1600><b><center>Outbound High Group({half})</center></b></td>
        	<td width = 1600><b><center>FollowUp</b></center></td>
        	</tr>
            """
            inbound = getStringConcat(value['Inbound Call Group'])
            outbound = getStringConcat(value['Outbound High Group'])
            followup = getStringConcat(value['FollowUp'])
            
            if len(inbound) == 0 or len(followup) == 0 or len(outbound) == 0: 
                average_inbound = average[increment] - len(value['Inbound Call Group'])
                if average_inbound < 0:
                    average_inbound = 0
                    
                Subject = "Automated Assignment Logic | No Agent Available | HYD"
                body = f"""
                            <tr height= 30px>
        	                <td width = 1600><b><center> Required </center></b></td>
        	                <td width = 1600><b><center>{average[increment]}</center></b></td>
        	                <td width = 1600><b><center> 2 </center></b></td>
        	                <td width = 1600><b><center> </b></center></td>
                        </tr>
                        <tr height= 30px>
                            <td width = 1600><b><center> Added </center></b></td>
        	                <td width = 1600><b><center> {len(value['Inbound Call Group'])} </center></b></td>
        	                <td width = 1600><b><center> {len(value['Outbound High Group'])} </center></b></td>
        	                <td width = 1600><b><center> {len(value['FollowUp'])}</b></center></td>
                        </tr>
                        <tr height= 30px>
                            <td width = 1600><b><center> Scarcity </center></b></td>
        	                <td width = 1600><b><center> {average_inbound}  </center></b></td>
        	                <td width = 1600><b><center> {2 - len(value['Outbound High Group'])} </center></b></td>
        	                <td width = 1600><b><center>  </b></center></td>
                        </tr>
                        </table>
                    """
                               
                email_recipient_list = ['vijay.g@ayu.health','ankur@ayu.health', 'nikeela@ayu.health', 'rohithm@ayu.health', 'chaitanya.s@ayu.health', 'shashank@ayu.health','neha@ayu.health']
                #email_recipient_list = ['ankur@ayu.health']
                send_email(None, email_recipient_list, Subject, 'None',result_1+body,[])
                
            
            data_inbound = [{"source": "INBOUND_EXOTEL_TEAM","emailId": inbound,"cityId": 5}]
            response = requests.post(URL_POST, json=data_inbound)
            print(response.text)
                
            
            data_outbound = [{"source": "OUTBOUND_HIGH_EXOTEL_TEAM","emailId": outbound,"cityId": 5}]
            response = requests.post(URL_POST, json=data_outbound)
            print(response.text)
            
            
            data_followup = [{"source": "OUTBOUND_LOW_EXOTEL_TEAM","emailId": followup,"cityId": 5}]
            response = requests.post(URL_POST, json=data_followup)
            print(response.text)
                
                
    	        
                
        
            result = result + f"""<tr>
    		<td width = 1600><center>{inbound}</center></td>
    		<td width = 1600><center>{outbound}</center></td>
    		<td width = 1600><center>{followup}</center></td>
    		</tr>
    		"""
    		
	        
    		    
        	
        increment += 1
	    
            
        
	
    result = result + f"""</table>"""
    return result

    
    
    

          

def lambda_handler(event, context):
    try:
        read_host, read_port, read_database = make_db_params(**{'init_from': 'script', 'db_url': READ_DB_DOMAIN_URL})
        read_connection_obj = InitDatabaseConnetion(db_url=read_host, port=int(read_port), username=READ_DB_USER,
                                                    password=READ_DB_PASSWORD, db_name=read_database)
                                                    
                                                    
                                                    
                                                    
        # 3. Calculate missed calls and website and just dial for the same time window add so many agents to outbound high 
        # ------------------------------------------------------------------------
        
        # leadsource_count = getDataFromBigQ(leadsource)
        # leadsource_count.to_csv(fpath)
        # caseId = set((leadsource_count['caseId'][leadsource_count['leadSource'] == 'Exotel'].astype(str)))
        # datasets = fetch_data(read_connection_obj,caseId)
        datasets = fetch_data(read_connection_obj)
        cs_not_included = pd.DataFrame(datasets['cs_agent_not_included'])
        cs_not_included_second = pd.DataFrame(datasets['cs_agent_not_included_second'])
        remove_cs = list(cs_not_included_second['email']) 
        email = list(cs_not_included['emailId'][0].split(","))
        print("hi",remove_cs)
        
        
        
        
        
        
        # patient_callback = getDataFromBigQ(patient_callback_query)
        # patient_callback.to_csv(fpath1)
        # # mask = (patient_callback['leadSource'] == 'Exotel' & patient_callback['reason'] == 'N.A')
        # patient_callback = patient_callback.loc[~((patient_callback['leadSource'] == 'Exotel') & 
        # (patient_callback['reason'] == 'N.A'))]
        # print(patient_callback['leadSource'].unique())
        
        
        
        # # leadsource_count = leadsource_count[leadsource_count['leadSource'].isin(['Website Submit','Justdial'])]
        # total_citywise_leadsource = getCountCityWiseLeadSource(patient_callback)
        '''
        exotel_response_o = getDataFromBigQ_ex(exotel_outbound) 
        exotel_response_o.to_csv(fpath1)
        total_citywise_leadsource = getCityWiseAverageCalls(exotel_response_o)
        '''
        total_citywise_leadsource = [2.0,2.0,2.0,2.0,2.0]
        #print(avg_exotel_response) 
        # # ------------------------------------------------------------------------
        
        
        
        
        
                                                    
        # 1. List all the agents for the city who are going to take exotel calls and order by conversion 
        # --------------------------------------------------------------------
        agents_exotel_calls = getDataFromBigQ(query_for_monthly_incentive)
        print("--------------------")
        print("not included",email)
        cs = pd.DataFrame(datasets['cs_agent'])
        # print("include1",list(cs['Agent_Action']))
        cs = cs[~cs['Agent_Action'].isin(email)]
        print("include",list(cs['Agent_Action']))
        agents_exotel_calls = agents_exotel_calls[agents_exotel_calls['Sequence'].isin(['1','2'])]
        agents_exotel_calls = pd.merge(agents_exotel_calls,cs,on = 'Agent_Action',how = 'outer') 
        agents_exotel_calls = agents_exotel_calls[~agents_exotel_calls['Agent_Action'].isin(remove_cs)]
        print(agents_exotel_calls['Agent_Action'])
        # agents_exotel_calls.to_csv(fpath4)

        agents1 = agents_exotel_calls.groupby(['Agent_Action'])['Sequence'].count().reset_index(name = 'conversion')
        agents1 = agents1.sort_values(by=['conversion'], ascending=False)
        agents1.to_csv(fpath2)
        citiwise_agents = getCityWiseConversion(agents_exotel_calls)
        
        
        # # # # --------------------------------------------------------------------
        
        
        
        # 2. from past data (last 30 days call data ) figure out avg calls coming in 
        # that 10 min window and add that number of agents to inbound calls. 
        # -------------------------------------------------------------------
        
        exotel_response = getDataFromBigQ_ex(exotel_summary)
        exotel_response.to_csv(fpath3)
        avg_exotel_response = getCityWiseAverageCalls(exotel_response)
        print(avg_exotel_response)
        #sys.exit(1)
        # # # -------------------------------------------------------------------
        
        
        
         

        
        
        # table
        data = getDictionary(citiwise_agents,avg_exotel_response,total_citywise_leadsource)
        resultant = resultant_table(data,avg_exotel_response,total_citywise_leadsource)
        
        
        Subject = "Capacity Planning For Agent Movement"
        email_recipient_list = ['ankur@ayu.health','vijay.g@ayu.health','nisha@ayu.health'] 
        # email_recipient_list = ['nisha@ayu.health','ankur@ayu.health','arjit@ayu.health', 'shashank@ayu.health','neha@ayu.health','anshul@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',resultant,[fpath2,fpath3])
        
    except Exception as e:
        Subject = 'Capacity_planning_for_agent_movement | Error'
        email_recipient_list = ['analytics@ayu.health']
        send_email(None, email_recipient_list, Subject, 'None',e,[])
        raise e
        
