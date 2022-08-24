import pdb 
import os
import pytz 
import re
import pandas as pd
from service.database import InitDatabaseConnetion, make_db_params, fetch_record
import json
from datetime import datetime , timedelta
from service.send_mail_client import send_email
import requests

from bq import main

mainbody = """<body width = \"1200\">
        <p> Hi All, </br></br>
        Report for {0} to {1} </p> </br>"""
result_base =""" 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr style="background-color:powderblue;"> 
        <td colspan = 7 width = \"1200\"><b><center> Final Detailed Status vs leg_1_status call distribution   </center></b></td>
    </tr>
    <tr bgcolor=\"#EEFCF0\"> 
        <td colspan = 1  width = \"200\"><b><center> Final Detailed Status  </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> N/A </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> Busy </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> Completed </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> No - Answer </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> None </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> Failed </center></b></td>
    </tr>
""" 

result_base1 ="""<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr style="background-color:powderblue;"> 
        <td colspan = 5 width = \"1200\"><b><center> Agent Email vs legStatus call distribution   </center></b></td>
    </tr>
    <tr bgcolor=\"#EEFCF0\"> 
        <td colspan = 1  width = \"200\"><b><center> Agent Email  </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> busy </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> completed </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> failed </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> no-answer </center></b></td>
    </tr>
""" 

result_base2 ="""<body width = \"1200\"> 
    <table style=\"border-collapse:collapse\" border=\"1\" >
    <tr style="background-color:powderblue;"> 
        <td colspan = 4 width = \"1200\"><b><center> Final Detailed Status vs No of sent call distribution   </center></b></td>
    </tr>
    <tr bgcolor=\"#EEFCF0\"> 
        <td colspan = 1  width = \"200\"><b><center> Final Detailed Status  </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> True </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> Not Requested </center></b></td>
        <td colspan = 1  width = \"200\"><b><center> False </center></b></td>
    </tr>
""" 

time = datetime.now()+ timedelta(hours = 5, minutes= 30)
today = time.date()
print(today)
d1_ = time - timedelta(minutes = 20)
d1 = d1_.strftime("%H:%M:%S")
print(d1)
d2_ = time - timedelta(minutes = 10)
d2 = d2_.strftime("%H:%M:%S")
print(d2)


def iterdata(data):
    
    table = [[0 for x in range(7)] for x in range(100)]
    
    count = 0
    call = {}
    
    for i,j in data.iterrows():
        if j['finalDetailedStatus'] not in call.keys():
            call[j['finalDetailedStatus']] = count
            
            table[count][0] = j['finalDetailedStatus']
            
            count +=1
            
        if j['leg_1_status'] == "N/A":
            table[call[j['finalDetailedStatus']]][1] += 1
        
        if j['leg_1_status'] == "busy":
            table[call[j['finalDetailedStatus']]][2] += 1
        
        if j['leg_1_status'] == "completed":
            table[call[j['finalDetailedStatus']]][3] += 1
            
        if j['leg_1_status'] == "no-answer":
            table[call[j['finalDetailedStatus']]][4] += 1
        
        if j['leg_1_status'] == "None":
            table[call[j['finalDetailedStatus']]][5] += 1
            
        if j['leg_1_status'] == "failed":
            table[call[j['finalDetailedStatus']]][6] += 1
            
        
        
    result1 = result_base
    
    for i in range(0, count): 
        result1 = result1 + "<tr>"
        for j in range(0, 7):
            if j == 0:
                result1 = result1 + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif table[i][j] == 0:
                result1 = result1 + "<td colspan = 1 ><center> </center></td>"
            else:
                result1 = result1 + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result1 = result1 + "</tr>"
    result1 = result1 + "</table><br>"
    

    return result1
        
def second(data):
    
    table = [[0 for x in range(5)] for x in range(100)]
    
    count = 0
    dict1 = {}
    
    for i,j in data.iterrows():
        if j['leg1Agent_email'] not in dict1.keys():
            dict1[j['leg1Agent_email']] = count
            
            table[count][0] = j['leg1Agent_email']
            
            count +=1
            
        if j['leg1Status'] == "busy":
            table[dict1[j['leg1Agent_email']]][1] +=1
        elif j['leg1Status'] == "completed":
            table[dict1[j['leg1Agent_email']]][2] +=1
        elif j['leg1Status'] == "failed":
            table[dict1[j['leg1Agent_email']]][3] +=1
        elif j['leg1Status'] == "no-answer":
            table[dict1[j['leg1Agent_email']]][4] +=1
        
        if j['leg2Agent_email'] not in dict1.keys():
            dict1[j['leg2Agent_email']] = count
            
            table[count][0] = j['leg2Agent_email']
            
            count +=1
            
        if j['leg2Status'] == "busy":
            table[dict1[j['leg2Agent_email']]][1] +=1
        elif j['leg2Status'] == "completed":
            table[dict1[j['leg2Agent_email']]][2] +=1
        elif j['leg2Status'] == "failed":
            table[dict1[j['leg2Agent_email']]][3] +=1
        elif j['leg2Status'] == "no-answer":
            table[dict1[j['leg2Agent_email']]][4] +=1
    
    
        if j['leg3Agent_email'] not in dict1.keys():
            dict1[j['leg3Agent_email']] = count
            
            table[count][0] = j['leg3Agent_email']
            
            count +=1
            
        if j['leg3Status'] == "busy":
            table[dict1[j['leg3Agent_email']]][1] +=1
        elif j['leg3Status'] == "completed":
            table[dict1[j['leg3Agent_email']]][2] +=1
        elif j['leg3Status'] == "failed":
            table[dict1[j['leg3Agent_email']]][3] +=1
        elif j['leg3Status'] == "no-answer":
            table[dict1[j['leg3Agent_email']]][4] +=1
     
     
    table.sort(key = lambda x:x[1])        
    result2 = result_base1
    
    for i in range(0, count): 
        result2 = result2 + "<tr>"
        for j in range(0, 5):
            if j == 0:
                result2 = result2 + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif table[i][j] == 0:
                result2 = result2 + "<td colspan = 1 ><center> </center></td>"
            else:
                result2 = result2 + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result2 = result2 + "</tr>"
    result2 = result2 + "</table><br>"
    

    return result2  
 
def third(data):
    table = [[0 for x in range(4)] for x in range(100)]
    
    count = 0
    dict2 = {} 
    
    for i,j in data.iterrows():
        if j['finalDetailedStatus'] not in dict2.keys():
            dict2[j['finalDetailedStatus']] = count
            
            table[count][0] = j['finalDetailedStatus']
            count +=1
            
        if j['noSent'] == "True":
            table[dict2[j['finalDetailedStatus']]][1] +=1
        if j['noSent'] == "Not Requested":
            table[dict2[j['finalDetailedStatus']]][2] +=1
        if j['noSent'] == "False":
            table[dict2[j['finalDetailedStatus']]][3] +=1
        
        
            
    result3 = result_base2
    
    for i in range(0, count): 
        result3 = result3 + "<tr>"
        for j in range(0, 4):
            if j == 0:
                result3 = result3 + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
            elif table[i][j] == 0:
                result3 = result3 + "<td colspan = 1 ><center> </center></td>"
            else:
                result3 = result3 + "<td colspan = 1 ><center>" + str(table[i][j]) + "</center></td>"
        result3 = result3 + "</tr>"
    result3 = result3 + "</table><br>"
    

    return result3    
        
    
def lambda_handler(event, context):
    try:
        data = main()
        
        if len(data):
            result1 = iterdata(data)
            result2 = second(data)
            result3 = third(data)
         
            res = mainbody.format(d1,d2) + result1 + result3 + result2
        
            fpath = os.path.join('/tmp','calling_view.csv')
            data.to_csv(fpath) 
            
            Subject = 'Inbound calling detailed view | HYD | {today}'
            # email_recipient_list = ['nikunj.r@ayu.health']
            email_recipient_list = ['nikunj.r@ayu.health','nikeela@ayu.health','rohithm@ayu.health','neha@ayu.health','shashank@ayu.health','vijay.g@ayu.health','tls@ayu.health','chaitanya.s@ayu.health']
            send_email(None, email_recipient_list, Subject.format(today=today), 'Hi All',res,[fpath])
    
    
        
    except Exception as e:
        raise e