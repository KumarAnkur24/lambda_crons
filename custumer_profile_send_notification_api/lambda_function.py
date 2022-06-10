import json
import logging
import requests
import pdb
from datetime import datetime, timedelta
from requests.exceptions import MissingSchema, ConnectionError, InvalidURL
import pytz
import json


def check_day(day):
    if day in ('Monday', 'Thursday'):
        return "FULL_BODY_CHECKUP"
    elif day in ('Tuesday', 'Saturday'):
        return "PRIVILEGE_CARD"


def lambda_handler(event, context):
    URL = "http://backend.cron.api.ayu.health/customer-profile/sendNotifications"
    try:

        time = datetime.now()
        day = time.strftime('%A')
        data = {"notificationTopic": check_day(day)}
        print(data)

        response = requests.post(URL, json=data)
        print(response)
    except Exception as e:
        raise e
        # TODO: write code...
