import json
import logging
import requests
import pdb
from datetime import datetime, timedelta
from requests.exceptions import MissingSchema, ConnectionError, InvalidURL
import pytz


def lambda_handler(event, context):
    URL = "http://backend.cron.api.ayu.health/ayu-cash/ayuCashAudit/{expiryDate}"

    try:
        yesterday = datetime.now() - timedelta(days=1, hours=-5, minutes=-30)
        yesterday = yesterday.strftime('%Y-%m-%d')

        response = requests.put(URL.format(expiryDate=yesterday))
        print(response.text)
        print(URL.format(expiryDate=yesterday))
    except Exception as e:
        raise e
        # TODO: write code...

