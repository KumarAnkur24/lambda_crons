import json
import logging
import requests
import pdb
from datetime import datetime, timedelta
from requests.exceptions import MissingSchema, ConnectionError, InvalidURL
import pytz
import json


def lambda_handler(event, context):
    URL = "http://backend.cron.api.ayu.health/patient-surgery/sendCommToDischargedPatient"
    try:

        response = requests.post(URL)
        print(response)
    except Exception as e:
        raise e

