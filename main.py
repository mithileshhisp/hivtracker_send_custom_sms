## install
#pip install python-dotenv
#pip install psycopg2-binary
#pip install clickhouse-connect
#pip install --upgrade certifi
#pip install --upgrade requests certifi urllib3 ## for post data in hmis production certificate issue

import urllib3 ## for disable warning of Certificate
urllib3.disable_warnings() ## for disable warning of Certificate

import ssl
#import requests

from concurrent.futures import ThreadPoolExecutor
import requests
import certifi  ## for post data in hmis production certificate issue
import json
from datetime import datetime,date
import nepali_datetime
# main.py
from dotenv import load_dotenv
import os

load_dotenv()

from utils import (
    configure_logging,
    log_info,log_error,get_sql_view_data,
    process_and_send_sms
)

#print("OpenSSL version:", ssl.OPENSSL_VERSION)
#print("Certifi CA bundle:", requests.certs.where())

DHIS2_GET_API_URL = os.getenv("DHIS2_GET_API_URL")
DHIS2_GET_USER = os.getenv("DHIS2_GET_USER")
DHIS2_GET_PASSWORD = os.getenv("DHIS2_GET_PASSWORD")

PILL_PICKUP_SQL_VIEW_1_DAY = os.getenv("PILL_PICKUP_SQL_VIEW_1_DAY")
PILL_PICKUP_SQL_VIEW_7_DAYS = os.getenv("PILL_PICKUP_SQL_VIEW_7_DAYS")

sql_view_api_url = f"{DHIS2_GET_API_URL}sqlViews"



def main_with_logger():

    configure_logging()

    session_get = requests.Session()
    session_get.auth = (DHIS2_GET_USER, DHIS2_GET_PASSWORD)

    current_time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print( f"Sending SMS process start for 1 day duedate. { current_time_start }" )
    log_info(f"Sending SMS process start for 1 day duedate . { current_time_start }")

    sql_view_response_1day = get_sql_view_data( sql_view_api_url, session_get, PILL_PICKUP_SQL_VIEW_1_DAY )
    #print(f"sql_view_response : {sql_view_response}")
    process_and_send_sms(sql_view_response_1day)
    
    print("-" * 50)
    log_info("-" * 50)

    current_time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print( f"Sending SMS process start for 7 day duedate. { current_time_start }" )
    log_info(f"Sending SMS process start for 7 day duedate . { current_time_start }")

    #sql_view_response_7day = get_sql_view_data( sql_view_api_url, session_get, PILL_PICKUP_SQL_VIEW_7_DAYS )
    #print(f"sql_view_response : {sql_view_response}")
    #process_and_send_sms(sql_view_response_7day)


    
if __name__ == "__main__":

    event_push_count = 0
    null_patient_id_count = 0
    total_patient_count = 0

    #main()
    main_with_logger()
    current_time_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print( f"Sending SMS process finished . { current_time_end }" )
    log_info(f"Sending SMS process finished . { current_time_end }")

    try:
        #sendEmail()
        print( f"Sending SMS process finished . { current_time_end }" )
    except Exception as e:
        log_error(f"Email failed: {e}")


    #sendEmail()
    #print(f"total_patient_count. {total_patient_count}, null_patient_id_count. {null_patient_id_count}, event_push_count {event_push_count}")
    #log_info(f"total_patient_count. {total_patient_count}, null_patient_id_count. {null_patient_id_count}, event_push_count {event_push_count}")
    