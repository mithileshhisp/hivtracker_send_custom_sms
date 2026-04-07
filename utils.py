# utils.py

import requests
import logging

import certifi  ## for post data in hmis production certificate issue


import json
import smtplib
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders
from urllib.parse import quote

## for nepali date
import nepali_datetime
from datetime import datetime, timedelta, date

#from datetime import timedelta

from dotenv import load_dotenv
import os
import glob
load_dotenv()

FROM_EMAIL_ADDR = os.getenv("FROM_EMAIL_ADDR")
FROM_EMAIL_PASSWORD = os.getenv("FROM_EMAIL_PASSWORD")

#from constants import LOG_FILE
#from app import QueueLogHandler

DHIS2_API_URL = os.getenv("DHIS2_API_URL")

SMS_API_URL = os.getenv("SMS_API_URL")
TOKEN = os.getenv("TOKEN")
FROM = os.getenv("FROM")

# ADD THIS PART (UI streaming) for print in HTML Page in response
#Add a global log queue
import queue
log_queue = queue.Queue()
#Add a Queue logging handler
#import logging

'''
class QueueLogHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))
'''

import logging
import queue

log_queue = queue.Queue()

class QueueHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))


def configure_logging_for_app(log_file=None):
    
    print(f"inside configure_logging_for_app")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 🔴 REMOVE OLD HANDLERS (THIS IS KEY)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # Console
    console  = logging.StreamHandler()
    console .setFormatter(formatter)
    logger.addHandler(console)

    # Queue (for UI)
    qh = QueueHandler()
    qh.setFormatter(formatter)
    logger.addHandler(qh)

    # File (ONLY if provided)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file, mode="w")
        fh.setFormatter(formatter)
        logger.addHandler(fh)



'''
def configure_logging_for_app():

    
    import os
    from constants import LOG_FILE

    LOG_DIR = "logs"
    os.makedirs(LOG_DIR, exist_ok=True)
    assert LOG_DIR != "/" and LOG_DIR != "" #### Never delete outside log folder.

    log_path = os.path.join(LOG_DIR, LOG_FILE)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    queue_handler = QueueLogHandler()
    queue_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # 🔴 CRITICAL: remove old handlers
    root.handlers.clear()

    root.addHandler(file_handler)
    root.addHandler(queue_handler)

    logging.info("Logging initialized for Flask app")

'''


def configure_logging(sms_log_file):

    #Optional (Advanced, but useful)
    '''
    import sys
    sys.stdout.write = lambda msg: logging.info(msg)
    logging.info(f"[job:{job_id}] step 1")
    '''

    LOG_DIR = "logs"
    #os.makedirs(LOG_DIR, exist_ok=True)

    os.makedirs(LOG_DIR, exist_ok=True)
    assert LOG_DIR != "/" and LOG_DIR != "" #### Never delete outside log folder.

    # Create unique log filename
    #log_filename = f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_filename = sms_log_file
    #log_filename = f"{LOG_FILE}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_path = os.path.join(LOG_DIR, log_filename)

    #logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    '''
    logging.basicConfig(filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            QueueLogHandler()   # 👈 THIS is the key
        ]
    )
    '''
    # ✅ ADD THIS (UI streaming)
    '''
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not any(isinstance(h, QueueLogHandler) for h in root_logger.handlers):
        queue_handler = QueueLogHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        queue_handler.setFormatter(formatter)
        root_logger.addHandler(queue_handler)
    '''

def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

#################################
## for HIV-TRACKER ######

def get_sql_view_data( sql_view_api_url, session_get, PILL_PICKUP_SQL_VIEW ):
    
    #https://tracker.hivaids.gov.np/save-child-2.27/api/sqlViews/ti5pvDMLCcT/data?paging=false

    sql_view_api = f"{sql_view_api_url}/{PILL_PICKUP_SQL_VIEW}/data?paging=false"
    response_sql_view = session_get.get (sql_view_api )
   
    #print(f"sql_view_api : {sql_view_api}")
    #print(f"sql_view_response : {response_sql_view.json()}")
    if response_sql_view.status_code == 200:
        response_sql_view.raise_for_status()
        return response_sql_view.json()
    else:
        return []

def send_sms(mobile, message, org_name):
    params = {
        "token": TOKEN,
        "from": FROM,
        "to": mobile,
        "text": message
    }

    #response = requests.post(SMS_API_URL, params=params)
    sms_response = requests.get(SMS_API_URL, params=params)
    #sms_response.raise_for_status()
    #print(f"SMS Response {mobile}: {sms_response.json()} ")
    try:
        sms_response.raise_for_status()
        print(f"✅ SMS sent to {mobile}, org_unit:  {org_name}")
        log_info(f"✅ SMS sent to {mobile} org_unit:  {org_name}")

        print(f"✅ SMS Response {mobile}: {sms_response.json()} ")
        log_info(f"✅ SMS Response {mobile}: {sms_response.json()} ")
    except Exception as e:
        print(f"❌ SMS Response {mobile}: {sms_response.json()} ")
        print(f"❌ Failed for {mobile}: {e} org_unit:  {org_name}")
        log_error(f"❌ Failed for {mobile}: {e} org_unit:  {org_name}")


def process_and_send_sms(sql_view_response):
    list_grid = sql_view_response.get("listGrid", {})
    
    headers = [h["name"] for h in list_grid.get("headers", [])]
    rows = list_grid.get("rows", [])

    log_info(f"tei_list_size {len(rows)}")
    print(f"tei_list_size {len(rows)}")
     # ✅ ADD HERE (outside loop)
    sent_numbers = set()

    for row in rows:
        row_dict = dict(zip(headers, row))

        sms_consent = row_dict.get("sms_consent")
        mobile = row_dict.get("mobile_number")
        due_date = row_dict.get("due_date")
        org_name = row_dict.get("org_name")

        # ✅ Validation
        if sms_consent != "true":
            continue
        
        if not mobile or len(mobile) < 10:
            continue

        #🔴 3. Avoid sending duplicates (optional but recommended)    
        # ✅ CHECK DUPLICATE (inside loop)
        if mobile in sent_numbers:
            continue

        # ✅ ADD TO SET
        sent_numbers.add(mobile)

        # ✅ Message
        #message = f"Reminder: Visit {org_name} on {due_date} for pill pickup."
        customMessagePillPick = f"तपाइको औषधि लिने बेला भयो, तपाई मिति {due_date} गते {org_name} को केन्द्रमा आउनुहोला |"
        #customMessagePillPick = f"तपाइको औषधि लिने बेला भयो, तपाई  मिति {due_date} गते {org_name} को केन्द्रमा आउनुहोला  |"
        # ✅ Send SMS
        print(f"✅ SMS sent to {mobile}, org_unit:  {org_name}, {customMessagePillPick}")
        log_info(f"✅ SMS sent to {mobile} org_unit:  {org_name}, {customMessagePillPick}")

        send_sms(mobile, customMessagePillPick, org_name )
    
    #send_sms(9745420205, customMessagePillPick, org_name )


def process_and_send_awareness_messages_sms(sql_view_response):
    
    list_grid = sql_view_response.get("listGrid", {})
    
    headers = [h["name"] for h in list_grid.get("headers", [])]
    rows = list_grid.get("rows", [])

    log_info(f"tei_list_size for general awareness messages  {len(rows)}")
    print(f"tei_list_size for general awareness messages {len(rows)}")
    
     # ✅ ADD HERE (outside loop)
    sent_numbers = set()

    for row in rows:
        row_dict = dict(zip(headers, row))

        sms_consent = row_dict.get("sms_consent")
        mobile = row_dict.get("mobile_number")
        due_date = row_dict.get("due_date")
        org_name = row_dict.get("org_name")

        # ✅ Validation
        if sms_consent != "true":
            continue
        
        if not mobile or len(mobile) < 10:
            continue

        #🔴 3. Avoid sending duplicates (optional but recommended)    
        # ✅ CHECK DUPLICATE (inside loop)
        if mobile in sent_numbers:
            continue

        # ✅ ADD TO SET
        sent_numbers.add(mobile)

        # ✅ Message
        #message = f"Reminder: Visit {org_name} on {due_date} for pill pickup."
        #customMessagePillPick = f"तपाइको औषधि लिने बेला भयो, तपाई मिति {due_date} गते {org_name} को केन्द्रमा आउनुहोला |"
        customMessagePillPick = f"This is General Aawareness Message"
        # ✅ Send SMS
        print(f"✅ SMS sent to {mobile}, org_unit:  {org_name}, {customMessagePillPick}")
        log_info(f"✅ SMS sent to {mobile} org_unit:  {org_name}, {customMessagePillPick}")

        send_sms(mobile, customMessagePillPick, org_name )
    
    #send_sms(9745420205, customMessagePillPick, org_name )
