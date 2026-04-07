## install
import requests
from datetime import datetime,date
from dotenv import load_dotenv
import os

from constants import AWARENESS_LOG_FILE

load_dotenv()

from utils import (
    configure_logging,log_info,log_error
)

DHIS2_GET_API_URL = os.getenv("DHIS2_GET_API_URL")
DHIS2_GET_USER = os.getenv("DHIS2_GET_USER")
DHIS2_GET_PASSWORD = os.getenv("DHIS2_GET_PASSWORD")

PILL_PICKUP_SQL_VIEW_1_DAY = os.getenv("PILL_PICKUP_SQL_VIEW_1_DAY")
PILL_PICKUP_SQL_VIEW_7_DAYS = os.getenv("PILL_PICKUP_SQL_VIEW_7_DAYS")

GENERAL_AWARENESS_MESSAGES_SQL_VIEW = os.getenv("GENERAL_AWARENESS_MESSAGES_SQL_VIEW")
DHIS2_API_URL = os.getenv("DHIS2_API_URL")

SMS_API_URL = os.getenv("SMS_API_URL")
TOKEN = os.getenv("TOKEN")
FROM = os.getenv("FROM")

sql_view_api_url = f"{DHIS2_GET_API_URL}sqlViews"

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

def main_with_logger(AWARENESS_LOG_FILE):

    configure_logging(AWARENESS_LOG_FILE)

    session_get = requests.Session()
    session_get.auth = (DHIS2_GET_USER, DHIS2_GET_PASSWORD)

    print("-" * 50)
    log_info("-" * 50)

    current_time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print( f"Sending SMS process start for General Awareness Messages. { current_time_start }" )
    log_info(f"Sending SMS process start for General Awareness Messages . { current_time_start }")

    sql_view_response_awareness_messages = get_sql_view_data( sql_view_api_url, session_get, GENERAL_AWARENESS_MESSAGES_SQL_VIEW )
    #print(f"sql_view_response : {sql_view_response}")
    process_and_send_awareness_messages_sms(sql_view_response_awareness_messages)


if __name__ == "__main__":

    #main()
    main_with_logger(AWARENESS_LOG_FILE)
    current_time_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print( f"Sending SMS process finished . { current_time_end }" )
    log_info(f"Sending SMS process finished . { current_time_end }")

    try:
        #sendEmail()
        print( f"Sending SMS process finished . { current_time_end }" )
    except Exception as e:
        log_error(f"Email failed: {e}")
