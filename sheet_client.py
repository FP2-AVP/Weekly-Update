import os
import json
import gspread
from google.oauth2.service_account import Credentials

def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS not set")

    creds_info = json.loads(creds_json)

    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    return gspread.authorize(creds)
