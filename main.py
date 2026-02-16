from sheet_client import get_gspread_client
from datetime import datetime

SPREADSHEET_ID = "1t2F5tH9t8G41qWhXEoSQFVwviIpQJ9USmRKTvtKxh8Q"
SHEET_NAME = "Sheet1"

def main():
    gc = get_gspread_client()
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    sheet.append_row([
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GitHub Actions",
        "SUCCESS"
    ])

    print("✅ Google Sheet updated")

if __name__ == "__main__":
    main()
