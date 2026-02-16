import json, os, time
import pandas as pd
import gspread
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime

# 🔐 ใช้ Service Account จาก GitHub Secrets
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
gc = gspread.service_account_from_dict(creds_dict)

# 📄 เปิด Google Sheet
SPREADSHEET_ID = "1t2F5tH9t8G41qWhXEoSQFVwviIpQJ9USmRKTvtKxh8Q"  # เอาจาก URL
sh = gc.open_by_key(SPREADSHEET_ID)
list_sheet = sh.worksheet("Lists")

symbols_list = list_sheet.get("C3:C32")
names_list = list_sheet.get("D3:D32")

FINAL_COLS = [
    "Datetime","Symbol","Open","High","Low",
    "Close","Volume","Date","Adj Close"
]

tv = TvDatafeed()

print("🚀 เริ่มดึงข้อมูล (Daily)...")

for s_row, n_row in zip(symbols_list, names_list):
    if not s_row or not n_row:
        continue

    symbol = s_row[0].strip()
    target_sheet_name = n_row[0].strip()

    try:
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(
                title=target_sheet_name, rows="2000", cols="25"
            )
            worksheet.update("A1:I1", [FINAL_COLS])

        existing = worksheet.get_all_values()
        last_date = (
            pd.to_datetime(pd.DataFrame(existing[1:], columns=existing[0])["Datetime"]).max()
            if len(existing) > 1 else None
        )

        df_new = tv.get_hist(
            symbol=symbol,
            exchange="",
            interval=Interval.in_daily,
            n_bars=1000
        )

        if df_new is None or df_new.empty:
            continue

        df_new = df_new.reset_index()
        if last_date is not None:
            df_new = df_new[df_new["datetime"] > last_date]

        if df_new.empty:
            continue

        data = [[
            r["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            symbol, r["open"], r["high"],
            r["low"], r["close"], r["volume"],
            r["datetime"].strftime("%Y-%m-%d"),
            r["close"]
        ] for _, r in df_new.iterrows()]

        worksheet.append_rows(data, value_input_option="USER_ENTERED")
        print(f"✅ {symbol}: เพิ่ม {len(data)} แถว")

    except Exception as e:
        print(f"❌ {symbol} error: {e}")

    time.sleep(1)

print("✨ เสร็จสิ้น")
