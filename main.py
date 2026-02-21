import json, os, time
import pandas as pd
import gspread
import requests
from datetime import datetime

# 🔐 1. ตั้งค่าการเชื่อมต่อ (แนะนำใช้ Environment Variable)
# แต่ถ้าต้องการทดสอบด่วน สามารถใส่แทนที่ os.environ.get ได้
try:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    gc = gspread.service_account_from_dict(creds_dict)
    
    # ดึง API Key (ถ้าใน GitHub Secrets ไม่มี มันจะใช้ค่า Default ที่เราใส่ไว้ด้านล่าง)
    API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY", "YJMOZRAEPCYRBHOY")
except KeyError as e:
    print(f"❌ Error: {e}")
    exit()

# 📄 2. เปิด Google Sheet
SPREADSHEET_ID = "1t2F5tH9t8G41qWhXEoSQFVwviIpQJ9USmRKTvtKxh8Q"
sh = gc.open_by_key(SPREADSHEET_ID)
list_sheet = sh.worksheet("Lists")

# 📍 3. ดึงข้อมูล Symbol และ Sheet Name
symbols_list = list_sheet.get("D3:D32")
names_list = list_sheet.get("E3:E32")

FINAL_COLS = ["Datetime", "Symbol", "Open", "High", "Low", "Close", "Volume", "Date", "Adj Close"]

print(f"🚀 เริ่มดึงข้อมูลด้วย API Key: {API_KEY[:4]}****")

# 🔄 4. Loop ทำงานทีละตัว
for i, (s_row, n_row) in enumerate(zip(symbols_list, names_list), start=3):
    
    if not s_row or not n_row or not s_row[0].strip() or not n_row[0].strip():
        continue

    symbol = s_row[0].strip()
    target_sheet_name = n_row[0].strip()

    print(f"🔍 กำลังจัดการ: {symbol} -> ชีท: {target_sheet_name}")

    try:
        # --- จัดการ Worksheet ---
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"🆕 สร้างชีทใหม่: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="2000", cols="25")
            worksheet.update("A1:I1", [FINAL_COLS])

        # ตรวจสอบวันที่ล่าสุด
        existing = worksheet.get_all_values()
        last_date = None
        if len(existing) > 1:
            df_existing = pd.DataFrame(existing[1:], columns=existing[0])
            last_date = pd.to_datetime(df_existing["Date"]).max()

        # --- 📈 5. ดึงข้อมูลจาก Alpha Vantage (Daily Adjusted) ---
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=compact&apikey={API_KEY}'
        response = requests.get(url)
        data = response.json()

        # ตรวจสอบว่าโดนจำกัดจำนวนครั้งหรือไม่ (Rate Limit)
        if "Note" in data:
            print(f"⏳ API Limit reached: {data['Note']}")
            break # หยุดการทำงานเพราะเกินโควตา

        if "Time Series (Daily)" not in data:
            print(f"⚠️ {symbol}: ไม่พบข้อมูลในระบบ (ตรวจสอบชื่อ Symbol)")
            continue

        # แปลงข้อมูล
        raw_data = data["Time Series (Daily)"]
        df_new = pd.DataFrame.from_dict(raw_data, orient='index')
        
        # Alpha Vantage columns: 1. open, 2. high, 3. low, 4. close, 5. adjusted close, 6. volume...
        df_new.columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Div", "Split"]
        df_new.index = pd.to_datetime(df_new.index)
        df_new = df_new.sort_index(ascending=True)
        df_new = df_new.reset_index().rename(columns={'index': 'Date'})

        # กรองข้อมูลใหม่
        if last_date is not None:
            df_new = df_new[df_new["Date"] > last_date]

        if df_new.empty:
            print(f"😴 {symbol}: ข้อมูลปัจจุบันล่าสุดแล้ว")
            continue

        # เตรียมข้อมูลเพื่อ Append
        data_to_append = [[
            r["Date"].strftime("%Y-%m-%d 00:00:00"),
            symbol,
            float(r["Open"]),
            float(r["High"]),
            float(r["Low"]),
            float(r["Close"]),
            float(r["Volume"]),
            r["Date"].strftime("%Y-%m-%d"),
            float(r["Adj Close"])
        ] for _, r in df_new.iterrows()]

        worksheet.append_rows(data_to_append, value_input_option="USER_ENTERED")
        print(f"✅ {symbol}: เพิ่มข้อมูลใหม่ {len(data_to_append)} แถว")

        # ⏳ หน่วงเวลา 15 วินาที เพื่อให้ไม่เกิน 5 Requests ต่อนาที (สำหรับ Free Plan)
        time.sleep(15)

    except Exception as e:
        print(f"❌ {symbol} เกิดข้อผิดพลาด: {e}")
        time.sleep(5)

print("-" * 30)
print("✨ ภารกิจเสร็จสิ้น!"
