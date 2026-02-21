import json, os, time
import pandas as pd
import gspread
import requests
from datetime import datetime

# 🔐 1. ตั้งค่าการเชื่อมต่อ Google Sheets และ API Key
try:
    # สำหรับ Google Sheets
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    gc = gspread.service_account_from_dict(creds_dict)
    
    # สำหรับ Alpha Vantage
    API_KEY = os.environ.get("7Y6MI3WL8VBWXNLK.")
    if not API_KEY:
        raise KeyError("ไม่พบ ALPHA_VANTAGE_API_KEY")
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

FINAL_COLS = [
    "Datetime", "Symbol", "Open", "High", "Low", 
    "Close", "Volume", "Date", "Adj Close"
]

print("🚀 เริ่มดึงข้อมูลจาก Alpha Vantage...")

# 🔄 4. Loop ทำงานทีละตัว
for i, (s_row, n_row) in enumerate(zip(symbols_list, names_list), start=3):
    
    if not s_row or not n_row or not s_row[0].strip() or not n_row[0].strip():
        continue

    symbol = s_row[0].strip()
    target_sheet_name = n_row[0].strip()

    # สำหรับหุ้นไทยใน Alpha Vantage ต้องต่อท้ายด้วย .BKK เช่น PTT.BKK
    # ถ้าในไฟล์ Google Sheet ไม่มี .BKK ให้ uncomment บรรทัดข้างล่างนี้:
    # api_symbol = f"{symbol}.BKK" 
    api_symbol = symbol

    print(f"🔍 กำลังจัดการ: {symbol} -> ชีท: {target_sheet_name}")

    try:
        # --- จัดการ Worksheet ---
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"🆕 สร้างชีทใหม่: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="2000", cols="25")
            worksheet.update("A1:I1", [FINAL_COLS])

        # --- ตรวจสอบวันที่ล่าสุดในชีท ---
        existing = worksheet.get_all_values()
        last_date = None
        if len(existing) > 1:
            df_existing = pd.DataFrame(existing[1:], columns=existing[0])
            last_date = pd.to_datetime(df_existing["Date"]).max()

        # --- 📈 5. ดึงข้อมูลจาก Alpha Vantage ---
        # ใช้ TIME_SERIES_DAILY_ADJUSTED เพื่อเอา Adj Close
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={api_symbol}&outputsize=full&apikey={API_KEY}'
        response = requests.get(url)
        data = response.json()

        if "Time Series (Daily)" not in data:
            print(f"⚠️ {symbol}: ไม่พบข้อมูล (Check API Key หรือ Symbol)")
            # Alpha Vantage Free Tier จำกัด 25 requests/day
            if "Note" in data: print(f"📢 Message: {data['Note']}") 
            continue

        # แปลง JSON เป็น DataFrame
        raw_data = data["Time Series (Daily)"]
        df_new = pd.DataFrame.from_dict(raw_data, orient='index')
        
        # ปรับชื่อ Column ให้ใช้งานง่าย
        df_new.columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividend", "Split"]
        df_new.index = pd.to_datetime(df_new.index)
        df_new = df_new.sort_index(ascending=True) # เรียงจากเก่าไปใหม่
        df_new = df_new.reset_index().rename(columns={'index': 'Date'})

        # --- กรองเอาเฉพาะข้อมูลที่ใหม่กว่าที่มีอยู่เดิม ---
        if last_date is not None:
            df_new = df_new[df_new["Date"] > last_date]

        if df_new.empty:
            print(f"😴 {symbol}: ข้อมูลปัจจุบันล่าสุดแล้ว")
            continue

        # --- เตรียม Data และ Append เข้า Google Sheets ---
        data_to_append = []
        for _, r in df_new.iterrows():
            data_to_append.append([
                r["Date"].strftime("%Y-%m-%d 00:00:00"), # Datetime (จำลองเวลา)
                symbol,
                float(r["Open"]),
                float(r["High"]),
                float(r["Low"]),
                float(r["Close"]),
                float(r["Volume"]),
                r["Date"].strftime("%Y-%m-%d"),          # Date
                float(r["Adj Close"])                    # Adj Close
            ])

        worksheet.append_rows(data_to_append, value_input_option="USER_ENTERED")
        print(f"✅ {symbol}: เพิ่มข้อมูลใหม่ {len(data_to_append)} แถว")

    except Exception as e:
        print(f"❌ {symbol} เกิดข้อผิดพลาด: {e}")

    # ⏳ Alpha Vantage Free Tier แนะนำให้เว้นระยะ (ประมาณ 5 ครั้งต่อนาที)
    # ถ้าใช้แบบ Premium สามารถปรับลด sleep ลงได้
    time.sleep(12) 

print("-" * 30)
print("✨ อัปเดตข้อมูลครบทุกตัวเรียบร้อยแล้ว!")
