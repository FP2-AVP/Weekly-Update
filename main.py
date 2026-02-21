import json, os, time
import pandas as pd
import gspread
import yfinance as yf
from datetime import datetime

# 🔐 1. เชื่อมต่อ Google Sheets
try:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    gc = gspread.service_account_from_dict(creds_dict)
except KeyError:
    print("❌ Error: ไม่พบ GOOGLE_CREDENTIALS")
    exit()

SPREADSHEET_ID = "1t2F5tH9t8G41qWhXEoSQFVwviIpQJ9USmRKTvtKxh8Q"
sh = gc.open_by_key(SPREADSHEET_ID)
list_sheet = sh.worksheet("Lists")

symbols_list = list_sheet.get("D3:D32")
names_list = list_sheet.get("E3:E32")

FINAL_COLS = ["Datetime", "Symbol", "Open", "High", "Low", "Close", "Volume", "Date", "Adj Close"]

print("🚀 เริ่มดึงข้อมูลแบบ Adjusted Close (Final Fixed)...")

for i, (s_row, n_row) in enumerate(zip(symbols_list, names_list), start=3):
    if not s_row or not n_row or not s_row[0].strip() or not n_row[0].strip():
        continue

    symbol = s_row[0].strip()
    target_sheet_name = n_row[0].strip()
    
    # แก้ไขชื่อหุ้นไทยจาก .BKK เป็น .BK เพื่อให้ yfinance หาเจอ
    api_symbol = symbol.replace(".BKK", ".BK")

    print(f"🔍 กำลังจัดการ: {api_symbol} -> {target_sheet_name}")

    try:
        # --- จัดการ Worksheet ---
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"🆕 สร้างชีทใหม่: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="2000", cols="25")
            # แก้ไขจุดที่ Error: ใช้รูปแบบ values= , range_name= และส่งเป็น List ซ้อน List
            worksheet.update(values=[FINAL_COLS], range_name="A1:I1")

        # ตรวจสอบวันที่ล่าสุด
        existing = worksheet.get_all_values()
        last_date = None
        if len(existing) > 1:
            # ใช้พารามิเตอร์ errors='coerce' เพื่อป้องกันข้อมูลวันที่ผิดพลาด
            df_existing = pd.DataFrame(existing[1:], columns=existing[0])
            last_date = pd.to_datetime(df_existing["Date"], errors='coerce').max()

        # --- 📈 2. ดึงข้อมูลจาก yfinance ---
        df_new = yf.download(api_symbol, period="1mo", interval="1d", auto_adjust=False, progress=False)

        if df_new.empty:
            print(f"⚠️ {api_symbol}: ไม่พบข้อมูลจาก Yahoo")
            continue

        # แก้ปัญหา Multi-index (กรณี yfinance คืนค่ามาหลายระดับ)
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)

        df_new = df_new.reset_index()
        df_new['Date'] = pd.to_datetime(df_new['Date']).dt.tz_localize(None)

        # กรองข้อมูลใหม่
        if last_date is not None:
            df_new = df_new[df_new["Date"] > last_date]

        if df_new.empty:
            print(f"😴 {api_symbol}: ไม่มีข้อมูลให้อัปเดต")
            continue

        # --- 3. เตรียมข้อมูลเข้า Sheet ---
        data_to_append = []
        for _, r in df_new.iterrows():
            # ป้องกันกรณีข้อมูลในบางคอลัมน์เป็น NaN
            try:
                dt_obj = r["Date"]
                data_to_append.append([
                    dt_obj.strftime("%Y-%m-%d 00:00:00"),
                    symbol,
                    float(r["Open"]),
                    float(r["High"]),
                    float(r["Low"]),
                    float(r["Close"]),
                    int(r["Volume"]),
                    dt_obj.strftime("%Y-%m-%d"),
                    float(r["Adj Close"])
                ])
            except (ValueError, TypeError):
                continue

        if data_to_append:
            worksheet.append_rows(data_to_append, value_input_option="USER_ENTERED")
            print(f"✅ {api_symbol}: เพิ่มข้อมูลใหม่ {len(data_to_append)} แถว")

    except Exception as e:
        print(f"❌ {api_symbol} เกิดข้อผิดพลาด: {e}")

    # หน่วงเวลาสั้นๆ เพื่อเลี่ยง API Rate Limit
    time.sleep(1)

print("-" * 30)
print("✨ อัปเดตข้อมูลทั้งหมดเรียบร้อยแล้ว!")
