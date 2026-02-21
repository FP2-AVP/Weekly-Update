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

print("🚀 เริ่มดึงข้อมูล (Deep Extraction Mode)...")

for i, (s_row, n_row) in enumerate(zip(symbols_list, names_list), start=3):
    if not s_row or not n_row or not s_row[0].strip() or not n_row[0].strip():
        continue

    symbol = s_row[0].strip()
    target_sheet_name = n_row[0].strip()
    
    # ปรับชื่อหุ้นสำหรับ Yahoo Finance
    api_symbol = symbol.replace(".BKK", ".BK")

    print(f"🔍 กำลังจัดการ: {api_symbol}")

    try:
        # --- จัดการ Worksheet ---
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"🆕 สร้างชีทใหม่: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="5000", cols="25")
            worksheet.update(values=[FINAL_COLS], range_name="A1:I1")

        # เช็ควันที่ล่าสุดในชีท
        existing = worksheet.get_all_values()
        last_date = None
        if len(existing) > 1:
            df_existing = pd.DataFrame(existing[1:], columns=existing[0])
            last_date = pd.to_datetime(df_existing["Date"], errors='coerce').max()

        # --- 📈 2. ดึงข้อมูล (เปลี่ยนเป็นย้อนหลังยาวขึ้น) ---
        # ใช้ period="max" เพื่อเอาข้อมูลทั้งหมด หรือ "1y" เพื่อเอา 1 ปี
        df_new = yf.download(api_symbol, period="max", interval="1d", auto_adjust=False, progress=False)

        if df_new.empty:
            print(f"⚠️ {api_symbol}: ไม่พบข้อมูล")
            continue

        # แก้ปัญหา Multi-index (หัวตารางซ้อนกัน)
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)

        df_new = df_new.reset_index()
        df_new['Date'] = pd.to_datetime(df_new['Date']).dt.tz_localize(None)

        # กรองเอาเฉพาะข้อมูลที่ใหม่กว่าวันล่าสุดใน Sheet
        if last_date is not None:
            df_new = df_new[df_new["Date"] > last_date]

        if df_new.empty:
            print(f"😴 {api_symbol}: ข้อมูลล่าสุดอยู่แล้ว")
            continue

        # --- 3. เตรียมข้อมูลเข้า Sheet ---
        data_to_append = []
        for _, r in df_new.iterrows():
            # ดึงค่าแบบตรวจสอบว่าเป็น Scalar หรือไม่ (ป้องกัน Error strftime/float)
            try:
                # ดึงค่าด้วย .item() เพื่อเอาค่าเดี่ยวๆ ออกมาจาก Series/Array
                dt_obj = r["Date"]
                op = float(r["Open"])
                hi = float(r["High"])
                lo = float(r["Low"])
                cl = float(r["Close"])
                vo = int(r["Volume"])
                ad = float(r["Adj Close"])

                # ตรวจสอบค่าว่าง (NaN)
                if pd.isna(ad): ad = cl

                data_to_append.append([
                    dt_obj.strftime("%Y-%m-%d 00:00:00"),
                    symbol,
                    round(op, 4),
                    round(hi, 4),
                    round(lo, 4),
                    round(cl, 4),
                    vo,
                    dt_obj.strftime("%Y-%m-%d"),
                    round(ad, 4)
                ])
            except:
                continue

        if data_to_append:
            # ใช้การแบ่งส่งทีละ 500 แถวเพื่อป้องกัน Google Sheets Time out
            for chunk in [data_to_append[x:x+500] for x in range(0, len(data_to_append), 500)]:
                worksheet.append_rows(chunk, value_input_option="USER_ENTERED")
                time.sleep(1)
            print(f"✅ {api_symbol}: อัปเดตเพิ่ม {len(data_to_append)} แถว")

    except Exception as e:
        print(f"❌ {api_symbol} Error: {e}")

    time.sleep(1)

print("-" * 30)
print("✨ ภารกิจเสร็จสิ้น!")
