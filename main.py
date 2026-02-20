import json, os, time
import pandas as pd
import gspread
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime

# 🔐 1. ตั้งค่าการเชื่อมต่อ Google Sheets
# ตรวจสอบว่าได้ตั้งค่า Environment Variable ชื่อ GOOGLE_CREDENTIALS ใน GitHub หรือเครื่องคอมพิวเตอร์แล้ว
try:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    gc = gspread.service_account_from_dict(creds_dict)
except KeyError:
    print("❌ Error: ไม่พบ GOOGLE_CREDENTIALS ใน Environment Variables")
    exit()

# 📄 2. เปิด Google Sheet (ใส่ ID ของคุณให้ถูกต้อง)
SPREADSHEET_ID = "1t2F5tH9t8G41qWhXEoSQFVwviIpQJ9USmRKTvtKxh8Q"
sh = gc.open_by_key(SPREADSHEET_ID)
list_sheet = sh.worksheet("Lists")

# 📍 3. ดึงข้อมูลช่วงใหม่: คอลัมน์ D (Symbol) และ E (Sheet Name)
# ดึงมาเป็นลิสต์ของลิสต์ เช่น [['PTT'], [''], ['CPALL']]
symbols_list = list_sheet.get("D3:D32")
names_list = list_sheet.get("E3:E32")

FINAL_COLS = [
    "Datetime", "Symbol", "Open", "High", "Low",
    "Close", "Volume", "Date", "Adj Close"
]

# 📈 4. เริ่มต้น TradingView Datafeed
tv = TvDatafeed()

print("🚀 เริ่มดึงข้อมูล (Daily Update)...")
print(f"📊 ตรวจพบข้อมูลทั้งหมด {len(symbols_list)} แถวในรายการ")

# 🔄 5. Loop ทำงานทีละตัว
# ใช้ zip เพื่อจับคู่ Symbol (D) กับ Sheet Name (E)
for i, (s_row, n_row) in enumerate(zip(symbols_list, names_list), start=3):
    
    # --- ส่วนตรวจสอบช่องว่าง (Skip empty cells) ---
    # ตรวจสอบว่า s_row หรือ n_row มีข้อมูลหรือไม่
    if not s_row or not n_row or not s_row[0].strip() or not n_row[0].strip():
        # print(f"⏩ แถวที่ {i}: ว่างเปล่า... ข้ามไป")
        continue

    symbol = s_row[0].strip()           # ชื่อหุ้นจากคอลัมน์ D
    target_sheet_name = n_row[0].strip() # ชื่อชีทจากคอลัมน์ E

    print(f"🔍 กำลังจัดการ: {symbol} -> ชีท: {target_sheet_name}")

    try:
        # --- จัดการ Worksheet (เปิดหรือสร้างใหม่) ---
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"🆕 สร้างชีทใหม่: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="2000", cols="25")
            worksheet.update("A1:I1", [FINAL_COLS])

        # --- ตรวจสอบวันที่ล่าสุดในชีท ---
        existing = worksheet.get_all_values()
        if len(existing) > 1:
            # แปลงข้อมูลใน Google Sheet เป็น DataFrame เพื่อหาค่าวันที่สูงสุด
            df_existing = pd.DataFrame(existing[1:], columns=existing[0])
            last_date = pd.to_datetime(df_existing["Datetime"]).max()
        else:
            last_date = None

        # --- ดึงข้อมูลจาก TradingView ---
        # exchange="" หมายถึงหาจากทุกตลาด หรือระบุ "SET" ถ้าเน้นหุ้นไทย
        df_new = tv.get_hist(
            symbol=symbol,
            exchange="",
            interval=Interval.in_weekly,
            n_bars=2000  # ปรับจำนวนแท่งเทียนที่ต้องการดึงย้อนหลัง
        )

        if df_new is None or df_new.empty:
            print(f"⚠️ {symbol}: ไม่พบข้อมูลใน TradingView")
            continue

        df_new = df_new.reset_index()
        df_new["datetime"] = pd.to_datetime(df_new["datetime"])

        # --- กรองเอาเฉพาะข้อมูลที่ใหม่กว่าที่มีอยู่เดิม ---
        if last_date is not None:
            # ป้องกันปัญหาเรื่อง Timezone (ลบ tz ออกถ้ามี)
            if last_date.tzinfo is not None:
                last_date = last_date.tz_localize(None)
            if df_new["datetime"].dt.tz is not None:
                df_new["datetime"] = df_new["datetime"].dt.tz_localize(None)
                
            df_new = df_new[df_new["datetime"] > last_date]

        if df_new.empty:
            print(f"😴 {symbol}: ข้อมูลปัจจุบันล่าสุดแล้ว")
            continue

        # --- เตรียม Data และ Append เข้า Google Sheets ---
        data_to_append = [[
            r["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            symbol, 
            float(r["open"]), 
            float(r["high"]),
            float(r["low"]), 
            float(r["close"]), 
            float(r["volume"]),
            r["datetime"].strftime("%Y-%m-%d"),
            float(r["close"])
        ] for _, r in df_new.iterrows()]

        worksheet.append_rows(data_to_append, value_input_option="USER_ENTERED")
        print(f"✅ {symbol}: เพิ่มข้อมูลใหม่ {len(data_to_append)} แถว")

    except Exception as e:
        print(f"❌ {symbol} เกิดข้อผิดพลาด: {e}")

    # หน่วงเวลาสั้นๆ เพื่อเลี่ยง Rate Limit ของ Google API
    time.sleep(1.2)

print("-" * 30)
print("✨ อัปเดตข้อมูลครบทุกตัวเรียบร้อยแล้ว!")
