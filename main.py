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

print("🚀 เริ่มดึงข้อมูล (Deep Check for Adj Close)...")

for i, (s_row, n_row) in enumerate(zip(symbols_list, names_list), start=3):
    if not s_row or not n_row or not s_row[0].strip() or not n_row[0].strip():
        continue

    symbol = s_row[0].strip()
    target_sheet_name = n_row[0].strip()
    
    # หุ้นไทยใช้ .BK, สิงคโปร์ใช้ .SI, อเมริกาไม่ต้องเติม
    api_symbol = symbol.replace(".BKK", ".BK")

    print(f"🔍 กำลังจัดการ: {api_symbol}")

    try:
        # --- จัดการ Worksheet ---
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="2000", cols="25")
            worksheet.update(values=[FINAL_COLS], range_name="A1:I1")

        existing = worksheet.get_all_values()
        last_date = None
        if len(existing) > 1:
            df_existing = pd.DataFrame(existing[1:], columns=existing[0])
            last_date = pd.to_datetime(df_existing["Date"], errors='coerce').max()

        # --- 📈 2. ดึงข้อมูล (ตั้งค่าดึงปันผลมาคำนวณด้วย) ---
        # auto_adjust=False สำคัญมากเพื่อให้ Adj Close แยกออกมา
        df_new = yf.download(api_symbol, period="1mo", interval="1d", auto_adjust=False, progress=False)

        if df_new.empty:
            print(f"⚠️ {api_symbol}: ไม่พบข้อมูล")
            continue

        # แก้ปัญหา Multi-index ที่ทำให้เข้าถึง Adj Close ไม่ได้
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)

        df_new = df_new.reset_index()
        df_new['Date'] = pd.to_datetime(df_new['Date']).dt.tz_localize(None)

        if last_date is not None:
            df_new = df_new[df_new["Date"] > last_date]

        if df_new.empty:
            print(f"😴 {api_symbol}: ไม่มีข้อมูลใหม่")
            continue

        # --- 3. เตรียมข้อมูลเข้า Sheet ---
        data_to_append = []
        for _, r in df_new.iterrows():
            # ตรวจสอบว่ามีคอลัมน์ 'Adj Close' หรือไม่ ถ้าไม่มีให้ใช้ 'Close' แทน
            # ป้องกันปัญหาหุ้นบางตลาดที่ Yahoo ไม่คำนวณ Adj ให้
            adj_val = r.get("Adj Close", r.get("Close"))
            
            # แปลงเป็น float แบบดึงค่าจาก Scalar (แก้ปัญหา FutureWarning)
            try:
                # กรณีเป็น Series ให้ดึงค่าแรก ถ้าเป็นเลขปกติใช้ได้เลย
                adj_price = float(adj_val.iloc[0]) if hasattr(adj_val, "iloc") else float(adj_val)
                close_price = float(r["Close"].iloc[0]) if hasattr(r["Close"], "iloc") else float(r["Close"])
                
                dt_obj = r["Date"]
                data_to_append.append([
                    dt_obj.strftime("%Y-%m-%d 00:00:00"),
                    symbol,
                    round(float(r["Open"]), 4),
                    round(float(r["High"]), 4),
                    round(float(r["Low"]), 4),
                    round(close_price, 4),
                    int(r["Volume"]),
                    dt_obj.strftime("%Y-%m-%d"),
                    round(adj_price, 4)
                ])
            except:
                continue

        if data_to_append:
            worksheet.append_rows(data_to_append, value_input_option="USER_ENTERED")
            print(f"✅ {api_symbol}: ดึงค่าสำเร็จ (Adj Close: {data_to_append[-1][-1]})")

    except Exception as e:
        print(f"❌ {api_symbol} ข้อผิดพลาด: {e}")

    time.sleep(1)

print("-" * 30)
print("✨ อัปเดตเรียบร้อย!")
