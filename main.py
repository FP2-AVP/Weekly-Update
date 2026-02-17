import json, os, time
import pandas as pd
import gspread
import yfinance as yf
from tvDatafeed import TvDatafeed, Interval

# ===============================
# 🔐 Google Service Account
# ===============================
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
gc = gspread.service_account_from_dict(creds_dict)

# ===============================
# 📄 Google Sheet
# ===============================
SPREADSHEET_ID = "1t2F5tH9t8G41qWhXEoSQFVwviIpQJ9USmRKTvtKxh8Q"
sh = gc.open_by_key(SPREADSHEET_ID)
list_sheet = sh.worksheet("Lists")

yahoo_symbols = list_sheet.get("C3:C32")
tv_symbols    = list_sheet.get("D3:D32")
sheet_names   = list_sheet.get("E3:E32")

FINAL_COLS = [
    "Datetime","Symbol","Open","High","Low",
    "Close","Volume","Date","Adj Close","Source"
]

# ===============================
# 📈 TradingView
# ===============================
tv = TvDatafeed()

print("🚀 เริ่มดึงข้อมูล (Daily)...")

for y_row, tv_row, n_row in zip(yahoo_symbols, tv_symbols, sheet_names):

    yahoo_symbol = y_row[0].strip() if y_row and y_row[0] else ""
    tv_symbol    = tv_row[0].strip() if tv_row and tv_row[0] else ""
    sheet_name   = n_row[0].strip() if n_row and n_row[0] else ""

    if not sheet_name:
        continue

    # ===============================
    # 🧾 Worksheet
    # ===============================
    try:
        worksheet = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(
            title=sheet_name, rows="2000", cols="25"
        )
        worksheet.update("A1:J1", [FINAL_COLS])

    existing = worksheet.get_all_values()
    last_date = (
        pd.to_datetime(
            pd.DataFrame(existing[1:], columns=existing[0])["Datetime"]
        ).max()
        if len(existing) > 1 else None
    )

    try:
        # ==================================================
        # 🟢 SOURCE = YAHOO
        # ==================================================
        if yahoo_symbol:
            print(f"📊 {sheet_name}: Yahoo ({yahoo_symbol})")

            df = yf.download(
                yahoo_symbol,
                start="2010-01-01",
                progress=False,
                auto_adjust=False
            )

            if df.empty:
                continue

            df = df.reset_index()

            if last_date is not None:
                df = df[df["Date"] > last_date]

            if df.empty:
                continue

            data = []
            for _, r in df.iterrows():
                data.append([
                    r["Date"].strftime("%Y-%m-%d 00:00:00"),
                    yahoo_symbol,
                    float(r["Open"]),
                    float(r["High"]),
                    float(r["Low"]),
                    float(r["Close"]),
                    int(r["Volume"]) if not pd.isna(r["Volume"]) else 0,
                    r["Date"].strftime("%Y-%m-%d"),
                    float(r["Adj Close"]),
                    "YAHOO"
                ])

        # ==================================================
        # 🔵 SOURCE = TRADINGVIEW
        # ==================================================
        elif tv_symbol:
            print(f"📈 {sheet_name}: TradingView ({tv_symbol})")

            df = tv.get_hist(
                symbol=tv_symbol,
                exchange="",
                interval=Interval.in_daily,
                n_bars=2000
            )

            if df is None or df.empty:
                continue

            df = df.reset_index()

            if last_date is not None:
                df = df[df["datetime"] > last_date]

            if df.empty:
                continue

            data = []
            for _, r in df.iterrows():
                data.append([
                    r["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
                    tv_symbol,
                    float(r["open"]),
                    float(r["high"]),
                    float(r["low"]),
                    float(r["close"]),
                    int(r["volume"]) if not pd.isna(r["volume"]) else 0,
                    r["datetime"].strftime("%Y-%m-%d"),
                    float(r["close"]),   # no adj from TV
                    "TV"
                ])

        else:
            continue

        # ===============================
        # 📤 Append
        # ===============================
        worksheet.append_rows(data, value_input_option="USER_ENTERED")
        print(f"✅ {sheet_name}: เพิ่ม {len(data)} แถว")

    except Exception as e:
        print(f"❌ {sheet_name} error: {e}")

    time.sleep(1)

print("✨ เสร็จสิ้น")
